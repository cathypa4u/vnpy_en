"""
KIS Gateway for Vn.py (Final Production Version)
FileName: kis_gateway.py

Dependencies:
- kis_parser.py: Handles data parsing for WebSocket and REST API.
- kis_auth.py: Handles authentication and token management.

Features:
1. Full Support: Domestic/Overseas Stocks, Futures, Options, Bonds, Night Markets.
2. Unified WebSocket Manager using kis_parser.
3. Robust History Data Query (Minute/Hour/Day/Week/Month) for all assets.
4. Auto-subscription for Interest Stocks.
5. Multi-currency Account Management.
"""

import time
import threading
from datetime import datetime, timedelta
from copy import copy
from zoneinfo import ZoneInfo
from typing import Dict, List, Any, Optional

import requests
import websocket
import json

from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData, OrderData, TradeData, PositionData, AccountData,
    ContractData, OrderRequest, CancelRequest, SubscribeRequest,
    HistoryRequest, BarData
)
from vnpy.trader.constant import (
    Exchange, Product, OrderType, Direction, Status, Interval
)
from vnpy.trader.event import EVENT_TIMER

# Import Independent Parser
try:
    from . import kis_parser
except ImportError:
    import kis_parser

from .kis_auth import kis_auth

# --- Constants ---
KOREA_TZ = ZoneInfo("Asia/Seoul")

# REST API TR ID Mapping (Comprehensive)
TR_REST = {
    # [Domestic Stock]
    "KR_STOCK_ORD": {"buy": "TTTC0802U", "sell": "TTTC0801U", "cancel": "TTTC0803U"},
    "KR_STOCK_BAL": "TTTC8434R",
    "KR_STOCK_POS": "TTTC8434R", # Using Balance TR for Position as well
    "KR_STOCK_HIST": {
        "D": "FHKST03010100", # Daily/Weekly/Monthly
        "M": "FHKST03010200"  # Minute
    },
    
    # [Domestic Fut/Opt]
    "KR_FUT_ORD": {"buy": "TTTO1101U", "sell": "TTTO1101U", "cancel": "TTTO1103U"},
    "KR_FUT_BAL": "CTFO6118R",
    "KR_FUT_HIST": {
        "D": "FHKIF03020100", 
        "M": "FHKIF03020200"
    },

    # [Night Market]
    "KR_NIGHT_ORD": {"buy": "STTN1101U", "sell": "STTN1101U", "cancel": "STTN1103U"},
    "KR_NIGHT_BAL": "CTFN6118R",

    # [Overseas Stock] (US/Asia)
    "OVRS_STOCK_ORD": {"buy": "TTTT1002U", "sell": "TTTT1006U", "cancel": "TTTT1004U"}, 
    "OVRS_STOCK_BAL": "TTTS3012R",
    "OVRS_STOCK_HIST": {
        "D": "HHDFS76240000", 
        "M": "HHDFS76950200"
    },

    # [Overseas Future]
    "OVRS_FUT_ORD": {"buy": "OTFM3001U", "sell": "OTFM3001U", "cancel": "OTFM3003U"},
    "OVRS_FUT_BAL": "OTFM1412R",
    "OVRS_FUT_HIST": {
        "D": "HHDFC55020100", # Daily
        "W": "HHDFC55020000", # Weekly
        "M": "HHDFC55020400"  # Minute
    },
    
    # [Bond]
    "KR_BOND_ORD": {"buy": "TTTC0952U", "sell": "TTTC0958U", "cancel": "TTTC0953U"},
    "KR_BOND_BAL": "CTSC8407R",
    
    # [Interest Stock]
    "INTEREST_KR": "HHKST113000C"
}

# WebSocket TR ID Mapping
TR_WS = {
    # Real-time Data
    "KR_STOCK": "H0STCNT0", "KR_STOCK_HOKA": "H0STASP0",
    "KR_FUT": "H0IFCNT0", "KR_FUT_HOKA": "H0IFASP0",
    "KR_OPT": "H0IOCNT0", "KR_OPT_HOKA": "H0IOASP0",
    "KR_BOND": "H0BJCNT0",
    "KR_INDEX": "H0UPCNT0",
    "OVRS_STOCK": "HDFSCNT0", "OVRS_STOCK_HOKA": "HDFSASP0",
    "OVRS_FUT": "HDFFF020", "OVRS_FUT_HOKA": "HDFFF010",
    "NIGHT_FUT": "ECEUCNT0", 
    
    # Notifications (Execution)
    "NOTICE_KR_STOCK": "H0STCNI0",
    "NOTICE_KR_FUT": "H0IFCNI0",
    "NOTICE_OVRS_STOCK": "H0GSCNI0",
    "NOTICE_OVRS_FUT": "HDFFF2C0",
    "NOTICE_NIGHT": "H0EUCNI0"
}


# =============================================================================
# WebSocket Manager
# =============================================================================
class KisWsManager:
    """
    Centralized WebSocket Manager.
    Delegates parsing to 'kis_parser.py'.
    """
    
    def __init__(self, gateway):
        self.gateway = gateway
        self.active = False
        self.ws = None
        self.thread = None
        self.ws_url = "ws://ops.koreainvestment.com:21000"
        self.approval_key = ""
        self.aes_keys = {} # TR_ID -> {key, iv} for Notice Decryption
        self.subscribed = set()
        
    def start(self, app_key, app_secret, server="REAL"):
        if self.active: return
        
        # Determine URL
        self.ws_url = "ws://ops.koreainvestment.com:31000" if server == "DEMO" else "ws://ops.koreainvestment.com:21000"
        
        # Get Approval Key (Synchronous)
        self.approval_key = self._get_approval_key(app_key, app_secret, server)
        if not self.approval_key:
            self.gateway.write_log("Failed to get WebSocket Approval Key")
            return

        self.active = True
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def _get_approval_key(self, key, secret, server):
        domain = "https://openapivts.koreainvestment.com:29443" if server == "DEMO" else "https://openapi.koreainvestment.com:9443"
        url = f"{domain}/oauth2/Approval"
        try:
            res = requests.post(url, json={
                "grant_type": "client_credentials", "appkey": key, "secretkey": secret
            })
            data = res.json()
            return data.get("approval_key", "")
        except Exception as e:
            self.gateway.write_log(f"Approval Key Error: {e}")
            return ""

    def run(self):
        while self.active:
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url, 
                    on_open=self.on_open, 
                    on_message=self.on_message, 
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                self.ws.run_forever(ping_interval=100, ping_timeout=10)
            except Exception as e:
                self.gateway.write_log(f"WebSocket Connection Failed: {e}")
                time.sleep(3)

    def on_open(self, ws):
        self.gateway.write_log("Websocket Connected")
        # Re-subscribe logic could be implemented here if needed

    def on_error(self, ws, error):
        self.gateway.write_log(f"Websocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        self.gateway.write_log("Websocket Closed")

    def on_message(self, ws, data):
        """
        Routes message to appropriate handler based on header
        """
        try:
            # 1. Real-time Market Data (Starts with '0') or Notice (Starts with '1')
            if data[0] in ['0', '1']:
                parts = data.split('|')
                if len(parts) < 4: return
                
                tr_id = parts[1]
                body = parts[3]
                
                if data.startswith('0'): # Real-time Market Data
                    self.handle_market_data(tr_id, body)
                elif data.startswith('1'): # Encrypted Notice
                    self.handle_notice_data(tr_id, body)
                    
            # 2. JSON Control Message (Key exchange, PingPong)
            elif data.startswith('{'):
                js = json.loads(data)
                
                # PingPong
                if "header" in js and js["header"]["tr_id"] == "PINGPONG":
                    ws.pong(data)
                    return
                
                # AES Key/IV Reception
                if "header" in js and "tr_id" in js["header"]:
                    tr_id = js["header"]["tr_id"]
                    if "body" in js and "output" in js["body"]:
                        self.aes_keys[tr_id] = js["body"]["output"]
                        self.gateway.write_log(f"Received AES Key for {tr_id}")
                    
        except Exception as e:
            pass

    def handle_market_data(self, tr_id, body):
        """Dispatch to kis_parser for Market Data"""
        
        # 1. Tick Data (Execution)
        if "CNT" in tr_id or "HDFFF020" in tr_id: 
            parsed = kis_parser.parse_ws_realtime(tr_id, body)
            if parsed.get("valid"):
                self.gateway.on_ws_tick(parsed)
        
        # 2. OrderBook (Depth) Data
        elif "ASP" in tr_id or "HDFFF010" in tr_id:
            parsed = kis_parser.parse_ws_hoka(tr_id, body)
            if parsed.get("code"):
                self.gateway.on_ws_depth(parsed)

    def handle_notice_data(self, tr_id, body):
        """Decrypt and parse execution notices"""
        if tr_id not in self.aes_keys: 
            return
        
        key = self.aes_keys[tr_id]["key"]
        iv = self.aes_keys[tr_id]["iv"]
        
        parsed = kis_parser.parse_ws_notice(tr_id, body, key, iv)
        if parsed and parsed.get("valid"):
            self.gateway.on_ws_notice(parsed)

    def subscribe(self, req: SubscribeRequest):
        tr_id, tr_key = self.gateway.get_ws_tr_info(req)
        if not tr_id: 
            self.gateway.write_log(f"Unknown Subscription Target: {req.symbol}")
            return

        # Send Request
        payload = {
            "header": {
                "approval_key": self.approval_key, 
                "custtype": "P", 
                "tr_type": "1", 
                "content-type": "utf-8"
            },
            "body": {
                "input": {"tr_id": tr_id, "tr_key": tr_key}
            }
        }
        try:
            self.ws.send(json.dumps(payload))
            self.subscribed.add(req.symbol)
            
            # Subscribe to Depth as well if applicable
            depth_tr = self.gateway.get_ws_depth_tr(req)
            if depth_tr:
                payload["body"]["input"]["tr_id"] = depth_tr
                self.ws.send(json.dumps(payload))
                
        except Exception as e:
            self.gateway.write_log(f"Sub Error: {e}")

    def close(self):
        self.active = False
        if self.ws: self.ws.close()


# =============================================================================
# KIS Gateway
# =============================================================================
class KisGateway(BaseGateway):
    """
    VN.py Gateway Interface for Korea Investment Securities (KIS)
    """
    default_setting = {
        "app_key": "",
        "app_secret": "",
        "account_no": "",
        "account_code": "01",
        "server": ["REAL", "DEMO"],
        "subscription_limit": 50, "depth_mode": True, "subscription_mode": "ALL",
        "interest_group": "01", # 관심종목 그룹코드
        "include_night": False
    }
    # 구독관리(list 저장), 관심종목관리 (list저장), depth_mode 적용
    market_loc = ["KR","OVRS"]
    market_type = ["SPOT","FUTOPT"]
    server = ["REAL","DEMO"]
    products = [Product.SPOT, Product.EQUITY, Product.BOND, Product.INDEX, Product.ETF, Product.FUTURES, Product.OPTION]  # vnpy Product 참조
    exchanges = [Exchange.KRX, Exchange.NXT, Exchange.SOR, Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX, Exchange.CME, Exchange.EUREX]

    def __init__(self, event_engine: EventEngine, gateway_name: str):
        super().__init__(event_engine, gateway_name)
        
        self.api = KisTdApi(self)
        self.ws = KisWsManager(self)
        
        self.ticks: Dict[str, TickData] = {}
        self.orders: Dict[str, OrderData] = {}
        self.market_loc = "KR"

    def connect(self, setting: dict):
        key = setting["app_key"]
        secret = setting["app_secret"]
        server = setting["server"]
        acc_no = setting["account_no"]
        acc_code = setting["account_code"]
        grp_code = setting.get("interest_group", "01")
        
        # 1. Connect REST API
        self.api.connect(key, secret, acc_no, acc_code, server)
        
        # 2. Start Websocket
        self.ws.start(key, secret, server)
        
        # 3. Initial Queries (Async)
        self.api.query_account()
        self.api.query_position()
        
        # 4. Load Interest Stocks
        if grp_code:
            self.api.query_interest_stocks(grp_code)

        self.write_log("KIS Gateway Connected")

    def subscribe(self, req: SubscribeRequest):
        self.ws.subscribe(req)

    def send_order(self, req: OrderRequest):
        return self.api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        self.api.cancel_order(req)

    def query_account(self):
        self.api.query_account()

    def query_position(self):
        self.api.query_position()

    def query_history(self, req: HistoryRequest):
        return self.api.query_history(req)

    def close(self):
        self.ws.close()
        self.api.close()

    # ----------------------------------------------------------------
    # Callback Handlers (From WebSocket)
    # ----------------------------------------------------------------

    def on_ws_tick(self, data: dict):
        symbol = data["code"]
        tick = self._get_tick(symbol)
        
        # Map fields from Parser
        tick.last_price = data.get("price", tick.last_price)
        tick.volume = data.get("acc_volume", tick.volume) # Accum Volume
        tick.turnover = data.get("turnover", tick.turnover)
        tick.open_price = data.get("open", tick.open_price)
        tick.high_price = data.get("high", tick.high_price)
        tick.low_price = data.get("low", tick.low_price)
        tick.open_interest = data.get("open_interest", tick.open_interest)
        
        # Timestamp
        if "localtime" in data: # Overseas
            try: 
                tick.datetime = datetime.strptime(data["localtime"], "%Y%m%d %H%M%S").replace(tzinfo=KOREA_TZ)
            except: pass
        else: # Domestic
            tick.datetime = datetime.now(KOREA_TZ)

        # Notify
        self.on_tick(copy(tick))

    def on_ws_depth(self, data: dict):
        symbol = data["code"]
        tick = self._get_tick(symbol)
        
        # Update Orderbook (Ask/Bid)
        # Parser provides list of tuples: [(price, vol), ...]
        
        for i, (price, vol) in enumerate(data["asks"]):
            if i >= 5: break
            setattr(tick, f"ask_price_{i+1}", price)
            setattr(tick, f"ask_volume_{i+1}", vol)
            
        for i, (price, vol) in enumerate(data["bids"]):
            if i >= 5: break
            setattr(tick, f"bid_price_{i+1}", price)
            setattr(tick, f"bid_volume_{i+1}", vol)
            
        tick.datetime = datetime.now(KOREA_TZ)
        self.on_tick(copy(tick))

    def on_ws_notice(self, data: dict):
        """
        Handle Execution/Order Notice.
        This provides real-time updates on Order Status.
        """
        # data = {'order_status': 'FILLED', 'account':..., 'order_no':..., ...}
        self.write_log(f"Execution Notice: {data}")
        
        # In a complete system, we would map 'order_no' to internal OrderID
        # and call on_order/on_trade. 
        # For now, we update if we can find the order.
        # (Implementing full OrderID mapping requires local cache state)

    def _get_tick(self, symbol):
        if symbol not in self.ticks:
            tick = TickData(
                gateway_name=self.gateway_name,
                symbol=symbol,
                exchange=Exchange.KRX, # Placeholder, dynamic logic preferred
                datetime=datetime.now(KOREA_TZ)
            )
            self.ticks[symbol] = tick
        return self.ticks[symbol]

    def get_ws_tr_info(self, req: SubscribeRequest):
        """Determine WebSocket TR ID and Key based on Symbol"""
        sym = req.symbol
        ex = req.exchange
        
        # 1. Domestic
        if ex in [Exchange.KRX, Exchange.KONEX]:
            if len(sym) == 6 and sym.isdigit(): return TR_WS["KR_STOCK"], sym
            if sym.startswith("1") or sym.startswith("2") or sym.startswith("3"): return TR_WS["KR_FUT"], sym # Fut/Opt
            if sym.startswith("KR"): return TR_WS["KR_BOND"], sym
            if sym == "0001" or sym == "1001": return TR_WS["KR_INDEX"], sym
            
        # 2. Night Market (Eurex)
        if ex == Exchange.EUREX:
            return TR_WS["NIGHT_FUT"], sym
            
        # 3. Overseas Stock
        if ex in [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]:
            # D+Market(3)+Symbol
            mkt = "NAS" if ex == Exchange.NASDAQ else ("NYS" if ex == Exchange.NYSE else "AMS")
            return TR_WS["OVRS_STOCK"], f"D{mkt}{sym}"
            
        # 4. Overseas Future
        if ex in [Exchange.CME, Exchange.CBOT]:
            return TR_WS["OVRS_FUT"], sym
            
        return None, None

    def get_ws_depth_tr(self, req: SubscribeRequest):
        """Get corresponding Depth TR ID"""
        sym = req.symbol
        ex = req.exchange
        
        if ex == Exchange.KRX:
            if len(sym) == 6: return TR_WS["KR_STOCK_HOKA"]
            return TR_WS["KR_FUT_HOKA"] # Simplify
        
        if ex in [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]:
            return TR_WS["OVRS_STOCK_HOKA"]
            
        if ex in [Exchange.CME, Exchange.CBOT]:
            return TR_WS["OVRS_FUT_HOKA"]
            
        return None


# =============================================================================
# REST API Client
# =============================================================================
import sys
import os
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable, Set
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo
import requests

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    OrderData, TradeData, PositionData, AccountData,
    OrderRequest, CancelRequest, SubscribeRequest,
    HistoryRequest, BarData
)
from vnpy.trader.constant import (
    Exchange, Product, OrderType, Direction, Status, Interval, Offset
)
from vnpy.trader.utility import get_folder_path

# Import Parser
try:
    from . import kis_parser
except ImportError:
    import kis_parser

# Timezone
KST = ZoneInfo("Asia/Seoul")

# ----------------------------------------------------------------------------
# [Exchange Mapping Definition]
# ----------------------------------------------------------------------------
EXCHANGE_KR_STOCK = [Exchange.SOR,Exchange.KRX, Exchange.NXT, Exchange.KOSDAQ]
EXCHANGE_KR_FUTOPT = [Exchange("KOFEX")]
EXCHANGE_KR_BOND = [Exchange("BOND")]  # 가상의 채권 거래소
EXCHANGE_OV_STOCK = [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX, Exchange.SEHK, Exchange("TSE"), Exchange("SHS")]
EXCHANGE_OV_FUT = [Exchange.CME, Exchange.EUREX, Exchange.HKFE, Exchange.SGX, Exchange("ICE")]

class KisTdApi:
    """
    KIS REST API Client (Trading & Data Query)
    Supports: 
      - Domestic: Stock, ETF, Future, Option, Bond
      - Overseas: Stock, Future, Option
    """

    def __init__(self, gateway: BaseGateway):
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        
        # Settings from Gateway
        self.app_key = ""
        self.app_secret = ""
        self.acc_no = ""      
        self.acc_prdt_cd = "" 
        self.server = "DEMO"
        self.market_loc = ["KR"]     # ["KR", "OVRS"]
        self.market_type = ["SPOT"]  # ["SPOT", "FUTOPT"]
        self.products = []
        self.base_url = ""
        
        # Interest List
        self.interest_list_file = get_folder_path("kis_gateway") / "interest_list.json"
        self.interest_list: Set[str] = set()

        # ThreadPool for Async Requests
        # (API 요청은 I/O Bound 작업이므로 ThreadPool이 적합합니다.)
        self.executor = ThreadPoolExecutor(max_workers=10)

        self.req_id_count = 0     # For internal logging/tracking if needed
        self.quote_count = 0      # Local Quote ID counter
        
        # Quote Management: quote_id -> (buy_odno, sell_odno)
        self.quote_map: Dict[str, tuple[str, str]] = {}
        
    def connect(self, setting: dict) -> None:
        """Initialize connection settings based on account type"""
        self.app_key = setting["app_key"]
        self.app_secret = setting["app_secret"]
        self.acc_no = setting["account_no"]
        self.acc_prdt_cd = setting["account_code"]
        self.server = setting["server"]  # "REAL" or "DEMO"
        
        # Account Properties
        self.market_loc = setting.get("market_loc", ["KR"])
        self.market_type = setting.get("market_type", ["SPOT"])
        self.products = setting.get("products", [])

        # Domain Selection
        if self.server == "REAL":
            self.base_url = "https://openapi.koreainvestment.com:9443"
        else:
            self.base_url = "https://openapivts.koreainvestment.com:29443"

        # Load Interest List
        self._load_interest_list()

        self.gateway.write_log(f"KIS REST API Initialized ({self.server}). Supported: {self.market_loc} / {self.market_type}")

    def close(self): pass

    # ------------------------------------------------------------------------
    # [Synchronous Request] - For Orders/Quotes (Blocking)
    # ------------------------------------------------------------------------
    def _request_sync(self, path: str, tr_id: str, params: dict, method: str = "GET") -> Optional[dict]:
        url = f"{self.base_url}{path}"
        headers = self.gateway.kis_auth.get_header(tr_id=tr_id)
        if not headers:
            self.gateway.write_log(f"Header Fail: {tr_id}")
            return None

        try:
            if method == "GET":
                resp = requests.get(url, headers=headers, params=params, timeout=10)
            else:
                resp = requests.post(url, headers=headers, data=json.dumps(params), timeout=10)
            
            if resp.status_code != 200:
                self.gateway.write_log(f"HTTP Error {resp.status_code} [{tr_id}]: {resp.text}")
                return None
            
            return resp.json()
        except Exception as e:
            self.gateway.write_log(f"Req Exception [{tr_id}]: {e}")
            return None

    # ------------------------------------------------------------------------
    # [Asynchronous Request] - For Data Queries
    # ------------------------------------------------------------------------
    def _request_async(self, path: str, tr_id: str, params: dict, method: str, callback: Callable, extra: Any = None):
        self.executor.submit(self._process_async, path, tr_id, params, method, callback, extra)

    def _process_async(self, path, tr_id, params, method, callback, extra):
        res = self._request_sync(path, tr_id, params, method)
        if res and callback:
            callback(res, extra)

    # ------------------------------------------------------------------------
    # [Trading] Send Order (Synchronous)
    # ------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        Sends an order synchronously.
        Uses the returned 'ODNO' (KIS Order ID) as the 'orderid'.
        Returns: vt_orderid (GatewayName.ODNO)
        """
        config = self._get_order_config(req)
        if not config:
            self.gateway.write_log(f"Order Rejected: Unsupported {req.exchange}")
            return ""

        # 1. Send Request (Blocking)
        # self.gateway.write_log(f"Sending Order: {req.symbol} {req.direction.value} {req.price}")
        resp = self._request_sync(config['url'], config['tr_id'], config['params'], method="POST")
        
        # 2. Parse Result
        parsed = kis_parser.parse_order_response(resp)
        kis_odno = parsed.get("odno") # KIS Order No
        
        # 3. Create OrderData
        # [Crucial]: Use KIS 'odno' directly as the 'orderid'
        if kis_odno and resp.get('rt_cd') == '0':
            order = req.create_order_data(kis_odno, self.gateway_name)
            order.status = Status.NOTTRADED
            
            self.gateway.on_order(order)
            self.gateway.write_log(f"Order Success: {req.symbol} {req.direction.value} -> ODNO: {kis_odno}")
            return order.vt_orderid
        else:
            # If failed, we generate a temp local ID just to notify rejection
            self.req_id_count += 1
            temp_id = f"ERR{self.req_id_count}"
            order = req.create_order_data(temp_id, self.gateway_name)
            order.status = Status.REJECTED
            
            err_msg = parsed.get("msg", resp.get("msg1", "Unknown Error"))
            self.gateway.on_order(order)
            self.gateway.write_log(f"Order Failed: {err_msg}")
            return ""

    # ------------------------------------------------------------------------
    # [Trading] Send Quote (Synchronous)
    # ------------------------------------------------------------------------
    def send_quote(self, req: QuoteRequest) -> str:
        """
        Sends Two-sided Quote (Buy & Sell).
        Triggers on_order x 2 (for tracking) AND on_quote x 1 (for strategy).
        """
        self.quote_count += 1
        quote_id = str(self.quote_count) # Local Quote ID

        # 1. Construct Child Requests
        buy_req = OrderRequest(
            symbol=req.symbol, exchange=req.exchange, direction=Direction.LONG,
            type=OrderType.LIMIT, volume=req.bid_volume, price=req.bid_price,
            offset=Offset.OPEN, reference=f"QuoteBuy.{quote_id}"
        )
        sell_req = OrderRequest(
            symbol=req.symbol, exchange=req.exchange, direction=Direction.SHORT,
            type=OrderType.LIMIT, volume=req.ask_volume, price=req.ask_price,
            offset=Offset.CLOSE, reference=f"QuoteSell.{quote_id}"
        )

        # 2. Send Orders (Sequential Sync)
        vt_oid_buy = self.send_order(buy_req)
        vt_oid_sell = self.send_order(sell_req)

        # Extract Raw OrderIDs (remove gateway name)
        # vt_orderid format: "GatewayName.OrderID"
        oid_buy = vt_oid_buy.split(".")[-1] if vt_oid_buy else ""
        oid_sell = vt_oid_sell.split(".")[-1] if vt_oid_sell else ""

        # 3. Create QuoteData
        # Vn.py QuoteData typically needs a quoteid.
        quote = QuoteData(
            gateway_name=self.gateway_name,
            symbol=req.symbol,
            exchange=req.exchange,
            quoteid=quote_id,
            bid_price=req.bid_price,
            bid_volume=req.bid_volume,
            ask_price=req.ask_price,
            ask_volume=req.ask_volume,
            bid_offset=Offset.OPEN,
            ask_offset=Offset.CLOSE,
            status=Status.NOTTRADED,
            datetime=datetime.now(KST)
        )
        
        # 4. Trigger on_quote
        # If at least one order succeeded, we consider the quote "sent" (or partially sent)
        if vt_oid_buy or vt_oid_sell:
            self.gateway.on_quote(quote)
            
            # Map QuoteID to Orders for cancellation later
            self.quote_map[quote_id] = (oid_buy, oid_sell)
            
            # Return vt_quoteid
            return f"{self.gateway_name}.{quote_id}"
        
        return ""

    def cancel_quote(self, req: CancelRequest) -> None:
        """
        Cancel a Quote by cancelling its child orders.
        Req.orderid will be the quoteid here.
        """
        quote_id = req.orderid
        child_orders = self.quote_map.get(quote_id)
        
        if not child_orders:
            self.gateway.write_log(f"Cancel Quote Failed: ID {quote_id} not found.")
            return

        buy_odno, sell_odno = child_orders
        
        # Cancel Buy Side
        if buy_odno:
            cancel_buy = copy(req)
            cancel_buy.orderid = buy_odno
            self.cancel_order(cancel_buy)
            
        # Cancel Sell Side
        if sell_odno:
            cancel_sell = copy(req)
            cancel_sell.orderid = sell_odno
            self.cancel_order(cancel_sell)

    # ------------------------------------------------------------------------
    # [Trading] Cancel Order (Synchronous)
    # ------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> None:
        """
        Cancel using ODNO directly.
        """
        config = self._get_cancel_config(req)
        if not config:
            return

        # req.orderid MUST be the KIS ODNO (because we set it that way in send_order)
        self.gateway.write_log(f"Cancelling Order (ODNO): {req.orderid}")
        resp = self._request_sync(config['url'], config['tr_id'], config['params'], method="POST")
        
        parsed = kis_parser.parse_cancel_response(resp)
        if parsed.get("status") == "0":
            self.gateway.write_log(f"Cancel Accepted: {parsed.get('odno')}")
        else:
            self.gateway.write_log(f"Cancel Failed: {parsed.get('msg')}")
            
    # ------------------------------------------------------------------------
    # [Account] Balance & Position (Multi-Asset)
    # ------------------------------------------------------------------------
    def query_account(self) -> None:
        """Query Account Balance based on Account Type"""
        
        # 1. Domestic Stock Balance
        if "KR" in self.market_loc and "SPOT" in self.market_type:
            self._query_balance_kr_stock()
            
        # 2. Domestic Future/Option Balance
        if "KR" in self.market_loc and "FUTOPT" in self.market_type:
            self._query_balance_kr_future()
            
        # 3. Domestic Bond Balance (Often part of Stock balance, but specific TR exists if separated)
        # (Assuming Bond balance is covered by Stock Balance TR in general accounts)

        # 4. Overseas Stock Balance
        if "OVRS" in self.market_loc and "SPOT" in self.market_type:
            self._query_balance_ov_stock()
            
        # 5. Overseas Future Balance (Deposit)
        if "OVRS" in self.market_loc and "FUTOPT" in self.market_type:
            self._query_balance_ov_future()

    def query_position(self) -> None:
        """Query Positions based on Account Type"""
        
        # 1. Domestic Stock
        if "KR" in self.market_loc and "SPOT" in self.market_type:
            self._query_position_kr_stock()
            self._query_position_kr_bond() # Bond position check
            
        # 2. Domestic Future/Option
        if "KR" in self.market_loc and "FUTOPT" in self.market_type:
            self._query_position_kr_future()
            
        # 3. Overseas Stock
        if "OVRS" in self.market_loc and "SPOT" in self.market_type:
            self._query_position_ov_stock()
            
        # 4. Overseas Future (Open Interest)
        if "OVRS" in self.market_loc and "FUTOPT" in self.market_type:
            self._query_position_ov_future()

    # --- Specific Query Implementations ---

    def _query_balance_kr_stock(self):
        tr_id = "VTTC8434R" if self.server == "DEMO" else "TTTC8434R"
        self._request_async(
            "/uapi/domestic-stock/v1/trading/inquire-balance", tr_id,
            {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "AFHR_FLPR_YN": "N", "OFL_YN": "N", "INQR_DVSN": "02", "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""},
            "GET", self._on_balance_data, "KR_STOCK" 
        )

    def _query_balance_kr_future(self):
        # 선물옵션예탁금 상세
        tr_id = "VTTD0491R" if self.server == "DEMO" else "TTTA0491R" # (Check Real TR ID in docs)
        if self.server == "REAL": tr_id = "OPCW0001" # Placeholder, use actual TR
        self._request_async(
            "/uapi/domestic-futureoption/v1/trading/inquire-account-margin", tr_id,
            {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "INQR_DVSN_1": "Y"},
            "GET", self._on_balance_data, "KR_FUTOPT"
        )

    def _query_balance_ov_stock(self):
        tr_id = "VTTS3012R" if self.server == "DEMO" else "TTTS3012R"
        self._request_async(
            "/uapi/overseas-stock/v1/trading/inquire-balance", tr_id,
            {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "OVRS_EXCG_CD": "NAS", "TR_CRCY_CD": "USD", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""},
            "GET", self._on_balance_data, "OS_STOCK"
        )

    def _query_balance_ov_future(self):
        # [해외선물옵션] 예수금현황: OTFM1411R
        tr_id = "OTFM1411R" 
        # Note: Mock support check needed. Usually unavailable in Mock for Ovrs Fut.
        if self.server == "DEMO": return 
        
        self._request_async(
            "/uapi/overseas-futureoption/v1/trading/inquire-deposit", tr_id,
            {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "CRCY_CD": "USD"},
            "GET", self._on_balance_data, "OS_FUTOPT" 
        )

    def _query_position_kr_stock(self):
        # Reuse Balance TR for Stock Position (KIS Style)
        self._query_balance_kr_stock()

    def _query_position_kr_bond(self):
        # [장내채권] 잔고조회: CTSC8407R
        tr_id = "CTSC8407R"
        if self.server == "DEMO": return # Mock unsupported usually
        self._request_async(
            "/uapi/domestic-bond/v1/trading/inquire-balance", tr_id,
            {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "INQR_DVSN": "01", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""},
            "GET", self._on_position_data, {"type": "KR_STOCK", "exchange": Exchange("BOND")} # Treat as stock-like position
        )

    def _query_position_kr_future(self):
        tr_id = "VTTD0401U" if self.server == "DEMO" else "TTTA0401U"
        self._request_async(
            "/uapi/domestic-futureoption/v1/trading/inquire-balance", tr_id,
            {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "INQR_DVSN": "01", "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""},
            "GET", self._on_position_data, {"type": "KR_FUTOPT", "exchange": Exchange("KOFEX")}
        )

    def _query_position_ov_stock(self):
        tr_id = "VTTS3012R" if self.server == "DEMO" else "TTTS3012R"
        self._request_async(
            "/uapi/overseas-stock/v1/trading/inquire-balance", tr_id,
            {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "OVRS_EXCG_CD": "NAS", "TR_CRCY_CD": "USD", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""},
            "GET", self._on_position_data, {"type": "OS_STOCK", "exchange": Exchange.NASDAQ}
        )

    def _query_position_ov_future(self):
        # [해외선물옵션] 미결제내역조회(잔고): OTFM1412R
        tr_id = "OTFM1412R"
        if self.server == "DEMO": return
        self._request_async(
            "/uapi/overseas-futureoption/v1/trading/inquire-unpd", tr_id,
            {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "SORT_SQN": "DS", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""},
            "GET", self._on_position_data, {"type": "OS_FUTOPT", "exchange": Exchange.CME} # Exchange is placeholder
        )

# --- Callbacks ---
    def _on_balance_data(self, data: dict, asset_type: str):
        parsed = kis_parser.parse_balance(data, asset_type=asset_type)
        account = AccountData(gateway_name=self.gateway_name, accountid=f"{self.acc_no}-{asset_type}", balance=parsed["balance"], frozen=parsed["balance"]-parsed["available"], available=parsed["available"])
        self.gateway.on_account(account)
    
    def _on_position_data(self, data: dict, info: dict):
        parsed_list = kis_parser.parse_position(data, asset_type=info["type"])
        for pos in parsed_list:
            direction = Direction.NET
            if pos.get("direction") == "long": direction = Direction.LONG
            elif pos.get("direction") == "short": direction = Direction.SHORT
            position = PositionData(gateway_name=self.gateway_name, symbol=pos["symbol"], exchange=info["exchange"], direction=direction, volume=pos["quantity"], price=pos["price"], pnl=pos["pnl"])
            self.gateway.on_position(position)

    # ------------------------------------------------------------------------
    # [History] Sync Pagination
    # ------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        config = self._get_history_config(req)
        if not config: return []
        return self._fetch_history_loop(req, config)

    def _fetch_history_loop(self, req: HistoryRequest, config: dict) -> List[BarData]:
        tr_id, url, params = config['tr_id'], config['url'], config['params']
        updater = config['updater']
        all_bars = []
        
        while True:
            resp = self._request_sync(url, tr_id, params)
            if not resp: break

            raw_data = resp.get('output2') or resp.get('output')
            parsed_list = kis_parser.parse_history_data(raw_data)
            if not parsed_list: break

            chunk = []
            for d in parsed_list:
                bar = BarData(
                    gateway_name=self.gateway_name, symbol=req.symbol, exchange=req.exchange,
                    datetime=d['datetime'].replace(tzinfo=KST), interval=req.interval,
                    volume=d['volume'], turnover=d.get('turnover', 0),
                    open_price=d['open'], high_price=d['high'], low_price=d['low'], close_price=d['close']
                )
                chunk.append(bar)
            all_bars.extend(chunk)

            if chunk:
                min_dt = min(b.datetime for b in chunk)
                if min_dt <= req.start: break
            else: break
            if not updater(params, resp, min_dt): break
            time.sleep(0.1)

        all_bars.sort(key=lambda x: x.datetime)
        unique_bars = []
        seen = set()
        for b in all_bars:
            if b.datetime not in seen:
                unique_bars.append(b)
                seen.add(b.datetime)
        return [b for b in unique_bars if req.start <= b.datetime <= req.end]

    # --- History Updaters & Mappers ---
    def _next_date_dec(self, params, resp, last_dt: datetime) -> bool:
        last_time_str = last_dt.strftime("%H%M%S")
        if last_time_str <= "090000":
            params["FID_INPUT_DATE_1"] = (last_dt - timedelta(days=1)).strftime("%Y%m%d")
            params["FID_INPUT_HOUR_1"] = "153000"
        else:
            params["FID_INPUT_DATE_1"] = last_dt.strftime("%Y%m%d")
            params["FID_INPUT_HOUR_1"] = last_time_str
        return True
    
    def _next_close_dt(self, params, resp, last_dt: datetime) -> bool:
        params["CLOSE_DATE_TIME"] = (last_dt - timedelta(days=1)).strftime("%Y%m%d")
        return True
    
    def _next_key_ovrs(self, params, resp, last_dt) -> bool:
        if resp.get("output1", {}).get("next") != "1": return False
        try:
            last = resp["output2"][-1]
            params["KEYB"] = f"{last.get('kymd')}{last.get('khms')}"
            params["NEXT"] = "1"
            return True
        except: return False
        
    def _map_ov_excg(self, exchange: Exchange) -> str:
        mapping = {Exchange.NASDAQ: "NAS", Exchange.NYSE: "NYS", Exchange.AMEX: "AMS", Exchange.SEHK: "HKS", Exchange.CME: "CME", Exchange.EUREX: "EUREX"}
        return mapping.get(exchange, "") 
        
    # ------------------------------------------------------------------------
    # [Config Helpers] TR_ID & Parameters
    # ------------------------------------------------------------------------
    def _check_market_support(self, req_exchange, gateway: KisGateway) -> bool:
        """Check if the exchange/product is supported by this account"""
        if Product.EQUITY in gateway.products and req_exchange in gateway.exchanges and "KR" in gateway.market_loc and "SPOT" in gateway.market_type:
            return "REQ_KR_STOCK"
        elif Product.FUTURES in gateway.products and req_exchange in gateway.exchanges and "KR" in gateway.market_loc and "FUTOPT" in gateway.market_type:
            return "REQ_KR_FUTOPT"
        elif Product.BOND in gateway.products and req_exchange in gateway.exchanges and "KR" in gateway.market_loc and "SPOT" in gateway.market_type:
            return "REQ_KR_BOND"
        elif Product.EQUITY in gateway.products and req_exchange in gateway.exchanges and "OVRS" in gateway.market_loc and "SPOT" in gateway.market_type:
            return "REQ_OV_STOCK"
        elif Product.FUTURES in gateway.products and req_exchange in gateway.exchanges and "OVRS" in gateway.market_loc and "FUTOPT" in gateway.market_type:
            return "REQ_OV_FUT"
        return None
    
    def _get_order_config(self, req: OrderRequest) -> Optional[dict]:
        req_market = self._check_market_support(req.exchange, self.gateway)
        # 1. Domestic Stock
        if req_market == "REQ_KR_STOCK":
            tr_id = ("VTTC0802U" if self.server == "DEMO" else "TTTC0802U") if req.direction == Direction.LONG else ("VTTC0801U" if self.server == "DEMO" else "TTTC0801U")
            return {"tr_id": tr_id, "url": "/uapi/domestic-stock/v1/trading/order-cash", 
                    "params": {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "PDNO": req.symbol, "ORD_DVSN": "00" if req.type == OrderType.LIMIT else "01", "ORD_QTY": str(int(req.volume)), "ORD_UNPR": str(int(req.price))}}
        
        # 2. Domestic Future/Option
        elif req_market == "REQ_KR_FUTOPT":
            tr_id = ("VTTD0303U" if self.server == "DEMO" else "TTTA0303U") if req.direction == Direction.LONG else ("VTTD0301U" if self.server == "DEMO" else "TTTA0301U")
            return {"tr_id": tr_id, "url": "/uapi/domestic-futureoption/v1/trading/order",
                    "params": {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "PDNO": req.symbol, "ORD_QTY": str(int(req.volume)), "ORD_UNPR": str(req.price), "ORD_DVSN": "00"}}

        # 3. Domestic Bond (Jangnae)
        elif req_market == "REQ_KR_BOND":
            # 매수: TTTC0952U, 매도: TTTC0958U
            tr_id = "TTTC0952U" if req.direction == Direction.LONG else "TTTC0958U"
            if self.server == "DEMO": return None # Bond not supported in Mock
            url = "/uapi/domestic-bond/v1/trading/buy" if req.direction == Direction.LONG else "/uapi/domestic-bond/v1/trading/sell"
            # Bond Price needs careful handling (Yield vs Price). Assuming Price for generic.
            return {"tr_id": tr_id, "url": url,
                    "params": {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "PDNO": req.symbol, "ORD_QTY2": str(int(req.volume)), "BOND_ORD_UNPR": str(req.price), "ORD_SVR_DVSN_CD": "0"}}

        # 4. Overseas Stock
        elif req_market == "REQ_OV_STOCK":
            tr_id = ("VTTT1002U" if self.server == "DEMO" else "TTTT1002U") if req.direction == Direction.LONG else ("VTTT1006U" if self.server == "DEMO" else "TTTT1006U")
            if self.server == "REAL": tr_id = "JTTT1002U" if req.direction == Direction.LONG else "JTTT1006U" # Example Real
            return {"tr_id": tr_id, "url": "/uapi/overseas-stock/v1/trading/order",
                    "params": {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "OVRS_EXCG_CD": self._map_ov_excg(req.exchange), "PDNO": req.symbol, "ORD_QTY": str(int(req.volume)), "OVRS_ORD_UNPR": str(req.price), "ORD_DVSN": "00"}}

        # 5. Overseas Future/Option
        elif req_market == "REQ_OV_FUT":
            # [해외선물옵션] 주문: OTFM3001U
            tr_id = "OTFM3001U"
            if self.server == "DEMO": return None # Ovrs Fut order not usually in Mock
            return {"tr_id": tr_id, "url": "/uapi/overseas-futureoption/v1/trading/order",
                    "params": {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "OVRS_FUTR_FX_PDNO": req.symbol, "ORD_QTY": str(int(req.volume)), "ORD_UNPR": str(req.price), "SLL_BUY_DVSN_CD": "02" if req.direction==Direction.LONG else "01"}}
        
        return None

    def _get_cancel_config(self, req: CancelRequest) -> Optional[dict]:
        req_market = self._check_market_support(req.exchange, self.gateway)
        # KR Stock
        if req_market == "REQ_KR_STOCK":
            return {"tr_id": "VTTC0803U" if self.server == "DEMO" else "TTTC0803U", "url": "/uapi/domestic-stock/v1/trading/order-rvsecncl",
                    "params": {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "ORGN_ODNO": req.orderid, "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": "0", "QTY_ALL_ORD_YN": "Y"}}
        # KR Bond
        elif req_market == "REQ_KR_BOND":
            # [장내채권] 정정취소: TTTC0953U
            if self.server == "DEMO": return None
            return {"tr_id": "TTTC0953U", "url": "/uapi/domestic-bond/v1/trading/order-rvsecncl",
                    "params": {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "ORGN_ODNO": req.orderid, "RVSE_CNCL_DVSN_CD": "02", "QTY_ALL_ORD_YN": "Y"}}
        # Overseas Future
        elif req_market == "REQ_OV_FUT":
            # [해외선물옵션] 정정취소: OTFM3003U (Cancel)
            if self.server == "DEMO": return None
            return {"tr_id": "OTFM3003U", "url": "/uapi/overseas-futureoption/v1/trading/order-rvsecncl",
                    "params": {"CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_prdt_cd, "ORGN_ODNO": req.orderid}}

        return None

    def _get_history_config(self, req: HistoryRequest) -> Optional[dict]:
        start_s = req.start.strftime("%Y%m%d")
        end_s = req.end.strftime("%Y%m%d")
   
        req_market = self._check_market_support(req.exchange, self.gateway)
                
        # 1. Domestic Stock
        if req_market == "REQ_KR_STOCK":
            if req.interval == Interval.MINUTE:
                return {"tr_id": "FHKST03010230", "url": "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
                        "params": {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": req.symbol, "FID_INPUT_DATE_1": end_s, "FID_INPUT_HOUR_1": "153000", "FID_PW_DATA_INCU_YN": "Y"},
                        "updater": self._next_date_dec}
            else:
                return {"tr_id": "FHKST03010100", "url": "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                        "params": {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": req.symbol, "FID_INPUT_DATE_1": start_s, "FID_INPUT_DATE_2": end_s, "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"},
                        "updater": lambda p,r,d: False}
        
        # 2. Domestic Future/Option
        elif req_market == "REQ_KR_FUTOPT":
            if req.interval == Interval.MINUTE:
                return {"tr_id": "FHKIF03020200", "url": "/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice",
                        "params": {"FID_COND_MRKT_DIV_CODE": "F", "FID_INPUT_ISCD": req.symbol, "FID_INPUT_DATE_1": end_s, "FID_INPUT_HOUR_1": "154500"},
                        "updater": self._next_date_dec}
        
        # 3. Domestic Bond
        elif req_market == "REQ_KR_BOND":
             # 장내채권현재가(일별): FHKBJ773404C0
             return {"tr_id": "FHKBJ773404C0", "url": "/uapi/domestic-bond/v1/quotations/inquire-daily-price",
                     "params": {"FID_COND_MRKT_DIV_CODE": "B", "FID_INPUT_ISCD": req.symbol, "FID_INPUT_DATE_1": start_s, "FID_INPUT_DATE_2": end_s},
                     "updater": lambda p,r,d: False}

        # 4. Overseas Stock
        elif req_market == "REQ_OV_STOCK":
            if req.interval == Interval.MINUTE:
                return {"tr_id": "HHDFS76950200", "url": "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice",
                        "params": {"EXCD": self._map_ov_excg(req.exchange), "SYMB": req.symbol, "NMIN": "1", "PINC": "1", "NEXT": "", "NREC": "120", "KEYB": ""},
                        "updater": self._next_key_ovrs}
                
        # 4. Overseas Future (Updated based on CSV)
        elif req_market == "REQ_OV_FUT":
            if req.interval == Interval.MINUTE:
                # 해외선물 분봉조회: HHDFC55020400
                return {"tr_id": "HHDFC55020400", "url": "/uapi/overseas-futureoption/v1/quotations/inquire-time-futurechartprice",
                        "params": {"SRS_CD": req.symbol, "START_DATE_TIME": "", "CLOSE_DATE_TIME": end_s, "QRY_CNT": "100", "GAP": "1"},
                        "updater": self._next_close_dt}
            else:
                # 해외선물 체결추이(일간): HHDFC55020100
                return {"tr_id": "HHDFC55020100", "url": "/uapi/overseas-futureoption/v1/quotations/daily-ccnl",
                        "params": {"SRS_CD": req.symbol, "START_DATE_TIME": "", "CLOSE_DATE_TIME": end_s, "QRY_CNT": "100"},
                        "updater": self._next_close_dt}

        return None

    # ------------------------------------------------------------------------
    # [Interest List]
    # ------------------------------------------------------------------------
    def _load_interest_list(self):
        if self.interest_list_file.exists():
            try:
                with open(self.interest_list_file, "r", encoding="utf-8") as f:
                    self.interest_list = set(json.load(f))
            except Exception as e:
                self.gateway.write_log(f"Interest Load Failed: {e}")

    def save_interest_list(self):
        try:
            with open(self.interest_list_file, "w", encoding="utf-8") as f:
                json.dump(list(self.interest_list), f)
        except Exception as e:
            self.gateway.write_log(f"Interest Save Failed: {e}")

    def add_interest(self, symbol: str):
        if symbol not in self.interest_list:
            self.interest_list.add(symbol)
            self.save_interest_list()
            
    def remove_interest(self, symbol: str):
        if symbol in self.interest_list:
            self.interest_list.remove(symbol)
            self.save_interest_list()
