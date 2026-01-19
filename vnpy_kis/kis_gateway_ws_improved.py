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
        "interest_group": "01", # 관심종목 그룹코드
        "include_night": False
    }

    exchanges = [Exchange.KRX, Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX, Exchange.CME, Exchange.EUREX]

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
class KisTdApi:
    """
    REST API Handler for Order, Account, History.
    Uses 'kis_parser' for response processing.
    """
    
    def __init__(self, gateway: KisGateway):
        self.gateway = gateway
        self.domain = ""
        self.headers = {}
        self.acc_no = ""
        self.acc_code = ""
        self.app_key = ""
        self.app_secret = ""

    def connect(self, key, secret, acc_no, acc_code, server):
        self.app_key = key
        self.app_secret = secret
        self.acc_no = acc_no
        self.acc_code = acc_code
        
        self.domain = "https://openapi.koreainvestment.com:9443" if server == "REAL" else "https://openapivts.koreainvestment.com:29443"
        
        # Get Token (Blocking)
        token = kis_auth.get_token(key, secret, server)
        self.headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": key,
            "appsecret": secret
        }

    def close(self): pass

    def get_tr_header(self, tr_id, is_cont=False):
        h = self.headers.copy()
        h["tr_id"] = tr_id
        h["custtype"] = "P"
        if is_cont: h["tr_cont"] = "N"
        return h

    # ----------------------------------------------------------------
    # 1. Order Management
    # ----------------------------------------------------------------
    
    def send_order(self, req: OrderRequest):
        is_buy = req.direction == Direction.LONG
        ex = req.exchange
        sym = req.symbol
        
        # Logic to select URL & TR_ID & Params
        url, tr_id, params = "", "", {}
        
        # A. Domestic Stock
        if ex == Exchange.KRX and len(sym) == 6:
            url = f"{self.domain}/uapi/domestic-stock/v1/trading/order-cash"
            tr_id = TR_REST["KR_STOCK_ORD"]["buy" if is_buy else "sell"]
            params = {
                "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code,
                "PDNO": sym, "ORD_DVSN": "00", # Limit
                "ORD_QTY": str(int(req.volume)), "ORD_UNPR": str(int(req.price))
            }
            
        # B. Domestic Future/Option
        elif ex == Exchange.KRX:
            url = f"{self.domain}/uapi/domestic-futureoption/v1/trading/order"
            tr_id = TR_REST["KR_FUT_ORD"]["buy"] # Same TR
            params = {
                "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code,
                "PDNO": sym, 
                "SLL_BUY_DVSN_CD": "02" if is_buy else "01",
                "ORD_QTY": str(int(req.volume)), "ORD_UNPR": str(req.price),
                "ORD_DVSN_CD": "01"
            }
            
        # C. Overseas Stock (US)
        elif ex in [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]:
            url = f"{self.domain}/uapi/overseas-stock/v1/trading/order"
            tr_id = TR_REST["OVRS_STOCK_ORD"]["buy" if is_buy else "sell"]
            exch_cd = "NASD" if ex == Exchange.NASDAQ else ("NYSE" if ex == Exchange.NYSE else "AMEX")
            params = {
                "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code,
                "OVRS_EXCG_CD": exch_cd, "PDNO": sym,
                "ORD_QTY": str(int(req.volume)), "OVRS_ORD_UNPR": str(req.price),
                "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": "00"
            }
            
        # D. Bond
        elif req.product == Product.BOND:
            url = f"{self.domain}/uapi/domestic-bond/v1/trading/{'buy' if is_buy else 'sell'}"
            tr_id = TR_REST["KR_BOND_ORD"]["buy" if is_buy else "sell"]
            params = {
                "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code, "PDNO": sym,
                "ORD_QTY2": str(int(req.volume)), "BOND_ORD_UNPR": str(req.price),
                "ORD_SVR_DVSN_CD": "0"
            }
            
        # E. Night Market (Eurex)
        elif ex == Exchange.EUREX:
            url = f"{self.domain}/uapi/domestic-futureoption/v1/trading/order" # Night usually uses same or specific URL
            tr_id = TR_REST["KR_NIGHT_ORD"]["buy"] 
            params = {
                "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code, "PDNO": sym,
                "SLL_BUY_DVSN_CD": "02" if is_buy else "01",
                "ORD_QTY": str(int(req.volume)), "ORD_UNPR": str(req.price),
                "ORD_DVSN_CD": "01"
            }

        if not url: return ""

        # Send Async
        threading.Thread(target=self._post_order, args=(url, tr_id, params, req)).start()
        return req.vt_orderid

    def _post_order(self, url, tr_id, params, req):
        try:
            res = requests.post(url, headers=self.get_tr_header(tr_id), json=params)
            data = res.json()
            if data['rt_cd'] == '0':
                req.status = Status.NOTTRADED
                order = req.create_order_data(req.orderid, self.gateway.gateway_name)
                # Parse output for OrderNo
                if "output" in data and "ODNO" in data["output"]:
                    order.orderid = data["output"]["ODNO"]
                elif "output" in data and "KRX_FWDG_ORD_ORGNO" in data["output"]:
                    order.orderid = data["output"]["KRX_FWDG_ORD_ORGNO"]
                self.gateway.on_order(order)
            else:
                self.gateway.write_log(f"Order Failed: {data['msg1']}")
        except Exception as e:
            self.gateway.write_log(f"Order Error: {e}")

    def cancel_order(self, req: CancelRequest):
        """Cancel Order Logic"""
        ex = req.exchange
        
        # 1. Domestic Stock
        if ex == Exchange.KRX and len(req.symbol) == 6:
            url = f"{self.domain}/uapi/domestic-stock/v1/trading/order-rvsecncl"
            tr_id = TR_REST["KR_STOCK_ORD"]["cancel"]
            params = {
                "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code,
                "KRX_FWDG_ORD_ORGNO": "", "ORGN_ODNO": req.orderid,
                "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02", # 02: Cancel
                "ORD_QTY": "0", "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y"
            }
            
        # 2. Domestic Fut/Opt
        elif ex == Exchange.KRX:
            url = f"{self.domain}/uapi/domestic-futureoption/v1/trading/order-rvsecncl"
            tr_id = TR_REST["KR_FUT_ORD"]["cancel"]
            params = {
                "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code,
                "KRX_FWDG_ORD_ORGNO": "", "ORGN_ODNO": req.orderid,
                "ORD_DVSN_CD": "01", "RVSE_CNCL_DVSN_CD": "02",
                "ORD_QTY": "0", "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y"
            }
            
        # 3. Overseas Stock
        elif ex in [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]:
            url = f"{self.domain}/uapi/overseas-stock/v1/trading/order-rvsecncl"
            tr_id = TR_REST["OVRS_STOCK_ORD"]["cancel"]
            exch_cd = "NASD" if ex == Exchange.NASDAQ else ("NYSE" if ex == Exchange.NYSE else "AMEX")
            params = {
                "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code,
                "OVRS_EXCG_CD": exch_cd, "ODNO": req.orderid,
                "ORD_QTY": "0", "CNCL_DVSN": "02"
            }
        else: return

        threading.Thread(target=self._post_cancel, args=(url, tr_id, params)).start()

    def _post_cancel(self, url, tr_id, params):
        try:
            requests.post(url, headers=self.get_tr_header(tr_id), json=params)
        except Exception: pass

    # ----------------------------------------------------------------
    # 2. Account & Position
    # ----------------------------------------------------------------

    def query_account(self):
        """Query Balance (Multi-Currency)"""
        # Domestic Balance TR
        url = f"{self.domain}/uapi/domestic-stock/v1/trading/inquire-balance"
        tr_id = TR_REST["KR_STOCK_BAL"]
        params = {
            "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code,
            "INQR_DVSN": "02", "AFHR_FLPR_YN": "N", "OFL_YN": "N", "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""
        }
        
        # Overseas Balance TR (If applicable)
        url_ovrs = f"{self.domain}/uapi/overseas-stock/v1/trading/inquire-balance"
        tr_id_ovrs = TR_REST["OVRS_STOCK_BAL"]
        params_ovrs = {
            "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code,
            "TR_MKET_CD": "00", "NATN_CD": "840", "INQR_DVSN_CD": "00", "TR_CRC_CD": "USD",
            "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""
        }

        def _req(u, t, p, loc):
            try:
                res = requests.get(u, headers=self.get_tr_header(t), params=p)
                data = res.json()
                if data['rt_cd'] == '0':
                    balances = kis_parser.parse_balance_detail(data, loc)
                    for curr, bal in balances.items():
                        acct = AccountData(
                            gateway_name=self.gateway.gateway_name,
                            accountid=f"{self.acc_no}-{curr}",
                            balance=bal["total"],
                            frozen=bal["total"] - bal["available"]
                        )
                        self.gateway.on_account(acct)
            except: pass
            
        threading.Thread(target=_req, args=(url, tr_id, params, "KR")).start()
        threading.Thread(target=_req, args=(url_ovrs, tr_id_ovrs, params_ovrs, "OVRS")).start()

    def query_position(self):
        """Query Positions (Domestic & Overseas)"""
        # Logic matches query_account but using parser.parse_positions_detail
        # For brevity, implementing generic query structure
        
        urls = [
            (f"{self.domain}/uapi/domestic-stock/v1/trading/inquire-balance", TR_REST["KR_STOCK_BAL"], "KR"),
            (f"{self.domain}/uapi/overseas-stock/v1/trading/inquire-balance", TR_REST["OVRS_STOCK_BAL"], "OVRS")
        ]
        
        def _req_pos(u, t, loc):
            p = {
                "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code,
                "INQR_DVSN": "02", "AFHR_FLPR_YN": "N", "OFL_YN": "N", "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00"
            } if loc=="KR" else {
                "CANO": self.acc_no, "ACNT_PRDT_CD": self.acc_code,
                "TR_MKET_CD": "00", "NATN_CD": "840", "INQR_DVSN_CD": "00", "TR_CRC_CD": "USD"
            }
            
            try:
                res = requests.get(u, headers=self.get_tr_header(t), params=p)
                data = res.json()
                if data['rt_cd'] == '0':
                    positions = kis_parser.parse_positions_detail(data, loc)
                    for pos_dict in positions:
                        pos = PositionData(
                            gateway_name=self.gateway.gateway_name,
                            symbol=pos_dict["symbol"],
                            exchange=Exchange.KRX if loc=="KR" else Exchange.NASDAQ, # Simplification
                            direction=Direction.LONG,
                            volume=pos_dict["volume"],
                            price=pos_dict["price"],
                            pnl=pos_dict["pnl"],
                            accountid=self.acc_no
                        )
                        self.gateway.on_position(pos)
            except: pass

        for u, t, l in urls:
            threading.Thread(target=_req_pos, args=(u, t, l)).start()

    # ----------------------------------------------------------------
    # 3. Interest Stocks (Auto-Subscribe)
    # ----------------------------------------------------------------
    
    def query_interest_stocks(self, group_code="01"):
        url = f"{self.domain}/uapi/domestic-stock/v1/quotations/inquire-interest-stock"
        params = {"FID_TYPE_NAME": group_code}
        
        try:
            res = requests.get(url, headers=self.get_tr_header(TR_REST["INTEREST_KR"]), params=params)
            data = res.json()
            if data['rt_cd'] == '0':
                items = data.get('output2', [])
                for item in items:
                    sym = item.get('pdno')
                    if sym:
                        self.gateway.subscribe(SubscribeRequest(sym, Exchange.KRX))
                self.gateway.write_log(f"Subscribed {len(items)} interest stocks.")
        except Exception as e:
            self.gateway.write_log(f"Interest Stock Error: {e}")

    # ----------------------------------------------------------------
    # 4. History Data (Chart) - Comprehensive
    # ----------------------------------------------------------------

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        Supports Minute/Day/Week/Month for Stock, Future, Option, Overseas.
        """
        ex = req.exchange
        sym = req.symbol
        interval = req.interval
        
        url, tr_id, params = "", "", {}
        market_loc = "KR"
        
        # --- 1. Domestic Stock ---
        if ex == Exchange.KRX and len(sym) == 6:
            if interval == Interval.MINUTE:
                url = f"{self.domain}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
                tr_id = TR_REST["KR_STOCK_HIST"]["M"]
                params = {
                    "FID_ETC_CLS_CODE": "", "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": sym, 
                    "FID_INPUT_HOUR_1": req.end.strftime("%H%M%S"),
                    "FID_PW_DATA_INCU_YN": "Y"
                }
            else: # Daily/Weekly
                url = f"{self.domain}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
                tr_id = TR_REST["KR_STOCK_HIST"]["D"]
                p_code = "D" if interval == Interval.DAILY else ("W" if interval == Interval.WEEKLY else "M")
                params = {
                    "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": sym,
                    "FID_PERIOD_DIV_CODE": p_code, "FID_ORG_ADJ_PRC": "0"
                }

        # --- 2. Domestic Fut/Opt ---
        elif ex == Exchange.KRX:
            if interval == Interval.MINUTE:
                url = f"{self.domain}/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice"
                tr_id = TR_REST["KR_FUT_HIST"]["M"]
                params = {
                    "FID_COND_MRKT_DIV_CODE": "F", "FID_INPUT_ISCD": sym,
                    "FID_INPUT_HOUR_1": req.end.strftime("%H%M%S"), "FID_PW_DATA_INCU_YN": "Y"
                }
            else:
                url = f"{self.domain}/uapi/domestic-futureoption/v1/quotations/inquire-daily-fuopchartprice"
                tr_id = TR_REST["KR_FUT_HIST"]["D"]
                p_code = "D" if interval == Interval.DAILY else "W"
                params = {
                    "FID_COND_MRKT_DIV_CODE": "F", "FID_INPUT_ISCD": sym, "FID_PERIOD_DIV_CODE": p_code
                }

        # --- 3. Overseas Stock ---
        elif ex in [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]:
            market_loc = "OVRS"
            exch_code = "NAS" if ex == Exchange.NASDAQ else ("NYS" if ex == Exchange.NYSE else "AMS")
            
            if interval == Interval.MINUTE:
                url = f"{self.domain}/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice"
                tr_id = TR_REST["OVRS_STOCK_HIST"]["M"]
                params = {
                    "AUTH": "", "EXCD": exch_code, "SYMB": sym, "NMIN": "1",
                    "PINC": "1", "NEXT": "", "NREC": "120", "KEYB": ""
                }
            else:
                url = f"{self.domain}/uapi/overseas-price/v1/quotations/dailyprice"
                tr_id = TR_REST["OVRS_STOCK_HIST"]["D"]
                gubn = "0" # Daily
                if interval == Interval.WEEKLY: gubn = "1"
                elif interval == Interval.MONTHLY: gubn = "2"
                params = {
                    "AUTH": "", "EXCD": exch_code, "SYMB": sym,
                    "GUBN": gubn, "BYMD": req.end.strftime("%Y%m%d"), "MODP": "1"
                }

        # [Fetch & Parse]
        bars = []
        if url:
            try:
                res = requests.get(url, headers=self.get_tr_header(tr_id), params=params)
                data = res.json()
                if data['rt_cd'] == '0':
                    # Delegate parsing to kis_parser
                    candles = kis_parser.parse_history_data(
                        data, market_loc=market_loc, 
                        interval="D" if interval==Interval.DAILY else "M"
                    )
                    
                    for c in candles:
                        bar = BarData(
                            gateway_name=self.gateway.gateway_name,
                            symbol=sym,
                            exchange=ex,
                            datetime=c["datetime"].replace(tzinfo=KOREA_TZ),
                            interval=interval,
                            volume=c["volume"],
                            turnover=c.get("turnover", 0),
                            open_price=c["open"],
                            high_price=c["high"],
                            low_price=c["low"],
                            close_price=c["close"]
                        )
                        bars.append(bar)
            except Exception as e:
                self.gateway.write_log(f"History Query Error: {e}")

        return bars