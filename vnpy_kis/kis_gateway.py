# kis_gateway.py
"""
KIS Gateway for Vn.py (Refactored & Integrated)
- Integrates: KisAuthManager, KisApiHelper, KisParser, KisDatafeed
- Features: Multi-Account Support, Shared Rate Limiting, Centralized Config
"""

import time
import threading
import json
import traceback
import requests
from datetime import datetime
from copy import copy
from zoneinfo import ZoneInfo
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict

from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData, OrderData, TradeData, PositionData, AccountData,
    ContractData, OrderRequest, CancelRequest, SubscribeRequest,
    HistoryRequest, BarData, QuoteData, QuoteRequest
)
from vnpy.trader.constant import (
    Exchange, Product, OrderType, Direction, Status, Interval, Offset
)
from vnpy.trader.utility import get_folder_path
from vnpy_rest.rest_client import RestClient, Request
from vnpy_websocket.websocket_client import WebsocketClient

# --- Local Modules Import ---
try:
    from .kis_auth import kis_auth  # Singleton Instance
    from .kis_parser import (
        parse_ws_realtime, parse_ws_hoka, parse_ws_notice,
        parse_order_response, parse_cancel_response,
        parse_balance, parse_position
    )
    from .kis_api_helper import KisApiHelper, AssetType
    from .kis_datafeed import KisDatafeed
except ImportError:
    from vnpy_kis.kis_auth import kis_auth
    from vnpy_kis.kis_parser import (
        parse_ws_realtime, parse_ws_hoka, parse_ws_notice,
        parse_order_response, parse_cancel_response,
        parse_balance, parse_position
    )
    from vnpy_kis.kis_api_helper import KisApiHelper, AssetType
    from vnpy_kis.kis_datafeed import KisDatafeed

# =============================================================================
# --- Constants ---
# =============================================================================
KOREA_TZ = ZoneInfo("Asia/Seoul")

# WebSocket TR IDs (Subscription Only)
# REST TR IDs are now managed by KisApiHelper
TR_WS = {
    "KR_STOCK": "H0STCNT0", "KR_STOCK_HOKA": "H0STASP0",
    "KR_FUT": "H0IFCNT0", "KR_FUT_HOKA": "H0IFASP0",
    "KR_OPT": "H0IOCNT0", "KR_OPT_HOKA": "H0IOASP0",
    "KR_BOND": "H0BJCNT0", "KR_INDEX": "H0UPCNT0",
    "OVRS_STOCK": "HDFSCNT0", "OVRS_STOCK_HOKA": "HDFSASP0",
    "OVRS_FUT": "HDFFF020", "OVRS_FUT_HOKA": "HDFFF010",
    "NIGHT_FUT": "ECEUCNT0", 
    "NOTICE_KR_STOCK_REAL": "H0STCNI0", "NOTICE_KR_FUT_REAL": "H0IFCNI0",
    "NOTICE_KR_STOCK_DEMO": "H0STCNI9", "NOTICE_KR_FUT_DEMO": "H0IFCNI9",
}

KR_EXCHANGES = [Exchange.KRX, Exchange.SOR]
OVRS_STOCK_EXCHANGES = KisApiHelper.OVERSEAS_STOCK_EXCHANGES
OVRS_FUTOPT_EXCHANGES = KisApiHelper.OVERSEAS_FUTOPT_EXCHANGES

# =============================================================================
# [CORE] KisGlobalSession (RestClient + WebsocketClient)
# =============================================================================
class KisGlobalSession(RestClient, WebsocketClient):
    """
    Combined Client for KIS API.
    Handles Transport Layer (REST/WS) and delegates Auth/Parsing.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(KisGlobalSession, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized: return
        
        RestClient.__init__(self)
        WebsocketClient.__init__(self)

        # Config
        self.app_key = ""
        self.app_secret = ""
        self.server = "REAL"
        self.user_id = "" 
        self.approval_key = ""
        
        # WS Security
        self.aes_keys = {} 
        
        # Routing
        self.tick_subscribers: Dict[str, List['KisBaseGateway']] = defaultdict(list)
        self.account_map: Dict[str, List['KisBaseGateway']] = defaultdict(list)        
        self.active_gateways: Set['KisBaseGateway'] = set() 
        self.subscribed_symbols = set()
        self.notice_subscribed = False

        self._initialized = True

    def log(self, msg: str):
        """Broadcast logs to all active gateways."""
        for gateway in self.active_gateways:
            gateway.write_log(f"[Session] {msg}")

    def init_config(self, app_key, app_secret, server, user_id):
        with self._lock:
            if not self.app_key:
                self.app_key = app_key
                self.app_secret = app_secret
                self.server = server
                self.user_id = user_id
                
                if server == "DEMO":
                    self.init(url_base="https://openapivts.koreainvestment.com:29443")
                    self.ws_host = "ws://ops.koreainvestment.com:31000"
                else:
                    self.init(url_base="https://openapi.koreainvestment.com:9443")
                    self.ws_host = "ws://ops.koreainvestment.com:21000"

    def register_gateway(self, gateway: 'KisBaseGateway', account_no: str):
        with self._lock:
            if gateway not in self.account_map[account_no]:
                self.account_map[account_no].append(gateway)
                
            self.active_gateways.add(gateway)
            if not self.active:
                self.start()

    def unregister_gateway(self, gateway: 'KisBaseGateway'):
        with self._lock:
            if gateway in self.active_gateways:
                self.active_gateways.remove(gateway)
            
            for acc in list(self.account_map.keys()):
                if gateway in self.account_map[acc]:
                    self.account_map[acc].remove(gateway)
                    if not self.account_map[acc]:
                        del self.account_map[acc]
                                    
            if not self.active_gateways:
                self.log("No active gateways. Stopping Session...")
                self.stop()

    def start(self):
        if self.active: return
        RestClient.start(self, n=10)
        
        # Approval Key for WebSocket
        self.approval_key = self._get_approval_key()
        if self.approval_key:
            WebsocketClient.init(self, host=self.ws_host, ping_interval=100)
            WebsocketClient.start(self)
        else:
            self.log("Failed to get Approval Key. WS not started.")

    def stop(self):
        if self.active:
            self.active = False
            WebsocketClient.stop(self)
            RestClient.stop(self)
            if hasattr(self, "pool") and self.pool:
                try:
                    self.pool.shutdown(wait=False)
                    self.pool = None
                except Exception: pass
            self.log("Session Stopped.")

    # -------------------------------------------------------------------------
    # REST API Overrides (Using KisAuthManager)
    # -------------------------------------------------------------------------
    def sign(self, request: Request) -> Request:
        # 1. Rate Limit Check (Shared)
        kis_auth.check_rate_limit(self.app_key)

        # 2. Extract Config
        tr_id = ""
        if request.extra:
            tr_id = request.extra.get("tr_id", "")
        if not tr_id and request.headers:
            tr_id = request.headers.get("tr_id", "")

        # 3. Generate Header (Delegated to KisAuthManager)
        #    Allows shared token management and HashKey generation
        body_data = None
        if request.method == "POST" and request.data:
            body_data = request.data

        request.headers = kis_auth.get_header(
            tr_id=tr_id,
            app_key=self.app_key,
            app_secret=self.app_secret,
            server=self.server,
            body_data=body_data
        )
        
        # 4. JSON Serialization
        if request.method == "POST" and request.data and isinstance(request.data, dict):
            request.data = json.dumps(request.data)

        return request

    def _get_approval_key(self):
        """Websocket Key is separate from REST Token"""
        url = "/oauth2/Approval"
        req = {"grant_type": "client_credentials", "appkey": self.app_key, "secretkey": self.app_secret}
        try:
            full_url = self.url_base + url
            res = requests.post(full_url, json=req, timeout=10)
            return res.json().get("approval_key", "")
        except Exception as e:
            self.log(f"Approval Key Error: {e}")
            return ""

    # -------------------------------------------------------------------------
    # WebSocket Overrides (Using KisParser)
    # -------------------------------------------------------------------------
    def unpack_data(self, data: str):
        if data.startswith('{'):
            return {"type": "json", "payload": json.loads(data)}
        elif data[0] in ['0', '1']:
            return {"type": "raw", "payload": data}
        return {"type": "unknown", "payload": data}

    def on_connected(self):
        self.log("WebSocket Connected")
        if self.user_id:
            self._subscribe_private_notices()

    def on_packet(self, packet: dict):
        if packet["type"] == "raw":
            data = packet["payload"]
            is_market = data.startswith('0')
            is_notice = data.startswith('1')
            
            if is_market or is_notice:
                parts = data.split('|')
                if len(parts) >= 4:
                    tr_id = parts[1]
                    body = parts[3]
                    if is_market:
                        self._handle_market_data(tr_id, body)
                    else:   
                        self._handle_notice_data(tr_id, body)
                        
        elif packet["type"] == "json":
            js = packet["payload"]
            header = js.get("header", {})
            tr_id = header.get("tr_id")
            
            if tr_id == "PINGPONG":
                self.send_packet(packet["payload"])
            elif tr_id and "body" in js:
                if "output" in js["body"]:
                    self.aes_keys[tr_id] = js["body"]["output"]
                    self.log(f"AES Key Received for {tr_id}")

    def _handle_market_data(self, tr_id, body):
        # Delegate parsing to KisParser
        parsed = parse_ws_realtime(tr_id, body)
        
        if parsed.get("valid"):
            symbol = parsed.get("code")
            # Tick Data
            if "CNT" in tr_id or "HDFFF020" in tr_id:
                for gateway in self.tick_subscribers.get(symbol, []):
                    gateway.on_ws_tick(parsed)
            # Depth Data (Hoka)
            elif "ASP" in tr_id or "HDFFF010" in tr_id:
                # KisParser separates Hoka parsing
                parsed_depth = parse_ws_hoka(tr_id, body)
                for gateway in self.tick_subscribers.get(symbol, []):
                    gateway.on_ws_depth(parsed_depth)

    def _handle_notice_data(self, tr_id, body):
        if tr_id not in self.aes_keys: return
        key_info = self.aes_keys[tr_id]
        
        # Delegate decryption and parsing to KisParser
        parsed = parse_ws_notice(tr_id, body, key_info["key"], key_info["iv"])
        
        if parsed and parsed.get("valid"):
            account_no = parsed.get("account")
            gateways = self.account_map.get(account_no, [])
            
            if not gateways: 
                return

            # Determine Market for Routing
            target_market = "KR"
            if tr_id in ["H0GSCNI0", "H0GSCNI9"]:
                target_market = "OVRS"
            
            # Route to appropriate gateway
            for gw in gateways:
                if target_market in gw.market_loc:
                    gw.on_ws_notice(parsed)

    # --- WS Subscription ---
    def subscribe_symbol(self, gateway, symbol, tr_id, tr_key, depth_tr_id=None):
        with self._lock:
            if gateway not in self.tick_subscribers[symbol]:
                self.tick_subscribers[symbol].append(gateway)
            
            if symbol in self.subscribed_symbols: return

            self._send_ws_request(tr_id, tr_key)
            if depth_tr_id and gateway.depth_mode:
                self._send_ws_request(depth_tr_id, tr_key)
            
            self.subscribed_symbols.add(symbol)

    def _subscribe_private_notices(self):
        if not self.user_id: return
        trs = [TR_WS["NOTICE_KR_STOCK_DEMO"], TR_WS["NOTICE_KR_FUT_DEMO"]] if self.server == "DEMO" else [TR_WS["NOTICE_KR_STOCK_REAL"], TR_WS["NOTICE_KR_FUT_REAL"]]
        for tr_id in trs:
            self._send_ws_request(tr_id, self.user_id)
        self.notice_subscribed = True
        self.log(f"Private Notices Subscribed for {self.user_id}")
        
    def _send_ws_request(self, tr_id, tr_key):
        payload = {
            "header": {"approval_key": self.approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
            "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}}
        }
        self.send_packet(payload)

# =============================================================================
# [HELPER] KisTdApi (Logic Layer)
# =============================================================================
class KisTdApi:
    """
    Business Logic Layer.
    Uses KisGlobalSession for IO and KisDatafeed for History.
    Uses KisApiHelper for Configuration.
    """
    def __init__(self, gateway: 'KisBaseGateway'):
        self.gateway = gateway
        self.session = KisGlobalSession()
        
        # [NEW] Integrate Datafeed for History
        self.datafeed = KisDatafeed()
        
        self.quote_count = 0
        self.quote_map: Dict[str, tuple[str, str]] = {} 
        self.interest_file = get_folder_path("kis_gateway") / "interest_list.json"
        self.interest_list: Set[str] = set()

    def connect(self):
        # Init Session
        self.session.init_config(
            self.gateway.app_key, self.gateway.app_secret, self.gateway.server, self.gateway.user_id
        )
        self.session.register_gateway(self.gateway, self.gateway.acc_no)
        self.gateway.write_log(f"Connected to Session ({self.gateway.server})")
        
        # Load Interests
        self._load_interest_list()
        self._init_interest_contracts()
        self._auto_subscribe_interest()

    def close(self):
        self.session.unregister_gateway(self.gateway)

    # --- History Query (Delegated to KisDatafeed) ---
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        [IMPROVED] Delegate history query to Datafeed module.
        Datafeed handles pagination, rate limits, and parsing independently.
        """
        if not self.datafeed:
            self.gateway.write_log("Datafeed module not initialized.")
            return []
            
        return self.datafeed.query_bar_history(req)

    # --- Interest List Management ---
    def _load_interest_list(self):
        if not self.interest_file.exists(): return
        try:
            with open(self.interest_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                for full_symbol in data:
                    parts = full_symbol.split('.')
                    if len(parts) == 2:
                        sym, exch_str = parts[0], parts[1]
                        if Exchange(exch_str) in self.gateway.exchanges:
                            self.interest_list.add(full_symbol)
        except Exception: pass

    def save_interest_list(self):
        current_data = set()
        if self.interest_file.exists():
            try:
                with open(self.interest_file, "r", encoding="utf-8") as f:
                    current_data = set(json.load(f))
            except: pass
        current_data.update(self.interest_list)
        try:
            with open(self.interest_file, "w", encoding="utf-8") as f:
                json.dump(list(current_data), f, indent=2)
        except: pass

    def add_interest(self, req: SubscribeRequest):
        full_symbol = f"{req.symbol}.{req.exchange.value}"
        if full_symbol not in self.interest_list:
            self.interest_list.add(full_symbol)
            self.save_interest_list()

    def _init_interest_contracts(self):
        for full_symbol in self.interest_list:
            try:
                sym, exch_str = full_symbol.split('.')
                self.gateway.ensure_contract(sym, Exchange(exch_str))
            except Exception: continue

    def _auto_subscribe_interest(self):
        for full_symbol in self.interest_list:
            try:
                sym, exch_str = full_symbol.split('.')
                req = SubscribeRequest(symbol=sym, exchange=Exchange(exch_str))
                self.gateway.subscribe(req)
                time.sleep(0.02)
            except Exception: continue

    # --- Contract Info Query ---
    def query_contract(self, symbol: str, exchange: Exchange) -> Optional[ContractData]:
        asset_type = KisApiHelper.get_asset_type(exchange, symbol)
        config = KisApiHelper.get_tr_config(asset_type, "QUOTE", self.gateway.server=="DEMO")
        
        if not config: return None

        # Build Params for Quote
        # Note: KisApiHelper.build_history_params is available, but Quote params are simple
        params = {}
        if asset_type == AssetType.KR_STOCK:
            params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        elif asset_type == AssetType.OS_STOCK:
            excd = KisApiHelper.get_kis_exchange_code(exchange, is_order=False)
            params = {"AUTH": "", "EXCD": excd, "SYMB": symbol}
        # Add other assets if needed...

        try:
            resp = self.session.request(
                method="GET", path=config['url'], params=params, 
                headers={"tr_id": config['tr_id']} 
            )
            
            if resp.status_code == 200:
                data = resp.json()
                output = data.get("output", {})
                
                name = symbol
                close_price = 0.0
                
                if asset_type == AssetType.KR_STOCK:
                    name = output.get("hts_kor_isnm", symbol)
                    close_price = float(output.get("stck_prpr", "0"))
                elif asset_type == AssetType.OS_STOCK:
                    name = output.get("prdt_name", output.get("name", symbol))
                    close_price = float(output.get("last", "0"))

                pricetick = self._get_pricetick(close_price, exchange)
                
                contract = ContractData(
                    gateway_name=self.gateway.gateway_name,
                    symbol=symbol,
                    exchange=exchange,
                    name=name,
                    product=Product.EQUITY, 
                    size=1,
                    pricetick=pricetick,
                    history_data=True
                )
                self.gateway.on_contract(contract)
                return contract
        except Exception as e:
            self.gateway.write_log(f"Query Contract Failed: {symbol} {e}")
        return None

    def _get_pricetick(self, price, exchange):
        if exchange in [Exchange.KRX, Exchange.SOR]:
            if price < 2000: return 1
            if price < 5000: return 5
            if price < 20000: return 10
            if price < 50000: return 50
            if price < 200000: return 100
            if price < 500000: return 500
            return 1000
        elif exchange in [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]:
            if price < 1.0: return 0.0001
            return 0.01
        return 1

    # --- Order Management ---
    def send_order(self, req: OrderRequest) -> str:
        # 1. Asset & Config
        asset_type = KisApiHelper.get_asset_type(req.exchange, req.symbol)
        is_vts = (self.gateway.server == "DEMO")
        
        action = "ORDER_BUY" if req.direction == Direction.LONG else "ORDER_SELL"
        config = KisApiHelper.get_tr_config(asset_type, action, is_vts)
        
        if not config: 
            self.gateway.write_log(f"Order Config Not Found: {asset_type} {action}")
            return ""

        # 2. Build Params
        params = KisApiHelper.build_order_params(req, asset_type, self.gateway.acc_no)

        try:
            # 3. Request
            resp = self.session.request(
                method="POST",
                path=config['url'],
                data=params,
                headers={"tr_id": config['tr_id']}
            )
            
            # 4. Parse Response (KisParser)
            parsed = parse_order_response(resp.json())
            
            if parsed.get("odno"):
                order = req.create_order_data(parsed["odno"], self.gateway.gateway_name)
                order.status = Status.NOTTRADED
                self.gateway.on_order(order)
                return order.vt_orderid
            else:
                self.gateway.write_log(f"Order Reject: {parsed.get('msg', resp.text)}")
                return ""
        except Exception as e:
            self.gateway.write_log(f"Send Order Error: {e}")
            return ""

    def cancel_order(self, req: CancelRequest):
        asset_type = KisApiHelper.get_asset_type(req.exchange, req.symbol)
        is_vts = (self.gateway.server == "DEMO")
        
        config = KisApiHelper.get_tr_config(asset_type, "ORDER_CANCEL", is_vts)
        if not config: return
        
        params = KisApiHelper.build_cancel_params(req, asset_type, self.gateway.acc_no)
        
        self.session.add_request(
            method="POST",
            path=config['url'],
            data=params,
            extra={"tr_id": config['tr_id']},
            callback=self.on_cancel_order_return
        )
        
    def on_cancel_order_return(self, data: dict, request: Request):
        parsed = parse_cancel_response(data)
        if parsed.get("status") == "0":
            self.gateway.write_log(f"Cancel Accepted: {parsed.get('odno')}")
        else:
            self.gateway.write_log(f"Cancel Failed: {parsed.get('msg')}")

    # --- Quote (Spread) Support ---
    def send_quote(self, req: QuoteRequest) -> str:
        self.quote_count += 1
        quote_id = str(self.quote_count)
        
        buy_req = req.create_order_request(Direction.LONG, Offset.OPEN, req.bid_price, req.bid_volume)
        sell_req = req.create_order_request(Direction.SHORT, Offset.CLOSE, req.ask_price, req.ask_volume)
        
        vt_buy = self.send_order(buy_req)
        vt_sell = self.send_order(sell_req)
        
        if vt_buy or vt_sell:
            quote = QuoteData(
                gateway_name=self.gateway.gateway_name, symbol=req.symbol, exchange=req.exchange,
                quoteid=quote_id, bid_price=req.bid_price, bid_volume=req.bid_volume,
                ask_price=req.ask_price, ask_volume=req.ask_volume,
                bid_offset=Offset.OPEN, ask_offset=Offset.CLOSE, status=Status.NOTTRADED, datetime=datetime.now(KOREA_TZ)
            )
            self.gateway.on_quote(quote)
            self.quote_map[quote_id] = (vt_buy.split(".")[-1] if vt_buy else "", vt_sell.split(".")[-1] if vt_sell else "")
            return f"{self.gateway.gateway_name}.{quote_id}"
        return ""

    def cancel_quote(self, req: CancelRequest):
        quote_id = req.orderid
        child = self.quote_map.get(quote_id)
        if not child: return
        for odno in child:
            if odno:
                c_req = copy(req)
                c_req.orderid = odno
                self.cancel_order(c_req)

    # --- Account & Position ---
    def query_account(self):
        # Determine which assets to query based on gateway configuration
        assets = []
        if "KR" in self.gateway.market_loc:
            if "SPOT" in self.gateway.market_type: assets.append(AssetType.KR_STOCK)
            if "FUTOPT" in self.gateway.market_type: assets.append(AssetType.KR_FUTOPT)
            if "BOND" in self.gateway.market_type: assets.append(AssetType.KR_BOND)
        if "OVRS" in self.gateway.market_loc:
            if "SPOT" in self.gateway.market_type: assets.append(AssetType.OS_STOCK)
            if "FUTOPT" in self.gateway.market_type: assets.append(AssetType.OS_FUTOPT)
            
        for asset in assets:
            self._async_query_balance(asset)

    def query_position(self):
        self.query_account() 

    def _async_query_balance(self, asset_type_str):
        # Map string to AssetType if needed, or use directly
        # Here we use the AssetType constants from helper
        is_vts = (self.gateway.server == "DEMO")
        config = KisApiHelper.get_tr_config(asset_type_str, "BALANCE", is_vts)
        
        if not config: return

        params = KisApiHelper.build_balance_params(asset_type_str, self.gateway.acc_no)

        self.session.add_request(
            method="GET",
            path=config['url'],
            params=params,
            extra={"tr_id": config['tr_id'], "asset_type": asset_type_str},
            callback=self.on_balance_return
        )

    def on_balance_return(self, data: dict, request: Request):
        if not data: return
        asset_type = request.extra.get("asset_type")
        
        # Delegate Parsing
        parsed_bal = parse_balance(data, asset_type)
        
        account = AccountData(
            gateway_name=self.gateway.gateway_name, 
            accountid=f"{self.gateway.acc_no}-{asset_type}", 
            balance=parsed_bal["balance"], 
            frozen=parsed_bal["balance"] - parsed_bal["available"]
        )
        self.gateway.on_account(account)
        
        # Position Parsing
        exch_map = {
            AssetType.KR_STOCK: Exchange.KRX, AssetType.KR_FUTOPT: Exchange.KRX, 
            AssetType.OS_STOCK: Exchange.NASDAQ, AssetType.OS_FUTOPT: Exchange.CME
        }
        
        parsed_pos = parse_position(data, parser_type)
        for pos in parsed_pos:
            direction = Direction.LONG if pos.get("direction") == "long" else Direction.SHORT
            position = PositionData(
                gateway_name=self.gateway.gateway_name, 
                symbol=pos["symbol"], 
                exchange=exch_map.get(asset_type, Exchange.KRX), 
                direction=direction, 
                volume=pos["quantity"], 
                price=pos["price"], 
                pnl=pos["pnl"]
            )
            self.gateway.on_position(position)


# =============================================================================
# [GATEWAY] KisBaseGateway & Concrete Implementations
# =============================================================================
class KisBaseGateway(BaseGateway):
    """
    Abstract Base Gateway
    """
    default_setting = {
        "usr_id": "", "app_key": "", "app_secret": "", "account_no": "", "account_code": "01", "server": ["REAL", "DEMO"], 
        "interest_group": [AssetType.KR_STOCK, AssetType.OS_STOCK, AssetType.KR_FUTOPT, AssetType.OS_FUTOPT, AssetType.ISA]
    }
    default_name = "KIS_BASE"
    exchanges = [Exchange.KRX, Exchange.SOR, Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX, Exchange.CME, Exchange.EUREX]
    
    def __init__(self, event_engine: EventEngine, gateway_name: str):
        super().__init__(event_engine, gateway_name)
        
        self.api = KisTdApi(self) 
        self.ticks: Dict[str, TickData] = {}
        
        # Config (Overridden by Subclasses)
        self.market_loc = ["KR","OVRS"]
        self.market_type = ["SPOT","FUTOPT"]
        self.products = []
        self.exchanges = []
        self.depth_mode = True

        # Config Values
        self.app_key = ""
        self.app_secret = ""
        self.acc_no = ""
        self.acc_code = "01"
        self.server = "REAL"
        self.user_id = ""

    def connect(self, setting: dict):
        self.user_id = setting["usr_id"]
        self.app_key = setting["app_key"]
        self.app_secret = setting["app_secret"]
        self.acc_no = setting["account_no"]
        self.acc_code = setting["account_code"]
        self.server = setting["server"]
        
        self.api.connect()
        self.api.query_account()
        self.api.query_position()
        self.write_log(f"Gateway {self.gateway_name} Connected")

    def subscribe(self, req: SubscribeRequest):
        contract = self.api.query_contract(req.symbol, req.exchange)
        if not contract:
            self.ensure_contract(req.symbol, req.exchange)
        
        self.api.add_interest(req)

        tr_id, tr_key = self.get_ws_tr_info(req)
        depth_tr = self.get_ws_depth_tr(req)
        if tr_id:
            self.api.session.subscribe_symbol(self, req.symbol, tr_id, tr_key, depth_tr)

    def ensure_contract(self, symbol: str, exchange: Exchange):
        contract = ContractData(
            gateway_name=self.gateway_name,
            symbol=symbol,
            exchange=exchange,
            name=symbol,
            product=Product.EQUITY, 
            size=1,
            pricetick=0.01 if "OVRS" in self.market_loc else 1,
            history_data=True
        )
        self.on_contract(contract)

    def send_order(self, req: OrderRequest):
        return self.api.send_order(req)
    
    def cancel_order(self, req: CancelRequest):
        self.api.cancel_order(req)

    def send_quote(self, req: QuoteRequest):
        return self.api.send_quote(req)
    
    def cancel_quote(self, req: CancelRequest):
        self.api.cancel_quote(req)

    def query_account(self):
        self.api.query_account()

    def query_position(self):
        self.api.query_position()

    def query_history(self, req: HistoryRequest):
        return self.api.query_history(req)

    def close(self):
        self.api.close()

    # --- Callbacks ---
    def on_ws_tick(self, data: dict):
        symbol = data["code"]
        tick = self._get_tick(symbol)
        
        tick.last_price = data.get("price", tick.last_price)
        tick.volume = data.get("acc_volume", tick.volume)
        tick.turnover = data.get("turnover", tick.turnover)
        tick.open_price = data.get("open", tick.open_price)
        tick.high_price = data.get("high", tick.high_price)
        tick.low_price = data.get("low", tick.low_price)
        
        if "localtime" in data:
            try: tick.datetime = datetime.strptime(data["localtime"], "%Y%m%d %H%M%S").replace(tzinfo=KOREA_TZ)
            except: pass
        else:
            tick.datetime = datetime.now(KOREA_TZ)
            
        self.on_tick(copy(tick))

    def on_ws_depth(self, data: dict):
        symbol = data["code"]
        tick = self._get_tick(symbol)
        
        for i, (p, v) in enumerate(data["asks"]):
            if i>=5: break
            setattr(tick, f"ask_price_{i+1}", p)
            setattr(tick, f"ask_volume_{i+1}", v)
        for i, (p, v) in enumerate(data["bids"]):
            if i>=5: break
            setattr(tick, f"bid_price_{i+1}", p)
            setattr(tick, f"bid_volume_{i+1}", v)
        
        tick.datetime = datetime.now(KOREA_TZ)
        self.on_tick(copy(tick))

    def on_ws_notice(self, data: dict):
        """
        Callback from KisGlobalSession -> KisParser.
        Handles explicit status codes.
        """
        if not data.get("valid"): return

        direction = Direction.NET
        if data.get("direction") == "LONG": direction = Direction.LONG
        elif data.get("direction") == "SHORT": direction = Direction.SHORT
        
        order_type = OrderType.LIMIT
        if data.get("order_type") == "MARKET": order_type = OrderType.MARKET
        
        # Map String status from Parser to Enum
        status_str = data.get("order_status")
        status = Status.NOTTRADED
        if status_str == "ALLTRADED": status = Status.ALLTRADED
        elif status_str == "PARTTRADED": status = Status.PARTTRADED
        elif status_str == "CANCELLED": status = Status.CANCELLED
        elif status_str == "REJECTED": status = Status.REJECTED
        
        exchange = Exchange.KRX 
        if "OVRS" in self.market_loc and self.exchanges: 
            exchange = self.exchanges[0]
        
        order = OrderData(
            symbol=data["code"],
            exchange=exchange,
            orderid=data['order_no'],
            type=order_type,
            direction=direction,
            offset=Offset.NONE, 
            price=data.get("order_price", 0.0),
            volume=data.get("order_qty", 0) + data.get("filled_qty", 0) + data.get("unfilled_qty", 0),
            traded=data.get("filled_qty", 0) if status in [Status.ALLTRADED, Status.PARTTRADED] else 0,
            status=status,
            datetime=datetime.now(KOREA_TZ),
            gateway_name=self.gateway_name
        )
        self.on_order(order)
        
        if data.get("filled_qty", 0) > 0 and status in [Status.ALLTRADED, Status.PARTTRADED]:
            trade = TradeData(
                symbol=data["code"],
                exchange=exchange,
                orderid=data['order_no'],
                tradeid=f"{data['order_no']}-{int(time.time()*100000)}", 
                direction=direction,
                offset=Offset.NONE,
                price=data.get("filled_price", 0.0),
                volume=data.get("filled_qty", 0),
                datetime=datetime.now(KOREA_TZ),
                gateway_name=self.gateway_name
            )
            self.on_trade(trade)
            self.api.query_account()
            self.api.query_position()

    def _get_tick(self, symbol):
        if symbol not in self.ticks:
            tick = TickData(
                gateway_name=self.gateway_name, symbol=symbol,
                exchange=Exchange.KRX, datetime=datetime.now(KOREA_TZ)
            )
            self.ticks[symbol] = tick
        return self.ticks[symbol]

    def get_ws_tr_info(self, req):
        sym = req.symbol
        ex = req.exchange
        if ex in [Exchange.KRX, Exchange.SOR, Exchange.KOSDAQ]:
            if len(sym)==6 and sym.isdigit(): return TR_WS["KR_STOCK"], sym
            if sym.startswith("1") or sym.startswith("2"): return TR_WS["KR_FUT"], sym
            if sym.startswith("KR"): return TR_WS["KR_BOND"], sym
        if ex == Exchange.EUREX: return TR_WS["NIGHT_FUT"], sym
        if ex in [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]:
            mkt = "NAS" if ex==Exchange.NASDAQ else ("NYS" if ex==Exchange.NYSE else "AMS")
            return TR_WS["OVRS_STOCK"], f"D{mkt}{sym}"
        if ex in [Exchange.CME]: return TR_WS["OVRS_FUT"], sym
        return None, None

    def get_ws_depth_tr(self, req):
        sym = req.symbol
        ex = req.exchange
        if ex == Exchange.KRX:
            if len(sym)==6: return TR_WS["KR_STOCK_HOKA"]
            return TR_WS["KR_FUT_HOKA"]
        if ex in [Exchange.NASDAQ, Exchange.NYSE]: return TR_WS["OVRS_STOCK_HOKA"]
        if ex == Exchange.CME: return TR_WS["OVRS_FUT_HOKA"]
        return None

# =============================================================================
# Concrete Gateway Implementations
# =============================================================================
class KisKrStockDemoGateway(KisBaseGateway):
    default_setting = { "usr_id": "", "app_key": "","app_secret": "", "account_no": "", "account_code": "01", "server": ["DEMO"], "interest_group": AssetType.KR_STOCK }
    default_name = "KIS_KR_STOCK_DEMO"
    exchanges = KR_EXCHANGES
    def __init__(self, event_engine, gateway_name):
        super().__init__(event_engine, gateway_name)
        self.server = "DEMO"
        self.market_loc = ["KR"]
        self.market_type = ["SPOT"]
        self.products = [Product.EQUITY, Product.ETF]
        self.exchanges = KR_EXCHANGES

class KisKrStockGateway(KisBaseGateway):
    default_setting = { "usr_id": "", "app_key": "","app_secret": "", "account_no": "", "account_code": "01", "server": ["REAL"], "interest_group": AssetType.KR_STOCK }
    default_name = "KIS_KR_STOCK"
    exchanges = KR_EXCHANGES
    def __init__(self, event_engine, gateway_name):
        super().__init__(event_engine, gateway_name)
        self.market_loc = ["KR"]
        self.market_type = ["SPOT", "BOND"]
        self.products = [Product.EQUITY, Product.ETF, Product.BOND]
        self.exchanges = KR_EXCHANGES

class KisKrFutOptGateway(KisBaseGateway):
    default_setting = { "usr_id": "", "app_key": "","app_secret": "", "account_no": "", "account_code": "01", "server": ["REAL"], "interest_group": "KR_FUTOPT" }
    default_name = "KIS_KR_FUTOPT"
    exchanges = KR_EXCHANGES
    def __init__(self, event_engine, gateway_name):
        super().__init__(event_engine, gateway_name)
        self.market_loc = ["KR"]
        self.market_type = ["FUTOPT"]
        self.products = [Product.FUTURES, Product.OPTION]
        self.exchanges = KR_EXCHANGES

class KisOvrsStockGateway(KisBaseGateway):
    default_setting = { "usr_id": "", "app_key": "","app_secret": "", "account_no": "", "account_code": "01", "server": ["REAL"], "interest_group": "OVRS_STOCK" }
    default_name = "KIS_OVRS_STOCK"
    exchanges = OVRS_STOCK_EXCHANGES
    def __init__(self, event_engine, gateway_name):
        super().__init__(event_engine, gateway_name)
        self.market_loc = ["OVRS"]
        self.market_type = ["SPOT"]
        self.products = [Product.EQUITY, Product.ETF]
        self.exchanges = OVRS_STOCK_EXCHANGES

class KisOvrsFutOptGateway(KisBaseGateway):
    default_setting = { "usr_id": "", "app_key": "","app_secret": "", "account_no": "", "account_code": "01", "server": ["REAL"], "interest_group": "OVRS_FUTOPT" }
    default_name = "KIS_OVRS_FUTOPT"
    exchanges = OVRS_FUTOPT_EXCHANGES
    def __init__(self, event_engine, gateway_name):
        super().__init__(event_engine, gateway_name)
        self.market_loc = ["OVRS"]
        self.market_type = ["FUTOPT"]
        self.products = [Product.FUTURES, Product.OPTION]
        self.exchanges = OVRS_FUTOPT_EXCHANGES

class KisIsaGateway(KisKrStockGateway):
    default_setting = { "usr_id": "", "app_key": "","app_secret": "", "account_no": "", "account_code": "01", "server": ["REAL"], "interest_group": "ISA" }
    default_name = "KIS_ISA"