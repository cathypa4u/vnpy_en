"""
KIS Gateway for Vn.py (Final Corrected Version)
FileName: kis_gateway.py

Key Fixes:
1. US Tick Parsing (HDFSCNT0):
   - Shifted indices by +4 based on log analysis.
   - Price: 15, Volume: 19, OHL: 12/13/14, Time: 11.
2. US Depth Parsing (HDFSASP0):
   - Starts at index 5 (skipping Date/Time at 3/4).
   - Supports up to 10 levels.
3. Persistence & Options:
   - 'depth_mode' (default True), 'subscription_mode' (default 'ALL').
   - Saves/Loads Contracts & Subscriptions.
"""

import json
import time
import threading
from datetime import datetime, timedelta
from copy import copy
from typing import Dict, Set, List, Any
from zoneinfo import ZoneInfo
from pathlib import Path

import requests
import websocket

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
from vnpy.trader.event import EVENT_TIMER, EVENT_CONTRACT
from vnpy.trader.utility import get_file_path, save_json, load_json
from .kis_auth import kis_auth

# --- Constants ---
KOREA_TZ = ZoneInfo("Asia/Seoul")

DOMAIN_MAP = {
    "REAL": "https://openapi.koreainvestment.com:9443",
    "DEMO": "https://openapivts.koreainvestment.com:29443"
}

WS_DOMAIN_MAP = {
    "REAL": "ws://ops.koreainvestment.com:21000",
    "DEMO": "ws://ops.koreainvestment.com:31000"
}

TR_WS_ID = {
    "KR_STOCK": "H0STCNT0", "KR_NXT": "H0NXCNT0", "KR_SOR": "H0UNCNT0",
    "OVRS_STOCK": "HDFSCNT0", "KR_FUT": "H0IFCNT0", "OVRS_FUT": "HDFFF020",
    "KR_BOND": "H0BJCNT0",
    "KR_STOCK_DEPTH": "H0STASP0", "OVRS_STOCK_DEPTH": "HDFSASP0",
    "KR_FUT_DEPTH": "H0IFASP0", "OVRS_FUT_DEPTH": "HDFFF010"
}


# ------------------------------------------------------------------------------
# Shared WebSocket Manager
# ------------------------------------------------------------------------------
class KisWsManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(KisWsManager, cls).__new__(cls)
                    cls._instance.initialized = False
        return cls._instance

    def __init__(self):
        if self.initialized: return
        self.active = False
        self.ws = None
        self.thread = None
        self.app_key = ""
        self.app_secret = ""
        self.server = "REAL"
        self.ws_url = ""
        self.approval_key = ""
        
        self.routes: Dict[str, BaseGateway] = {}
        self.symbol_names: Dict[str, str] = {}
        self.ticks: Dict[str, TickData] = {}
        self.subscribed_symbols = set()
        
        self.depth_mode = True
        self.initialized = True

    def start(self, app_key, app_secret, server):
        with self._lock:
            if self.active: return 
            self.app_key = app_key
            self.app_secret = app_secret
            self.server = server
            self.ws_url = WS_DOMAIN_MAP[server]
            self._get_approval_key()
            self.active = True
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()

    def stop(self):
        self.active = False
        if self.ws: self.ws.close()

    def _get_approval_key(self):
        domain = DOMAIN_MAP[self.server]
        url = f"{domain}/oauth2/Approval"
        try:
            res = requests.post(url, json={
                "grant_type": "client_credentials", "appkey": self.app_key, "secretkey": self.app_secret
            })
            data = res.json()
            if "approval_key" in data: self.approval_key = data["approval_key"]
        except Exception: pass

    def register_route(self, symbol: str, gateway: BaseGateway):
        self.routes[symbol] = gateway

    def update_name(self, symbol: str, name: str):
        self.symbol_names[symbol] = name

    def subscribe(self, req: SubscribeRequest, gateway: BaseGateway, depth_mode: bool = True):
        symbol = req.symbol.strip()
        self.register_route(symbol, gateway)
        self.subscribed_symbols.add(symbol)
        self.depth_mode = depth_mode
        
        if self.active and self.ws and self.ws.sock and self.ws.sock.connected:
            self._send_sub_packet(symbol, req, gateway, depth_mode)

    def _send_sub_packet(self, symbol, req: SubscribeRequest, gateway: BaseGateway, depth_mode: bool):
        # 1. Basic Tick Subscription
        tr_id_tick, tr_key = "", symbol
        gw_type = getattr(gateway, "market_type", "SPOT")
        gw_loc = getattr(gateway, "market_loc", "KR")
        exchange = req.exchange

        if gw_loc == "KR":
            if gw_type == "SPOT":
                if exchange == Exchange.NXT: tr_id_tick = TR_WS_ID["KR_NXT"]
                elif exchange == Exchange.SOR: tr_id_tick = TR_WS_ID["KR_SOR"]
                else: tr_id_tick = TR_WS_ID["KR_STOCK"]
            elif gw_type == "FUTOPT": tr_id_tick = TR_WS_ID["KR_FUT"]
        else:
            if gw_type == "SPOT":
                tr_id_tick = TR_WS_ID["OVRS_STOCK"]
                mkt_prefix = "NAS"
                if exchange == Exchange.NYSE: mkt_prefix = "NYS"
                elif exchange == Exchange.AMEX: mkt_prefix = "AMS"
                elif exchange == Exchange.SEHK: mkt_prefix = "HKS"
                elif exchange == Exchange.TSE: mkt_prefix = "TSE"
                elif exchange == Exchange.SHFE: mkt_prefix = "SHS"
                elif exchange == Exchange.SZSE: mkt_prefix = "SZS"
                tr_key = f"D{mkt_prefix}{symbol}" 
            elif gw_type == "FUTOPT": tr_id_tick = TR_WS_ID["OVRS_FUT"]

        if tr_id_tick: self._send_request(tr_id_tick, tr_key)

        # 2. Rich Tick (Depth) Subscription
        if depth_mode:
            tr_id_depth = ""
            if gw_loc == "KR":
                if gw_type == "SPOT": tr_id_depth = TR_WS_ID["KR_STOCK_DEPTH"]
                elif gw_type == "FUTOPT": tr_id_depth = TR_WS_ID["KR_FUT_DEPTH"]
            else:
                if gw_type == "SPOT": tr_id_depth = TR_WS_ID["OVRS_STOCK_DEPTH"]
            
            if tr_id_depth: self._send_request(tr_id_depth, tr_key)

    def _send_request(self, tr_id, tr_key):
        req = {
            "header": { "approval_key": self.approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8" },
            "body": { "input": {"tr_id": tr_id, "tr_key": tr_key} }
        }
        try: self.ws.send(json.dumps(req))
        except Exception: pass

    def run(self):
        while self.active:
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url, on_open=self.on_open, on_message=self.on_message, 
                    on_error=self.on_error, on_close=self.on_close
                )
                self.ws.run_forever(ping_interval=100, ping_timeout=10)
            except Exception: time.sleep(1)

    def on_open(self, ws): pass 
    def on_error(self, ws, error): pass
    def on_close(self, ws, *args): pass

    def on_message(self, ws, data):
        try:
            if data.startswith('{') or data[0] not in ['0', '1']: return
            parts = data.split('|')
            if len(parts) < 4: return
            
            tr_id = parts[1]
            body = parts[3]
            
            if "CNT" in tr_id or tr_id == "HDFFF020":
                self.process_tick(tr_id, body)
            elif "ASP" in tr_id:
                self.process_depth(tr_id, body)
        except Exception: pass

    def get_tick(self, symbol, gateway):
        if symbol not in self.ticks:
            name = self.symbol_names.get(symbol, symbol)
            exch = Exchange.KRX
            if getattr(gateway, "market_loc", "KR") == "US": exch = Exchange.NASDAQ
            self.ticks[symbol] = TickData(
                gateway_name=gateway.gateway_name, symbol=symbol, exchange=exch,
                name=name, datetime=datetime.now(KOREA_TZ)
            )
        return self.ticks[symbol]

    def process_tick(self, tr_id, data):
        items = data.split('^')
        code = ""
        try:
            # 1. Domestic Stock (KR_STOCK)
            if tr_id in [TR_WS_ID["KR_STOCK"], TR_WS_ID["KR_NXT"], TR_WS_ID["KR_SOR"]]:
                code = items[0]
                gateway = self.routes.get(code)
                if not gateway: return
                tick = self.get_tick(code, gateway)

                # 실시간 거래정보
                tick.last_price = float(items[2])  # 현재가 [2]
                tick.open_price = float(items[7]) # 시가
                tick.high_price = float(items[8])   # 고가
                tick.low_price = float(items[9])    # 저가
                tick.volume = float(items[13]) # 누적거래량 [13]
                tick.turnover = float(items[14])  # 누적거래대금 [14]
                tick.open_interest = float(0)  # 미결제약정 (선물/옵션 전용)
                # tick.last_volume = float(items[12]) # 체결거래량
                # tick.pre_close = float(items[4])    # 전일종가
                # tick.limit_up = float(items[8])  # 상한가
                # tick.limit_down = float(items[8])  # 하한가                

                if not self.depth_mode:
                    tick.ask_price_1 = float(items[10])  # 매도1호가
                    tick.bid_price_1 = float(items[11])  # 매수1호가
                    tick.ask_volume_1 = float(items[36])  # 매도1잔량
                    tick.bid_volume_1 = float(items[37])  # 매수1잔량                       
                                
                tick.datetime = datetime.now(KOREA_TZ)
                gateway.on_tick(copy(tick))

            # 2. Overseas Stock (OVRS_STOCK) - HDFSCNT0
            elif tr_id == TR_WS_ID["OVRS_STOCK"]:
                code = items[1]
                if len(code) > 4: code = code[4:] 
                gateway = self.routes.get(code)
                if not gateway: return
                tick = self.get_tick(code, gateway)
                
                if len(items) > 19:
                    # 실시간 거래정보
                    tick.last_price = float(items[11])  # 현재가 [2]
                    tick.open_price = float(items[8]) # 시가
                    tick.high_price = float(items[9])   # 고가
                    tick.low_price = float(items[10])    # 저가
                    tick.volume = float(items[20]) # 누적거래량 [13]
                    tick.turnover = float(items[21])  # 누적거래대금 [14]
                    tick.open_interest = float(0)  # 미결제약정 (선물/옵션 전용)
                    # tick.last_volume = float(items[12]) # 체결거래량
                    # tick.pre_close = float(items[4])    # 전일종가
                    # tick.limit_up = float(items[8])  # 상한가
                    # tick.limit_down = float(items[8])  # 하한가                
                    
                    if not self.depth_mode:
                        tick.ask_price_1 = float(items[16])  # 매도1호가
                        tick.bid_price_1 = float(items[15])  # 매수1호가
                        tick.ask_volume_1 = float(items[18])  # 매도1잔량
                        tick.bid_volume_1 = float(items[17])  # 매수1잔량
                        
                tick.localtime = None # (items[4], items[5]) # (현지일자, 현지시간)
                
                tick.datetime = datetime.now(KOREA_TZ)
                gateway.on_tick(copy(tick))

            # 3. Domestic Futures (KR_FUT)
            elif tr_id == TR_WS_ID["KR_FUT"]:
                code = items[0]
                gateway = self.routes.get(code)
                if not gateway: return
                tick = self.get_tick(code, gateway)
                tick.last_price = float(items[2])
                tick.volume = float(items[12])
                tick.datetime = datetime.now(KOREA_TZ)
                gateway.on_tick(copy(tick))

            # 4. Overseas Futures (OVRS_FUT)
            elif tr_id == TR_WS_ID["OVRS_FUT"]:
                code = items[0]
                gateway = self.routes.get(code)
                if not gateway: return
                tick = self.get_tick(code, gateway)
                tick.last_price = float(items[10])
                tick.volume = float(items[12])
                tick.datetime = datetime.now(KOREA_TZ)
                gateway.on_tick(copy(tick))

        except Exception: pass

    def process_depth(self, tr_id, data):
        items = data.split('^')
        code = ""
        try:
            # 1. Domestic Stock Depth (KR_STOCK_DEPTH)
            if tr_id == TR_WS_ID["KR_STOCK_DEPTH"]:
                code = items[0]
                gateway = self.routes.get(code)
                if not gateway: return
                tick = self.get_tick(code, gateway)
                tick.datetime = datetime.now(KOREA_TZ)
                
                # Vertical: Ask(3~7), Bid(13~17)
                for i in range(5):
                    tick.__setattr__(f"ask_price_{i+1}", float(items[3 + i]))
                    tick.__setattr__(f"bid_price_{i+1}", float(items[13 + i]))
                    tick.__setattr__(f"ask_volume_{i+1}", float(items[23 + i]))
                    tick.__setattr__(f"bid_volume_{i+1}", float(items[33 + i]))
                gateway.on_tick(copy(tick))

            # 2. Overseas Stock Depth (OVRS_STOCK_DEPTH)
            elif tr_id == TR_WS_ID["OVRS_STOCK_DEPTH"]:
                # Log: items[3]=Date, items[4]=Time
                # Inferred Start: Index 5
                # Structure: Ask1(5), Bid1(6), AskVol1(7), BidVol1(8) ...
                code = items[1]
                if len(code) > 4: code = code[4:]
                gateway = self.routes.get(code)
                if not gateway: return
                tick = self.get_tick(code, gateway)
                tick.datetime = datetime.now(KOREA_TZ)
                
                # Loop up to 10 levels (if available)
                for i in range(5):
                    base = 11 + (i * 6) 
                    if base + 3 < len(items):
                        tick.__setattr__(f"bid_price_{i+1}", float(items[base]))     # Bid
                        tick.__setattr__(f"ask_price_{i+1}", float(items[base+1]))   # Ask
                        tick.__setattr__(f"bid_volume_{i+1}", float(items[base+2]))  # Bid Vol
                        tick.__setattr__(f"ask_volume_{i+1}", float(items[base+3]))  # Ask Vol
                
                gateway.on_tick(copy(tick))
        except Exception: pass

kis_ws_manager = KisWsManager()


# ------------------------------------------------------------------------------
# Base Gateway
# ------------------------------------------------------------------------------
class KisBaseGateway(BaseGateway):
    default_setting = {
        "app_key": "", "app_secret": "", "account_no": "", "account_code": "01",
        "subscription_limit": 50, "depth_mode": True, "subscription_mode": "ALL"
    }
    market_loc = "KR"
    market_type = "SPOT"
    server = "REAL"
    products = []
    exchanges = []

    def __init__(self, event_engine: EventEngine, gateway_name: str):
        super().__init__(event_engine, gateway_name)
        self.td_api = KisTdApi(self)
        
        self.sub_file = get_file_path(f"kis_sub_{self.gateway_name}.json")
        self.contract_file = get_file_path(f"kis_contract_{self.gateway_name}.json")
        self.saved_subscriptions: Set[str] = set()
        self.contracts: Dict[str, ContractData] = {}
        
        self.app_key = ""
        self.app_secret = ""
        self.depth_mode = True
        self.sub_mode = "ALL"

    def connect(self, setting: dict):
        self.app_key = setting["app_key"]
        self.app_secret = setting["app_secret"]
        self.depth_mode = setting.get("depth_mode", True)
        self.sub_mode = setting.get("subscription_mode", "ALL")
        acc_no = setting["account_no"]
        acc_code = setting["account_code"]

        self.load_contract_data()
        
        self.td_api.connect(
            self.app_key, self.app_secret, acc_no, acc_code,
            self.market_loc, self.market_type, self.server
        )
        kis_ws_manager.start(self.app_key, self.app_secret, self.server)
        
        self.load_subscription_file()
        self.init_query()
        self.event_engine.register(EVENT_TIMER, self.process_timer)

    def subscribe(self, req: SubscribeRequest):
        req.symbol = req.symbol.strip()
        if not req.symbol: return
        
        if req.symbol not in self.contracts:
            self._update_contract_info(req.symbol, req.exchange)
        else:
            self.on_contract(self.contracts[req.symbol])
        
        name = self.contracts[req.symbol].name if req.symbol in self.contracts else req.symbol
        kis_ws_manager.update_name(req.symbol, name)
        kis_ws_manager.subscribe(req, self, self.depth_mode)
        
        if req.symbol not in self.saved_subscriptions:
            self.saved_subscriptions.add(req.symbol)
            self.save_subscription_file()

    def _update_contract_info(self, symbol: str, exchange: Exchange):
        name = self.td_api.get_stock_name(symbol)
        if not name: name = symbol
        
        contract = ContractData(
            gateway_name=self.gateway_name,
            symbol=symbol,
            exchange=exchange,
            name=name,
            product=self.products[0] if self.products else Product.EQUITY,
            size=1,
            pricetick=0.01 if self.market_loc == "US" else 100, 
            history_data=True
        )
        self.contracts[symbol] = contract
        self.on_contract(contract)
        self.save_contract_data()

    def send_order(self, req: OrderRequest): return self.td_api.send_order(req)
    def cancel_order(self, req: CancelRequest): self.td_api.cancel_order(req)
    def query_account(self): self.td_api.query_account()
    def query_position(self): self.td_api.query_position()
    def query_history(self, req: HistoryRequest): return self.td_api.query_history(req)

    def close(self):
        kis_ws_manager.stop()
        self.save_subscription_file()
        self.save_contract_data()
        self.td_api.close()

    def process_timer(self, event):
        self.count += 1
        if self.count == 5: self.restore_subscriptions()
        if self.count % 10 == 0:
            self.query_account()
            self.query_position()

    def init_query(self): self.count = 0

    def load_subscription_file(self):
        self.saved_subscriptions.clear()
        files_to_load = []
        if self.sub_mode == "ALL":
            base_dir = get_file_path("dummy").parent
            files_to_load = list(base_dir.glob("kis_sub_*.json"))
        else:
            if self.sub_file.exists(): files_to_load = [self.sub_file]

        for file_path in files_to_load:
            try:
                data = load_json(file_path)
                for sym in data.get("symbols", []):
                    if sym.strip(): self.saved_subscriptions.add(sym.strip())
            except Exception: pass

    def save_subscription_file(self):
        data = {"symbols": list(self.saved_subscriptions)}
        save_json(self.sub_file, data)

    def load_contract_data(self):
        try:
            data = load_json(self.contract_file)
            for d in data.get("contracts", []):
                contract = ContractData(
                    gateway_name=self.gateway_name,
                    symbol=d["symbol"],
                    exchange=Exchange(d["exchange"]),
                    name=d["name"],
                    product=Product(d["product"]),
                    size=d["size"],
                    pricetick=d["pricetick"]
                )
                self.contracts[contract.symbol] = contract
        except Exception: pass

    def save_contract_data(self):
        data = {"contracts": []}
        for contract in self.contracts.values():
            data["contracts"].append({
                "symbol": contract.symbol,
                "exchange": contract.exchange.value,
                "name": contract.name,
                "product": contract.product.value,
                "size": contract.size,
                "pricetick": contract.pricetick
            })
        save_json(self.contract_file, data)

    def restore_subscriptions(self):
        for symbol in self.saved_subscriptions:
            exch = Exchange.KRX
            if symbol in self.contracts:
                exch = self.contracts[symbol].exchange
                self.on_contract(self.contracts[symbol])
            else:
                exch = self.exchanges[0] if self.exchanges else Exchange.KRX
            req = SubscribeRequest(symbol=symbol, exchange=exch)
            self.subscribe(req)


# Concrete Gateways
class KisKrSpotRealGateway(KisBaseGateway):
    default_name = "KIS_KR_SPOT"
    market_loc, market_type, server = "KR", "SPOT", "REAL"
    products = [Product.EQUITY, Product.ETF]
    exchanges = [Exchange.SOR, Exchange.NXT, Exchange.KRX]

class KisKrSpotDemoGateway(KisKrSpotRealGateway):
    default_name = "KIS_KR_SPOT_DEMO"
    server = "DEMO"
    exchanges = [Exchange.KRX]

class KisKrFutOptRealGateway(KisBaseGateway):
    default_name = "KIS_KR_FUT"
    market_loc, market_type, server = "KR", "FUTOPT", "REAL"
    products = [Product.FUTURES, Product.OPTION]
    exchanges = [Exchange.KRX]

class KisOvrsSpotRealGateway(KisBaseGateway):
    default_name = "KIS_OVRS_SPOT"
    market_loc, market_type, server = "US", "SPOT", "REAL"
    products = [Product.EQUITY]
    exchanges = [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX, Exchange.SEHK, Exchange.SHFE, Exchange.SZSE, Exchange.TSE]

class KisOvrsFutOptRealGateway(KisBaseGateway):
    default_name = "KIS_OVRS_FUT"
    market_loc, market_type, server = "US", "FUTOPT", "REAL"
    products = [Product.FUTURES, Product.OPTION]
    exchanges = [Exchange.CME, Exchange.CBOT, Exchange.EUREX, Exchange.ICE, Exchange.HKFE, Exchange.SGX]


# ------------------------------------------------------------------------------
# Trading API
# ------------------------------------------------------------------------------
class KisTdApi:
    def __init__(self, gateway: KisBaseGateway):
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_count = 0
        self.orders: Dict[str, OrderData] = {}
        self.order_no_map: Dict[str, str] = {}
        
        self.TR_MAP = {
            "KR": {
                "REAL": {
                    "BALANCE": "TTTC8434R", "DEPOSIT": "TTTC8908R",
                    "ORDER_BUY": "TTTC0802U", "ORDER_SELL": "TTTC0801U",
                    "CANCEL": "TTTC0803U", "HIST_D": "FHKST01010400", "HIST_M": "FHKST03010200"
                },
                "DEMO": {
                    "BALANCE": "VTTC8434R", "DEPOSIT": "VTTC8908R",
                    "ORDER_BUY": "VTTC0802U", "ORDER_SELL": "VTTC0801U",
                    "CANCEL": "VTTC0803U", "HIST_D": "FHKST01010400", "HIST_M": "FHKST03010200"
                }
            },
            "US": {
                "REAL": {
                    "BALANCE": "TTTS3012R", "DEPOSIT": "CTRP6010R", 
                    "ORDER_BUY": "TTTT1002U", "ORDER_SELL": "TTTT1006U",
                    "CANCEL": "TTTT1004U", "HIST_D": "HHDFS76240000", "HIST_M": "HHDFS76950200"
                },
                "DEMO": {
                    "BALANCE": "VTTS3012R", "DEPOSIT": "VTTS3007R",
                    "ORDER_BUY": "VTTT1002U", "ORDER_SELL": "VTTT1006U",
                    "CANCEL": "VTTT1004U", "HIST_D": "HHDFS76240000", "HIST_M": "HHDFS76950200"
                }
            }
        }

    def connect(self, key, secret, acc, code, loc, type_, server):
        self.app_key, self.app_secret = key, secret
        self.account_no, self.account_code = acc, code
        self.market_loc, self.market_type, self.server = loc, type_, server
        self.domain = DOMAIN_MAP[server]
        try:
            kis_auth.get_token(self.app_key, self.app_secret, self.server)
            self.gateway.write_log(f"TdApi Connected: {loc}/{type_}/{server}")
        except Exception as e:
            self.gateway.write_log(f"TdApi Connect Failed: {e}")

    def get_header(self, tr_id):
        token = kis_auth.get_token(self.app_key, self.app_secret, self.server)
        return {
            "content-type": "application/json; charset=utf-8", "authorization": f"Bearer {token}",
            "appkey": self.app_key, "appsecret": self.app_secret, "tr_id": tr_id, "custtype": "P"
        }

    def get_stock_name(self, symbol: str):
        try:
            if self.market_loc == "KR":
                url = f"{self.domain}/uapi/domestic-stock/v1/quotations/search-stock-info"
                params = {"PRDT_TYPE_CD": "300", "PDNO": symbol}
                tr_id = "CTPF1002R"
            else:
                url = f"{self.domain}/uapi/overseas-price/v1/quotations/search-info"
                params = {"PRDT_TYPE_CD": "512", "PDNO": symbol}
                tr_id = "CTPF1702R"
            res = requests.get(url, headers=self.get_header(tr_id), params=params)
            data = res.json()
            if data['rt_cd'] == '0':
                return data['output'].get('prdt_name') or data['output'].get('prdt_eng_name')
        except: pass
        return ""

    def query_account(self):
        try: tr_id = self.TR_MAP[self.market_loc][self.server]["DEPOSIT"]
        except: return
        
        if self.market_loc == "KR":
            url = f"{self.domain}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
            params = {"CANO": self.account_no, "ACNT_PRDT_CD": self.account_code, "PDNO": "", "ORD_UNPR": "", "ORD_DVSN": "00", "CMA_EVLU_AMT_ICLD_YN": "N", "OVRS_ICLD_YN": "N"}
        else:
            today = datetime.now().strftime("%Y%m%d")
            url = f"{self.domain}/uapi/overseas-stock/v1/trading/inquire-paymt-stdr-balance"
            params = {"CANO": self.account_no, "ACNT_PRDT_CD": self.account_code, "TR_MKET_CD": "00", "NATN_CD": "840", "WCRC_FRCR_DVSN_CD": "01", "INQR_DVSN_CD": "00", "BASS_DT": today}

        self._send_query_request(url, tr_id, params, "Account")

    def query_position(self):
        try: tr_id = self.TR_MAP[self.market_loc][self.server]["BALANCE"]
        except: return
        
        if self.market_loc == "KR":
            url = f"{self.domain}/uapi/domestic-stock/v1/trading/inquire-balance"
            params = {"CANO": self.account_no, "ACNT_PRDT_CD": self.account_code, "AFHR_FLPR_YN": "N", "OFL_YN": "N", "INQR_DVSN": "02", "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""}
        else:
            url = f"{self.domain}/uapi/overseas-stock/v1/trading/inquire-balance"
            params = {"CANO": self.account_no, "ACNT_PRDT_CD": self.account_code, "TR_MKET_CD": "00", "NATN_CD": "840", "INQR_DVSN_CD": "00", "TR_CRC_CD": "USD", "TR_CRCY_CD": "USD", "WCRC_FRCR_DVSN_CD": "02", "OVRS_EXCG_CD": "NASD", "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""}

        self._send_query_request(url, tr_id, params, "Position")

    def _send_query_request(self, url, tr_id, params, type_):
        threading.Thread(target=self._process_query, args=(url, tr_id, params, type_)).start()

    def _process_query(self, url, tr_id, params, type_):
        try:
            res = requests.get(url, headers=self.get_header(tr_id), params=params)
            data = res.json()
            if data.get('rt_cd') != '0':
                self.gateway.write_log(f"Query {type_} Fail: {data.get('msg1')}")
                return
            
            if type_ == "Account": self._parse_account(data)
            elif type_ == "Position": self._parse_position(data)
        except Exception as e:
            self.gateway.write_log(f"Query Exception: {e}")

    def _parse_account(self, data):
        balance, frozen = 0.0, 0.0
        try:
            if self.market_loc == "KR":
                balance = float(data['output'].get('ord_psbl_cash', 0))
            else:
                items = data.get('output2', [])
                if items:
                    balance = float(items[0].get('frcr_dncl_amt_2', 0))
                else:
                    out = data.get('output', {})
                    balance = float(out.get('ovrs_ord_psbl_amt') or out.get('frcr_ord_psbl_amt1') or 0)
            
            acct = AccountData(
                gateway_name=self.gateway_name,
                accountid=f"{self.account_no}-{self.account_code}",
                balance=balance, frozen=0.0
            )
            self.gateway.on_account(acct)
        except Exception: pass

    def _parse_position(self, data):
        try:
            items = data.get('output1', [])
            for item in items:
                symbol, vol, price, pnl, available = "", 0, 0.0, 0.0, 0
                if self.market_loc == "KR":
                    symbol = item['pdno']
                    vol = int(item['hldg_qty'])
                    available = int(item.get('ord_psbl_qty', 0))
                    price = float(item.get('pchs_avg_pric', 0))
                    pnl = float(item.get('evlu_pfls_amt', 0))
                else:
                    symbol = item.get('ovrs_pdno')
                    vol = int(float(item.get('ovrs_cblc_qty') or 0)) 
                    available = int(float(item.get('ord_psbl_qty') or 0))
                    price = float(item.get('pchs_avg_pric', 0))
                    pnl = float(item.get('frcr_evlu_pfls_amt', 0))

                frozen = vol - available
                if vol > 0:
                    pos = PositionData(
                        gateway_name=self.gateway_name,
                        symbol=symbol,
                        exchange=Exchange.KRX if self.market_loc == "KR" else Exchange.NASDAQ,
                        direction=Direction.LONG,
                        volume=vol, frozen=frozen, price=price, pnl=pnl,
                        accountid=f"{self.account_no}-{self.account_code}"
                    )
                    self.gateway.on_position(pos)
        except Exception: pass

    def _get_kis_exchange_code(self, exchange: Exchange, for_history: bool = False) -> str:
        mapping_order = {
            Exchange.NASDAQ: "NASD", Exchange.NYSE: "NYSE", Exchange.AMEX: "AMEX",
            Exchange.SEHK: "SEHK", Exchange.SHFE: "SHAA", Exchange.SZSE: "SZAA", Exchange.TSE: "TKSE"
        }
        mapping_hist = {
            Exchange.NASDAQ: "NAS", Exchange.NYSE: "NYS", Exchange.AMEX: "AMS",
            Exchange.SEHK: "HKS", Exchange.SHFE: "SHS", Exchange.SZSE: "SZS", Exchange.TSE: "TSE"
        }
        target_map = mapping_hist if for_history else mapping_order
        return target_map.get(exchange, "NASD" if not for_history else "NAS")

    def send_order(self, req: OrderRequest):
        kis_auth.check_rate_limit(self.app_key)
        self.order_count += 1
        orderid = f"{int(time.time())}-{self.order_count}"
        order = req.create_order_data(orderid, self.gateway_name)
        self.orders[orderid] = order
        self.gateway.on_order(order)
        threading.Thread(target=self._send_order_thread, args=(req, order)).start()
        return order.vt_orderid

    def _send_order_thread(self, req, order):
        try:
            is_buy = req.direction == Direction.LONG
            kis_exchange_code = self._get_kis_exchange_code(req.exchange)

            if self.market_loc == "KR":
                tr_id = self.TR_MAP["KR"][self.server]["ORDER_BUY" if is_buy else "ORDER_SELL"]
                url = f"{self.domain}/uapi/domestic-stock/v1/trading/order-cash"
                params = {
                    "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                    "PDNO": req.symbol, "ORD_DVSN": "00" if req.type==OrderType.LIMIT else "01",
                    "ORD_QTY": str(int(req.volume)), "ORD_UNPR": str(int(req.price))
                }
            else:
                tr_id = self.TR_MAP["US"][self.server]["ORDER_BUY" if is_buy else "ORDER_SELL"]
                url = f"{self.domain}/uapi/overseas-stock/v1/trading/order"
                params = {
                    "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                    "OVRS_EXCG_CD": kis_exchange_code,
                    "PDNO": req.symbol, "ORD_QTY": str(int(req.volume)), 
                    "OVRS_ORD_UNPR": str(req.price), "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": "00"
                }

            res = requests.post(url, headers=self.get_header(tr_id), json=params)
            data = res.json()
            if data['rt_cd'] == '0':
                kis_no = data['output'].get('ODNO') or data['output'].get('KRX_FWDG_ORD_ORGNO', '')
                self.order_no_map[order.orderid] = kis_no
                order.status = Status.NOTTRADED
            else:
                order.status = Status.REJECTED
                self.gateway.write_log(f"Order Rejected: {data['msg1']}")
            self.gateway.on_order(order)
        except Exception as e:
            order.status = Status.REJECTED
            self.gateway.on_order(order)
            self.gateway.write_log(f"Order Error: {e}")

    def cancel_order(self, req: CancelRequest):
        threading.Thread(target=self._cancel_order_thread, args=(req,)).start()

    def _cancel_order_thread(self, req: CancelRequest):
        kis_no = self.order_no_map.get(req.orderid)
        if not kis_no: return
        kis_exchange_code = self._get_kis_exchange_code(req.exchange)

        try:
            if self.market_loc == "KR":
                tr_id = self.TR_MAP["KR"][self.server]["CANCEL"]
                url = f"{self.domain}/uapi/domestic-stock/v1/trading/order-rvsecncl"
                params = {
                    "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                    "KRX_FWDG_ORD_ORGNO": "", "ORGN_ODNO": kis_no,
                    "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02",
                    "ORD_QTY": "0", "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y"
                }
            else:
                tr_id = self.TR_MAP["US"][self.server]["CANCEL"]
                url = f"{self.domain}/uapi/overseas-stock/v1/trading/order-rvsecncl"
                params = {
                    "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                    "OVRS_EXCG_CD": kis_exchange_code, "ODNO": kis_no, 
                    "ORD_QTY": "0", "CNCL_DVSN": "02", "OVRS_ORD_UNPR": "0"
                }
            requests.post(url, headers=self.get_header(tr_id), json=params)
        except Exception: pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        history = []
        try:
            is_daily = req.interval == Interval.DAILY
            tr_key = "HIST_D" if is_daily else "HIST_M"
            tr_id = self.TR_MAP[self.market_loc][self.server].get(tr_key)
            if not tr_id: return []
            
            end_date = req.end.strftime("%Y%m%d")
            
            if self.market_loc == "KR":
                url = f"{self.domain}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice" if is_daily else f"{self.domain}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
                params = {"FID_COND_MRKT_DIV_CODE":"J", "FID_INPUT_ISCD":req.symbol, "FID_PERIOD_DIV_CODE":"D", "FID_ORG_ADJ_PRC":"1"} if is_daily else {"FID_ETC_CLS_CODE":"", "FID_COND_MRKT_DIV_CODE":"J", "FID_INPUT_ISCD":req.symbol, "FID_INPUT_HOUR_1":"", "FID_PW_DATA_INCU_YN":"Y"}
            else:
                exch_code = self._get_kis_exchange_code(req.exchange, for_history=True)
                url = f"{self.domain}/uapi/overseas-price/v1/quotations/dailyprice" if is_daily else f"{self.domain}/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice"
                params = {"AUTH":"", "EXCD":exch_code, "SYMB":req.symbol, "GUBN":"0", "BYMD":end_date, "MODP":"1"} if is_daily else {"AUTH":"", "EXCD":exch_code, "SYMB":req.symbol, "NMIN":"1", "PINC":"1", "NEXT":"", "NREC":"120", "KEYB":""}

            res = requests.get(url, headers=self.get_header(tr_id), params=params)
            data = res.json()

            if data['rt_cd'] == '0':
                items = data.get('output2') or data.get('output') or []
                for item in items:
                    try:
                        dt = None
                        if self.market_loc == "KR":
                            d = item.get('stck_bsop_date')
                            t = item.get('stck_cntg_hour')
                            if not d: continue
                            if is_daily:
                                dt = datetime.strptime(d, "%Y%m%d")
                            else:
                                dt = datetime.strptime(f"{d} {t}", "%Y%m%d %H%M%S")
                        else:
                            if is_daily:
                                d = item.get('xymd')
                                if d: dt = datetime.strptime(d, "%Y%m%d")
                            else:
                                d = item.get('kymd') or item.get('xymd')
                                t = item.get('khms') or item.get('xhms')
                                if d and t:
                                    dt = datetime.strptime(f"{d} {t}", "%Y%m%d %H%M%S")
                        
                        if not dt: continue
                        dt = dt.replace(tzinfo=KOREA_TZ)

                        close = float(item.get('stck_clpr') or item.get('clos') or item.get('last') or 0)
                        open_ = float(item.get('stck_oprc') or item.get('open') or close)
                        high = float(item.get('stck_hgpr') or item.get('high') or close)
                        low = float(item.get('stck_lwpr') or item.get('low') or close)
                        vol = float(item.get('acml_vol') or item.get('tvol') or item.get('evol') or 0)
                        
                        history.append(BarData(
                            gateway_name=self.gateway_name,
                            symbol=req.symbol,
                            exchange=req.exchange,
                            datetime=dt,
                            interval=req.interval,
                            volume=vol,
                            open_price=open_,
                            high_price=high,
                            low_price=low,
                            close_price=close
                        ))
                    except Exception: continue
                
                history.sort(key=lambda x: x.datetime)
            else:
                self.gateway.write_log(f"History Query Fail: {data.get('msg1')}")
        except Exception as e:
            self.gateway.write_log(f"History Error: {e}")
        return history

    def close(self): pass