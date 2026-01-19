"""
KIS Gateway for Vn.py (Final Enhanced Version)
FileName: kis_gateway.py

Fixes & Features:
1. US Parsing Fix: Logs RAW JSON on failure, safe parsing logic.
2. Thread Exit Fix: Uses daemon threads and proper socket closing.
3. UI Enhancements:
   - Fetches Real Symbol Names (Samsung, Apple) via REST on subscribe.
   - Appends (KR)/(US) suffix to Account IDs.
"""

import json
import time
import threading
from datetime import datetime
from copy import copy
from typing import Dict, Set, List, Any, Optional
from zoneinfo import ZoneInfo

import requests
import websocket

from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData, OrderData, TradeData, PositionData, AccountData,
    ContractData, OrderRequest, CancelRequest, SubscribeRequest,
    Product
)
from vnpy.trader.constant import (
    Exchange, OrderType, Direction, Status, Interval
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.utility import get_file_path
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


# ------------------------------------------------------------------------------
# Shared WebSocket Manager (Singleton)
# ------------------------------------------------------------------------------
class KisWsManager:
    """
    Manages a SINGLE WebSocket connection.
    - Handles Daemon Thread for safe exit.
    - Maps Symbol Names (Code -> Name) for UI display.
    """
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
        self.symbol_names: Dict[str, str] = {} # Code -> Name Map
        self.ticks: Dict[str, TickData] = {}
        self.subscribed_symbols = set()
        
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
            # [Fix] Daemon Thread ensures process exits even if thread is running
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()

    def stop(self):
        """Force close connection"""
        self.active = False
        if self.ws:
            self.ws.close()
        print("[KisWsManager] Stopped")

    def _get_approval_key(self):
        domain = DOMAIN_MAP[self.server]
        url = f"{domain}/oauth2/Approval"
        try:
            res = requests.post(url, json={
                "grant_type": "client_credentials",
                "appkey": self.app_key, "secretkey": self.app_secret
            })
            data = res.json()
            if "approval_key" in data:
                self.approval_key = data["approval_key"]
            else:
                print(f"[KisWsManager] Key Failed: {data}")
        except Exception as e:
            print(f"[KisWsManager] Key Error: {e}")

    def register_route(self, symbol: str, gateway: BaseGateway):
        self.routes[symbol] = gateway

    def update_name(self, symbol: str, name: str):
        self.symbol_names[symbol] = name

    def subscribe(self, req: SubscribeRequest, gateway: BaseGateway, market_loc: str, depth_mode: bool):
        symbol = req.symbol.strip()
        self.register_route(symbol, gateway)
        
        self.subscribed_symbols.add(symbol)
        
        if self.active and self.ws and self.ws.sock and self.ws.sock.connected:
            self._send_sub_packet(symbol, market_loc, depth_mode)

    def _send_sub_packet(self, symbol, market_loc, depth_mode):
        tr_id_tick = "H0STCNT0" if market_loc == "KR" else "HDFSCNT0"
        tr_key = symbol
        if market_loc == "US": tr_key = f"DNAS{symbol}" 
        self._send_request(tr_id_tick, tr_key)

        if depth_mode:
            tr_id_depth = "H0STASP0" if market_loc == "KR" else "HDFSASP0"
            self._send_request(tr_id_depth, tr_key)

    def _send_request(self, tr_id, tr_key):
        req = {
            "header": { "approval_key": self.approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8" },
            "body": { "input": {"tr_id": tr_id, "tr_key": tr_key} }
        }
        try:
            self.ws.send(json.dumps(req))
        except Exception: pass

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
                print(f"[KisWsManager] Connection Error: {e}")
                time.sleep(1)

    def on_open(self, ws):
        print(f"[KisWsManager] WS Connected ({self.server})")
        for sym, gw in self.routes.items():
            loc = getattr(gw, "market_loc", "KR")
            depth = getattr(gw, "depth_mode", False)
            self._send_sub_packet(sym, loc, depth)

    def on_message(self, ws, data):
        try:
            if data.startswith('{'): return 

            if data[0] in ['0', '1']:
                parts = data.split('|')
                if len(parts) < 4: return
                
                tr_id = parts[1]
                body = parts[3]
                
                market_loc = "KR" if "H0" in tr_id else "US"
                
                if tr_id in ["H0STCNT0", "HDFSCNT0"]:
                    self.process_tick(body, market_loc)
                elif tr_id in ["H0STASP0", "HDFSASP0"]:
                    self.process_depth(body, market_loc)
        except Exception: pass

    def get_tick(self, symbol, gateway):
        if symbol not in self.ticks:
            # [Feature] Use cached Name
            name = self.symbol_names.get(symbol, symbol)
            self.ticks[symbol] = TickData(
                gateway_name=gateway.gateway_name,
                symbol=symbol,
                exchange=Exchange.KRX if gateway.market_loc == "KR" else Exchange.NASDAQ,
                name=name,
                datetime=datetime.now(KOREA_TZ)
            )
        return self.ticks[symbol]

    def process_tick(self, data, market_loc):
        items = data.split('^')
        raw_symbol = items[0]
        symbol = raw_symbol
        if market_loc == "US" and len(raw_symbol) > 4: symbol = raw_symbol[4:]

        gateway = self.routes.get(symbol)
        if not gateway: return 

        tick = self.get_tick(symbol, gateway)
        tick.datetime = datetime.now(KOREA_TZ)
        
        try:
            if market_loc == "KR":
                tick.last_price = float(items[2])
                tick.open_price = float(items[8])
                tick.high_price = float(items[9])
                tick.low_price = float(items[10])
                tick.volume = float(items[12])
            else:
                tick.last_price = float(items[2])
                tick.volume = float(items[11])
            gateway.on_tick(copy(tick))
        except Exception: pass

    def process_depth(self, data, market_loc):
        items = data.split('^')
        raw_symbol = items[0]
        symbol = raw_symbol
        if market_loc == "US" and len(raw_symbol) > 4: symbol = raw_symbol[4:]

        gateway = self.routes.get(symbol)
        if not gateway: return 

        tick = self.get_tick(symbol, gateway)
        tick.datetime = datetime.now(KOREA_TZ)

        try:
            if market_loc == "KR":
                tick.ask_price_1 = float(items[3])
                tick.ask_price_2 = float(items[4])
                tick.ask_price_3 = float(items[5])
                tick.ask_price_4 = float(items[6])
                tick.ask_price_5 = float(items[7])
                
                tick.bid_price_1 = float(items[13])
                tick.bid_price_2 = float(items[14])
                tick.bid_price_3 = float(items[15])
                tick.bid_price_4 = float(items[16])
                tick.bid_price_5 = float(items[17])
                
                tick.ask_volume_1 = float(items[23])
                tick.ask_volume_2 = float(items[24])
                tick.ask_volume_3 = float(items[25])
                tick.ask_volume_4 = float(items[26])
                tick.ask_volume_5 = float(items[27])

                tick.bid_volume_1 = float(items[33])
                tick.bid_volume_2 = float(items[34])
                tick.bid_volume_3 = float(items[35])
                tick.bid_volume_4 = float(items[36])
                tick.bid_volume_5 = float(items[37])
            gateway.on_tick(copy(tick))
        except Exception: pass

    def on_error(self, ws, error):
        print(f"[KisWsManager] Error: {error}")

    def on_close(self, ws, status_code, msg):
        print("[KisWsManager] Closed")

kis_ws_manager = KisWsManager()


# ------------------------------------------------------------------------------
# Base Gateway
# ------------------------------------------------------------------------------
class KisBaseGateway(BaseGateway):
    default_setting = {
        "app_key": "", "app_secret": "", "account_no": "", "account_code": "01",
        "subscription_limit": 50, "depth_mode": False
    }
    
    market_loc = "KR"    
    market_type = "SPOT" 
    server = "REAL"      
    exchanges = [Exchange.KRX]

    def __init__(self, event_engine: EventEngine, gateway_name: str):
        super().__init__(event_engine, gateway_name)
        
        self.td_api = KisTdApi(self)
        self.sub_file = get_file_path(f"kis_sub_{self.gateway_name}.json")
        self.saved_subscriptions: Set[str] = set()
        
        self.app_key = ""
        self.app_secret = ""
        self.sub_limit = 50
        self.depth_mode = False

    def connect(self, setting: dict):
        self.app_key = setting["app_key"]
        self.app_secret = setting["app_secret"]
        self.sub_limit = setting.get("subscription_limit", 50)
        self.depth_mode = setting.get("depth_mode", False)
        
        acc_no = setting["account_no"]
        acc_code = setting["account_code"]

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

        # [Feature] Fetch Name immediately upon subscription
        name = self.td_api.get_stock_name(req.symbol)
        if name:
            kis_ws_manager.update_name(req.symbol, name)

        kis_ws_manager.subscribe(req, self, self.market_loc, self.depth_mode)
        
        if req.symbol not in self.saved_subscriptions:
            self.saved_subscriptions.add(req.symbol)
            self.save_subscription_file()

        contract = ContractData(
            gateway_name=self.gateway_name,
            symbol=req.symbol,
            exchange=req.exchange,
            name=name if name else req.symbol, # Use Real Name
            product=Product.EQUITY,
            size=1,
            pricetick=100 if self.market_loc == "KR" else 0.01
        )
        self.on_contract(contract)

    def send_order(self, req: OrderRequest):
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        self.td_api.cancel_order(req)

    def query_account(self):
        self.td_api.query_account()

    def query_position(self):
        self.td_api.query_position()

    def close(self):
        # [Fix] Stop WS manager on close
        kis_ws_manager.stop()
        self.save_subscription_file()
        self.td_api.close()

    def process_timer(self, event):
        self.count += 1
        if self.count == 5: self.resubscribe_saved()
        if self.count % 10 == 0:
            self.query_account()
            self.query_position()

    def init_query(self):
        self.count = 0

    def load_subscription_file(self):
        try:
            if self.sub_file.exists():
                with open(self.sub_file, "r") as f:
                    data = json.load(f)
                    self.saved_subscriptions = set(s.strip() for s in data.get("symbols", []) if s.strip())
        except Exception: pass

    def save_subscription_file(self):
        try:
            data = {"symbols": list(self.saved_subscriptions)}
            with open(self.sub_file, "w") as f:
                json.dump(data, f, indent=4)
        except Exception: pass

    def resubscribe_saved(self):
        for symbol in self.saved_subscriptions:
            exch = self.exchanges[0] if self.exchanges else Exchange.KRX
            req = SubscribeRequest(symbol=symbol, exchange=exch)
            # Re-fetch name on reload
            name = self.td_api.get_stock_name(symbol)
            if name: kis_ws_manager.update_name(symbol, name)
            
            kis_ws_manager.subscribe(req, self, self.market_loc, self.depth_mode)


# ------------------------------------------------------------------------------
# Concrete Gateways
# ------------------------------------------------------------------------------
class KisKrSpotRealGateway(KisBaseGateway):
    default_name = "KIS_KR_SPOT"
    market_loc = "KR"
    market_type = "SPOT"
    server = "REAL"
    exchanges = [Exchange.KRX]

class KisKrSpotDemoGateway(KisBaseGateway):
    default_name = "KIS_KR_SPOT_DEMO"
    market_loc = "KR"
    market_type = "SPOT"
    server = "DEMO"
    exchanges = [Exchange.KRX]

class KisUsSpotRealGateway(KisBaseGateway):
    default_name = "KIS_US_SPOT"
    market_loc = "US"
    market_type = "SPOT"
    server = "REAL"
    exchanges = [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]

class KisUsSpotDemoGateway(KisBaseGateway):
    default_name = "KIS_US_SPOT_DEMO"
    market_loc = "US"
    market_type = "SPOT"
    server = "DEMO"
    exchanges = [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]


# ------------------------------------------------------------------------------
# Trading API (REST)
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
                "REAL": { "BALANCE": "TTTC8434R", "DEPOSIT": "TTTC8908R", "ORDER_CASH": "TTTC0802U", "ORDER_SELL": "TTTC0801U", "CANCEL": "TTTC0803U", "PRICE": "FHKST01010100" },
                "DEMO": { "BALANCE": "VTTC8434R", "DEPOSIT": "VTTC8908R", "ORDER_CASH": "VTTC0802U", "ORDER_SELL": "VTTC0801U", "CANCEL": "VTTC0803U", "PRICE": "FHKST01010100" }
            },
            "US": {
                "REAL": { "BALANCE": "TTTS3012R", "DEPOSIT": "TTTS3012R", "ORDER_CASH": "JTTT1002U", "ORDER_SELL": "JTTT1006U", "CANCEL": "JTTT1004U", "PRICE": "HHDFS00000300" },
                "DEMO": { "BALANCE": "VTTS3012R", "DEPOSIT": "VTTS3012R", "ORDER_CASH": "VTTT1002U", "ORDER_SELL": "VTTT1006U", "CANCEL": "VTTT1004U", "PRICE": "HHDFS00000300" }
            }
        }

    def connect(self, key, secret, acc, code, loc, type_, server):
        self.app_key = key
        self.app_secret = secret
        self.account_no = acc
        self.account_code = code
        self.market_loc = loc
        self.server = server
        self.domain = DOMAIN_MAP[server]
        try:
            kis_auth.get_token(self.app_key, self.app_secret, self.server)
            self.gateway.write_log(f"TdApi Init: {loc}/{server}")
        except Exception as e:
            self.gateway.write_log(f"Init Failed: {e}")

    def get_header(self, tr_id):
        token = kis_auth.get_token(self.app_key, self.app_secret, self.server)
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key, "appsecret": self.app_secret,
            "tr_id": tr_id, "tr_cont": "", "custtype": "P"
        }

    # [Feature] Fetch Stock Name
    def get_stock_name(self, symbol: str):
        try:
            if self.market_loc == "KR":
                tr_id = "FHKST01010100"
                url = f"{self.domain}/uapi/domestic-stock/v1/quotations/inquire-price"
                params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
            else:
                tr_id = "HHDFS00000300"
                url = f"{self.domain}/uapi/overseas-price/v1/quotations/price"
                params = {"AUTH": "", "EXCD": "NAS", "SYMB": symbol}
            
            headers = self.get_header(tr_id)
            res = requests.get(url, headers=headers, params=params)
            data = res.json()
            if data['rt_cd'] == '0':
                if self.market_loc == "KR":
                    return data['output']['bstp_kor_isnm'] # KR Name
                else:
                    return data['output']['hts_kor_isnm'] # US KR Name
        except Exception:
            pass
        return ""

    def query_account(self):
        try:
            tr_id = self.TR_MAP[self.market_loc][self.server]["DEPOSIT"]
        except KeyError: return

        if self.market_loc == "KR":
            url = f"{self.domain}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
            params = {
                "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                "PDNO": "", "ORD_UNPR": "", "ORD_DVSN": "00",
                "CMA_EVLU_AMT_ICLD_YN": "N", "OVRS_ICLD_YN": "N"
            }
        else: 
            url = f"{self.domain}/uapi/overseas-stock/v1/trading/inquire-balance"
            params = {
                "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                "TR_CRC_CD": "USD", "TR_CRCY_CD": "USD", 
                "TR_MKET_CD": "00", "NATN_CD": "840", "INQR_DVSN_CD": "00",
                "OVRS_EXCG_CD": "NAS",
                "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""
            }

        self._send_query_request(url, tr_id, params, "Account")

    def query_position(self):
        try:
            tr_id = self.TR_MAP[self.market_loc][self.server]["BALANCE"]
        except KeyError: return

        if self.market_loc == "KR":
            url = f"{self.domain}/uapi/domestic-stock/v1/trading/inquire-balance"
            params = {
                "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                "AFHR_FLPR_YN": "N", "OFL_YN": "N", "INQR_DVSN": "02", "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00",
                "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""
            }
        else: 
            url = f"{self.domain}/uapi/overseas-stock/v1/trading/inquire-balance"
            params = {
                "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                "TR_MKET_CD": "00", "NATN_CD": "840", "INQR_DVSN_CD": "00",
                "TR_CRC_CD": "USD", "TR_CRCY_CD": "USD", 
                "WCRC_FRCR_DVSN_CD": "02", "OVRS_EXCG_CD": "NAS",
                "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""
            }

        self._send_query_request(url, tr_id, params, "Position")

    def _send_query_request(self, url, tr_id, params, query_type):
        threading.Thread(target=self._process_query, args=(url, tr_id, params, query_type)).start()

    def _process_query(self, url, tr_id, params, query_type):
        headers = self.get_header(tr_id)
        try:
            resp = requests.get(url, headers=headers, params=params)
            data = resp.json()
            
            # [Fix] Log RAW JSON for US Account to Debug "0" Error
            if self.market_loc == "US" and query_type == "Account" and data.get('rt_cd') != '0':
                self.gateway.write_log(f"US Account Query Raw: {data}")

            rt_cd = data.get('rt_cd')
            if rt_cd != '0':
                self.gateway.write_log(f"[{query_type}] Failed: {data.get('msg1')} (Code:{rt_cd})")
                return
            
            if query_type == "Account":
                self._parse_account(data)
            elif query_type == "Position":
                self._parse_position(data)
        except Exception as e:
            self.gateway.write_log(f"[{query_type}] Parse Error: {e}")

    def _parse_account(self, data):
        try:
            balance = 0.0
            if self.market_loc == "KR":
                output = data.get('output', {})
                balance = float(output.get('ord_psbl_cash', 0))
            else:
                # [Fix] Safe Parsing for US Account
                output2 = data.get('output2', [])
                if output2:
                    item = output2[0]
                    # Attempt multiple keys (frcr_dncl_amt_2 or dncl_amt)
                    raw_val = item.get('frcr_dncl_amt_2') or item.get('ovrs_ord_psbl_amt') or item.get('dncl_amt') or "0"
                    balance = float(raw_val)
            
            # [Feature] Add (KR)/(US) Suffix
            suffix = "(KR)" if self.market_loc == "KR" else "(US)"
            account = AccountData(
                gateway_name=self.gateway_name,
                accountid=f"{self.account_no}-{self.account_code}{suffix}",
                balance=balance, frozen=0.0
            )
            self.gateway.on_account(account)
        except Exception as e:
            self.gateway.write_log(f"Account Parse Fail: {e}")

    def _parse_position(self, data):
        try:
            output1 = data.get('output1', [])
            for item in output1:
                if self.market_loc == "KR":
                    symbol = item['pdno']
                    vol = int(item['hldg_qty'])
                    price = float(item.get('pchs_avg_pric', 0))
                    pnl = float(item.get('evlu_pfls_amt', 0))
                else: 
                    symbol = item.get('ovrs_pdno')
                    vol_str = item.get('ovrs_cblc_qty') or item.get('ccld_qty_smtl') or "0"
                    vol = int(float(vol_str))
                    price = float(item.get('pchs_avg_pric', 0) or 0)
                    pnl = float(item.get('frcr_evlu_pfls_amt', 0) or 0)

                if vol > 0:
                    suffix = "(KR)" if self.market_loc == "KR" else "(US)"
                    pos = PositionData(
                        gateway_name=self.gateway_name,
                        symbol=symbol,
                        exchange=Exchange.KRX if self.market_loc == "KR" else Exchange.NASDAQ,
                        direction=Direction.LONG,
                        volume=vol, price=price, pnl=pnl,
                        accountid=f"{self.account_no}-{self.account_code}{suffix}"
                    )
                    self.gateway.on_position(pos)
        except Exception as e:
            self.gateway.write_log(f"Position Parse Fail: {e}")

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
            if req.direction == Direction.LONG:
                tr_id = self.TR_MAP[self.market_loc][self.server]["ORDER_CASH"]
            else:
                tr_id = self.TR_MAP[self.market_loc][self.server]["ORDER_SELL"]

            url = f"{self.domain}/uapi/domestic-stock/v1/trading/order-cash" if self.market_loc == "KR" else f"{self.domain}/uapi/overseas-stock/v1/trading/order"
            
            params = {}
            if self.market_loc == "KR":
                params = {
                    "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                    "PDNO": req.symbol, "ORD_DVSN": "00", 
                    "ORD_QTY": str(int(req.volume)), "ORD_UNPR": str(int(req.price))
                }
            else:
                params = {
                    "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                    "OVRS_EXCG_CD": "NAS", "PDNO": req.symbol,
                    "ORD_QTY": str(int(req.volume)), "OVRS_ORD_UNPR": str(req.price),
                    "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": "00"
                }

            headers = self.get_header(tr_id)
            res = requests.post(url, headers=headers, json=params)
            data = res.json()
            
            if data['rt_cd'] == '0':
                kis_no = data['output'].get('ODNO') or data['output'].get('KRX_FWDG_ORD_ORGNO', '')
                self.order_no_map[order.orderid] = kis_no
                order.status = Status.NOTTRADED
                self.gateway.write_log(f"Order Sent: {order.orderid} -> KIS:{kis_no}")
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

        try:
            tr_id = self.TR_MAP[self.market_loc][self.server]["CANCEL"]
            url = f"{self.domain}/uapi/domestic-stock/v1/trading/order-rvsecncl" if self.market_loc == "KR" else f"{self.domain}/uapi/overseas-stock/v1/trading/order-rvsecncl"
            
            params = {}
            if self.market_loc == "KR":
                params = {
                    "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                    "KRX_FWDG_ORD_ORGNO": kis_no, "ORGN_ODNO": kis_no,
                    "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02", 
                    "ORD_QTY": "0", "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y"
                }
            else:
                params = {
                    "CANO": self.account_no, "ACNT_PRDT_CD": self.account_code,
                    "OVRS_EXCG_CD": "NAS", "ODNO": kis_no, 
                    "ORD_QTY": "0", "CNCL_DVSN": "02", "OVRS_ORD_UNPR": "0"
                }

            headers = self.get_header(tr_id)
            res = requests.post(url, headers=headers, json=params)
            data = res.json()
            
            if data['rt_cd'] == '0':
                self.gateway.write_log(f"Cancel Sent: {req.orderid}")
            else:
                self.gateway.write_log(f"Cancel Rejected: {data['msg1']}")
        except Exception as e:
            self.gateway.write_log(f"Cancel Error: {e}")

    def close(self):
        pass