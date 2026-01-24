# kis_gateway.py
"""
KIS Unified Gateway for Vn.py (Final Production Version)
- Structure: Single Gateway Instance -> Multiple Account Contexts
- Features:
  1. External Account Management (kis_accounts.json) -> Clean UI
  2. Multi-Account/Multi-Asset Support (Unified)
  3. Dynamic Domain Switching (Stateless Session for REAL/DEMO mixing)
  4. [Improved] Stateless Contract Cache & Clean Shutdown
"""

import time
import threading
import json
import traceback
import requests
from datetime import datetime
from copy import copy
from zoneinfo import ZoneInfo
from typing import Dict, List, Any, Optional, Set, Tuple, Union
from collections import defaultdict
from pathlib import Path

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
from vnpy.trader.utility import get_folder_path, get_file_path
from vnpy_rest.rest_client import RestClient, Request
from vnpy_websocket.websocket_client import WebsocketClient

# --- Local Modules Import ---
try:
    from .kis_auth import kis_auth
    from .kis_parser import (
        parse_ws_realtime, parse_ws_hoka, parse_ws_notice,
        parse_order_response, parse_cancel_response,
        parse_balance, parse_position, parse_contract
    )
    from .kis_api_helper import KisApiHelper, AssetType, TR_WS
    from .kis_datafeed import KisDatafeed
except ImportError:
    from kis_auth import kis_auth
    from kis_parser import (
        parse_ws_realtime, parse_ws_hoka, parse_ws_notice,
        parse_order_response, parse_cancel_response,
        parse_balance, parse_position, parse_contract
    )
    from kis_api_helper import KisApiHelper, AssetType, TR_WS
    from kis_datafeed import KisDatafeed

# =============================================================================
# --- Constants ---
# =============================================================================
KOREA_TZ = ZoneInfo("Asia/Seoul")
REAL_DOMAIN = "https://openapi.koreainvestment.com:9443"
VIRTUAL_DOMAIN = "https://openapivts.koreainvestment.com:29443"

KR_EXCHANGES = KisApiHelper.DOMESTIC_EXCHANGES
OS_STOCK_EXCHANGES = KisApiHelper.OVERSEAS_STOCK_EXCHANGES
OS_FUTOPT_EXCHANGES = KisApiHelper.OVERSEAS_FUTOPT_EXCHANGES

# =============================================================================
# [NEW] Load Accounts from External JSON
# =============================================================================
def load_kis_accounts() -> Dict[str, dict]:
    """
    Load account settings from .vntrader/kis_accounts.json
    """
    file_path = get_file_path("kis_gateway/kis_accounts.json")
    if not file_path.exists():
        # Create a template if not exists
        template = {
            "가상국내주식": {
                "server": "DEMO",
                "app_key": "PSaYJYiqUO0CJfPD40nxeoehTa6ANiygCzWy",
                "app_secret": "kWfOEtpRCpsGh06UGWkBNGF0gdjJ+jmAsMYPjWezsQxFpfsxPY1Nd8/Ys+p9iZBxHpJH6837LgqzYBq2UdeGNEso0UnpQC0Nl3MD2tR8xva7ELOqbfks7C++v3Xp0qtXY7R9mXe4Gvn8LUtQQUpGe9Q7KQUmdYgZoqjjTEZbVahTOwEBwSU=",
                "account_no": "50158896",
                "account_code": "01",
                "assets": ["KR_STOCK"]
            },
        }
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(template, f, indent=4, ensure_ascii=False)
        except Exception: pass
        return {}

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

# Global Account Registry
KIS_ACCOUNTS = load_kis_accounts()
ACCOUNT_NAMES = list(KIS_ACCOUNTS.keys()) if KIS_ACCOUNTS else ["계좌설정필요(kis_accounts.json)"]


# =============================================================================
# Account Context
# =============================================================================
class AccountContext:
    def __init__(self, setting: dict):
        self.app_key: str = setting.get("app_key", "")
        self.app_secret: str = setting.get("app_secret", "")
        self.acc_no: str = setting.get("account_no", "")     
        self.acc_code: str = setting.get("account_code", "01")
        self.name: str = setting.get("name", self.acc_no) # Alias
        self.server: str = setting.get("server", "REAL")
        
        assets = setting.get("assets", [])
        if isinstance(assets, str):
            if "," in assets: self.assets = [s.strip() for s in assets.split(",")]
            else: self.assets = [assets]
        else: self.assets = assets
        
    @property
    def full_acc(self) -> str: return f"{self.acc_no}{self.acc_code}"

    def supports_asset(self, asset_type: str) -> bool:
        if not self.assets or "ALL" in self.assets: return True
        return asset_type in self.assets

# =============================================================================
# Contract Cache & WS Component
# =============================================================================
class KisContractCache:
    def __init__(self, filename="kis_contract_cache.json"):
        self.file_path = get_folder_path("kis_gateway") / filename

    def load(self, gateway: BaseGateway):
        if not self.file_path.exists(): return
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            count = 0
            for symbol, info in data.items():
                try:
                    contract = ContractData(
                        symbol=info["symbol"], exchange=Exchange(info["exchange"]),
                        name=info["name"], product=Product(info["product"]),
                        size=info["size"], pricetick=info["pricetick"],
                        min_volume=info.get("min_volume", 1),
                        gateway_name=gateway.gateway_name, history_data=True
                    )
                    gateway.on_contract(contract)
                    count += 1
                except Exception: continue
            if count > 0: gateway.write_log(f"Loaded {count} contracts from local cache.")
        except Exception as e: gateway.write_log(f"Cache Load Failed: {e}")

    def save(self, gateway: BaseGateway):
        data = {}
        for contract in gateway.contracts.values():
            data[contract.symbol] = {
                "symbol": contract.symbol, "exchange": contract.exchange.value,
                "name": contract.name, "product": contract.product.value,
                "size": contract.size, "pricetick": contract.pricetick,
                "min_volume": contract.min_volume
            }
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            gateway.write_log(f"Saved {len(data)} contracts to local cache.")
        except Exception as e: gateway.write_log(f"Cache Save Failed: {e}")

global_contract_cache = KisContractCache()

class KisWebsocketComponent(WebsocketClient):
    def __init__(self, session: 'KisGlobalSession'):
        super().__init__()
        self.session = session
    
    def on_connected(self):
        self.session.log("✅ WebSocket Connected")
        self.session.on_ws_connected()
    
    def on_disconnected(self, status_code: int, msg: str):
        self.session.log(f"❌ WebSocket Disconnected: {msg}")
        self.session.on_ws_disconnected(status_code, msg)

    def on_message(self, packet):
        try:
            if isinstance(packet, str) and packet.startswith('{'):
                self.on_packet({"type": "json", "payload": json.loads(packet)})
            else:
                self.on_packet({"type": "raw", "payload": packet})
        except Exception as e: self.session.log(f"🔥 Packet Parsing Error: {e}")

    def on_packet(self, packet: dict): self.session.on_ws_packet(packet)
    def on_error(self, exception: Exception, *args, **kwargs): self.session.log(f"🔥 WebSocket Error: {exception}")


# =============================================================================
# [CORE] KisGlobalSession
# =============================================================================       
class KisGlobalSession(RestClient):
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
        self.ws_client = KisWebsocketComponent(self)
        
        self.user_id = "" 
        self._primary_app_key = ""
        self._primary_app_secret = ""
        self._primary_server = "REAL"
        
        self.approval_key = ""
        self.ws_host = ""
        self.ws_connected = False
        self.aes_keys = {} 
        
        self.tick_routing: Dict[Tuple[str, str], List[Tuple['KisUnifiedGateway', Exchange]]] = defaultdict(list)
        self.account_routing: Dict[Tuple[str, str], 'KisUnifiedGateway'] = {}
        self.active_subscriptions: Set[Tuple[str, Exchange, str, str]] = set()
        
        self._initialized = True

    def log(self, msg: str): print(f"[KIS_SESSION] {msg}")

    def init_config(self, user_id: str, primary_ctx: AccountContext):
        with self._lock:
            if not self._primary_app_key:
                self.user_id = user_id
                self._primary_app_key = primary_ctx.app_key
                self._primary_app_secret = primary_ctx.app_secret
                self._primary_server = primary_ctx.server
                
                default_url = VIRTUAL_DOMAIN if primary_ctx.server == "DEMO" else REAL_DOMAIN
                self.init(url_base=default_url)
                
                if primary_ctx.server == "DEMO": self.ws_host = "ws://ops.koreainvestment.com:31000"
                else: self.ws_host = "ws://ops.koreainvestment.com:21000"

    def register_gateway(self, gateway: 'KisUnifiedGateway', contexts: List[AccountContext]):
        with self._lock:
            for ctx in contexts:
                needed_trs = []
                assets = ctx.assets if ctx.assets and "ALL" not in ctx.assets else [AssetType.KR_STOCK, AssetType.KR_FUTOPT, AssetType.OS_STOCK, AssetType.OS_FUTOPT]
                for asset in assets:
                    tr_map = KisApiHelper.NOTICE_TR_MAP.get(asset, {})
                    ids = tr_map.get(ctx.server, [])
                    needed_trs.extend(ids)
                for tr_id in set(needed_trs):
                    key = (ctx.full_acc, tr_id)
                    self.account_routing[key] = gateway
            self.start()

    def start(self):
        if not self.active:
            try:
                RestClient.start(self, n=10)
                self.log("REST Client Started.")
            except Exception as e: self.log(f"REST Start Failed: {e}")
        
        if not self.ws_client.active and self._primary_app_key:
            self.approval_key = self._get_approval_key()
            if self.approval_key:
                try:
                    self.ws_client.init(host=self.ws_host, ping_interval=10)
                    self.ws_client.start()
                    self.log(f"WS Client Started ({self._primary_server})")
                except Exception as e: self.log(f"CRITICAL: Failed to start WebsocketClient: {e}")

    def stop(self):
        self.active = False
        if self.ws_client:
            if self.ws_client.active:
                self.ws_client.stop()
                if self.ws_client.thread and self.ws_client.thread.is_alive():
                    self.ws_client.thread.join(timeout=1.0)
        
        RestClient.stop(self)
        self.log("Session Stopped (Threads Joined).")

    def request(self, method: str, path: str, params: dict = None, data: dict = None, headers: dict = None, extra: dict = None):
        server_type = extra.get("server", "REAL") if extra else "REAL"
        self.url_base = VIRTUAL_DOMAIN if server_type == "DEMO" else REAL_DOMAIN
        return super().request(method, path, params, data, headers, extra)

    def sign(self, request: Request) -> Request:
        target_app_key = request.extra.get("app_key") or self._primary_app_key
        target_secret = request.extra.get("app_secret") or self._primary_app_secret
        target_server = request.extra.get("server", "REAL")

        kis_auth.check_rate_limit(target_app_key)
        
        tr_id = request.extra.get("tr_id", "")
        body_data = request.data if request.method == "POST" else None
        
        request.headers = kis_auth.get_header(
            tr_id=tr_id, app_key=target_app_key, app_secret=target_secret,
            server=target_server, body_data=body_data
        )
        if request.method == "POST" and request.data and isinstance(request.data, dict):
            request.data = json.dumps(request.data)
        return request

    def _get_approval_key(self):
        url = "/oauth2/Approval"
        req = {"grant_type": "client_credentials", "appkey": self._primary_app_key, "secretkey": self._primary_app_secret}
        try:
            domain = VIRTUAL_DOMAIN if self._primary_server == "DEMO" else REAL_DOMAIN
            full_url = domain + url
            res = requests.post(full_url, json=req, timeout=10)
            return res.json().get("approval_key", "")
        except Exception as e:
            self.log(f"Approval Key Error: {e}")
            return ""

    def subscribe_symbol(self, gateway: 'KisUnifiedGateway', symbol: str, exchange: Exchange, 
                         tr_id: str, tr_key: str, depth_tr_id: str = None):        
        with self._lock:
            self._add_routing(symbol, tr_id, gateway, exchange)
            if depth_tr_id: self._add_routing(symbol, depth_tr_id, gateway, exchange)
            
            if self.ws_connected:
                if not self._is_physically_subscribed(symbol, exchange, tr_id, tr_key):
                    self.log(f"Subscribing WS: {symbol}.{exchange} (TR: {tr_id})")
                    self._send_subscription_request(symbol, exchange, tr_id, tr_key)
                if depth_tr_id and gateway.depth_mode:
                    if not self._is_physically_subscribed(symbol, exchange, depth_tr_id, tr_key):
                        self._send_subscription_request(symbol, exchange, depth_tr_id, tr_key)
            else:
                self._mark_as_subscribed(symbol, exchange, tr_id, tr_key)
                if depth_tr_id: self._mark_as_subscribed(symbol, exchange, depth_tr_id, tr_key)
                if not self.ws_client.active: self.ws_client.start()

    def _add_routing(self, symbol, tr_id, gateway, exchange):
        key = (symbol, tr_id)
        entry = (gateway, exchange)
        if entry not in self.tick_routing[key]: self.tick_routing[key].append(entry)

    def _is_physically_subscribed(self, symbol, exchange, tr_id, tr_key):
        return (symbol, exchange, tr_id, tr_key) in self.active_subscriptions

    def _mark_as_subscribed(self, symbol, exchange, tr_id, tr_key):
        self.active_subscriptions.add((symbol, exchange, tr_id, tr_key))

    def _send_subscription_request(self, symbol, exchange, tr_id, tr_key):
        self._mark_as_subscribed(symbol, exchange, tr_id, tr_key)
        self._send_ws_packet(tr_id, tr_key)

    def _send_ws_packet(self, tr_id, tr_key):
        payload = {
            "header": {"approval_key": self.approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
            "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}}
        }
        self.ws_client.send_packet(payload)

    def on_ws_connected(self):
        self.ws_connected = True
        for symbol, exchange, tr_id, tr_key in self.active_subscriptions:
            self._send_ws_packet(tr_id, tr_key)
        if self.user_id:
            needed_trs = set(k[1] for k in self.account_routing.keys())
            for tr_id in needed_trs: self._send_ws_packet(tr_id, self.user_id)            

    def on_ws_disconnected(self, status_code: int, msg: str): self.ws_connected = False 
        
    def on_ws_packet(self, packet):
        if packet["type"] == "raw":
            data = packet["payload"]
            parts = data.split('|')
            if len(parts) < 4: return
            header_flag = parts[0]; tr_id = parts[1]; body = parts[3]
            if header_flag == '0': self._dispatch_tick(tr_id, body)
            elif header_flag == '1': self._dispatch_notice(tr_id, body)
        elif packet["type"] == "json": self._handle_json(packet["payload"])
        
    def _dispatch_tick(self, tr_id, body):
        if any(x in tr_id for x in ["CNT", "HDFFF020", "ECEUCNT0"]):
            parsed = parse_ws_realtime(tr_id, body); method = "on_ws_tick"
        elif any(x in tr_id for x in ["ASP", "HDFFF010"]):
            parsed = parse_ws_hoka(tr_id, body); method = "on_ws_depth"
        else: return
        if not parsed or not parsed.get("valid"): return
        symbol = parsed.get("code")
        targets = self.tick_routing.get((symbol, tr_id))
        if targets:
            for gateway, exchange in targets: getattr(gateway, method)(parsed, exchange)

    def _dispatch_notice(self, tr_id, body):
        if tr_id not in self.aes_keys: return
        key_info = self.aes_keys[tr_id]
        parsed = parse_ws_notice(tr_id, body, key_info["key"], key_info["iv"])
        if parsed and parsed.get("valid"):
            account_no = parsed.get("account")
            target_gw = self.account_routing.get((account_no, tr_id))
            if target_gw: target_gw.on_ws_notice(parsed)

    def _handle_json(self, js):
        header = js.get("header", {})
        tr_id = header.get("tr_id")
        if tr_id == "PINGPONG": self.ws_client.send_packet(js)
        elif "body" in js and "output" in js["body"]: self.aes_keys[tr_id] = js["body"]["output"]


# =============================================================================
# [HELPER] KisUnifiedApi
# =============================================================================
class KisUnifiedApi:
    def __init__(self, gateway: 'KisUnifiedGateway'):
        self.gateway = gateway
        self.session = KisGlobalSession()
        self.datafeed = KisDatafeed()
        self.quote_count = 0
        self.quote_map: Dict[str, tuple[str, str]] = {} 
        self.interest_file = get_folder_path("kis_gateway") / "interest_list.json"
        self.interest_map: Dict[str, dict] = {}        

    def connect(self):
        if not self.gateway.account_contexts:
            self.gateway.write_log("No accounts configured.")
            return

        primary_ctx = list(self.gateway.account_contexts.values())[0]
        self.session.init_config(self.gateway.user_id, primary_ctx)
        self.session.register_gateway(self.gateway, list(self.gateway.account_contexts.values()))
        self.gateway.write_log(f"Connected to Session. Primary: {primary_ctx.name} ({primary_ctx.server})")
        
        if self.datafeed:
            self.datafeed.app_key = primary_ctx.app_key
            self.datafeed.app_secret = primary_ctx.app_secret
            self.datafeed.vts = (primary_ctx.server == "DEMO")
            self.datafeed.base_url = VIRTUAL_DOMAIN if self.datafeed.vts else REAL_DOMAIN
        
        global_contract_cache.load(self.gateway)
        self._load_interest_list()
        self._auto_subscribe_interest()

    def close(self):
        self.gateway.write_log("Saving gateway data...")
        try:
            global_contract_cache.save(self.gateway)
            self._save_interest_list()
        except Exception as e: self.gateway.write_log(f"Data Save Failed: {e}")
        if self.session: self.session.stop()

    def query_contract(self, symbol: str, exchange: Exchange) -> Optional[ContractData]:
        cached_contract = self.gateway.get_contract(symbol)
        if cached_contract: return cached_contract
        
        self.gateway.write_log(f"Query Contract API: {symbol} ({exchange})")
        asset_type = KisApiHelper.get_asset_type(exchange, symbol) or AssetType.KR_STOCK
        context = self.gateway.get_context_for_asset(asset_type)
        if not context: return None

        params = {}
        if exchange in KR_EXCHANGES:
            asset_type, product = KisApiHelper.infer_kr_asset_product(symbol)
            market_code = KisApiHelper.get_market_code(exchange, asset_type)
            params = {"FID_COND_MRKT_DIV_CODE": market_code, "FID_INPUT_ISCD": symbol}
        elif exchange in OS_STOCK_EXCHANGES:
            asset_type = AssetType.OS_STOCK
            excd = KisApiHelper.get_kis_exchange_code(asset_type, exchange)
            params = {"AUTH": "", "EXCD": excd, "SYMB": symbol}
            product = Product.EQUITY    
        elif exchange in OS_FUTOPT_EXCHANGES:
            asset_type = AssetType.OS_FUTOPT
            excd = exchange.value
            params = {"AUTH": "", "EXCD": excd, "SYMB": symbol}
            product = Product.FUTURES
        else: return None

        config = KisApiHelper.get_tr_config(asset_type, "QUOTE", context.server=="DEMO")
        if not config: return None
            
        try:
            resp = self.session.request(
                method="GET", path=config['url'], params=params, 
                extra={"tr_id": config['tr_id'], "app_key": context.app_key, "app_secret": context.app_secret, "server": context.server}
            )
            if resp.status_code == 200:
                data = resp.json()
                contract = ContractData(
                    gateway_name=self.gateway.gateway_name, symbol=symbol, exchange=exchange,
                    name=symbol, product=product, size=1, pricetick=0, history_data=True
                )
                contract = parse_contract(data, asset_type, contract)
                self.gateway.on_contract(contract)
                return contract
        except Exception as e: self.gateway.write_log(f"Query Contract Failed: {symbol} {e}")
        return None

    def _load_interest_list(self):
        try:
            if not self.interest_file.exists(): return
            with open(self.interest_file, "r", encoding="utf-8") as f: data = json.load(f)
            if not data: return
            if isinstance(data, list):
                self.interest_map = {s: {"name": s, "exchange": "KRX", "product": "EQUITY"} for s in data}
            else: self.interest_map = data
            for symbol, info in self.interest_map.items():
                if isinstance(info, str): info = {"name": info, "exchange": "KRX", "product": "EQUITY"}
                if not self.gateway.get_contract(symbol):
                    contract = ContractData(
                        symbol=symbol, exchange=Exchange(info.get("exchange", "KRX")),
                        name=info.get("name", symbol), product=Product(info.get("product", "EQUITY")),
                        size=1, pricetick=0, gateway_name=self.gateway.gateway_name
                    )
                    self.gateway.on_contract(contract)
        except Exception: pass

    def _save_interest_list(self):
        try:
            with open(self.interest_file, "w", encoding="utf-8") as f:
                json.dump(self.interest_map, f, ensure_ascii=False, indent=4)
        except: pass

    def add_interest(self, req: SubscribeRequest):
        symbol = req.symbol
        contract = self.gateway.get_contract(symbol)
        if not contract: contract = self.query_contract(symbol, req.exchange)
        name = contract.name if contract else symbol
        product = contract.product if contract else Product.EQUITY
        self.interest_map[symbol] = {"name": name, "exchange": req.exchange.value, "product": product.value}
        self._save_interest_list()
            
    def _auto_subscribe_interest(self):
        for symbol, info in self.interest_map.items():
            try: exchange = Exchange(info.get("exchange", "KRX"))
            except: exchange = Exchange.KRX
            self.gateway.subscribe(SubscribeRequest(symbol=symbol, exchange=exchange))

    def send_order(self, req: OrderRequest) -> str:
        asset_type = KisApiHelper.get_asset_type(req.exchange, req.symbol)
        if not asset_type: 
            self.gateway.write_log(f"Unknown Asset Type for {req.symbol}")
            return ""

        target_account = req.extra.get("account") if req.extra else None
        context = None
        if target_account:
            for ctx in self.gateway.account_contexts.values():
                if ctx.name == target_account or ctx.acc_no == target_account:
                    context = ctx; break
        if not context: context = self.gateway.get_context_for_asset(asset_type)
        if not context:
            self.gateway.write_log(f"No account configured for asset: {asset_type} (Target: {target_account})")
            return ""

        is_vts = (context.server == "DEMO")
        action = "ORDER_BUY" if req.direction == Direction.LONG else "ORDER_SELL"
        config = KisApiHelper.get_tr_config(asset_type, action, is_vts)
        if not config: return ""

        params = KisApiHelper.build_order_params(req, asset_type, context.full_acc)
        try:
            resp = self.session.request(
                method="POST", path=config['url'], data=params, 
                extra={"tr_id": config['tr_id'], "app_key": context.app_key, "app_secret": context.app_secret, "server": context.server}
            )
            parsed = parse_order_response(resp.json())
            if parsed.get("odno"):
                self.gateway.order_exchange_map[parsed["odno"]] = req.exchange
                order = req.create_order_data(parsed["odno"], self.gateway.gateway_name)
                order.status = Status.NOTTRADED
                order.extra = {"account": context.name} 
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
        if not asset_type: return
        context = self.gateway.get_context_for_asset(asset_type)
        if not context: return

        is_vts = (context.server == "DEMO")
        config = KisApiHelper.get_tr_config(asset_type, "ORDER_CANCEL", is_vts)
        if not config: return
        
        params = KisApiHelper.build_cancel_params(req, asset_type, context.full_acc)
        self.session.add_request(
            method="POST", path=config['url'], data=params, 
            extra={"tr_id": config['tr_id'], "app_key": context.app_key, "app_secret": context.app_secret, "server": context.server}, 
            callback=self.on_cancel_order_return
        )

    def on_cancel_order_return(self, data: dict | None, request: Request):
        if not data: return
        parsed = parse_cancel_response(data)
        if parsed.get("status") == "0": self.gateway.write_log(f"Cancel Accepted: {parsed.get('odno')}")
        else: self.gateway.write_log(f"Cancel Failed: {parsed.get('msg')}")

    def send_quote(self, req: QuoteRequest) -> str:
        self.quote_count += 1
        quote_id = str(self.quote_count)
        buy_req = OrderRequest(symbol=req.symbol, exchange=req.exchange, direction=Direction.LONG, offset=Offset.OPEN, type=OrderType.LIMIT, price=req.bid_price, volume=req.bid_volume)
        sell_req = OrderRequest(symbol=req.symbol, exchange=req.exchange, direction=Direction.SHORT, offset=Offset.CLOSE, type=OrderType.LIMIT, price=req.ask_price, volume=req.ask_volume)
        vt_buy = self.send_order(buy_req)
        vt_sell = self.send_order(sell_req)
        if vt_buy or vt_sell:
            quote = QuoteData(gateway_name=self.gateway.gateway_name, symbol=req.symbol, exchange=req.exchange, quoteid=quote_id, bid_price=req.bid_price, bid_volume=req.bid_volume, ask_price=req.ask_price, ask_volume=req.ask_volume, bid_offset=Offset.OPEN, ask_offset=Offset.CLOSE, status=Status.NOTTRADED, datetime=datetime.now(KOREA_TZ))
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

    def query_account(self):
        for context in self.gateway.account_contexts.values():
            assets_to_query = context.assets if context.assets and "ALL" not in context.assets else [AssetType.KR_STOCK, AssetType.KR_FUTOPT, AssetType.OS_STOCK, AssetType.OS_FUTOPT, AssetType.KR_BOND]
            for asset_type in assets_to_query: self._async_query_balance(asset_type, context)

    def query_position(self): self.query_account() 

    def _async_query_balance(self, asset_type, context: AccountContext):
        config = KisApiHelper.get_tr_config(asset_type, "BALANCE", context.server == "DEMO")
        if not config: return
        params = KisApiHelper.build_balance_params(asset_type, context.full_acc)
        self.session.add_request(
            method="GET", path=config['url'], params=params, 
            extra={
                "tr_id": config['tr_id'], "asset_type": asset_type, "app_key": context.app_key, "app_secret": context.app_secret, "server": context.server,
                "acc_name": context.name 
            }, 
            callback=self.on_balance_return
        )

    def on_balance_return(self, data: dict | None, request: Request):
        if not data: return
        asset_type = request.extra.get("asset_type")
        acc_name = request.extra.get("acc_name") 
        
        parsed_bal = parse_balance(data, asset_type)
        account_id = f"{acc_name}-{asset_type}"
        
        account = AccountData(
            gateway_name=self.gateway.gateway_name, accountid=account_id, 
            balance=parsed_bal["balance"], frozen=parsed_bal["balance"] - parsed_bal["available"]
        )
        self.gateway.on_account(account)
        
        exch_map = {AssetType.KR_STOCK: Exchange.KRX, AssetType.KR_FUTOPT: Exchange.KRX, AssetType.OS_STOCK: Exchange.NASDAQ, AssetType.OS_FUTOPT: Exchange.CME, AssetType.KR_BOND: Exchange.KRX}
        parsed_pos = parse_position(data, asset_type)
        for pos in parsed_pos:
            position = PositionData(
                gateway_name=self.gateway.gateway_name, symbol=pos["symbol"], 
                exchange=exch_map.get(asset_type, Exchange.KRX), 
                direction=pos['direction'], volume=pos["quantity"], price=pos["price"], pnl=pos["pnl"]
            )
            position.extra = {"account": acc_name}
            self.gateway.on_position(position)
            
    def query_history(self, req: HistoryRequest):
        bars = self.datafeed.query_bar_history(req)
        if not bars: return []
        for bar in bars: bar.gateway_name = self.gateway.gateway_name
        return bars


# =============================================================================
# [GATEWAY] KisUnifiedGateway
# =============================================================================
class KisUnifiedGateway(BaseGateway):
    """
    KIS Unified Gateway (Account List Selection UI)
    """
    default_setting = {
        "User ID": "",
        "계좌 선택": ACCOUNT_NAMES  # Combo Box
    }
    
    default_name = "KIS"
    exchanges = [Exchange.KRX, Exchange.SOR, Exchange.NXT, Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX, Exchange.CME, Exchange.EUREX]
    
    def __init__(self, event_engine: EventEngine, gateway_name: str):
        super().__init__(event_engine, gateway_name)
        self.api = KisUnifiedApi(self) 
        self.ticks: Dict[str, TickData] = {}
        self.order_exchange_map: Dict[str, Exchange] = {}
        self.contracts: Dict[str, ContractData] = {}        
        self.account_contexts: Dict[str, AccountContext] = {}
        self.user_id = ""
        self.depth_mode = True

    def connect(self, setting: dict):
        self.user_id = setting.get("User ID", "")
        
        # [NEW] Load Account from selected name
        selected_acc_name = setting.get("계좌 선택")
        
        if selected_acc_name and selected_acc_name in KIS_ACCOUNTS:
            acc_cfg = KIS_ACCOUNTS[selected_acc_name]
            # Inject name for reference
            acc_cfg["name"] = selected_acc_name
            
            ctx = AccountContext(acc_cfg)
            self.account_contexts[ctx.full_acc] = ctx
        else:
            self.write_log(f"❌ 연결 실패: 선택된 계좌({selected_acc_name})의 정보가 kis_accounts.json에 없습니다.")
            return

        self.api.connect()
        self.api.query_account()
        self.api.query_position()
        self.write_log(f"✅ Gateway {self.gateway_name} 연결됨 (계좌: {selected_acc_name})")
        
    def close(self):
        self.api.close()
        super().close()

    def get_context_for_asset(self, asset_type: str) -> Optional[AccountContext]:
        for ctx in self.account_contexts.values():
            if asset_type in ctx.assets: return ctx
        for ctx in self.account_contexts.values():
            if not ctx.assets or "ALL" in ctx.assets: return ctx
        if self.account_contexts: return list(self.account_contexts.values())[0]
        return None

    def subscribe(self, req: SubscribeRequest):
        contract = self.api.query_contract(req.symbol, req.exchange)
        self.api.add_interest(req)
        tr_id, tr_key = self.get_ws_tr_info(req)
        depth_tr = self.get_ws_depth_tr(req)
        if tr_id: self.api.session.subscribe_symbol(self, req.symbol, req.exchange, tr_id, tr_key, depth_tr)
        else: self.write_log(f"Sub Failed: No TR ID for {req.symbol}")            

    def on_contract(self, contract: ContractData):
        self.contracts[contract.symbol] = contract
        super().on_contract(contract)

    def get_contract(self, symbol: str): return self.contracts.get(symbol)
    def send_order(self, req: OrderRequest): return self.api.send_order(req)
    def cancel_order(self, req: CancelRequest): self.api.cancel_order(req)
    def send_quote(self, req: QuoteRequest): return self.api.send_quote(req)
    def cancel_quote(self, req: CancelRequest): self.api.cancel_quote(req)
    def query_account(self): self.api.query_account()
    def query_position(self): self.api.query_position()
    def query_history(self, req: HistoryRequest): return self.api.query_history(req)

    # --- WS Callbacks ---
    def on_ws_tick(self, data: dict, exchange: Exchange = Exchange.KRX):
        symbol = data["code"]
        tick = self._get_tick(symbol, exchange)
        tick.last_price = data.get("price", tick.last_price)
        tick.volume = data.get("acc_volume", tick.volume)
        tick.last_volume = data.get("volume", tick.volume)
        tick.turnover = data.get("turnover", tick.turnover)
        tick.open_price = data.get("open", tick.open_price)
        tick.high_price = data.get("high", tick.high_price)
        tick.low_price = data.get("low", tick.low_price)
        tick.bid_price_1 = data.get("bid_1", tick.bid_price_1)
        tick.ask_price_1 = data.get("ask_1", tick.ask_price_1)
        tick.datetime = datetime.now(KOREA_TZ)
        if "localtime" in data:
             try: tick.datetime = datetime.strptime(data["localtime"], "%Y%m%d %H%M%S").replace(tzinfo=KOREA_TZ)
             except: pass
        self.on_tick(copy(tick))

    def on_ws_depth(self, data: dict, exchange: Exchange = Exchange.KRX):
        symbol = data["code"]
        tick = self._get_tick(symbol,exchange)
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
        if not data.get("valid"): return
        order_no = data.get('order_no')
        exchange = self.order_exchange_map.get(order_no, Exchange.KRX) 
        status = data.get("order_status", Status.NOTTRADED)
        
        order = OrderData(
            symbol=data["code"], exchange=exchange, orderid=order_no,
            type=data.get("order_type", OrderType.LIMIT),
            direction=data.get("direction", Direction.NET),
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
                symbol=data["code"], exchange=exchange, orderid=order_no,
                tradeid=f"{order_no}-{int(time.time()*100000)}", 
                direction=order.direction, offset=Offset.NONE,
                price=data.get("filled_price", 0.0), volume=data.get("filled_qty", 0),
                datetime=datetime.now(KOREA_TZ), gateway_name=self.gateway_name
            )
            self.on_trade(trade)
            self.api.query_account()
            self.api.query_position()
        if status in [Status.ALLTRADED, Status.CANCELLED, Status.REJECTED]:
            if order_no in self.order_exchange_map:
                del self.order_exchange_map[order_no]

    def _get_tick(self, symbol, exchange=Exchange.KRX):
        if symbol not in self.ticks:
            self.ticks[symbol] = TickData(gateway_name=self.gateway_name, symbol=symbol, exchange=exchange, datetime=datetime.now(KOREA_TZ))
        return self.ticks[symbol]

    def get_ws_tr_info(self, req): return self.get_ws_tr_info_by_symbol(req.symbol, req.exchange)
    def get_ws_tr_info_by_symbol(self, sym, ex=Exchange.KRX):
        if ex in KR_EXCHANGES:
            if len(sym) == 6 and sym.isdigit():
                return (TR_WS["KR_STOCK"] if ex==Exchange.KRX else (TR_WS["KR_NXT"] if ex==Exchange.NXT else TR_WS["KR_SOR"]), sym)
            if sym.startswith("1") or sym.startswith("2") or sym.startswith("3"): return TR_WS["KR_FUT"], sym
            if sym.startswith("KR"): return TR_WS["KR_BOND"], sym
        if ex == Exchange.EUREX: return TR_WS["NIGHT_FUT"], sym
        if ex in OS_STOCK_EXCHANGES:
            mkt = KisApiHelper.get_kis_exchange_code(AssetType.OS_STOCK, ex, is_order=False)
            return TR_WS["OS_STOCK"], f"D{mkt}{sym}"
        if ex in OS_FUTOPT_EXCHANGES: return TR_WS["OS_FUT"], sym
        return None, None

    def get_ws_depth_tr(self, req): return self.get_ws_depth_tr_by_symbol(req.symbol, req.exchange)
    def get_ws_depth_tr_by_symbol(self, sym, ex=Exchange.KRX):
        if ex in KR_EXCHANGES:
            if len(sym)==6: return (TR_WS["KR_STOCK_HOKA"] if ex==Exchange.KRX else (TR_WS["KR_NXT_HOKA"] if ex==Exchange.NXT else TR_WS["KR_SOR_HOKA"]))
            elif sym.startswith("KR"): return TR_WS["KR_BOND_HOKA"]
            elif sym.startswith("101") or sym.startswith("106"): return TR_WS["KR_FUT_HOKA"]
        if ex in OS_STOCK_EXCHANGES: return TR_WS["OS_STOCK_HOKA"]
        if ex in OS_FUTOPT_EXCHANGES: return TR_WS["OS_FUT_HOKA"]
        return None