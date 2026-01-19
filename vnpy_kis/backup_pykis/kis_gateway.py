"""
VNPY Gateway for KIS - Final Snapshot Version
FileName: kis_gateway_pro.py
Dependencies: vnpy, python-kis>=2.1.6

[Changes]
1. Added 'Snapshot' fetch on subscription (Populates UI immediately).
2. Simplified Thread Exit (Prevents hanging on Quit).
3. Fixed Event Callback Signature (Prevents silent failures).
"""

import threading
import asyncio
import time
import json
import os
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from collections import defaultdict
from enum import Enum

from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData, OrderData, TradeData, PositionData, AccountData,
    ContractData, OrderRequest, CancelRequest, SubscribeRequest
)
from vnpy.trader.constant import (
    Exchange, Product, OrderType, Direction, Status
)

# Soju06/python-kis 라이브러리 임포트
try:
    from pykis import PyKis, KisAccount
except ImportError:
    print("[Error] 'python-kis' library is missing.")
    PyKis = Any; KisAccount = Any

# 파일 상수
SUBSCRIPTION_FILE = "kis_subscriptions.json"

# ------------------------------------------------------------------------------
# 1. Enums & Constants
# ------------------------------------------------------------------------------

class KisServer(Enum):
    REAL = "REAL"
    DEMO = "DEMO"

class KisMarketLoc(Enum):
    KR = "KR"
    US = "US"

class KisMarketType(Enum):
    SPOT = "SPOT"
    FUT = "FUT"
    OPT = "OPT"

EXCHANGE_MAP = {
    KisMarketLoc.KR: Exchange.KRX,
    KisMarketLoc.US: Exchange.NASDAQ,
}

# ------------------------------------------------------------------------------
# 2. KisBackend (Logic Engine)
# ------------------------------------------------------------------------------

class KisBackend:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(KisBackend, cls).__new__(cls)
                    cls._instance.initialized = False
        return cls._instance

    def __init__(self):
        if self.initialized: return
        
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None
        self.active = False
        self.ref_count = 0
        
        # Resources
        self.clients: Dict[str, PyKis] = {}       
        self.accounts: Dict[str, KisAccount] = {} 
        self.gateways: Dict[str, 'KisBaseGateway'] = {}
        
        self.active_tickets: List[Any] = [] 
        self.subscriptions: Dict[str, Set[str]] = defaultdict(set)
        self.max_subscriptions = 40 
        
        self.local_order_map: Dict[str, str] = {} 
        self.kis_order_map: Dict[str, OrderRequest] = {} 
        self.contract_cache: Set[str] = set()
        self.tick_buffer: Dict[str, TickData] = {}

        self.initialized = True

    def start(self):
        with self._lock:
            if not self.active:
                self.active = True
                print("[KIS] Starting Event Loop...")
                self.loop = asyncio.new_event_loop()
                # daemon=True ensures it dies when main process dies
                self.thread = threading.Thread(target=self._run_loop, daemon=True)
                self.thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def stop(self):
        """Fast Shutdown"""
        with self._lock:
            if not self.active: return
            self.active = False
            print("[KIS] Stopping Backend...")

            if self.loop and self.loop.is_running():
                # Just stop the loop, don't wait for tasks (let daemon thread die)
                self.loop.call_soon_threadsafe(self.loop.stop)
            
            # Don't join thread if it's daemon, prevents hanging
            self.loop = None
            self.thread = None
            self.active_tickets.clear()

    # --- Persistence ---
    def _load_saved_subscriptions(self, gateway_name: str) -> List[str]:
        if not os.path.exists(SUBSCRIPTION_FILE): return []
        try:
            with open(SUBSCRIPTION_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get(gateway_name, [])
        except: return []

    def _save_subscriptions(self):
        data = {gw: list(subs) for gw, subs in self.subscriptions.items()}
        try:
            with open(SUBSCRIPTION_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        except: pass

    # --- Initialization ---
    def register_gateway(self, gateway: 'KisBaseGateway'):
        self.start()
        with self._lock: self.ref_count += 1
        self.gateways[gateway.gateway_name] = gateway
        if hasattr(gateway, 'max_subs'): self.max_subscriptions = gateway.max_subs
        asyncio.run_coroutine_threadsafe(self._init_client(gateway), self.loop)

    def close_gateway(self, gateway_name: str):
        if gateway_name not in self.gateways: return
        self.gateways.pop(gateway_name, None)
        self.subscriptions.pop(gateway_name, None)
        with self._lock:
            self.ref_count -= 1
            if self.ref_count <= 0: self.stop()

    async def _init_client(self, gateway: 'KisBaseGateway'):
        try:
            gateway.write_log(f"[{gateway.gateway_name}] Init PyKis...")
            full_account = f"{gateway.account_no}-{gateway.account_code}"
            
            kis_args = {"id": gateway.htsid, "account": full_account, "keep_token": True}
            if gateway.server_type == KisServer.DEMO:
                kis_args.update({
                    "appkey": gateway.app_key, "secretkey": gateway.app_secret,
                    "virtual_id": gateway.htsid, "virtual_appkey": gateway.app_key,
                    "virtual_secretkey": gateway.app_secret
                })
            else:
                kis_args.update({"appkey": gateway.app_key, "secretkey": gateway.app_secret})

            kis = PyKis(**kis_args)
            self.clients[gateway.gateway_name] = kis
            account = kis.account(full_account)
            self.accounts[gateway.gateway_name] = account
            gateway.write_log(f"Connected. Account: {full_account}")

            await asyncio.sleep(2.0) # WS Connection Wait

            # Account Execution Event
            if hasattr(account, "on"):
                try:
                    # Lambda wrapper with try-except to prevent silent crashes
                    def exec_handler(e):
                        self._on_ws_execution(gateway, account, e)
                    
                    t_exec = account.on("execution", exec_handler)
                    self.active_tickets.append(t_exec)
                except: pass

            await self._query_account_data(gateway)
            await self._query_position_data(gateway)

            # Auto Restore
            saved_symbols = self._load_saved_subscriptions(gateway.gateway_name)
            if saved_symbols:
                gateway.write_log(f"Restoring {len(saved_symbols)} subscriptions...")
                for symbol in saved_symbols:
                    req = SubscribeRequest(
                        symbol=symbol, exchange=EXCHANGE_MAP.get(gateway.loc, Exchange.KRX)
                    )
                    req.gateway_name = gateway.gateway_name
                    await self._subscribe_coro(gateway.gateway_name, req, save_to_file=False)
                    await asyncio.sleep(0.2)
                gateway.write_log("Auto-subscription completed.")

        except Exception as e:
            gateway.write_log(f"Init Failed: {e}")
            gateway.write_log(traceback.format_exc())

    # --- Contract ---
    async def _ensure_contract(self, gateway, symbol: str):
        if symbol in self.contract_cache: return
        try:
            contract = ContractData(
                symbol=symbol, exchange=EXCHANGE_MAP.get(gateway.loc, Exchange.KRX),
                name=symbol, product=Product.EQUITY,
                size=1, pricetick=1, gateway_name=gateway.gateway_name
            )
            gateway.on_contract(contract)
            self.contract_cache.add(symbol)
        except: pass

    # --- Subscription (With Snapshot) ---
    def subscribe(self, gateway_name: str, req: SubscribeRequest):
        asyncio.run_coroutine_threadsafe(self._subscribe_coro(gateway_name, req, save_to_file=True), self.loop)

    async def _subscribe_coro(self, gateway_name: str, req: SubscribeRequest, save_to_file: bool = True):
        gateway = self.gateways.get(gateway_name)
        client = self.clients.get(gateway_name)
        symbol = req.symbol
        
        if symbol in self.subscriptions[gateway_name]: return
        if len(self.subscriptions[gateway_name]) >= self.max_subscriptions: return

        await self._ensure_contract(gateway, symbol)

        try:
            target = None
            if gateway.loc == KisMarketLoc.KR: target = client.stock(symbol)
            elif gateway.loc == KisMarketLoc.US: target = client.stock(symbol)

            if target:
                # 1. Fetch Snapshot FIRST (Updates UI immediately)
                try:
                    gateway.write_log(f"[Snapshot] Fetching {symbol}...")
                    quote = await target.quote()
                    # Convert quote to tick and update
                    self._process_quote_as_tick(gateway, symbol, quote)
                except Exception as e:
                    gateway.write_log(f"[Snapshot] Failed for {symbol}: {e}")

                # 2. Bind WebSocket Events
                def price_handler(e):
                    print(f" >>> [WS] Price {symbol}")
                    self._on_ws_price(gateway, target, e)
                
                t_price = target.on("price", price_handler)
                self.active_tickets.append(t_price)
                
                if gateway.use_orderbook:
                    def book_handler(e):
                        self._on_ws_orderbook(gateway, target, e)
                    try:
                        t_book = target.on("orderbook", book_handler)
                        self.active_tickets.append(t_book)
                    except: pass
                
                self.subscriptions[gateway_name].add(symbol)
                if save_to_file: self._save_subscriptions()
                gateway.write_log(f"Subscribed: {symbol}")
                
        except Exception as e:
            gateway.write_log(f"Subscribe Error: {e}")

    # --- Data Processing ---

    def _get_or_create_tick(self, gateway, symbol) -> TickData:
        key = f"{gateway.gateway_name}:{symbol}"
        if key not in self.tick_buffer:
            self.tick_buffer[key] = TickData(
                symbol=symbol,
                exchange=EXCHANGE_MAP.get(gateway.loc, Exchange.KRX),
                datetime=datetime.now(),
                name=symbol,
                gateway_name=gateway.gateway_name
            )
        return self.tick_buffer[key]

    def _process_quote_as_tick(self, gateway, symbol, quote):
        """Convert static quote to TickData for UI init"""
        try:
            tick = self._get_or_create_tick(gateway, symbol)
            tick.datetime = datetime.now()
            
            # Field mapping depends on PyKis object structure (Check attributes safely)
            # This is a generic fallback attempt
            tick.last_price = float(getattr(quote, 'price', getattr(quote, 'last', 0)))
            tick.volume = float(getattr(quote, 'volume', getattr(quote, 'vol', 0)))
            tick.open_price = float(getattr(quote, 'open', 0))
            tick.high_price = float(getattr(quote, 'high', 0))
            tick.low_price = float(getattr(quote, 'low', 0))
            
            gateway.on_tick(copy_tick(tick))
        except: pass

    def _on_ws_price(self, gateway, sender, e):
        try:
            symbol = getattr(sender, 'symbol', '')
            if not symbol: return
            
            tick = self._get_or_create_tick(gateway, symbol)
            tick.datetime = datetime.now()
            tick.last_price = float(getattr(e, 'price', 0))
            tick.volume = float(getattr(e, 'volume', 0))
            
            if hasattr(e, 'open'): tick.open_price = float(e.open)
            if hasattr(e, 'high'): tick.high_price = float(e.high)
            if hasattr(e, 'low'): tick.low_price = float(e.low)
            
            gateway.on_tick(copy_tick(tick))
        except: pass

    def _on_ws_orderbook(self, gateway, sender, e):
        try:
            symbol = getattr(sender, 'symbol', '')
            if not symbol: return
            
            tick = self._get_or_create_tick(gateway, symbol)
            tick.datetime = datetime.now()
            
            # Attempt to map fields (safe get)
            try:
                tick.bid_price_1 = float(getattr(e, 'bid1', 0)); tick.bid_volume_1 = float(getattr(e, 'bid1_qty', 0))
                tick.ask_price_1 = float(getattr(e, 'ask1', 0)); tick.ask_volume_1 = float(getattr(e, 'ask1_qty', 0))
            except: pass

            gateway.on_tick(copy_tick(tick))
        except: pass

    def _on_ws_execution(self, gateway, sender, e):
        try:
            kis_ord_no = str(getattr(e, 'order_number', ''))
            if not kis_ord_no: return
            
            local_orderid = ""
            for lid, kid in self.local_order_map.items():
                if kid == kis_ord_no:
                    local_orderid = lid
                    break
            
            if not local_orderid: return 

            req = self.kis_order_map.get(kis_ord_no)
            exec_qty = int(getattr(e, 'exec_qty', 0))
            accum_qty = int(getattr(e, 'accum_exec_qty', getattr(e, 'qty', 0)))
            price = float(getattr(e, 'exec_price', getattr(e, 'price', 0)))
            
            order = req.create_order_data(local_orderid, gateway.gateway_name)
            if accum_qty >= req.volume: order.status = Status.ALLTRADED
            elif accum_qty > 0: order.status = Status.PARTTRADED
            else: order.status = Status.NOTTRADED
            
            order.traded = accum_qty
            gateway.on_order(order)

            if exec_qty > 0:
                trade = TradeData(
                    symbol=req.symbol, exchange=req.exchange, orderid=local_orderid,
                    tradeid=str(time.time()), direction=req.direction, offset=req.offset,
                    price=price, volume=exec_qty, datetime=datetime.now(),
                    gateway_name=gateway.gateway_name
                )
                gateway.on_trade(trade)
                self.query_account(gateway.gateway_name)
                self.query_position(gateway.gateway_name)
        except: pass

    # --- Query & Order ---
    def query_account(self, gateway_name: str):
        gateway = self.gateways.get(gateway_name)
        if gateway: asyncio.run_coroutine_threadsafe(self._query_account_data(gateway), self.loop)

    def query_position(self, gateway_name: str):
        gateway = self.gateways.get(gateway_name)
        if gateway: asyncio.run_coroutine_threadsafe(self._query_position_data(gateway), self.loop)

    async def _query_account_data(self, gateway: 'KisBaseGateway'):
        try:
            account = self.accounts[gateway.gateway_name]
            resp = account.balance() 
            acc_id = f"{gateway.account_no}-{gateway.account_code}@{gateway.gateway_name}"
            bal = float(getattr(resp, 'dnca_tot_amt', 0)) if gateway.loc == KisMarketLoc.KR else float(getattr(resp, 'frcr_dncl_amt_2', 0))
            gateway.on_account(AccountData(accountid=acc_id, balance=bal, frozen=0.0, gateway_name=gateway.gateway_name))
        except: pass

    async def _query_position_data(self, gateway: 'KisBaseGateway'):
        try:
            account = self.accounts[gateway.gateway_name]
            resp = account.balance() 
            items = getattr(resp, 'stocks', [])
            if gateway.market == KisMarketType.FUT: items = getattr(resp, 'futures', [])

            for item in items:
                qty = float(getattr(item, 'qty', getattr(item, 'hldg_qty', 0)))
                if qty == 0: continue
                gateway.on_position(PositionData(
                    symbol=getattr(item, 'symbol', ''),
                    exchange=EXCHANGE_MAP.get(gateway.loc, Exchange.KRX),
                    direction=Direction.LONG if qty > 0 else Direction.SHORT,
                    volume=abs(qty), price=float(getattr(item, 'price', 0)),
                    pnl=float(getattr(item, 'profit', 0)), gateway_name=gateway.gateway_name
                ))
        except: pass

    def send_order(self, gateway_name: str, req: OrderRequest, local_orderid: str):
        asyncio.run_coroutine_threadsafe(self._process_order(gateway_name, req, local_orderid), self.loop)

    async def _process_order(self, gateway_name: str, req: OrderRequest, local_orderid: str):
        gateway = self.gateways.get(gateway_name)
        account = self.accounts.get(gateway_name)
        if not gateway: return
        try:
            target = None
            if gateway.loc == KisMarketLoc.KR:
                if gateway.market == KisMarketType.SPOT: target = account.stock(req.symbol)
                elif gateway.market == KisMarketType.FUT: target = account.future(req.symbol)
            elif gateway.loc == KisMarketLoc.US: target = account.stock(req.symbol)

            if not target: raise Exception("Invalid Target")
            qty = int(req.volume)
            price = int(req.price) if req.price else 0
            
            resp = None
            if req.direction == Direction.LONG: resp = await target.buy(amount=qty, price=price)
            else: resp = await target.sell(amount=qty, price=price)
            
            if resp and hasattr(resp, 'order_number'):
                kis_ord = str(resp.order_number)
                self.local_order_map[local_orderid] = kis_ord
                self.kis_order_map[kis_ord] = req
                ord_data = req.create_order_data(local_orderid, gateway_name)
                ord_data.status = Status.NOTTRADED
                gateway.on_order(ord_data)
                gateway.write_log(f"Order Sent: {local_orderid} (KIS: {kis_ord})")
            else:
                raise Exception(f"Failed: {getattr(resp, 'message', 'Unknown')}")
        except Exception as e:
            gateway.write_log(f"Order Error: {e}")
            ord_data = req.create_order_data(local_orderid, gateway_name)
            ord_data.status = Status.REJECTED
            gateway.on_order(ord_data)

    def cancel_order(self, gateway_name: str, req: CancelRequest):
        asyncio.run_coroutine_threadsafe(self._process_cancel(gateway_name, req), self.loop)

    async def _process_cancel(self, gateway_name: str, req: CancelRequest):
        gateway = self.gateways.get(gateway_name)
        kis_ord = self.local_order_map.get(req.orderid)
        if not kis_ord: return
        try:
            account = self.accounts[gateway_name]
            if gateway.loc == KisMarketLoc.KR: await account.cancel(order_number=kis_ord, amount=0, total=True)
            elif gateway.loc == KisMarketLoc.US: await account.cancel_overseas(order_number=kis_ord, amount=0)
            gateway.write_log(f"Cancel Sent: {req.orderid}")
        except Exception as e:
            gateway.write_log(f"Cancel Error: {e}")

def copy_tick(tick: TickData) -> TickData:
    new_tick = TickData(
        symbol=tick.symbol, exchange=tick.exchange, datetime=tick.datetime,
        name=tick.name, volume=tick.volume, last_price=tick.last_price,
        open_price=tick.open_price, high_price=tick.high_price, low_price=tick.low_price,
        gateway_name=tick.gateway_name
    )
    new_tick.bid_price_1 = tick.bid_price_1; new_tick.bid_volume_1 = tick.bid_volume_1
    new_tick.ask_price_1 = tick.ask_price_1; new_tick.ask_volume_1 = tick.ask_volume_1
    return new_tick

# ------------------------------------------------------------------------------
# 3. Gateway Classes
# ------------------------------------------------------------------------------

class KisBaseGateway(BaseGateway):
    default_setting = {
        "htsid": "", "app_key": "", "app_secret": "",
        "account_no": "", "account_code": "01", 
        "server": ["REAL", "DEMO"], "max_subs": 40, "use_orderbook": True 
    }

    def __init__(self, event_engine: EventEngine, gateway_name: str):
        super().__init__(event_engine, gateway_name)
        self.backend = KisBackend()
        self.htsid = ""; self.app_key = ""; self.app_secret = ""
        self.account_no = ""; self.account_code = ""; 
        self.server_type = KisServer.REAL
        self.max_subs = 40; self.use_orderbook = True 
        self.market = KisMarketType.SPOT; self.loc = KisMarketLoc.KR
        self.order_count = 0; self.exchanges = [Exchange.KRX]

    def connect(self, setting: dict):
        self.htsid = setting.get("htsid", "")
        self.app_key = setting.get("app_key", "")
        self.app_secret = setting.get("app_secret", "")
        self.account_no = setting.get("account_no", "")
        self.account_code = setting.get("account_code", "01")
        self.max_subs = setting.get("max_subs", 40)
        self.use_orderbook = setting.get("use_orderbook", True)
        
        srv = setting.get("server", "REAL")
        self.server_type = KisServer.DEMO if srv == "DEMO" else KisServer.REAL
        
        if not self.app_key:
            self.write_log("Error: app_key required")
            return
        self.backend.register_gateway(self)

    def subscribe(self, req: SubscribeRequest):
        self.backend.subscribe(self.gateway_name, req)

    def send_order(self, req: OrderRequest):
        self.order_count += 1
        local_id = f"{self.gateway_name}.{str(self.order_count).zfill(6)}"
        order = req.create_order_data(local_id, self.gateway_name)
        order.status = Status.SUBMITTING
        self.on_order(order)
        self.backend.send_order(self.gateway_name, req, local_id)
        return order.vt_orderid

    def cancel_order(self, req: CancelRequest):
        self.backend.cancel_order(self.gateway_name, req)

    def query_account(self):
        self.backend.query_account(self.gateway_name)

    def query_position(self):
        self.backend.query_position(self.gateway_name)
    
    def close(self):
        self.backend.close_gateway(self.gateway_name)
        super().close()

# Implementation Classes
class KisKrSpotGateway(KisBaseGateway):
    default_name = "KIS_KR_SPOT"
    def __init__(self, event_engine, gateway_name="KIS_KR_SPOT"):
        super().__init__(event_engine, gateway_name)
        self.loc = KisMarketLoc.KR; self.market = KisMarketType.SPOT; self.exchanges = [Exchange.KRX]

class KisKrSpotDemoGateway(KisBaseGateway):
    default_name = "KIS_KR_SPOT_DEMO"
    default_setting = KisBaseGateway.default_setting.copy()
    default_setting["server"] = ["DEMO"]
    def __init__(self, event_engine, gateway_name="KIS_KR_SPOT_DEMO"):
        super().__init__(event_engine, gateway_name)
        self.loc = KisMarketLoc.KR; self.market = KisMarketType.SPOT; self.exchanges = [Exchange.KRX]

class KisKrFutGateway(KisBaseGateway):
    default_name = "KIS_KR_FUT"
    def __init__(self, event_engine, gateway_name="KIS_KR_FUT"):
        super().__init__(event_engine, gateway_name)
        self.loc = KisMarketLoc.KR; self.market = KisMarketType.FUT; self.exchanges = [Exchange.KRX]

class KisKrFutDemoGateway(KisBaseGateway):
    default_name = "KIS_KR_FUT_DEMO"
    default_setting = KisBaseGateway.default_setting.copy()
    default_setting["server"] = ["DEMO"]
    def __init__(self, event_engine, gateway_name="KIS_KR_FUT_DEMO"):
        super().__init__(event_engine, gateway_name)
        self.loc = KisMarketLoc.KR; self.market = KisMarketType.FUT; self.exchanges = [Exchange.KRX]

class KisUsSpotGateway(KisBaseGateway):
    default_name = "KIS_US_SPOT"
    def __init__(self, event_engine, gateway_name="KIS_US_SPOT"):
        super().__init__(event_engine, gateway_name)
        self.loc = KisMarketLoc.US; self.market = KisMarketType.SPOT; self.exchanges = [Exchange.NASDAQ, Exchange.NYSE]

class KisIsaGateway(KisBaseGateway):
    default_name = "KIS_ISA"
    default_setting = {
        "htsid": "", "app_key": "", "app_secret": "", "account_no": "", "account_code": "14", 
        "server": ["REAL"], "max_subs": 40, "use_orderbook": True
    }
    def __init__(self, event_engine, gateway_name="KIS_ISA"):
        super().__init__(event_engine, gateway_name)
        self.loc = KisMarketLoc.KR; self.market = KisMarketType.SPOT; self.exchanges = [Exchange.KRX]