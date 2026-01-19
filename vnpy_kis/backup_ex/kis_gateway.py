"""
VNPY Gateway for KIS Open API (Bug Fix: WS Init & Account ID)
Path: vnpy_kis/kis_gateway.py
"""
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from copy import copy
import traceback

from vnpy.event import EventEngine
from vnpy.trader.constant import (
    Direction, Exchange, OrderType, Product, Status, OptionType
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData, OrderData, TradeData, PositionData, AccountData,
    ContractData, OrderRequest, CancelRequest, SubscribeRequest
)
from vnpy.trader.event import EVENT_TIMER

# --- Global Modules Initialization ---
ka = None
ds = None
dsws = None
osf = None
osfws = None
dfo = None
dfows = None
ofo = None
ofows = None

# --- Import User Modules from 'vnpy_kis.kis_api' package ---
try:
    from vnpy_kis.kis_api import kis_auth as ka
    from vnpy_kis.kis_api import domestic_stock_functions as ds
    from vnpy_kis.kis_api import domestic_stock_functions_ws as dsws
    from vnpy_kis.kis_api import overseas_stock_functions as osf
    from vnpy_kis.kis_api import overseas_stock_functions_ws as osfws
    from vnpy_kis.kis_api import domestic_futureoption_functions as dfo
    from vnpy_kis.kis_api import domestic_futureoption_functions_ws as dfows
    from vnpy_kis.kis_api import overseas_futureoption_functions as ofo
    from vnpy_kis.kis_api import overseas_futureoption_functions_ws as ofows
except ImportError as e:
    print(f"\n[Critical Error] KIS 모듈 Import 실패: {e}")

# --- Constants & Maps ---
EXCHANGE_MAP_KIS_TO_VNPY = {
    "NASD": Exchange.NASDAQ, "NAS": Exchange.NASDAQ,
    "NYSE": Exchange.NYSE, "NYS": Exchange.NYSE,
    "AMEX": Exchange.AMEX, "AMS": Exchange.AMEX,
    "CME": Exchange.CME,
}
EXCHANGE_MAP_VNPY_TO_KIS = {v: k for k, v in EXCHANGE_MAP_KIS_TO_VNPY.items()}


# =============================================================================
# [Base Gateway]
# =============================================================================
class KisBaseGateway(BaseGateway):
    default_setting = {
        "account_no": "", "account_code": "", "server": ["REAL", "DEMO"]
    }

    def __init__(self, event_engine: EventEngine, gateway_name: str, loc: str, asset: str):
        super().__init__(event_engine, gateway_name)
        self.loc = loc      # "KR", "US"
        self.asset = asset  # "SPOT", "FUT"
        self.cano = ""
        self.acnt_prdt_cd = ""
        self.env = "real"
        
        self.td_api = KisTdApi(self)
        self.md_api = KisMdApi(self)

    def connect(self, setting: Dict[str, Any]):
        if ka is None:
            self.write_log("KIS 필수 모듈 로드 실패. 경로를 확인하세요.")
            return

        self.cano = setting.get("account_no", "")
        self.acnt_prdt_cd = setting.get("account_code", "01")
        self.env = "real" if setting.get("server", "REAL") == "REAL" else "demo"

        self.write_log(f"[{self.gateway_name}] Connecting... Loc:{self.loc}, Asset:{self.asset}, Env:{self.env}")

        try:
            svr = "prod" if self.env == "real" else "vps"
            ka.auth(svr=svr, product=self.acnt_prdt_cd)
            ka.auth_ws(svr=svr, product=self.acnt_prdt_cd)
            self.write_log("Authentication Passed")
            
            self.td_api.connect()
            self.md_api.connect()
            self.init_query()
            
        except Exception as e:
            self.write_log(f"Auth or Connect Failed: {e}")
            traceback.print_exc()

    def subscribe(self, req: SubscribeRequest):
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest):
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        self.td_api.cancel_order(req)

    def query_account(self):
        self.td_api.query_account()

    def query_position(self):
        self.td_api.query_position()

    def close(self):
        self.md_api.close()
        super().close()

    def init_query(self):
        self.count = 0
        self.event_engine.register(EVENT_TIMER, self.process_timer)

    def process_timer(self, event):
        self.count += 1
        if self.count < 10: return
        self.count = 0
        self.query_account()
        self.query_position()


# =============================================================================
# [Gateway Implementations]
# =============================================================================
class KisKrGateway(KisBaseGateway):
    default_name = "KIS_KR"
    exchanges = [Exchange.KRX]
    def __init__(self, event_engine: EventEngine, gateway_name: str = "KIS_KR"):
        super().__init__(event_engine, gateway_name, loc="KR", asset="SPOT")

class KisKrMockGateway(KisBaseGateway):
    default_name = "KIS_KR_MOCK"
    default_setting = {"account_no": "", "account_code": "", "server": ["DEMO"]}
    exchanges = [Exchange.KRX]
    def __init__(self, event_engine: EventEngine, gateway_name: str = "KIS_KR_MOCK"):
        super().__init__(event_engine, gateway_name, loc="KR", asset="SPOT")

class KisIsaGateway(KisBaseGateway):
    default_name = "KIS_ISA"
    exchanges = [Exchange.KRX]
    def __init__(self, event_engine: EventEngine, gateway_name: str = "KIS_ISA"):
        super().__init__(event_engine, gateway_name, loc="KR", asset="SPOT")

class KisUsGateway(KisBaseGateway):
    default_name = "KIS_US"
    exchanges = [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]
    def __init__(self, event_engine: EventEngine, gateway_name: str = "KIS_US"):
        super().__init__(event_engine, gateway_name, loc="US", asset="SPOT")

class KisUsMockGateway(KisBaseGateway):
    default_name = "KIS_US_MOCK"
    default_setting = {"account_no": "", "account_code": "", "server": ["DEMO"]}
    exchanges = [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX]
    def __init__(self, event_engine: EventEngine, gateway_name: str = "KIS_US_MOCK"):
        super().__init__(event_engine, gateway_name, loc="US", asset="SPOT")

class KisKrFutGateway(KisBaseGateway):
    default_name = "KIS_KRFUT"
    exchanges = [Exchange.KRX]
    def __init__(self, event_engine: EventEngine, gateway_name: str = "KIS_KRFUT"):
        super().__init__(event_engine, gateway_name, loc="KR", asset="FUT")

class KisKrFutMockGateway(KisBaseGateway):
    default_name = "KIS_KRFUT_MOCK"
    default_setting = {"account_no": "", "account_code": "", "server": ["DEMO"]}
    exchanges = [Exchange.KRX]
    def __init__(self, event_engine: EventEngine, gateway_name: str = "KIS_KRFUT_MOCK"):
        super().__init__(event_engine, gateway_name, loc="KR", asset="FUT")

class KisUsFutGateway(KisBaseGateway):
    default_name = "KIS_USFUT"
    exchanges = [Exchange.CME]
    def __init__(self, event_engine: EventEngine, gateway_name: str = "KIS_USFUT"):
        super().__init__(event_engine, gateway_name, loc="US", asset="FUT")

class KisUsFutMockGateway(KisBaseGateway):
    default_name = "KIS_USFUT_MOCK"
    default_setting = {"account_no": "", "account_code": "", "server": ["DEMO"]}
    exchanges = [Exchange.CME]
    def __init__(self, event_engine: EventEngine, gateway_name: str = "KIS_USFUT_MOCK"):
        super().__init__(event_engine, gateway_name, loc="US", asset="FUT")


# =============================================================================
# [Unified Trade API]
# =============================================================================
class KisTdApi:
    def __init__(self, gateway: KisBaseGateway):
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_count = 0
        self.local_map = {} 

    def connect(self):
        if any(x is None for x in [ds, osf, dfo, ofo]): return
        threading.Thread(target=self._init_data).start()

    def _init_data(self):
        time.sleep(1)
        self.query_account()
        self.query_position()

    def query_account(self):
        threading.Thread(target=self._query_account_blocking).start()

    def _query_account_blocking(self):
        try:
            loc, asset = self.gateway.loc, self.gateway.asset
            
            if loc == "KR" and asset == "SPOT":
                self._query_kr_spot_account()
            elif loc == "US" and asset == "SPOT":
                self._query_us_spot_account()
            elif loc == "KR" and asset == "FUT":
                self._query_kr_fut_account()
            elif loc == "US" and asset == "FUT":
                self._query_us_fut_account()
        except Exception as e:
            self.gateway.write_log(f"Account Query Error: {e}")

    # --- Account Query Logic ---
    def _query_kr_spot_account(self):
        df1, df2 = ds.inquire_account_balance(self.gateway.cano, self.gateway.acnt_prdt_cd)
        if not df2.empty:
            bal = float(df2.iloc[0].get("dnca_tot_amt", 0))
            self._push_account(bal, "KRW")
        else:
            self._push_account(0, "KRW")
            self.gateway.write_log("KR Spot Account DF Empty")

    def _query_us_spot_account(self):
        # 미국 주식 예수금 (USD)
        df1, df2, df3 = osf.inquire_present_balance(
            self.gateway.cano, self.gateway.acnt_prdt_cd, "02", "840", "00", "00", self.gateway.env
        )
        if not df2.empty:
            # frcr_drwg_psbl_amt_1 (외화인출가능금액) 또는 frcr_dncl_amt_2 (외화예수금)
            bal = float(df2.iloc[0].get("frcr_drwg_psbl_amt_1", 0))
            self._push_account(bal, "USD")
        else:
            self._push_account(0.0, "USD")
            self.gateway.write_log("US Spot Account DF Empty")

    def _query_kr_fut_account(self):
        df = dfo.inquire_deposit(self.gateway.cano, self.gateway.acnt_prdt_cd)
        if not df.empty:
            bal = float(df.iloc[0].get("fncg_tot_amt", 0))
            self._push_account(bal, "KRW")
        else:
            self._push_account(0.0, "KRW")
            self.gateway.write_log("KR Fut Account DF Empty")

    def _query_us_fut_account(self):
        df = ofo.inquire_deposit(self.gateway.cano, self.gateway.acnt_prdt_cd, "USD", datetime.now().strftime("%Y%m%d"))
        if df is not None and not df.empty:
            bal = float(df.iloc[0].get("tot_wl_amt", 0))
            self._push_account(bal, "USD")
        else:
            self._push_account(0.0, "USD")
            self.gateway.write_log("US Spot Account DF Empty")

    def _push_account(self, balance: float, currency: str):
        # [중요] 계좌 ID를 유니크하게 만들기 위해 Suffix 추가
        # 예: 12345678-KR, 12345678-US
        # 이렇게 해야 VNPY UI에서 덮어씌워지지 않고 여러 줄로 나옵니다.
        suffix = self.gateway.gateway_name.split('_')[-1] # KR, US, FUT...
        
        acct = AccountData(
            accountid=f"{self.gateway.cano}-{suffix}",
            balance=balance,
            frozen=0,
            gateway_name=self.gateway_name
        )
        self.gateway.on_account(acct)
        # 디버그 로그: 실제 잔고가 푸시되었는지 확인
        # self.gateway.write_log(f"Account Update: {acct.accountid} Balance: {balance}")

    def query_position(self):
        threading.Thread(target=self._query_position_blocking).start()

    def _query_position_blocking(self):
        try:
            loc, asset = self.gateway.loc, self.gateway.asset
            if loc == "KR" and asset == "SPOT":
                self._query_kr_spot_pos()
            elif loc == "US" and asset == "SPOT":
                self._query_us_spot_pos()
            elif loc == "KR" and asset == "FUT":
                self._query_kr_fut_pos()
            elif loc == "US" and asset == "FUT":
                self._query_us_fut_pos()
        except Exception as e:
            self.gateway.write_log(f"Pos Query Error: {e}")

    def _query_kr_spot_pos(self):
        df, _ = ds.inquire_balance(self.gateway.env, self.gateway.cano, self.gateway.acnt_prdt_cd, "N", "02", "01", "N", "N", "00")
        if not df.empty:
            for _, row in df.iterrows():
                pos = PositionData(
                    symbol=row['pdno'], exchange=Exchange.KRX, direction=Direction.LONG,
                    volume=float(row['hldg_qty']), price=float(row['pchs_avg_pric']),
                    pnl=float(row['evlu_pfls_amt']), gateway_name=self.gateway_name
                )
                self.gateway.on_position(pos)

    def _query_us_spot_pos(self):
        df, _, _ = osf.inquire_present_balance(
            self.gateway.cano, self.gateway.acnt_prdt_cd, "02", "000", "00", "00", self.gateway.env
        )
        if not df.empty:
            for _, row in df.iterrows():
                kis_exch = row.get('ovrs_excg_cd', 'NASD')
                vn_exch = EXCHANGE_MAP_KIS_TO_VNPY.get(kis_exch, Exchange.NASDAQ)
                pos = PositionData(
                    symbol=row['pdno'], exchange=vn_exch, direction=Direction.LONG,
                    volume=float(row['ccld_qty_smtl1']), price=float(row['pchs_avg_pric']),
                    pnl=float(row['frcr_evlu_pfls_amt']), gateway_name=self.gateway_name
                )
                self.gateway.on_position(pos)

    def _query_kr_fut_pos(self):
        df, _ = dfo.inquire_balance(self.gateway.env, self.gateway.cano, self.gateway.acnt_prdt_cd, "01", "1")
        if not df.empty:
            for _, row in df.iterrows():
                pos = PositionData(
                    symbol=row['pdno'], exchange=Exchange.KRX, direction=Direction.NET,
                    volume=float(row.get('hldg_qty', 0)), price=float(row.get('pchs_avg_pric', 0)),
                    gateway_name=self.gateway_name
                )
                self.gateway.on_position(pos)

    def _query_us_fut_pos(self):
        df = ofo.inquire_unpd(self.gateway.cano, self.gateway.acnt_prdt_cd, "00", "", "")
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                direction = Direction.LONG if row['sll_buy_dvsn_cd'] == '02' else Direction.SHORT
                pos = PositionData(
                    symbol=row['ovrs_futr_fx_pdno'], exchange=Exchange.CME, direction=direction,
                    volume=float(row['ccld_qty']), price=float(row['ord_unpr']),
                    gateway_name=self.gateway_name
                )
                self.gateway.on_position(pos)

    def send_order(self, req: OrderRequest):
        self.order_count += 1
        orderid = f"{self.gateway_name}.{self.order_count}"
        order = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)
        threading.Thread(target=self._send_order_blocking, args=(req, order)).start()
        return order.vt_orderid

    def _send_order_blocking(self, req: OrderRequest, order: OrderData):
        try:
            loc, asset = self.gateway.loc, self.gateway.asset
            kis_odno = ""
            if loc == "KR" and asset == "SPOT":
                kis_odno = self._order_kr_spot(req)
            elif loc == "US" and asset == "SPOT":
                kis_odno = self._order_us_spot(req)
            elif loc == "KR" and asset == "FUT":
                kis_odno = self._order_kr_fut(req)
            elif loc == "US" and asset == "FUT":
                kis_odno = self._order_us_fut(req)

            if kis_odno:
                self.local_map[order.orderid] = kis_odno
                order.status = Status.NOTTRADED
                self.gateway.write_log(f"Order Placed. ID: {kis_odno}")
            else:
                order.status = Status.REJECTED
                self.gateway.write_log("Order Rejected (No ID)")
        except Exception as e:
            order.status = Status.REJECTED
            self.gateway.write_log(f"Order Exception: {e}")
        self.gateway.on_order(order)

    def _order_kr_spot(self, req):
        ord_dv = "buy" if req.direction == Direction.LONG else "sell"
        ord_dvsn = "01" if req.type == OrderType.MARKET else "00"
        df = ds.order_cash(self.gateway.env, ord_dv, self.gateway.cano, self.gateway.acnt_prdt_cd, req.symbol, ord_dvsn, str(int(req.volume)), str(int(req.price)), "SOR")
        if not df.empty: return str(df.iloc[0]['ODNO'])
        return ""

    def _order_us_spot(self, req):
        kis_exch = EXCHANGE_MAP_VNPY_TO_KIS.get(req.exchange, "NASD")
        ord_dv = "buy" if req.direction == Direction.LONG else "sell"
        df = osf.order(self.gateway.cano, self.gateway.acnt_prdt_cd, kis_exch, req.symbol, str(int(req.volume)), str(req.price), ord_dv, "", "", "0", "00", self.gateway.env)
        if not df.empty: return str(df.iloc[0]['ODNO'])
        return ""

    def _order_kr_fut(self, req):
        sll_buy = "02" if req.direction == Direction.LONG else "01"
        df = dfo.order(self.gateway.env, "day", "02", self.gateway.cano, self.gateway.acnt_prdt_cd, sll_buy, req.symbol, str(int(req.volume)), str(req.price), "01", "0", "01")
        if not df.empty: return str(df.iloc[0]['ODNO'])
        return ""

    def _order_us_fut(self, req):
        sll_buy = "02" if req.direction == Direction.LONG else "01"
        pric_dvsn = "2" if req.type == OrderType.MARKET else "1"
        df = ofo.order(self.gateway.cano, self.gateway.acnt_prdt_cd, req.symbol, sll_buy, "", "", pric_dvsn, str(req.price), "", str(int(req.volume)), "", "", "6", "0", "N", "N")
        if df is not None and not df.empty: return str(df.iloc[0]['ODNO'])
        return ""

    def cancel_order(self, req: CancelRequest):
        threading.Thread(target=self._cancel_blocking, args=(req,)).start()

    def _cancel_blocking(self, req):
        kis_odno = self.local_map.get(req.orderid)
        if not kis_odno: return
        try:
            loc, asset = self.gateway.loc, self.gateway.asset
            if loc == "KR" and asset == "SPOT":
                ds.order_rvsecncl(self.gateway.env, self.gateway.cano, self.gateway.acnt_prdt_cd, "", kis_odno, "00", "02", "0", "0", "Y", "KRX")
            elif loc == "US" and asset == "SPOT":
                kis_exch = EXCHANGE_MAP_VNPY_TO_KIS.get(req.exchange, "NASD")
                osf.order_rvsecncl(self.gateway.cano, self.gateway.acnt_prdt_cd, kis_exch, req.symbol, kis_odno, "02", str(int(req.volume)), "0", "", "0", self.gateway.env)
            elif loc == "KR" and asset == "FUT":
                dfo.order_rvsecncl(self.gateway.env, "day", "02", self.gateway.cano, self.gateway.acnt_prdt_cd, "02", kis_odno, "0", "0", "01", "0", "Y", "01")
            elif loc == "US" and asset == "FUT":
                ofo.order_rvsecncl(self.gateway.cano, "1", self.gateway.acnt_prdt_cd, datetime.now().strftime("%Y%m%d"), kis_odno, "", "", "", "", "N", "N")
        except Exception as e:
            self.gateway.write_log(f"Cancel Error: {e}")


# =============================================================================
# [Unified Market Data API]
# =============================================================================
class KisMdApi:
    def __init__(self, gateway: KisBaseGateway):
        self.gateway = gateway
        self.active = False
        self.kws = None
        self.subscribed = set()
        self.ticks: Dict[str, TickData] = {}

    def connect(self):
        try:
            if ka is None: return
            # [FIXED] Pass api_url explicitly
            self.kws = ka.KISWebSocket(api_url="/tryitout")
            self.active = True
            t = threading.Thread(target=self._run)
            t.daemon = True
            t.start()
        except Exception as e:
            self.gateway.write_log(f"WS Error: {e}")

    def _run(self):
        if self.kws:
            self.kws.start(on_result=self.on_result)

    def subscribe(self, req: SubscribeRequest):
        if self.kws is None: return

        if req.symbol in self.subscribed: return
        self.subscribed.add(req.symbol)
        
        if req.symbol not in self.ticks:
            self.ticks[req.symbol] = TickData(
                symbol=req.symbol, exchange=req.exchange, datetime=datetime.now(),
                gateway_name=self.gateway.gateway_name, name=req.symbol
            )
        
        loc, asset = self.gateway.loc, self.gateway.asset
        try:
            if loc == "KR" and asset == "SPOT":
                self.kws.subscribe(request=dsws.ccnl_total, data=[req.symbol])
                self.kws.subscribe(request=dsws.asking_price_total, data=[req.symbol])
            elif loc == "US" and asset == "SPOT":
                self.kws.subscribe(request=osfws.delayed_ccnl, data=[req.symbol])
                self.kws.subscribe(request=osfws.asking_price, data=[req.symbol])
            elif loc == "KR" and asset == "FUT":
                self.kws.subscribe(request=dfows.commodity_futures_realtime_conclusion, data=[req.symbol])
            elif loc == "US" and asset == "FUT":
                pass
        except Exception as e:
            self.gateway.write_log(f"Subscribe Failed: {e}")

    def on_result(self, ws, tr_id, result, data_info):
        try:
            loc, asset = self.gateway.loc, self.gateway.asset
            symbol = self._get_symbol(result, loc)
            if not symbol or symbol not in self.ticks: return

            tick = self.ticks[symbol]
            if loc == "KR" and asset == "SPOT":
                self._update_kr_spot(tick, result)
            elif loc == "US" and asset == "SPOT":
                self._update_us_spot(tick, result)
            elif loc == "KR" and asset == "FUT":
                self._update_kr_fut(tick, result)
            elif loc == "US" and asset == "FUT":
                self._update_us_fut(tick, result)
            
            if tick.datetime is None: tick.datetime = datetime.now()
            self.gateway.on_tick(copy(tick))
        except Exception:
            pass

    def _get_symbol(self, result: dict, loc: str) -> Optional[str]:
        if loc == "KR":
            return result.get("MKSC_SHRN_ISCD") or result.get("ISCD") or result.get("optn_shrn_iscd")
        else: 
            return result.get("symb") or result.get("symbol") or result.get("key_user_symb")

    def _update_kr_spot(self, tick: TickData, data: dict):
        if "STCK_PRPR" in data:
            tick.last_price = float(data["STCK_PRPR"])
            tick.volume = float(data.get("ACML_VOL", tick.volume))
            tick.open_price = float(data.get("STCK_OPRC", tick.open_price))
            tick.high_price = float(data.get("STCK_HGPR", tick.high_price))
            tick.low_price = float(data.get("STCK_LWPR", tick.low_price))
            if "STCK_CNCNT_HOUR" in data: self._parse_time(tick, data["STCK_CNCNT_HOUR"])
        elif "askp1" in data or "ASKP1" in data:
            for i in range(1, 6):
                bp = data.get(f"bidp{i}") or data.get(f"BIDP{i}")
                ap = data.get(f"askp{i}") or data.get(f"ASKP{i}")
                bv = data.get(f"bidp_rsqn{i}") or data.get(f"BIDP_RSQN{i}")
                av = data.get(f"askp_rsqn{i}") or data.get(f"ASKP_RSQN{i}")
                if bp: setattr(tick, f"bid_price_{i}", float(bp))
                if ap: setattr(tick, f"ask_price_{i}", float(ap))
                if bv: setattr(tick, f"bid_volume_{i}", float(bv))
                if av: setattr(tick, f"ask_volume_{i}", float(av))

    def _update_us_spot(self, tick: TickData, data: dict):
        if "last" in data:
            tick.last_price = float(data["last"])
            tick.volume = float(data.get("vol", tick.volume))
        if "p_bid1" in data or "bidp" in data:
            bp = data.get("p_bid1") or data.get("bidp")
            ap = data.get("p_ask1") or data.get("askp")
            bv = data.get("v_bid1") or data.get("bidv")
            av = data.get("v_ask1") or data.get("askv")
            if bp: tick.bid_price_1 = float(bp)
            if ap: tick.ask_price_1 = float(ap)
            if bv: tick.bid_volume_1 = float(bv)
            if av: tick.ask_volume_1 = float(av)

    def _update_kr_fut(self, tick: TickData, data: dict):
        price = data.get("futs_prpr") or data.get("prpr") or data.get("ndtp_prpr")
        if price:
            tick.last_price = float(price)
            tick.volume = float(data.get("acml_vol", tick.volume))
            tick.open_price = float(data.get("oprc", tick.open_price))
            tick.high_price = float(data.get("hgpr", tick.high_price))
            tick.low_price = float(data.get("lwpr", tick.low_price))
            oi = data.get("open_int") or data.get("otst_stpl_qty")
            if oi: tick.open_interest = float(oi)
            if "cntr_hour" in data: self._parse_time(tick, data["cntr_hour"])
        if "askp1" in data or "ASKP1" in data:
            for i in range(1, 6):
                bp = data.get(f"bidp{i}") or data.get(f"BIDP{i}")
                ap = data.get(f"askp{i}") or data.get(f"ASKP{i}")
                bv = data.get(f"bidp_qty{i}") or data.get(f"BIDP_QTY{i}")
                av = data.get(f"askp_qty{i}") or data.get(f"ASKP_QTY{i}")
                if bp: setattr(tick, f"bid_price_{i}", float(bp))
                if ap: setattr(tick, f"ask_price_{i}", float(ap))
                if bv: setattr(tick, f"bid_volume_{i}", float(bv))
                if av: setattr(tick, f"ask_volume_{i}", float(av))

    def _update_us_fut(self, tick: TickData, data: dict):
        if "last" in data:
            tick.last_price = float(data["last"])
            tick.volume = float(data.get("vol", tick.volume))
        bp = data.get("bidp") or data.get("p_bid1")
        ap = data.get("askp") or data.get("p_ask1")
        bv = data.get("v_bid1") or data.get("bidv")
        av = data.get("v_ask1") or data.get("v_ask1")
        if bp: tick.bid_price_1 = float(bp)
        if ap: tick.ask_price_1 = float(ap)
        if bv: tick.bid_volume_1 = float(bv)
        if av: tick.ask_volume_1 = float(av)

    def _parse_time(self, tick: TickData, time_str: str):
        try:
            now = datetime.now()
            if len(time_str) == 6:
                tick.datetime = now.replace(
                    hour=int(time_str[0:2]),
                    minute=int(time_str[2:4]),
                    second=int(time_str[4:6]),
                    microsecond=0
                )
        except: pass

    def close(self):
        self.active = False