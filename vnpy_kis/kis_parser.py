"""
KIS API 응답 파서 — REST/WebSocket 데이터를 vn.py 표준 객체로 변환

- parse_tick: 실시간 시세 → TickData (MCP 실시간시세: ccnl_krx, ccnl_nxt, delayed_ccnl 등)
- parse_order_notice: 체결/주문 통보 → OrderData, TradeData (MCP: ccnl_notice)
- parse_contract_info: 종목검색 응답 → ContractData (MCP 종목정보/기본시세)
- parse_history_bar: 차트 응답 → BarData (MCP 기본시세 inquire_daily_itemchartprice 등)

MCP Reference (한국투자 코딩도우미 MCP):
  - 국내주식 실시간: search_domestic_stock_api, subcategory="실시간시세" (ccnl_krx, ccnl_nxt, ccnl_notice)
  - 국내선물옵션: search_domestic_futureoption_api, subcategory="실시간시세"
  - 해외주식 실시간: search_overseas_stock_api, subcategory="실시간시세" (delayed_ccnl, ccnl_notice)
  - TR ID·필드 순서는 공식 실시간시세 문서/CSV 또는 read_source_code(url_main)로 확인
"""
from datetime import datetime, timedelta, time
from typing import Dict, List, Tuple, Optional, Union, Any

from vnpy.trader.object import TickData, OrderData, TradeData, PositionData, AccountData, ContractData, BarData
from vnpy.trader.constant import Exchange, Product, OptionType, Direction, Offset, Status, OrderType, Interval
from vnpy.trader.database import DB_TZ

try:
    from .kis_api_helper import AssetType, KisApiHelper, KisConfig
except ImportError:
    from vnpy_kis.kis_api_helper import AssetType, KisApiHelper, KisConfig

KIS_TZ = DB_TZ 
# --------------------------------------------------------------------------------
# 유틸리티
# --------------------------------------------------------------------------------
def _parse_time_hhmmss(time_str: str, use_today: bool = True) -> datetime:
    """HHMMSS 문자열을 KIS_TZ 기준 datetime으로 변환. 실패 시 now(KIS_TZ)."""
    try:
        t_obj = datetime.strptime(time_str.strip(), "%H%M%S").time()
        if use_today:
            now_dt = datetime.now(KIS_TZ)
            return datetime.combine(now_dt.date(), t_obj, tzinfo=KIS_TZ)
        return datetime.now(KIS_TZ).replace(hour=t_obj.hour, minute=t_obj.minute, second=t_obj.second, microsecond=0)
    except Exception:
        return datetime.now(KIS_TZ)


def _safe_float(value: Any) -> float:
    try:
        if isinstance(value, str): 
            return float(value.replace(',', '').strip())
        return float(value)
    except: 
        return 0.0

def _safe_int(value: Any) -> int:
    try:
        if isinstance(value, str): 
            return int(value.replace(',', '').strip())
        return int(value)
    except: 
        return 0

def get_krx_pricetick(price: float, product_type: str = "STOCK") -> float:
    """
    국내 자산 가격대별 호가단위(Tick Size) 계산
    """
    if price <= 0: return 0.0
    
    # [주식] 코스피 기준 (약식)
    if product_type in [ "STOCK", "KOSPI"]:
        if price < 1000: return 1.0
        if price < 5000: return 5.0
        if price < 10000: return 10.0
        if price < 50000: return 50.0
        if price < 100000: return 100.0
        if price < 500000: return 500.0
        return 1000.0
    
    # [주식] 코스닥
    elif product_type == "KOSDAQ":
        if price < 1000: return 1.0
        if price < 5000: return 5.0
        if price < 10000: return 10.0
        if price < 50000: return 50.0
        return 100.0

    # [선물] KOSPI200 선물 기준 (0.05)
    elif product_type == "FUTURES":
        return 0.05
    
    # [옵션] KOSPI200 옵션 기준
    elif product_type == "OPTION":
        if price < 3.0: return 0.01
        return 0.05
        
    return 1.0

# --------------------------------------------------------------------------------
# KIS Parser 클래스
# --------------------------------------------------------------------------------
class KisParser:
    """
    KIS API(REST/WebSocket) 데이터를 vn.py 표준 객체로 변환.
    Supports: KRX/NXT/SOR 주식, 국내 선물/옵션/채권/지수, 야간(Eurex), 해외 주식/선물.
    """

    # 실시간 시세 TR ID (MCP 실시간시세 문서와 일치)
    TR_KR_STOCK_CCNL = ("H0STCNT0", "H0NXCNT0", "H0UNCNT0")   # domestic_stock: ccnl_krx, ccnl_nxt
    TR_KR_STOCK_HOKA = ("H0STASP0", "H0NXASP0", "H0UNASP0")
    TR_KR_FUTOPT_CCNL = ("H0IFCNT0", "H0IOCNT0")
    TR_KR_FUTOPT_HOKA = ("H0IFASP0", "H0IOASP0")
    TR_KR_BOND_CCNL = ("H0BJCNT0",)
    TR_KR_BOND_HOKA = ("H0BJASP0",)
    TR_KR_INDEX_CCNL = ("H0UPCNT0",)
    TR_NIGHT_FUT_CCNL = ("H0MFCNT0", "H0EUCNT0", "ECEUCNT0")
    TR_OS_STOCK_CCNL = ("HDFSCNT0", "HDFSC203")               # overseas_stock: delayed_ccnl
    TR_OS_STOCK_HOKA = ("HDFSASP0",)
    TR_OS_FUT_CCNL = ("HDFFF020",)
    TR_OS_FUT_HOKA = ("HDFFF010",)

    # 체결통보 방향 코드 (SLL_BUY_DVSN_CD)
    DIR_SELL = "01"
    DIR_BUY = "02"
    # 체결여부 (cntg_yn): 1=정정/취소, 2=체결
    CNTG_CANCEL_OR_REV = "1"
    CNTG_FILL = "2"

    # ----------------------------------------------------------------------------
    # 1. WebSocket Tick (실시간 시세) — MCP: 실시간시세
    # ----------------------------------------------------------------------------
    @staticmethod
    def parse_tick(gateway_name: str, tr_id: str, raw_msg: str) -> Optional[TickData]:
        try:
            tokens = raw_msg.split("|")
            if len(tokens) < 4:
                return None
            f = tokens[3].split("^")

            if tr_id in KisParser.TR_KR_STOCK_CCNL:
                return KisParser._parse_kr_stock_tick(gateway_name, f, tr_id)
            if tr_id in KisParser.TR_KR_STOCK_HOKA:
                return KisParser._parse_kr_stock_hoka(gateway_name, f)
            if tr_id in KisParser.TR_KR_FUTOPT_CCNL:
                return KisParser._parse_kr_future_tick(gateway_name, f)
            if tr_id in KisParser.TR_KR_FUTOPT_HOKA:
                return KisParser._parse_kr_future_hoka(gateway_name, f)
            if tr_id in KisParser.TR_KR_BOND_CCNL:
                return KisParser._parse_kr_bond_tick(gateway_name, f)
            if tr_id in KisParser.TR_KR_BOND_HOKA:
                return KisParser._parse_kr_bond_hoka(gateway_name, f)
            if tr_id in KisParser.TR_KR_INDEX_CCNL:
                return KisParser._parse_kr_index_tick(gateway_name, f)
            if tr_id in KisParser.TR_NIGHT_FUT_CCNL:
                return KisParser._parse_night_future_tick(gateway_name, f, tr_id)
            if tr_id in KisParser.TR_OS_STOCK_CCNL:
                return KisParser._parse_os_stock_tick(gateway_name, f)
            if tr_id in KisParser.TR_OS_STOCK_HOKA:
                return KisParser._parse_os_stock_hoka(gateway_name, f)
            if tr_id in KisParser.TR_OS_FUT_CCNL:
                return KisParser._parse_os_future_tick(gateway_name, f)
            if tr_id in KisParser.TR_OS_FUT_HOKA:
                return KisParser._parse_os_future_hoka(gateway_name, f)

            return None
        except Exception:
            return None

    @staticmethod
    def _parse_kr_stock_tick(gateway_name: str, f: List[str], tr_id: str = "") -> TickData:
        """
        국내 주식(KRX, NXT, 통합) 실시간 체결가 파싱
        """
        symbol = f[0]
        # Exchange 결정
        exchange = Exchange.KRX
        if tr_id == "H0NXCNT0": exchange = Exchange.NXT
        elif tr_id == "H0UNCNT0": exchange = Exchange.SOR

        dt = _parse_time_hhmmss(f[1])
        tick = TickData(gateway_name=gateway_name, symbol=symbol, exchange=exchange, datetime=dt)
        tick.last_price = _safe_float(f[2])
        tick.volume = _safe_float(f[13])
        tick.turnover = _safe_float(f[14])
        tick.open_interest = 0
        setattr(tick, "last_volume", _safe_float(f[12]))
        
        # OHLC
        if len(f) > 9:
            tick.open_price = _safe_float(f[7])
            tick.high_price = _safe_float(f[8])
            tick.low_price = _safe_float(f[9])
        return tick

    @staticmethod
    def _parse_kr_stock_hoka(gateway_name: str, f: List[str]) -> TickData:
        dt = _parse_time_hhmmss(f[1])
        tick = TickData(gateway_name=gateway_name, symbol=f[0], exchange=Exchange.KRX, datetime=dt)
        for i in range(10): 
            idx = 3 + (i * 4)
            if len(f) > idx + 3:
                setattr(tick, f"ask_price_{i+1}", _safe_float(f[idx]))
                setattr(tick, f"bid_price_{i+1}", _safe_float(f[idx+1]))
                setattr(tick, f"ask_volume_{i+1}", _safe_float(f[idx+2]))
                setattr(tick, f"bid_volume_{i+1}", _safe_float(f[idx+3]))
        return tick

    @staticmethod
    def _parse_kr_future_tick(gateway_name: str, f: List[str]) -> TickData:
        dt = _parse_time_hhmmss(f[1])
        tick = TickData(gateway_name=gateway_name, symbol=f[0], exchange=Exchange.KRX, datetime=dt)
        tick.last_price = _safe_float(f[2])
        tick.volume = _safe_float(f[12])        
        tick.turnover = _safe_float(f[13])      
        tick.open_interest = _safe_float(f[14]) 
        setattr(tick, "last_volume", _safe_float(f[11])) 
        if len(f) > 9:
            tick.open_price = _safe_float(f[7])
            tick.high_price = _safe_float(f[8])
            tick.low_price = _safe_float(f[9])
        return tick

    @staticmethod
    def _parse_kr_future_hoka(gateway_name: str, f: List[str]) -> TickData:
        dt = _parse_time_hhmmss(f[1])
        tick = TickData(gateway_name=gateway_name, symbol=f[0], exchange=Exchange.KRX, datetime=dt)
        for i in range(5):
            setattr(tick, f"ask_price_{i+1}", _safe_float(f[2 + i]))
            setattr(tick, f"bid_price_{i+1}", _safe_float(f[7 + i]))
            if len(f) > 31:
                setattr(tick, f"ask_volume_{i+1}", _safe_float(f[22 + i]))
                setattr(tick, f"bid_volume_{i+1}", _safe_float(f[27 + i]))
        return tick

    @staticmethod
    def _parse_kr_bond_tick(gateway_name: str, f: List[str]) -> TickData:
        dt = _parse_time_hhmmss(f[2])
        tick = TickData(gateway_name=gateway_name, symbol=f[0], exchange=Exchange.KRX, datetime=dt)
        tick.last_price = _safe_float(f[3])
        tick.volume = _safe_float(f[9])
        setattr(tick, "last_volume", _safe_float(f[8]))
        if len(f) > 13:
            tick.open_price = _safe_float(f[11])
            tick.high_price = _safe_float(f[12])
            tick.low_price = _safe_float(f[13])
        return tick

    @staticmethod
    def _parse_kr_bond_hoka(gateway_name: str, f: List[str]) -> TickData:
        dt = _parse_time_hhmmss(f[1])
        tick = TickData(gateway_name=gateway_name, symbol=f[0], exchange=Exchange.KRX, datetime=dt)
        for i in range(5):
            idx = 3 + (i * 4)
            if len(f) > idx + 3:
                setattr(tick, f"ask_price_{i+1}", _safe_float(f[idx]))
                setattr(tick, f"bid_price_{i+1}", _safe_float(f[idx+1]))
                setattr(tick, f"ask_volume_{i+1}", _safe_float(f[idx+2]))
                setattr(tick, f"bid_volume_{i+1}", _safe_float(f[idx+3]))
        return tick

    @staticmethod
    def _parse_kr_index_tick(gateway_name: str, f: List[str]) -> TickData:
        dt = _parse_time_hhmmss(f[1])
        tick = TickData(gateway_name=gateway_name, symbol=f[0], exchange=Exchange.KRX, datetime=dt)
        tick.last_price = _safe_float(f[2])
        tick.volume = _safe_float(f[5])     # 누적 거래량
        tick.turnover = _safe_float(f[6])   # 누적 거래대금
        
        return tick

    @staticmethod
    def _parse_night_future_tick(gateway_name: str, f: List[str], tr_id: str) -> TickData:
        dt = _parse_time_hhmmss(f[1])
        tick = TickData(gateway_name=gateway_name, symbol=f[0], exchange=Exchange.EUREX, datetime=dt)
        tick.last_price = _safe_float(f[2])
        
        # OHLC
        if len(f) > 8:
            tick.open_price = _safe_float(f[6])
            tick.high_price = _safe_float(f[7])
            tick.low_price = _safe_float(f[8])
            
        return tick

    @staticmethod
    def _parse_os_stock_tick(gateway_name: str, f: List[str]) -> TickData:
        """
        해외주식 실시간 체결가 (HDFSCNT0) 파싱
        문서: [해외주식] 실시간시세.xlsx - 해외주식 실시간지연체결가.csv 참고
        """
        # CSV 문서 기준 인덱스 매핑:
        # f[0]: RSYM (실시간종목코드)
        # f[1]: SYMB (종목코드)
        # f[8]: OPEN (시가)
        # f[9]: HIGH (고가)
        # f[10]: LOW (저가)
        # f[11]: LAST (현재가)
        # f[19]: EVOL (체결량 - Instant Volume)
        # f[20]: TVOL (거래량 - Accum Volume)
        # f[21]: TAMT (거래대금)

        rsym = f[0]
        # RSYM 예: DNASTSLA (D+시장+종목) -> 시장코드 추출
        exch_str = rsym[1:4] if len(rsym) >= 4 else ""
        exchange = KisApiHelper.get_vnpy_exchange(exch_str)
        if exchange == Exchange.LOCAL: exchange = Exchange.NASDAQ 
        
        # [Time Parsing] f[3]: 현지영업일자(TYMD), f[4]: 현지일자(XYMD), f[5]: 현지시간(XHMS)
        # 문서상 현지시간은 index 5 입니다. (CSV Response Body 순서 참조)
        try:
            # f[4](현지일자) + f[5](현지시간) 조합 권장
            dt_str = f"{f[4]}{f[5]}" 
            dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S").replace(tzinfo=KIS_TZ)
        except:
            dt = datetime.now(KIS_TZ)
        
        tick = TickData(gateway_name=gateway_name, symbol=f[1], exchange=exchange, datetime=dt)
        
        # [수정] 문서 인덱스에 맞춰 재매핑
        tick.open_price = _safe_float(f[8])
        tick.high_price = _safe_float(f[9])
        tick.low_price = _safe_float(f[10])
        tick.last_price = _safe_float(f[11])
        
        setattr(tick, "last_volume", _safe_float(f[19]))  # EVOL (체결량)
        tick.volume = _safe_float(f[20])                  # TVOL (누적거래량)
        tick.turnover = _safe_float(f[21])                # TAMT (거래대금)
        
        return tick
    
    @staticmethod
    def _parse_os_stock_hoka(gateway_name: str, f: List[str]) -> TickData:
        rsym = f[0]
        exch_str = rsym[1:4] if len(rsym) >= 4 else ""
        dt = _parse_time_hhmmss(f[3])
        tick = TickData(gateway_name=gateway_name, symbol=f[1], exchange=KisApiHelper.get_vnpy_exchange(exch_str), datetime=dt)
        for i in range(10):
            base = 11 + (i * 6)
            if len(f) > base + 3:
                if i < 5: 
                    setattr(tick, f"bid_price_{i+1}", _safe_float(f[base]))
                    setattr(tick, f"ask_price_{i+1}", _safe_float(f[base+1]))
                    setattr(tick, f"bid_volume_{i+1}", _safe_float(f[base+2]))
                    setattr(tick, f"ask_volume_{i+1}", _safe_float(f[base+3]))
        return tick

    @staticmethod
    def _parse_os_future_tick(gateway_name: str, f: List[str]) -> TickData:
        """
        해외선물옵션 실시간 체결가 (HDFFF020) 파싱
        문서: [해외선물옵션]실시간시세.xlsx - 해외선물옵션 실시간체결가.csv 참고
        """
        # CSV 문서 기준:
        # f[0]: SERIES_CD (종목코드)
        # f[1]: BSNS_DATE (영업일자)
        # f[5]: MRKT_CLOSE_TIME (장종료시각... 순서 주의)
        # ...
        # f[11]: LAST_PRIC (현재가)
        # f[12]: CNTR_QNTT (체결수량 - Instant)
        # f[13]: VOL (누적거래량)
        
        # [Time Parsing] f[1](영업일자) + f[10](체결시각)
        try:
            dt = datetime.strptime(f"{f[1]}{f[10]}", "%Y%m%d%H%M%S").replace(tzinfo=KIS_TZ)
        except:
            dt = datetime.now(KIS_TZ)

        tick = TickData(gateway_name=gateway_name, symbol=f[0], exchange=Exchange.CME, datetime=dt)
        
        # [확인] 현재 코드는 11, 12, 13을 사용 중이므로 대체로 맞지만, OHLC 위치를 재확인해야 합니다.
        # 문서상:
        # 7: OPEN_PRIC
        # 8: HIGH_PRIC
        # 9: LOW_PRIC
        
        tick.last_price = _safe_float(f[11])
        setattr(tick, "last_volume", _safe_float(f[12])) # Instant
        tick.volume = _safe_float(f[13])                 # Accum
        
        if len(f) > 9:
            tick.open_price = _safe_float(f[7])
            tick.high_price = _safe_float(f[8])
            tick.low_price = _safe_float(f[9])
            
        return tick
    
    @staticmethod
    def _parse_os_future_hoka(gateway_name: str, f: List[str]) -> TickData:
        dt = _parse_time_hhmmss(f[2])
        tick = TickData(gateway_name=gateway_name, symbol=f[0], exchange=Exchange.CME, datetime=dt)
        for i in range(5):
            base = 4 + (i * 6)
            if len(f) > base + 5:
                setattr(tick, f"bid_volume_{i+1}", _safe_float(f[base]))
                setattr(tick, f"bid_price_{i+1}", _safe_float(f[base + 2]))
                setattr(tick, f"ask_volume_{i+1}", _safe_float(f[base + 3]))
                setattr(tick, f"ask_price_{i+1}", _safe_float(f[base + 5]))
        return tick

    # ----------------------------------------------------------------------------
    # 2. WebSocket Order/Trade — MCP: ccnl_notice (체결통보)
    # ----------------------------------------------------------------------------
    @staticmethod
    def parse_order_notice(gateway_name: str, payload: Dict[str, Any]) -> Tuple[Optional[OrderData], Optional[TradeData]]:
        raw_data = payload.get("data", "")
        tr_id = payload.get("tr_id", "")
        f = raw_data.split("^")
        if len(f) < 5:
            f = raw_data.split("|")
        if len(f) < 5:
            return None, None

        now = datetime.now(KIS_TZ)
        dir_buy = KisParser.DIR_BUY
        cntg_fill = KisParser.CNTG_FILL
        cntg_rev = KisParser.CNTG_CANCEL_OR_REV

        # [A] 국내 주식 체결통보 — MCP domestic_stock: ccnl_notice (H0STCNI0)
        if tr_id.startswith("H0STCNI"):
            orderid = f[2]
            symbol = f[18]
            direction = Direction.LONG if f[5] == dir_buy else Direction.SHORT
            ord_qty = _safe_float(f[9])
            cntg_qty = _safe_float(f[11])
            remn_qty = _safe_float(f[13])
            cntg_yn = f[14]
            cumulative_traded = max(0, ord_qty - remn_qty)
            status = Status.SUBMITTING
            if cntg_yn == cntg_rev:
                status = Status.CANCELLED if (ord_qty > 0 and remn_qty == 0) else Status.NOTTRADED
            elif cntg_yn == cntg_fill:
                status = Status.ALLTRADED if remn_qty <= 0 else Status.PARTTRADED
            order = OrderData(gateway_name=gateway_name, symbol=symbol, exchange=Exchange.KRX, orderid=orderid, direction=direction, type=OrderType.LIMIT, price=_safe_float(f[10]), volume=ord_qty, traded=cumulative_traded, status=status, datetime=now)
            trade = None
            if cntg_yn == cntg_fill and cntg_qty > 0:
                trade = TradeData(gateway_name=gateway_name, symbol=symbol, exchange=Exchange.KRX, orderid=orderid, tradeid=f"{orderid}_{now.strftime('%H%M%S%f')}", direction=direction, price=_safe_float(f[12]), volume=cntg_qty, datetime=now)
            return order, trade

        # [B] 국내 선물/옵션 체결통보 — MCP domestic_futureoption: fuopt_ccnl_notice (H0IFCNI0)
        if tr_id.startswith("H0IFCNI"):
            orderid = f[2]
            symbol = f[19] if len(f[18]) < 3 else f[18]
            direction = Direction.LONG if f[5] == dir_buy else Direction.SHORT
            ord_qty = _safe_float(f[9])
            cntg_qty = _safe_float(f[11])
            remn_qty = _safe_float(f[13])
            cntg_yn = f[14]
            cumulative_traded = max(0, ord_qty - remn_qty)
            status = Status.SUBMITTING
            if cntg_yn == cntg_rev:
                status = Status.CANCELLED if (ord_qty > 0 and remn_qty == 0) else Status.NOTTRADED
            elif cntg_yn == cntg_fill:
                status = Status.ALLTRADED if remn_qty <= 0 else Status.PARTTRADED
            order = OrderData(gateway_name=gateway_name, symbol=symbol, exchange=Exchange.KRX, orderid=orderid, direction=direction, type=OrderType.LIMIT, price=_safe_float(f[10]), volume=ord_qty, traded=cumulative_traded, status=status, datetime=now)
            trade = None
            if cntg_yn == cntg_fill and cntg_qty > 0:
                trade = TradeData(gateway_name=gateway_name, symbol=symbol, exchange=Exchange.KRX, orderid=orderid, tradeid=f"{orderid}_{now.strftime('%H%M%S%f')}", direction=direction, price=_safe_float(f[12]), volume=cntg_qty, datetime=now)
            return order, trade

        # [C] 해외 선물 체결/주문 (HDFFF1C0 주문, HDFFF2C0 체결)
        if tr_id in ("HDFFF1C0", "HDFFF2C0"):
            is_trade = tr_id == "HDFFF2C0"
            symbol = f[6]
            exchange = Exchange.CME
            orderid = f[3]
            direction = Direction.LONG if f[10] == dir_buy else Direction.SHORT
            if not is_trade:
                return OrderData(gateway_name=gateway_name, symbol=symbol, exchange=exchange, orderid=orderid, direction=direction, price=0.0, volume=_safe_float(f[13]), status=Status.NOTTRADED, datetime=now), None
            trade = TradeData(gateway_name=gateway_name, symbol=symbol, exchange=exchange, orderid=orderid, tradeid=f"T{f[3]}", direction=direction, price=_safe_float(f[12]), volume=_safe_float(f[11]), datetime=now)
            return None, trade

        # [D] 해외 주식 체결통보 — MCP overseas_stock: ccnl_notice (H0GSCNI0)
        if tr_id.startswith("H0GSCNI"):
            orderid = f[2]
            symbol = f[15]
            ord_qty = _safe_float(f[9])
            cntg_qty = _safe_float(f[11])
            nccs_qty = _safe_float(f[13])
            cntg_yn = f[14]
            cumulative_traded = max(0, ord_qty - nccs_qty)
            status = Status.ALLTRADED if (cntg_yn == cntg_fill and nccs_qty <= 0) else (Status.PARTTRADED if (cntg_yn == cntg_fill) else Status.NOTTRADED)
            direction = Direction.LONG if f[5] == dir_buy else Direction.SHORT
            order = OrderData(gateway_name=gateway_name, symbol=symbol, exchange=Exchange.NASDAQ, orderid=orderid, direction=direction, type=OrderType.LIMIT, price=_safe_float(f[10]), volume=ord_qty, traded=cumulative_traded, status=status, datetime=now)
            trade = None
            if cntg_yn == cntg_fill and cntg_qty > 0:
                trade = TradeData(gateway_name=gateway_name, symbol=order.symbol, exchange=order.exchange, orderid=order.orderid, tradeid=f"T{order.orderid}", direction=order.direction, price=_safe_float(f[12]), volume=cntg_qty, datetime=now)
            return order, trade

        return None, None

    @staticmethod
    def parse_account_balance(gateway_name: str, data: dict) -> List[PositionData]:
        """잔고 조회 응답 → PositionData 리스트. MCP: inquire_balance 응답 output1."""
        positions = []
        for item in data.get("output1", []):
            symbol = item.get("pdno", "")
            if not symbol: continue
            pos = PositionData(
                gateway_name=gateway_name, symbol=symbol, exchange=Exchange.KRX,
                direction=Direction.NET, volume=_safe_float(item.get("hldg_qty")),
                price=_safe_float(item.get("pchs_avg_pric")), pnl=_safe_float(item.get("evlu_pfls_amt")), frozen=0 
            )
            positions.append(pos)
        return positions

    # -------------------------------------------------------------------------
    # 3. Contract Info — MCP: 종목정보/기본시세 (inquire_price, search_stock_info 등)
    # -------------------------------------------------------------------------
    @staticmethod
    def parse_contract_info(gateway_name: str, data: dict, asset_type: str) -> Optional[ContractData]:
        output = data.get("output", {})
        if not output:
            output = data.get("output1", {})
        if not output:
            return None

        # 1. 국내 주식 — MCP domestic_stock: inquire_price, search_stock_info
        if asset_type == AssetType.KR_STOCK:
            symbol = output.get("stck_shrn_iscd", "")
            name = output.get("hts_kor_isnm", "") or output.get("bstp_kor_isnm", "")
            if not name: name = symbol
    
            ref_price = _safe_float(output.get("stck_sdpr") or output.get("stck_prpr"))
            pricetic_type = "STOCK"
            if output.get("rprs_mrkt_cls_code") == "Q": pricetic_type = "KOSDAQ"
            
            return ContractData(
                gateway_name=gateway_name,
                symbol=symbol,
                exchange=Exchange.KRX,
                name=name,
                product=Product.EQUITY,
                size=1,
                pricetick=get_krx_pricetick(ref_price, pricetic_type),
                history_data=True,
                min_volume=1
            )

        # 2. 국내 선물/옵션 — MCP domestic_futureoption: inquire_price, search_info
        elif asset_type == AssetType.KR_FUTOPT:
            symbol = output.get("futs_shrn_iscd") or output.get("optn_shrn_iscd") or output.get("stck_shrn_iscd")
            name = output.get("hts_kor_isnm", "")
            
            product = Product.FUTURES
            pricetick_type = "FUTURES"
            option_strike = 0.0
            option_expiry = None
            option_underlying = ""
            option_type = None

            if symbol and (symbol.startswith("2") or symbol.startswith("3")):
                product = Product.OPTION
                pricetick_type = "OPTION"
                expiry_str = output.get("futs_last_tr_date") or output.get("xpir_date")
                if expiry_str and len(expiry_str) == 8:
                    try: option_expiry = datetime.strptime(expiry_str, "%Y%m%d").replace(tzinfo=KIS_TZ)
                    except: pass
                option_underlying = output.get("unly_shrn_iscd") or output.get("base_item_cd") or "KOSPI200"
                if symbol.startswith("2") or output.get("cp_dv") == "C": option_type = OptionType.CALL
                elif symbol.startswith("3") or output.get("cp_dv") == "P": option_type = OptionType.PUT
            else:
                expiry_str = output.get("futs_last_tr_date")
                if expiry_str and len(expiry_str) == 8:
                    try: option_expiry = datetime.strptime(expiry_str, "%Y%m%d").replace(tzinfo=KIS_TZ)
                    except: pass

            ref_price = _safe_float(output.get("stck_prpr") or output.get("futs_prpr") or 0.0)

            return ContractData(
                gateway_name=gateway_name,
                symbol=symbol,
                exchange=Exchange.KRX,
                name=name,
                product=product,
                size=1,
                pricetick=get_krx_pricetick(ref_price, pricetick_type),
                history_data=True,
                min_volume=1,
                option_strike=option_strike,
                option_underlying=option_underlying,
                option_type=option_type,
                option_expiry=option_expiry,
                option_portfolio=option_underlying
            )

        # 3. 국내 채권 — MCP domestic_bond: inquire_price, search_bond_info
        elif asset_type == AssetType.KR_BOND:
            symbol = output.get("stnd_iscd") or output.get("bond_shrn_iscd") or output.get("pdno")
            name = output.get("hts_kor_isnm") or output.get("bond_kor_isnm") or output.get("prdt_name")
            return ContractData(
                gateway_name=gateway_name, symbol=symbol, exchange=Exchange.KRX, name=name, product=Product.BOND, size=1, pricetick=1, history_data=True, min_volume=1
            )

        # 4. 해외 주식 — MCP overseas_stock: search_info
        elif asset_type == AssetType.OS_STOCK:
            symbol = output.get("pdno") or output.get("symb")
            name = output.get("prdt_name") or output.get("prdt_eng_name")
            exchange_code = output.get("ovrs_excg_cd", "")
            exchange = KisApiHelper.get_vnpy_exchange(exchange_code)
            if exchange == Exchange.LOCAL: exchange = Exchange.NASDAQ
            return ContractData(
                gateway_name=gateway_name, symbol=symbol, exchange=exchange, name=name, product=Product.EQUITY, size=1, pricetick=_safe_float(output.get("pion_unit") or 0.01), history_data=True, min_volume=1
            )

        # 5. 해외 선물/옵션 — MCP overseas_futureoption: stock-detail, opt-detail
        elif asset_type == AssetType.OS_FUTOPT:
            symbol = output.get("srs_cd") or output.get("pdno") or output.get("rsym")
            name = output.get("srs_nm") or output.get("prdt_name")
            multiplier = _safe_float(output.get("ctrt_size") or output.get("futs_prdt_mult") or 1.0)
            tick_size = _safe_float(output.get("tick_sz") or output.get("atm_unit") or 0.01)
            exchange_code = output.get("exch_cd", "")
            exchange = KisApiHelper.get_vnpy_exchange(exchange_code)
            if exchange == Exchange.LOCAL: exchange = Exchange.CME
            product = Product.FUTURES
            option_strike = 0.0
            option_expiry = None
            option_type = None
            option_underlying = ""
            
            is_option = False
            if output.get("type") == "OPTION" or output.get("call_put_cls_code") or "opt" in str(output.get("prdt_name", "")).lower(): is_option = True
            elif symbol and (" " in symbol and (symbol[-1].isdigit() or "C" in symbol or "P" in symbol)): is_option = True

            if is_option:
                product = Product.OPTION
                option_strike = _safe_float(output.get("act_prc") or output.get("strk_pric") or 0.0)
                expiry_str = output.get("expr_date") or output.get("mat_date") or output.get("last_trad_day")
                if expiry_str and len(expiry_str) == 8:
                    try: option_expiry = datetime.strptime(expiry_str, "%Y%m%d").replace(tzinfo=KIS_TZ)
                    except: pass
                option_underlying = output.get("und_asset_cd") or (symbol.split(" ")[0] if symbol else "")
                cp_code = output.get("call_put_cls_code")
                if cp_code in ["C", "Call"]: option_type = OptionType.CALL
                elif cp_code in ["P", "Put"]: option_type = OptionType.PUT
                if not option_type and symbol and " " in symbol:
                    parts = symbol.split()
                    if len(parts) >= 2:
                        type_str = parts[-1]
                        if type_str.startswith("C"): option_type = OptionType.CALL
                        elif type_str.startswith("P"): option_type = OptionType.PUT
                        if option_strike == 0: option_strike = _safe_float(type_str[1:])
            else:
                expiry_str = output.get("expr_date") or output.get("trd_to_date") or output.get("last_trad_day")
                if expiry_str and len(expiry_str) == 8:
                    try: option_expiry = datetime.strptime(expiry_str, "%Y%m%d").replace(tzinfo=KIS_TZ)
                    except: pass

            return ContractData(
                gateway_name=gateway_name, symbol=symbol, exchange=exchange, name=name, product=product, size=multiplier, pricetick=tick_size, history_data=True, min_volume=1,
                option_strike=option_strike, option_underlying=option_underlying, option_type=option_type, option_expiry=option_expiry, option_portfolio=option_underlying
            )
        return None

    @staticmethod
    def parse_history_bar(gateway_name: str, data: dict, symbol: str, exchange: Exchange, interval: Interval) -> List[BarData]:
        """차트 응답 → BarData 리스트. MCP: inquire_daily_itemchartprice, inquire_time_* 등 output1/output2."""
        bars = []
        candles = data.get("output2", []) or data.get("output1", [])
        for item in candles:
            try:
                dt_str = item.get("stck_bsop_date") or item.get("xymd") or item.get("data_date") or "" 
                tm_str = item.get("stck_cntg_hour") or item.get("xhms") or item.get("data_time") or "000000" 
                if not dt_str: continue
                
                open_p = _safe_float(item.get("stck_oprc") or item.get("open") or item.get("open_price"))
                high_p = _safe_float(item.get("stck_hgpr") or item.get("high") or item.get("high_price"))
                low_p =  _safe_float(item.get("stck_lwpr") or item.get("low") or item.get("low_price"))
                close_p = _safe_float(item.get("stck_prpr") or item.get("last") or item.get("last_price") or item.get("clos"))
                vol = _safe_float(item.get("cntg_vol") or item.get("evol") or item.get("vol") or item.get("acml_vol")) 
                turnover = _safe_float(item.get("acml_tr_pbmn") or item.get("eamt") or item.get("tamt"))
                
                # [Time Parsing] YYYYMMDD + HHMMSS (KIS_TZ 적용)
                dt = datetime.strptime(f"{dt_str}{tm_str}", "%Y%m%d%H%M%S").replace(tzinfo=KIS_TZ)

                bar = BarData(gateway_name=gateway_name, symbol=symbol, exchange=exchange, datetime=dt, interval=interval, volume=vol, turnover=turnover, open_price=open_p, high_price=high_p, low_price=low_p, close_price=close_p, open_interest=0)
                bars.append(bar)
            except: continue
        bars.sort(key=lambda x: x.datetime)
        return bars