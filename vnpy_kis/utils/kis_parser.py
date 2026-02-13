import re
from datetime import datetime, timedelta, time
from functools import lru_cache
from typing import Dict, List, Tuple, Optional, Union, Any

from vnpy.trader.object import TickData, OrderData, TradeData, PositionData, AccountData, ContractData
from vnpy.trader.constant import (
    Exchange, Product, OptionType, Direction, Offset, Status, OrderType, Interval
)

# --------------------------------------------------------------------------------
# 상수 및 매핑 정의
# --------------------------------------------------------------------------------

# KIS 거래소 코드 -> vn.py Exchange 매핑
KIS_EXCHANGE_MAP = {
    # [국내]
    "KRX": Exchange.KRX,
    "KOSPI": Exchange.KRX,
    "KOSDAQ": Exchange.KRX, # vn.py는 KRX로 통합 관리하나, 필요시 별도 거래소 상수 추가 가능
    "NXT": Exchange.KRX,    # 넥스트레이드 (vn.py 표준이 없다면 KRX로 매핑 후 symbol로 구분)
    
    # [해외 주식 - 미국]
    "NAS": Exchange.NASDAQ, "NASD": Exchange.NASDAQ,
    "NYS": Exchange.NYSE, "NYSE": Exchange.NYSE,
    "AMS": Exchange.AMEX, "AMEX": Exchange.AMEX,
    # 주간 거래
    "BAQ": Exchange.NASDAQ, "BAY": Exchange.NYSE, "BAA": Exchange.AMEX,

    # [해외 주식 - 아시아]
    "HKS": Exchange.HKFE, "SEHK": Exchange.HKFE, # 홍콩
    "TSE": Exchange.TSE,  "TKSE": Exchange.TSE,  # 일본
    "SHS": Exchange.SSE,  "SHAA": Exchange.SSE,  # 상해
    "SZS": Exchange.SZSE, "SZAA": Exchange.SZSE, # 심천
    "HSX": Exchange.HOSE, "VNSE": Exchange.HOSE, # 호치민
    "HNX": Exchange.OSE,  "HASE": Exchange.OSE,  # 하노이 (임시 OSE 매핑)

    # [해외 선물]
    "CME": Exchange.CME,
    "CBOT": Exchange.CBOT,
    "NYMEX": Exchange.NYMEX,
    "COMEX": Exchange.COMEX,
    "EUREX": Exchange.EUREX,
    "SGX": Exchange.SGX,
    "HKF": Exchange.HKFE,
    "ICE": Exchange.ICE,
    "LME": Exchange.LME,
}

# 역매핑 (vn.py -> KIS)
INVERSE_EXCHANGE_MAP = {v: k for k, v in KIS_EXCHANGE_MAP.items()}

# 주문 상태 매핑
STATUS_MAP = {
    "01": Status.SUBMITTING,
    "02": Status.NOTTRADED,
    "03": Status.REJECTED,
    "04": Status.NOTTRADED, 
    "05": Status.NOTTRADED, 
    "11": Status.ALLTRADED, 
    "12": Status.NOTTRADED,
    "13": Status.CANCELLED,
}

# --------------------------------------------------------------------------------
# 유틸리티 함수
# --------------------------------------------------------------------------------

@lru_cache(maxsize=4096)
def _parse_datetime(date_str: str, time_str: str) -> datetime:
    """YYYYMMDD + HHMMSSuuuuuu 파싱 (캐싱 적용)"""
    now = datetime.now()
    try:
        if date_str and len(date_str) == 8:
            year = int(date_str[0:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            now = now.replace(year=year, month=month, day=day)
            
        if time_str and len(time_str) >= 6:
            hour = int(time_str[0:2])
            minute = int(time_str[2:4])
            second = int(time_str[4:6])
            micro = int(time_str[6:12]) if len(time_str) >= 12 else 0
            return now.replace(hour=hour, minute=minute, second=second, microsecond=micro)
    except:
        pass
    return now

def _safe_float(value: Any) -> float:
    try:
        if isinstance(value, str):
            return float(value.replace(',', ''))
        return float(value)
    except:
        return 0.0

def _safe_int(value: Any) -> int:
    try:
        if isinstance(value, str):
            return int(value.replace(',', ''))
        return int(value)
    except:
        return 0

def _get_krx_pricetick(price: float, market_type: str = "KOSPI") -> float:
    """
    국내주식 가격대별 호가단위 계산
    :param market_type: 'KOSPI' (유가증권), 'KOSDAQ' (코스닥), 'ETF', 'ELW' 등
    """
    if price <= 0: return 0.0
    
    # ETF/ETN/ELW는 5원 단위 (단, 2000원 미만 등 세부 예외 존재하나 통상 5원)
    if market_type in ["ETF", "ETN", "ELW"]:
        return 5.0

    # KOSPI (유가증권)
    if market_type == "KOSPI":
        if price < 1000: return 1.0
        if price < 5000: return 5.0
        if price < 10000: return 10.0
        if price < 50000: return 50.0
        if price < 100000: return 100.0
        if price < 500000: return 500.0
        return 1000.0
        
    # KOSDAQ (코스닥) - KONEX 포함
    elif market_type == "KOSDAQ":
        if price < 1000: return 1.0
        if price < 5000: return 5.0
        if price < 10000: return 10.0
        if price < 50000: return 50.0
        return 100.0
        
    return 1.0

# --------------------------------------------------------------------------------
# KIS Parser 클래스
# --------------------------------------------------------------------------------
class KisParser:
    """KIS API 데이터를 vn.py 객체로 변환"""

    # ----------------------------------------------------------------------------
    # 1. WebSocket Tick (실시간 시세)
    # ----------------------------------------------------------------------------
    @staticmethod
    def parse_tick(gateway_name: str, tr_id: str, raw_msg: str) -> Optional[TickData]:
        try:
            tokens = raw_msg.split('|')
            if len(tokens) < 4: return None
            
            data_body = tokens[3]
            f = data_body.split('^')
            
            # --- [국내 주식] ---
            if tr_id == "H0STCNT0":      # 실시간 체결가
                return KisParser._parse_kr_stock_tick(gateway_name, f)
            elif tr_id == "H0STASP0":    # 실시간 호가
                return KisParser._parse_kr_stock_hoka(gateway_name, f)
            
            # --- [국내 선물/옵션] ---
            elif tr_id in ["H0IFCNT0", "H0IOCNT0"]: # 지수선물/옵션 체결
                return KisParser._parse_kr_future_tick(gateway_name, f)
            elif tr_id in ["H0IFASP0", "H0IOASP0"]: # 지수선물/옵션 호가
                return KisParser._parse_kr_future_hoka(gateway_name, f)
            
            # --- [해외 주식] ---
            elif tr_id == "HDFSCNT0":    # 해외주식 체결
                return KisParser._parse_os_stock_tick(gateway_name, f)
            elif tr_id == "HDFSASP0":    # 해외주식 호가
                return KisParser._parse_os_stock_hoka(gateway_name, f)
                
            # --- [해외 선물] ---
            elif tr_id == "HDFFF020":    # 해외선물 체결
                return KisParser._parse_os_future_tick(gateway_name, f)
            elif tr_id == "HDFFF010":    # 해외선물 호가
                return KisParser._parse_os_future_hoka(gateway_name, f)

            return None
        except Exception:
            return None

    # --- [상세 구현] 국내 주식 ---
    @staticmethod
    def _parse_kr_stock_tick(gateway_name: str, f: List[str]) -> TickData:
        # H0STCNT0
        # f[0]:단축코드, f[1]:시간, f[2]:현재가
        # f[12]:체결량(CNTG_VOL), f[13]:누적거래량(ACML_VOL), f[14]:누적거래대금(ACML_TR_PBMN)
        symbol = f[0]
        tick = TickData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=Exchange.KRX,
            datetime=_parse_datetime("", f[1])
        )
        tick.last_price = _safe_float(f[2])
        tick.volume = _safe_float(f[13])    # 누적거래량
        tick.turnover = _safe_float(f[14])  # 누적거래대금 (필드 추가 완료)
        tick.open_interest = 0              # 주식은 미결제약정 없음
        
        # 동적 속성: 순간 체결량
        setattr(tick, "last_volume", _safe_float(f[12])) 
        
        if len(f) > 9:
            tick.open_price = _safe_float(f[7])
            tick.high_price = _safe_float(f[8])
            tick.low_price = _safe_float(f[9])
        return tick

    @staticmethod
    def _parse_kr_stock_hoka(gateway_name: str, f: List[str]) -> TickData:
        # H0STASP0: 10호가
        # f[3]~f[6] (매도1,매수1,매도잔량1,매수잔량1) ... 반복
        symbol = f[0]
        tick = TickData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=Exchange.KRX,
            datetime=_parse_datetime("", f[1])
        )
        for i in range(10): 
            idx = 3 + (i * 4)
            if len(f) > idx + 3:
                ask_p = _safe_float(f[idx])
                bid_p = _safe_float(f[idx+1])
                ask_v = _safe_float(f[idx+2])
                bid_v = _safe_float(f[idx+3])
                
                if i < 5: # vn.py Standard 5 levels
                    setattr(tick, f"ask_price_{i+1}", ask_p)
                    setattr(tick, f"bid_price_{i+1}", bid_p)
                    setattr(tick, f"ask_volume_{i+1}", ask_v)
                    setattr(tick, f"bid_volume_{i+1}", bid_v)
        return tick

    # --- [상세 구현] 국내 선물 ---
    @staticmethod
    def _parse_kr_future_tick(gateway_name: str, f: List[str]) -> TickData:
        # H0IFCNT0
        # f[11]:체결량, f[12]:누적거래량, f[13]:누적거래대금, f[14]:미결제약정
        symbol = f[0]
        tick = TickData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=Exchange.KRX,
            datetime=_parse_datetime("", f[1])
        )
        tick.last_price = _safe_float(f[2])
        tick.volume = _safe_float(f[12]) 
        tick.turnover = _safe_float(f[13]) # 거래대금
        tick.open_interest = _safe_float(f[14]) # 미결제약정
        
        setattr(tick, "last_volume", _safe_float(f[11]))
        
        if len(f) > 9:
            tick.open_price = _safe_float(f[7])
            tick.high_price = _safe_float(f[8])
            tick.low_price = _safe_float(f[9])
        return tick

    @staticmethod
    def _parse_kr_future_hoka(gateway_name: str, f: List[str]) -> TickData:
        symbol = f[0]
        tick = TickData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=Exchange.KRX,
            datetime=_parse_datetime("", f[1])
        )
        for i in range(5):
            # Price: 2~6(Ask), 7~11(Bid)
            # Vol: 22~26(Ask), 27~31(Bid)
            setattr(tick, f"ask_price_{i+1}", _safe_float(f[2 + i]))
            setattr(tick, f"bid_price_{i+1}", _safe_float(f[7 + i]))
            if len(f) > 31:
                setattr(tick, f"ask_volume_{i+1}", _safe_float(f[22 + i]))
                setattr(tick, f"bid_volume_{i+1}", _safe_float(f[27 + i]))
        return tick

    # --- [상세 구현] 해외 주식 ---
    @staticmethod
    def _parse_os_stock_tick(gateway_name: str, f: List[str]) -> TickData:
        # HDFSCNT0: f[12] TVOL, f[13] EVOL(LastVol), f[14] TAMT(Turnover)
        rsym = f[0]
        symbol = f[1]
        exch_str = rsym[1:4] if len(rsym) >= 4 else ""
        
        tick = TickData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=KIS_EXCHANGE_MAP.get(exch_str, Exchange.NASDAQ),
            datetime=_parse_datetime("", f[3])
        )
        tick.last_price = _safe_float(f[8])
        tick.volume = _safe_float(f[12]) # 누적거래량
        
        if len(f) > 13: setattr(tick, "last_volume", _safe_float(f[13]))
        if len(f) > 14: tick.turnover = _safe_float(f[14]) # 거래대금
        
        if len(f) > 7:
            tick.open_price = _safe_float(f[5])
            tick.high_price = _safe_float(f[6])
            tick.low_price = _safe_float(f[7])
        return tick

    @staticmethod
    def _parse_os_stock_hoka(gateway_name: str, f: List[str]) -> TickData:
        # HDFSASP0 (10호가)
        rsym = f[0]
        symbol = f[1]
        tick = TickData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=KIS_EXCHANGE_MAP.get(rsym[1:4], Exchange.NASDAQ),
            datetime=_parse_datetime("", f[3])
        )
        # CSV 기준: Index 11부터 PBID1, PASK1, VBID1, VASK1 ...
        start_idx = 11
        for i in range(10):
            base = start_idx + (i * 6)
            if len(f) > base + 3:
                if i < 5: 
                    setattr(tick, f"bid_price_{i+1}", _safe_float(f[base]))
                    setattr(tick, f"ask_price_{i+1}", _safe_float(f[base+1]))
                    setattr(tick, f"bid_volume_{i+1}", _safe_float(f[base+2]))
                    setattr(tick, f"ask_volume_{i+1}", _safe_float(f[base+3]))
        return tick

    # --- [상세 구현] 해외 선물 ---
    @staticmethod
    def _parse_os_future_tick(gateway_name: str, f: List[str]) -> TickData:
        # HDFFF020: f[1] BSNS_DATE, f[10] TIME
        symbol = f[0]
        tick = TickData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=Exchange.CME,
            datetime=_parse_datetime(f[1], f[10]) # Timezone 고려 (현지영업일 기준)
        )
        tick.last_price = _safe_float(f[11])
        tick.volume = _safe_float(f[13]) # 누적
        setattr(tick, "last_volume", _safe_float(f[12])) # 체결량
        tick.open_interest = 0
        
        if len(f) > 9:
            tick.open_price = _safe_float(f[7])
            tick.high_price = _safe_float(f[8])
            tick.low_price = _safe_float(f[9])
        return tick

    @staticmethod
    def _parse_os_future_hoka(gateway_name: str, f: List[str]) -> TickData:
        # HDFFF010: 5단계 호가
        symbol = f[0]
        tick = TickData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=Exchange.CME,
            datetime=_parse_datetime("", f[2])
        )
        # CSV 기준: 4부터 6개씩 반복 [BidVol, BidCnt, BidPrc, AskVol, AskCnt, AskPrc]
        start_idx = 4
        for i in range(5):
            base = start_idx + (i * 6)
            if len(f) > base + 5:
                setattr(tick, f"bid_volume_{i+1}", _safe_float(f[base]))
                setattr(tick, f"bid_price_{i+1}", _safe_float(f[base + 2]))
                setattr(tick, f"ask_volume_{i+1}", _safe_float(f[base + 3]))
                setattr(tick, f"ask_price_{i+1}", _safe_float(f[base + 5]))
        return tick

    # ----------------------------------------------------------------------------
    # 2. WebSocket Order/Trade
    # ----------------------------------------------------------------------------
    @staticmethod
    def parse_order_notice(gateway_name: str, payload: Dict[str, Any]) -> Tuple[Optional[OrderData], Optional[TradeData]]:
        raw_data = payload.get("data", "")
        tr_id = payload.get("tr_id", "")
        f = raw_data.split('^')
        if len(f) < 5: f = raw_data.split('|')
        if len(f) < 5: return None, None

        # [A] 국내 주식
        if tr_id.startswith("H0STCNI"):
            orderid = f[2]
            symbol = f[18]
            direction = Direction.LONG if f[5] == "02" else Direction.SHORT
            order = OrderData(
                gateway_name=gateway_name,
                symbol=symbol,
                exchange=Exchange.KRX,
                orderid=orderid,
                direction=direction,
                price=_safe_float(f[10]),
                volume=_safe_float(f[9]),
                traded=_safe_float(f[11]),
                status=STATUS_MAP.get(f[14], Status.NOTTRADED),
                datetime=datetime.now()
            )
            trade = None
            if f[14] == "2" and order.traded > 0:
                trade = TradeData(
                    gateway_name=gateway_name,
                    symbol=symbol,
                    exchange=Exchange.KRX,
                    orderid=orderid,
                    tradeid=f"{orderid}_{datetime.now().strftime('%H%M%S%f')}",
                    direction=direction,
                    price=_safe_float(f[12]),
                    volume=order.traded, 
                    datetime=datetime.now()
                )
            return order, trade

        # [B] 해외 선물
        elif tr_id == "HDFFF1C0": # 접수/거부
            order = OrderData(
                gateway_name=gateway_name,
                symbol=f[6],
                exchange=Exchange.CME, 
                orderid=f[3],
                direction=Direction.LONG if f[10] == "02" else Direction.SHORT,
                price=0.0,
                volume=_safe_float(f[13]),
                status=Status.NOTTRADED,
                datetime=datetime.now()
            )
            return order, None
        elif tr_id == "HDFFF2C0": # 체결
            trade = TradeData(
                gateway_name=gateway_name,
                symbol=f[6],
                exchange=Exchange.CME,
                orderid=f[3],
                tradeid=f"T{f[3]}",
                direction=Direction.LONG if f[10] == "02" else Direction.SHORT,
                price=_safe_float(f[12]),
                volume=_safe_float(f[11]),
                datetime=datetime.now()
            )
            return None, trade

        # [C] 해외 주식
        elif tr_id.startswith("H0GSCNI"):
            orderid = f[2]
            symbol = f[15]
            order = OrderData(
                gateway_name=gateway_name,
                symbol=symbol,
                exchange=Exchange.NASDAQ, 
                orderid=orderid,
                direction=Direction.LONG if f[5]=="02" else Direction.SHORT,
                price=_safe_float(f[10]),
                volume=_safe_float(f[9]),
                traded=_safe_float(f[11]),
                status=Status.ALLTRADED if f[14]=="2" else Status.NOTTRADED,
                datetime=datetime.now()
            )
            trade = None
            if f[14] == "2":
                trade = TradeData(
                    gateway_name=gateway_name,
                    symbol=symbol,
                    exchange=Exchange.NASDAQ,
                    orderid=orderid,
                    tradeid=f"T{orderid}",
                    direction=order.direction,
                    price=_safe_float(f[12]),
                    volume=_safe_float(f[11]),
                    datetime=datetime.now()
                )
            return order, trade
        return None, None

    # ----------------------------------------------------------------------------
    # 3. REST API Parsing (Account)
    # ----------------------------------------------------------------------------
    @staticmethod
    def parse_account_balance(gateway_name: str, data: dict) -> List[PositionData]:
        positions = []
        for item in data.get("output1", []):
            symbol = item.get("pdno", "")
            if not symbol: continue
            pos = PositionData(
                gateway_name=gateway_name,
                symbol=symbol,
                exchange=Exchange.KRX,
                direction=Direction.NET,
                volume=_safe_float(item.get("hldg_qty")),
                price=_safe_float(item.get("pchs_avg_pric")),
                pnl=_safe_float(item.get("evlu_pfls_amt")),
                frozen=0 
            )
            positions.append(pos)
        return positions

    # ----------------------------------------------------------------------------
    # 4. Contract Info Parsing (Master/REST)
    # ----------------------------------------------------------------------------
    @staticmethod
    def parse_contract_info(gateway_name: str, data: dict, asset_type: str = "stock") -> Optional[ContractData]:
        """
        REST API 응답을 ContractData로 변환 (시장구분 및 호가단위 적용)
        :param data: API 응답의 'output' 딕셔너리
        """
        try:
            # 1. 국내 주식 (CTPF1002R / CTPF1604R)
            if asset_type == "stock":
                name = data.get("prdt_name", "") or data.get("prdt_abrv_name", "")
                symbol = data.get("pdno", "")
                if not symbol: return None

                # 시장 구분 (prdt_clsf_cd)
                # 301: 코스피, 302: 코스닥, 306: 코넥스
                # ETF 여부는 etf_type_cd 값이 존재하거나 특정 코드로 확인 가능
                clsf_cd = data.get("prdt_clsf_cd", "")
                etf_check = data.get("etf_type_cd", "")
                
                market_type = "KOSPI" # Default
                if clsf_cd == "302": market_type = "KOSDAQ"
                if etf_check: market_type = "ETF" # ETF/ETN

                # 호가 단위(Price Tick) 계산
                # 기준가(stck_sdpr) 또는 현재가(prpr)가 있으면 계산
                ref_price = _safe_float(data.get("stck_sdpr") or data.get("stck_prpr"))
                tick_size = _get_krx_pricetick(ref_price, market_type)

                return ContractData(
                    gateway_name=gateway_name,
                    symbol=symbol,
                    exchange=Exchange.KRX,
                    name=name,
                    product=Product.EQUITY,
                    size=1,
                    pricetick=tick_size,
                    min_volume=1
                )

            # 2. 해외 주식 (CTPF1702R)
            elif asset_type == "overseas_stock":
                raw_exch = data.get("ovrs_excg_cd", "")
                exchange = KIS_EXCHANGE_MAP.get(raw_exch, Exchange.NASDAQ)
                
                symbol = data.get("ovrs_pdno", "") 
                name = data.get("prdt_name", "")
                
                return ContractData(
                    gateway_name=gateway_name,
                    symbol=symbol,
                    exchange=exchange,
                    name=name,
                    product=Product.EQUITY,
                    size=1,
                    pricetick=0.01, # 미국주식은 보통 0.01달러
                    min_volume=1
                )

            # 3. 해외 선물 (HHDFC55200000)
            elif asset_type == "overseas_future":
                symbol = data.get("sub_srs_cd", "") or data.get("srs_cd", "")
                name = data.get("hngl_item_name", "") 
                
                raw_exch = data.get("exch_cd", "")
                exchange = KIS_EXCHANGE_MAP.get(raw_exch, Exchange.CME)
                
                tick_sz = _safe_float(data.get("tick_sz"))
                tick_val = _safe_float(data.get("tick_val"))
                size = _safe_float(data.get("ctrt_size")) 
                
                # 계약 승수 계산 (Size가 0인 경우 역산)
                if size == 0 and tick_sz > 0:
                    size = tick_val / tick_sz

                return ContractData(
                    gateway_name=gateway_name,
                    symbol=symbol,
                    exchange=exchange,
                    name=name,
                    product=Product.FUTURES,
                    size=size,
                    pricetick=tick_sz,
                    min_volume=1
                )

        except Exception:
            return None
        return None