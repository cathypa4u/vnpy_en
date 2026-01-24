# kis_api_helper.py
from datetime import datetime
from typing import Dict, Optional, Tuple, Any, List

from vnpy.trader.constant import Exchange, Direction, OrderType, Interval, Product
from vnpy.trader.object import HistoryRequest, OrderRequest, CancelRequest


# WebSocket TR IDs (Subscription Only)
TR_WS = {
    # 1. Domestic Stock
    "KR_STOCK": "H0STCNT0",       "KR_STOCK_HOKA": "H0STASP0",
    "KR_NXT": "H0NXCNT0",         "KR_NXT_HOKA": "H0NXASP0",   # Nextrade
    "KR_SOR": "H0UNCNT0",         "KR_SOR_HOKA": "H0UNASP0",   # SOR
    
    # 2. Derivatives & Bond
    "KR_FUT": "H0IFCNT0", "KR_FUT_HOKA": "H0IFASP0",
    "KR_OPT": "H0IOCNT0", "KR_OPT_HOKA": "H0IOASP0",
    "KR_BOND": "H0BJCNT0", "KR_BOND_HOKA": "H0BJASP0", # 채권 호가는 예시
    "KR_INDEX": "H0UPCNT0",

    # 3. Overseas
    "OS_STOCK": "HDFSCNT0", "OS_STOCK_HOKA": "HDFSASP0",
    "OS_FUT": "HDFFF020",   "OS_FUT_HOKA": "HDFFF010",
    
    # 4. Night Market
    "NIGHT_FUT": "ECEUCNT0", "NIGHT_OPT": "ECEUCNT0",

    # 5. Notice TRs (Execution)
    "NOTICE_KR_STOCK_REAL": "H0STCNI0", "NOTICE_KR_FUT_REAL": "H0IFCNI0",
    "NOTICE_KR_STOCK_DEMO": "H0STCNI9", "NOTICE_KR_FUT_DEMO": "H0IFCNI9",
    "NOTICE_OS_REAL": "H0GSCNI0",     "NOTICE_OS_DEMO": "H0GSCNI9"
}

class AssetType:
    """자산 분류 상수"""
    KR_STOCK = "KR_STOCK"       # 국내 주식 (SOR/NXT 포함)
    KR_FUTOPT = "KR_FUTOPT"     # 국내 선물/옵션
    KR_BOND = "KR_BOND"         # 국내 장내채권
    OS_STOCK = "OS_STOCK"       # 해외 주식
    OS_FUTOPT = "OS_FUTOPT"     # 해외 선물/옵션
    ISA = "ISA"                 # ISA (국내 주식)
       

class KisApiHelper:
    """
    KIS API 설정 통합 관리자 (Gateway & Datafeed 공용)
    Features:
    - Multi-Asset Support (Stock, FutOpt, Bond, Overseas)
    - Full Timeframe Support (1m, 1h, Daily, Weekly, Monthly)
    - Balance & Deposit Query Support
    - Automatic TR Selection based on Interval
    """

    # ----------------------------------------------------------------
    # [Error Handling]
    # ----------------------------------------------------------------
    # 재시도 가능한 에러 (TPS, Gateway Timeout, Server Error)
    RETRYABLE_ERRORS = ["E00001", "OPS", "500", "502", "504"]
    # 재시도 불가능한 에러 (권한 없음, 계좌 오류 등)
    FATAL_ERRORS = ["IGW00121", "IGW00201", "E10000", "E00002"]

    @classmethod
    def check_retryable_error(cls, msg_cd: str) -> bool:
        if not msg_cd: return False
        for code in cls.FATAL_ERRORS:
            if code in msg_cd: return False
        for code in cls.RETRYABLE_ERRORS:
            if code in msg_cd: return True
        return False

    # ----------------------------------------------------------------
    # 1. 거래소 및 자산 정의
    # ----------------------------------------------------------------
    DOMESTIC_EXCHANGES = [Exchange.KRX, Exchange.NXT, Exchange.SOR]
    
    OVERSEAS_STOCK_EXCHANGES = [
        Exchange.NYSE, Exchange.NASDAQ, Exchange.AMEX, 
        Exchange.SEHK, Exchange.TSE, Exchange.SSE, Exchange.SZSE, 
        Exchange.HNX, Exchange.HSX
    ]
    
    OVERSEAS_FUTOPT_EXCHANGES = [
        Exchange.CME, Exchange.EUREX, Exchange.CBOT, 
        Exchange.HKFE, Exchange.SGX, Exchange.ICE
    ]

    @staticmethod
    def get_kis_exchange_code(asset_type: AssetType, exchange: Exchange, is_order: bool = False) -> str:
        """vnpy Exchange -> KIS Code 변환"""
        if asset_type == AssetType.OS_STOCK:
            if not is_order:
                # 시세/조회용 (3자리)
                mapping = {
                    Exchange.NYSE: "NYS", Exchange.NASDAQ: "NAS", Exchange.AMEX: "AMS",
                    Exchange.SEHK: "HKS", Exchange.TSE: "TSE", Exchange.SSE: "SHS",
                    Exchange.SZSE: "SZS", Exchange.HSX: "HSX", Exchange.HNX: "HNX"
                }
                return mapping.get(exchange, "NAS")
            else:
                # 주문용 (4자리)
                mapping = {
                    Exchange.NYSE: "NYSE", Exchange.NASDAQ: "NASD", Exchange.AMEX: "AMEX",
                    Exchange.SEHK: "SEHK", Exchange.TSE: "TKSE", Exchange.SSE: "SHAA",
                    Exchange.SZSE: "SZAA", Exchange.HSX: "VNSE", Exchange.HNX: "HASE"
                }
                return mapping.get(exchange, "NASD")
        elif asset_type == AssetType.OS_FUTOPT:
            # 해외선물은 거래소 코드가 그대로 쓰이는 경우가 많음 (CME, EUREX 등)
            return exchange.value
        return ""

    @staticmethod
    def get_vnpy_exchange_from_kis(asset_type: str, kis_code: str) -> Exchange:
        """KIS Exchange Code -> vnpy Exchange (Response Parsing용)"""
        kis_code = kis_code.upper().strip()
        
        if asset_type == AssetType.OS_STOCK:
            mapping = {
                "NAS": Exchange.NASDAQ, "NASD": Exchange.NASDAQ,
                "NYS": Exchange.NYSE, "NYSE": Exchange.NYSE,
                "AMS": Exchange.AMEX, "AMEX": Exchange.AMEX,
                "HKS": Exchange.SEHK, "SEHK": Exchange.SEHK,
                "TSE": Exchange.TSE, "JP": Exchange.TSE,
                "SHS": Exchange.SHFE, "SZS": Exchange.SZSE,
                "HSX": Exchange.HSX, "HNX": Exchange.HNX
            }
            return mapping.get(kis_code, Exchange.NASDAQ) # Default
            
        elif asset_type == AssetType.OS_FUTOPT:
            try:
                return Exchange(kis_code)
            except:
                return Exchange.CME # Default fallback
        
        return Exchange.KRX

    @staticmethod
    def infer_kr_asset_product(symbol: str) -> Tuple[str, Product]:
        """
        국내 종목코드로 AssetType과 Product를 동시에 유추
        Returns: (AssetType, Product)
        """
        # 1. 채권 (KR로 시작하는 12자리 표준코드)
        if symbol.startswith("KR"):
            return AssetType.KR_BOND, Product.BOND
        
        # 2. 주식/ETF/ETN (숫자 6자리 또는 Q/J로 시작하는 6자리)
        if len(symbol) == 6:
            # Q로 시작하면 ETN, 그 외 숫자는 주식/ETF
            # vnpy Product에 ETF가 있다면 Product.ETF 반환 가능, 없으면 EQUITY
            return AssetType.KR_STOCK, Product.EQUITY

        # 3. 선물/옵션 (보통 단축코드 사용)
        # 지수선물(1), 지수옵션(2,3), 주식선물(1), 주식옵션(2,3)
        # KIS 단축코드는 보통 8자리 (예: 101T6000)
        if len(symbol) >= 8 or (len(symbol) >= 3 and symbol[0] in ['1', '2', '3']):
            head = symbol[0]
            if head == '1': 
                return AssetType.KR_FUTOPT, Product.FUTURES
            elif head in ['2', '3']:
                return AssetType.KR_FUTOPT, Product.OPTION
        
        # 기본값: 주식으로 간주
        return AssetType.KR_STOCK, Product.EQUITY
    
    @classmethod
    def get_asset_type(cls, exchange: Exchange, symbol: str = "") -> Optional[str]:
        """자산 타입 판별"""
        if exchange in cls.DOMESTIC_EXCHANGES or str(exchange) in ["KRX", "NXT", "SOR"]:
            asset_type, _ = cls.infer_kr_asset_product(symbol)
            return asset_type
        if exchange in cls.OVERSEAS_STOCK_EXCHANGES: return AssetType.OS_STOCK
        if exchange in cls.OVERSEAS_FUTOPT_EXCHANGES: return AssetType.OS_FUTOPT
        return None

    @staticmethod
    def get_market_code(exchange: Exchange, asset_type: AssetType) -> str:
        if asset_type == AssetType.KR_STOCK:
            """시장 구분 코드 (주문/현재가용)"""
            if exchange == Exchange.KRX: return "J"
            if exchange == Exchange.NXT: return "NX"
            if exchange == Exchange.SOR: return "UN"
        elif asset_type == AssetType.KR_FUTOPT: return "F"
        elif asset_type == AssetType.KR_BOND: return "B"
        return "J"

    # ----------------------------------------------------------------
    # 2. TR 레지스트리 (자산별/기능별 URL 매핑)
    # ----------------------------------------------------------------
    TR_REGISTRY = {
        # --- [A] 국내 주식 (KR_STOCK) ---
        (AssetType.KR_STOCK, "HISTORY"): { # 분봉/시봉
            "url": "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            "tr_id": "FHKST03010200", "pg_method": "TIME"
        },
        (AssetType.KR_STOCK, "HISTORY_PERIOD"): { # 일봉/주봉/월봉
            "url": "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            "tr_id": "FHKST03010100", "pg_method": "DATE"
        },
        (AssetType.KR_STOCK, "QUOTE"): { "url": "/uapi/domestic-stock/v1/quotations/inquire-price", "tr_id": "FHKST01010100" },
        (AssetType.KR_STOCK, "ORDER_BUY"): { "url": "/uapi/domestic-stock/v1/trading/order-cash", "tr_id": ("TTTC0802U", "VTTC0802U") },
        (AssetType.KR_STOCK, "ORDER_SELL"): { "url": "/uapi/domestic-stock/v1/trading/order-cash", "tr_id": ("TTTC0801U", "VTTC0801U") },
        (AssetType.KR_STOCK, "ORDER_MODIFY"): { "url": "/uapi/domestic-stock/v1/trading/order-rvsecncl", "tr_id": ("TTTC0803U", "VTTC0803U") },
        (AssetType.KR_STOCK, "BALANCE"): { "url": "/uapi/domestic-stock/v1/trading/inquire-balance", "tr_id": ("TTTC8434R", "VTTC8434R") },
        (AssetType.KR_STOCK, "DEPOSIT"): { "url": "/uapi/domestic-stock/v1/trading/inquire-psbl-order", "tr_id": ("TTTC8908R", "VTTC8908R") },

        # --- [B] 해외 주식 (OS_STOCK) ---
        (AssetType.OS_STOCK, "HISTORY"): { # 분봉/시봉
            "url": "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice",
            "tr_id": "HHDFS76950200", "pg_method": "KEY"
        },
        (AssetType.OS_STOCK, "HISTORY_PERIOD"): { # 일봉/주봉/월봉
            "url": "/uapi/overseas-price/v1/quotations/dailyprice",
            "tr_id": "HHDFS76240000", "pg_method": "KEY"
        },
        (AssetType.OS_STOCK, "QUOTE"): { "url": "/uapi/overseas-price/v1/quotations/price", "tr_id": "HHDFS00000300" },
        (AssetType.OS_STOCK, "ORDER_BUY"): { "url": "/uapi/overseas-stock/v1/trading/order", "tr_id": ("TTTT1002U", "VTTT1002U") },
        (AssetType.OS_STOCK, "ORDER_SELL"): { "url": "/uapi/overseas-stock/v1/trading/order", "tr_id": ("TTTT1006U", "VTTT1006U") },
        (AssetType.OS_STOCK, "ORDER_MODIFY"): { "url": "/uapi/overseas-stock/v1/trading/order-rvsecncl", "tr_id": ("TTTT1004U", "VTTT1004U") },
        (AssetType.OS_STOCK, "BALANCE"): { "url": "/uapi/overseas-stock/v1/trading/inquire-balance", "tr_id": ("TTTS3012R", "VTTS3012R") },
        (AssetType.OS_STOCK, "DEPOSIT"): { "url": "/uapi/overseas-stock/v1/trading/inquire-present-balance", "tr_id": ("CTRP6504R", "VTRP6504R") },

        # --- [C] 국내 선옵 (KR_FUTOPT) ---
        (AssetType.KR_FUTOPT, "HISTORY"): { # 분봉 (시간별)
            "url": "/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice",
            "tr_id": "FHKIF03020200", "pg_method": "TIME"
        },
        (AssetType.KR_FUTOPT, "HISTORY_PERIOD"): { # 일봉/주봉/월봉 (기간별)
            "url": "/uapi/domestic-futureoption/v1/quotations/inquire-daily-futureoptionchartprice",
            "tr_id": "FHKIF03020100", "pg_method": "DATE"
        },
        # (AssetType.KR_FUTOPT, "QUOTE"): { "url": "/uapi/domestic-futureoption/v1/quotations/inquire-price", "tr_id": "FHMIF10000000" },
        (AssetType.KR_FUTOPT, "QUOTE"): { "url": "/uapi/domestic-futureoption/v1/quotations/inquire-price", "tr_id": "FHKIF02010200" },
        (AssetType.KR_FUTOPT, "ORDER"): { "url": "/uapi/domestic-futureoption/v1/trading/order", "tr_id": ("TTTO1101U", "VTTO1101U") },
        (AssetType.KR_FUTOPT, "ORDER_MODIFY"): { "url": "/uapi/domestic-futureoption/v1/trading/order-rvsecncl", "tr_id": ("TTTO1103U", "VTTO1103U") },
        (AssetType.KR_FUTOPT, "BALANCE"): { "url": "/uapi/domestic-futureoption/v1/trading/inquire-balance", "tr_id": ("CTFO6118R", "VTFO6118R") },
        (AssetType.KR_FUTOPT, "DEPOSIT"): { "url": "/uapi/domestic-futureoption/v1/trading/inquire-deposit", "tr_id": ("CTRP6550R", None) },

        # --- [D] 해외 선옵 (OS_FUTOPT) ---
        (AssetType.OS_FUTOPT, "HISTORY"): { # 분봉
            "url": "/uapi/overseas-futureoption/v1/quotations/inquire-time-futurechartprice",
            "tr_id": "HHDFC55020400", "pg_method": "KEY"
        },
        (AssetType.OS_FUTOPT, "HISTORY_PERIOD"): { # 일봉/주봉/월봉
            "url": "/uapi/overseas-futureoption/v1/quotations/inquire-daily-futurechartprice",
            "tr_id": "HHDFC55020300", "pg_method": "KEY"
        },
        # (AssetType.OS_FUTOPT, "QUOTE"): { "url": "/uapi/overseas-futureoption/v1/quotations/inquire-price", "tr_id": "HHDFC55010000" },
        (AssetType.OS_FUTOPT, "QUOTE"): { "url": "/uapi/overseas-futureoption/v1/quotations/inquire-price", "tr_id": "HHDFS76200200" },
        (AssetType.OS_FUTOPT, "ORDER"): { "url": "/uapi/overseas-futureoption/v1/trading/order", "tr_id": ("OTFM3001U", None) },
        (AssetType.OS_FUTOPT, "ORDER_MODIFY"): { "url": "/uapi/overseas-futureoption/v1/trading/order-rvsecncl", "tr_id": ("OTFM3002U", None) },
        (AssetType.OS_FUTOPT, "ORDER_CANCEL"): { "url": "/uapi/overseas-futureoption/v1/trading/order-rvsecncl", "tr_id": ("OTFM3003U", None) },
        (AssetType.OS_FUTOPT, "BALANCE"): { "url": "/uapi/overseas-futureoption/v1/trading/inquire-unpd", "tr_id": ("OTFM1412R", None) },
        (AssetType.OS_FUTOPT, "DEPOSIT"): { "url": "/uapi/overseas-futureoption/v1/trading/inquire-deposit", "tr_id": ("OTFM1411R", None) },

        # --- [E] 채권 (KR_BOND) ---
        (AssetType.KR_BOND, "HISTORY"): { # 채권은 기간별(일봉)만 지원
            "url": "/uapi/domestic-bond/v1/quotations/inquire-daily-itemchartprice",
            "tr_id": "FHKBJ773701C0", "pg_method": "NONE"
        },
        (AssetType.KR_BOND, "HISTORY_PERIOD"): { 
            "url": "/uapi/domestic-bond/v1/quotations/inquire-daily-itemchartprice",
            "tr_id": "FHKBJ773701C0", "pg_method": "NONE"
        },
        # (AssetType.KR_BOND, "QUOTE"): { "url": "/uapi/domestic-bond/v1/quotations/inquire-price", "tr_id": "FHKBJ773400C0" },
        (AssetType.KR_BOND, "QUOTE"): { "url": "/uapi/domestic-bond/v1/quotations/inquire-price", "tr_id": "FHKBN02010100" },
        (AssetType.KR_BOND, "ORDER_BUY"): { "url": "/uapi/domestic-bond/v1/trading/buy", "tr_id": ("TTTC0952U", None) },
        (AssetType.KR_BOND, "ORDER_SELL"): { "url": "/uapi/domestic-bond/v1/trading/sell", "tr_id": ("TTTC0958U", None) },
        (AssetType.KR_BOND, "ORDER_MODIFY"): { "url": "/uapi/domestic-bond/v1/trading/order-rvsecncl", "tr_id": ("TTTC0953U", None) },
        (AssetType.KR_BOND, "BALANCE"): { "url": "/uapi/domestic-bond/v1/trading/inquire-balance", "tr_id": ("CTSC8407R", None) },
    }

    # -----------------------------------------------------------
    # [Routing Configuration] 체결통보(Notice) 라우팅 맵
    # Structure: { AssetType: { ServerType: [TR_ID_List] } }
    # -----------------------------------------------------------
    NOTICE_TR_MAP = {
        AssetType.KR_STOCK: {
            "REAL": ["H0STCNI0"],  # 국내주식 실전
            "DEMO": ["H0STCNI9"]   # 국내주식 모의
        },
        AssetType.OS_STOCK: {
            "REAL": ["H0GSCNI0"],  # 해외주식 실전
            "DEMO": ["H0GSCNI9"]   # 해외주식 모의
        },
        AssetType.KR_FUTOPT: {
            "REAL": ["H0IFCNI0"],  # 국내선물 실전
            "DEMO": ["H0IFCNI9"]   # 국내선물 모의
        },
        AssetType.OS_FUTOPT: {
            "REAL": ["HDFFF2C0", "HDFFF1C0"], # 해외선물 실전
            "DEMO": [] 
        },
        AssetType.KR_BOND: {
            "REAL": [], # 채권 체결통보 TR 확인 필요 (현재 공란)
            "DEMO": []
        }
    }

    # ----------------------------------------------------------------
    # 3. 설정 조회 (Action & Interval 자동 분기)
    # ----------------------------------------------------------------
    @classmethod
    def get_tr_config(cls, asset_type: AssetType, action: str, is_vts: bool = False, interval: Interval = None) -> Optional[dict]:
        """
        API 설정 반환
        Action과 Interval을 기반으로 최적의 TR(분봉 vs 기간별) 자동 선택
        """
        target_action = action
        
        # HISTORY 요청인 경우 Interval에 따라 Action 분기
        if action == "HISTORY" and interval:
            # 1. 기간별(일/주/월) TR 사용
            if interval in [Interval.DAILY, Interval.WEEKLY, Interval.MONTHLY]:
                target_action = "HISTORY_PERIOD"
            # 2. 타임라인(분/시) TR 사용 (기본값)
            elif interval in [Interval.MINUTE, Interval.HOUR]:
                target_action = "HISTORY"
        
        # 1차 조회
        config = cls.TR_REGISTRY.get((asset_type, target_action))
        
        # Fallback (HISTORY <-> HISTORY_PERIOD)
        if not config and target_action == "HISTORY_PERIOD":
            config = cls.TR_REGISTRY.get((asset_type, "HISTORY"))
        
        # Order 통합 처리 (매수/매도/정정/취소)
        if not config:
            if action in ["ORDER_BUY", "ORDER_SELL"] and asset_type in [AssetType.KR_FUTOPT, AssetType.OS_FUTOPT]:
                config = cls.TR_REGISTRY.get((asset_type, "ORDER"))
            elif action == "ORDER_CANCEL" and asset_type != AssetType.OS_FUTOPT:
                config = cls.TR_REGISTRY.get((asset_type, "ORDER_MODIFY"))
        
        if not config:
            return None
        
        # 실전/모의 ID 선택 (Tuple 처리)
        tr_val = config["tr_id"]
        final_tr_id = tr_val[1] if (isinstance(tr_val, tuple) and is_vts) else (tr_val[0] if isinstance(tr_val, tuple) else tr_val)

        if not final_tr_id:
            return None 

        result = config.copy()
        result["tr_id"] = final_tr_id
        return result

    # ----------------------------------------------------------------
    # 4. 파라미터 빌더 (전 자산 Interval 완벽 지원)
    # ----------------------------------------------------------------
    @classmethod
    def build_history_params(cls, req: HistoryRequest, config: dict, end_dt: datetime, next_key: str = "") -> dict:
        """
        히스토리 파라미터 생성 (분봉/시봉/일봉/주봉/월봉 지원)
        """
        asset_type = cls.get_asset_type(req.exchange, req.symbol)
        date_str = end_dt.strftime("%Y%m%d")
        time_str = end_dt.strftime("%H%M%S")
        
        # 공통 기간 코드 매핑 (D:일, W:주, M:월)
        period_code = "D"
        if req.interval == Interval.WEEKLY: period_code = "W"
        elif req.interval == Interval.MONTHLY: period_code = "M"
        
        # --- [A] 국내 주식 (KR_STOCK) ---
        if asset_type == AssetType.KR_STOCK:
            force_market_code = "J" # 히스토리는 항상 KRX(J) 기준
            
            # Case 1: 기간별 시세 (일/주/월)
            if config["tr_id"] == "FHKST03010100":
                return {
                    "FID_COND_MRKT_DIV_CODE": force_market_code,
                    "FID_INPUT_ISCD": req.symbol,
                    "FID_INPUT_DATE_1": req.start.strftime("%Y%m%d"),
                    "FID_INPUT_DATE_2": date_str,
                    "FID_PERIOD_DIV_CODE": period_code,
                    "FID_ORG_ADJ_PRC": "0" 
                }
            # Case 2: 분봉/시봉 시세
            else:
                return {
                    "FID_ETC_CLS_CODE": "",
                    "FID_COND_MRKT_DIV_CODE": force_market_code,
                    "FID_INPUT_ISCD": req.symbol,
                    "FID_INPUT_DATE_1": date_str,
                    "FID_INPUT_HOUR_1": time_str,
                    "FID_PW_DATA_INCU_YN": "Y"
                }

        # --- [B] 해외 주식 (OS_STOCK) ---
        elif asset_type == AssetType.OS_STOCK:
            excd = cls.get_kis_exchange_code(asset_type, req.exchange, is_order=False)
            
            # Case 1: 기간별 시세 (일/주/월)
            if config["tr_id"] == "HHDFS76240000":
                gubn = "0" # 일
                if req.interval == Interval.WEEKLY: gubn = "1"
                elif req.interval == Interval.MONTHLY: gubn = "2"
                
                return {
                    "AUTH": "", "EXCD": excd, "SYMB": req.symbol,
                    "GUBN": gubn, "BYMD": date_str, "MODP": "1", "KEYB": next_key
                }
            # Case 2: 분봉/시봉 시세
            else:
                nmin = "1"
                if req.interval == Interval.HOUR: nmin = "60"
                return {
                    "AUTH": "", "EXCD": excd, "SYMB": req.symbol,
                    "NMIN": nmin, "PINC": "1", "NEXT": "1" if next_key else "",
                    "NREC": "120", "KEYB": next_key
                }

        # --- [C] 국내 선물옵션 (KR_FUTOPT) ---
        elif asset_type == AssetType.KR_FUTOPT:
            # Case 1: 기간별 시세 (일/주/월)
            if config["tr_id"] == "FHKIF03020100":
                return {
                    "FID_COND_MRKT_DIV_CODE": "",
                    "FID_INPUT_ISCD": req.symbol,
                    "FID_INPUT_DATE_1": req.start.strftime("%Y%m%d"),
                    "FID_INPUT_DATE_2": date_str,
                    "FID_PERIOD_DIV_CODE": period_code # D/W/M
                }
            # Case 2: 분봉 시세
            else:
                return {
                    "FID_COND_MRKT_DIV_CODE": "",
                    "FID_INPUT_ISCD": req.symbol,
                    "FID_INPUT_DATE_1": date_str,
                    "FID_INPUT_HOUR_1": time_str,
                    "FID_PW_DATA_INCU_YN": "Y"
                }

        # --- [D] 채권 (KR_BOND) ---
        elif asset_type == AssetType.KR_BOND:
            # 채권은 기간별(일봉)만 지원하므로 통합 처리
            return {
                "FID_COND_MRKT_DIV_CODE": "B",
                "FID_INPUT_ISCD": req.symbol,
                "FID_INPUT_DATE_1": req.start.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": date_str,
                "FID_PERIOD_DIV_CODE": period_code # D/W/M 지원
            }
            
        # --- [E] 해외 선물옵션 (OS_FUTOPT) ---
        elif asset_type == AssetType.OS_FUTOPT:
            # Case 1: 기간별 시세 (일/주/월)
            if config["tr_id"] == "HHDFC55020300":
                gubn = "0"
                if req.interval == Interval.WEEKLY: gubn = "1"
                elif req.interval == Interval.MONTHLY: gubn = "2"
                
                return {
                    "SRS_CD": req.symbol,
                    "GUBN": gubn, # 0:일, 1:주, 2:월
                    "QRY_TP": "P" if next_key else "Q", # 페이징
                    "CNT": "100", # 조회 건수
                    "INDEX_KEY": next_key
                }
            # Case 2: 분봉 시세
            else:
                return {
                    "SRS_CD": req.symbol,
                    "CNT": "120",
                    "QRY_TP": "P" if next_key else "Q",
                    "INDEX_KEY": next_key
                }
        return {}

    # ----------------------------------------------------------------
    # 5. 주문 및 기타 파라미터 빌더 (DEPOSIT 포함)
    # ----------------------------------------------------------------
    @classmethod
    def build_order_params(cls, req: OrderRequest, asset_type: str, account_no: str) -> dict:
        cano = account_no[:8]
        acnt_prdt_cd = account_no[8:] if len(account_no) > 8 else "01"
        qty = str(int(req.volume))
        price = str(req.price) if req.price else "0"
        ord_dvsn = "00" if req.type == OrderType.LIMIT else "01"

        if asset_type == AssetType.KR_STOCK:
            return {
                "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "PDNO": req.symbol,
                "ORD_DVSN": ord_dvsn, "ORD_QTY": qty, "ORD_UNPR": price
            }
        elif asset_type == AssetType.OS_STOCK:
            ovrs_excg = cls.get_kis_exchange_code(asset_type, req.exchange, is_order=True)
            return {
                "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "OVRS_EXCG_CD": ovrs_excg,
                "PDNO": req.symbol, "ORD_QTY": qty, "OVRS_ORD_UNPR": price,
                "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": ord_dvsn
            }
        elif asset_type == AssetType.KR_FUTOPT:
            sll_buy = "02" if req.direction == Direction.LONG else "01"
            return {
                "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "PDNO": req.symbol,
                "ORD_DVSN_CD": ord_dvsn, "SLL_BUY_DVSN_CD": sll_buy,
                "ORD_QTY": qty, "ORD_UNPR": price
            }
        elif asset_type == AssetType.OS_FUTOPT:
            sll_buy = "02" if req.direction == Direction.LONG else "01"
            return {
                "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "OVRS_FUTR_FX_PDNO": req.symbol,
                "SLL_BUY_DVSN_CD": sll_buy, "FM_ORD_QTY": qty, "FM_ORD_PRIC": price,
                "ORD_DVSN_CD": ord_dvsn, "FM_LQD_LMT_ORD_PRIC": "", "CCLD_CNDT_CD": "6"
            }
        elif asset_type == AssetType.KR_BOND:
            return {
                "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "PDNO": req.symbol,
                "ORD_QTY2": qty, "BOND_ORD_UNPR": price,
                "SAMT_MKET_PTCI_YN": "N", "BOND_RTL_MKET_YN": "N", "ORD_SVR_DVSN_CD": "0"
            }
        return {}

    @classmethod
    def build_cancel_params(cls, req: CancelRequest, asset_type: str, account_no: str) -> dict:
        cano = account_no[:8]
        acnt_prdt_cd = account_no[8:] if len(account_no) > 8 else "01"
        params = {
            "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd,
            "ORGN_ODNO": str(req.orderid), "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": "0", "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y"
        }
        if asset_type == AssetType.OS_STOCK:
            params["OVRS_EXCG_CD"] = cls.get_kis_exchange_code(asset_type, req.exchange, is_order=True)
            params["PDNO"] = req.symbol
            params["OVRS_ORD_UNPR"] = "0"
            del params["ORD_UNPR"]
        return params

    @classmethod
    def build_balance_params(cls, asset_type: str, account_no: str) -> dict:
        """잔고 조회 파라미터 (DEPOSIT/BALANCE 공용)"""
        cano = account_no[:8]
        acnt_prdt_cd = account_no[8:] if len(account_no) > 8 else "01"
        
        # 기본 계좌 파라미터 (대부분의 조회 API가 이 포맷을 따름)
        params = {"CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd}

        if asset_type == AssetType.KR_STOCK:
            # 주식 잔고 상세 조회 옵션
            params.update({
                "AFHR_FLPR_YN": "N", "OFL_YN": "N", "INQR_DVSN": "02", 
                "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", 
                "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00", 
                "CTX_AREA_FK": "", "CTX_AREA_NK": ""
            })
        elif asset_type == AssetType.OS_STOCK:
            # 해외주식 통화/국가 옵션
            params.update({
                "WCRC_FRCR_DVSN_CD": "02", "NATN_CD": "840", 
                "TR_MKET_CD": "00", "CTX_AREA_FK": "", "CTX_AREA_NK": ""
            })
        elif asset_type == AssetType.KR_FUTOPT:
            params.update({"FUTR_OPT_GTFO_DVSN_CD": "1", "CTX_AREA_FK": "", "CTX_AREA_NK": ""})
        elif asset_type == AssetType.OS_FUTOPT:
            params.update({"CRCY_CD": "USD", "SORT_SQN": "DS", "CTX_AREA_FK": "", "CTX_AREA_NK": ""})
        elif asset_type == AssetType.KR_BOND:
            params.update({"INQR_DVSN": "01", "CTX_AREA_FK": "", "CTX_AREA_NK": ""})
            
        return params
    
    @classmethod
    def build_deposit_params(cls, asset_type: str, account_no: str) -> dict:
        """예수금(주문가능금액) 조회 파라미터 (Builds on Balance params or customizes)"""
        cano = account_no[:8]
        acnt_prdt_cd = account_no[8:] if len(account_no) > 8 else "01"
        
        # 일부 예수금 TR은 잔고 TR과 파라미터가 다를 수 있음
        if asset_type == AssetType.KR_STOCK:
            # 매수가능조회 (TTTC8908R) - PDNO, ORD_UNPR 등이 필요할 수 있으나 전체 조회시 공란 가능
            return {
                "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd,
                "PDNO": "", "ORD_UNPR": "0", "ORD_DVSN": "02", 
                "CMA_EVLU_AMT_ICLD_YN": "Y", "OVRS_ICLD_YN": "Y"
            }
        
        # 나머지 자산은 Balance 파라미터와 유사하거나 공유
        return cls.build_balance_params(asset_type, account_no)
    