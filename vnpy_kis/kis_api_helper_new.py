from datetime import datetime
from typing import Dict, Optional, Tuple, Any, List

from vnpy.trader.constant import Exchange, Direction, OrderType, Interval
from vnpy.trader.object import HistoryRequest, OrderRequest, CancelRequest

class AssetType:
    """자산 분류 상수"""
    KR_STOCK = "KR_STOCK"       # 국내 주식
    KR_FUTOPT = "KR_FUTOPT"     # 국내 선물/옵션
    KR_BOND = "KR_BOND"         # 국내 장내채권
    OS_STOCK = "OS_STOCK"       # 해외 주식 (미국/아시아)
    OS_FUTOPT = "OS_FUTOPT"     # 해외 선물/옵션

class KisApiHelper:
    """
    KIS API 설정 통합 관리자 (Gateway & Datafeed 공용)
    [Improved] Error Code Mapping & Retry Logic Support
    """

    # ----------------------------------------------------------------
    # [NEW] Error Code Mapping & Retry Logic
    # ----------------------------------------------------------------
    # 재시도 가능한 에러 코드 (TPS 초과, 일시적 서버 오류, 타임아웃 등)
    RETRYABLE_ERRORS = [
        "E00001", # 일반적인 시스템 오류 (간혹 재시도 성공)
        "OPS",    # KIS Rate Limit 관련 Prefix (OPS로 시작하는 경우 많음)
        "500", "502", "504" # HTTP Server Errors
    ]
    
    # 명백한 실패 코드 (재시도 금지 - 즉시 실패 처리)
    FATAL_ERRORS = [
        "IGW00121", # 증거금 부족
        "IGW00201", # 주문가능수량 초과
        "E10000",   # 인증 실패 (토큰 만료 제외, 아예 잘못된 접근)
        "E00002"    # 잘못된 파라미터
    ]

    @classmethod
    def check_retryable_error(cls, msg_cd: str) -> bool:
        """
        [NEW] 에러 코드가 재시도 가능한 유형인지 확인합니다.
        """
        if not msg_cd: return False
        
        # 1. 명시적 Fatal Error 확인
        for code in cls.FATAL_ERRORS:
            if code in msg_cd: return False
            
        # 2. Retryable Error 확인
        for code in cls.RETRYABLE_ERRORS:
            if code in msg_cd: return True
            
        return False

    # ----------------------------------------------------------------
    # 1. 자산 및 거래소 분류 기준
    # ----------------------------------------------------------------
    DOMESTIC_EXCHANGES = [Exchange.KRX, Exchange.NXT, "SOR"]
    
    # 해외 주식 거래소 목록
    OVERSEAS_STOCK_EXCHANGES = [
        Exchange.NYSE, Exchange.NASDAQ, Exchange.AMEX, 
        Exchange.SEHK, Exchange.TSE, 
        Exchange.SSE, Exchange.SZSE, Exchange.HNX, Exchange.HSX
    ]
    
    # 해외 선물 거래소 목록
    OVERSEAS_FUTOPT_EXCHANGES = [
        Exchange.CME, Exchange.EUREX, Exchange.CBOT, 
        Exchange.HKFE, Exchange.SGX, Exchange.ICE
    ]

    # ----------------------------------------------------------------
    # 2. 거래소 코드 매핑 (VNPY Enum -> KIS Code)
    # ----------------------------------------------------------------
    @classmethod
    def get_kis_exchange_code(cls, exchange: Exchange, is_order: bool = False) -> str:
        """
        vnpy Exchange를 KIS API용 거래소 코드로 변환
        
        Args:
            exchange: vnpy Exchange Enum
            is_order: True(주문용 4자리 코드), False(시세/히스토리용 3자리 코드)
        """
        # --- A. 시세/조회용 코드 (3~4자리 축약) ---
        if not is_order:
            mapping = {
                Exchange.NYSE: "NYS", Exchange.NASDAQ: "NAS", Exchange.AMEX: "AMS",
                Exchange.SEHK: "HKS", Exchange.TSE: "TSE", 
                Exchange.SSE: "SHS", Exchange.SZSE: "SZS",
                Exchange.HSX: "HSX", Exchange.HNX: "HNX"
            }
            return mapping.get(exchange, "NAS") # 기본값 NAS

        # --- B. 주문용 코드 (해외주문 시 사용되는 4자리 표준) ---
        else:
            mapping = {
                Exchange.NYSE: "NYSE", Exchange.NASDAQ: "NASD", Exchange.AMEX: "AMEX",
                Exchange.SEHK: "SEHK", Exchange.TSE: "TKSE",
                Exchange.SSE: "SHAA", Exchange.SZSE: "SZAA",
                Exchange.HSX: "VNSE", Exchange.HNX: "HASE"
            }
            return mapping.get(exchange, "NASD") # 기본값 NASD

    @classmethod
    def get_asset_type(cls, exchange: Exchange, symbol: str = "") -> Optional[str]:
        """거래소와 심볼 패턴을 기반으로 자산 타입 판별"""
        
        # A. 국내 시장
        if exchange in cls.DOMESTIC_EXCHANGES or str(exchange) in ["KRX", "NXT", "SOR"]:
            if len(symbol) >= 12 and symbol.startswith("KR"):
                return AssetType.KR_BOND
            elif len(symbol) == 6 and symbol.isdigit():
                return AssetType.KR_STOCK
            elif len(symbol) >= 8:
                return AssetType.KR_FUTOPT
            
        # B. 해외 시장
        if exchange in cls.OVERSEAS_STOCK_EXCHANGES:
            return AssetType.OS_STOCK
        if exchange in cls.OVERSEAS_FUTOPT_EXCHANGES:
            return AssetType.OS_FUTOPT
            
        return None

    # ----------------------------------------------------------------
    # 3. TR 레지스트리 (자산별/기능별 URL & TR_ID 매핑)
    # ----------------------------------------------------------------
    # tuple: (Real_TR_ID, VTS_TR_ID) 또는 Single String
    TR_REGISTRY = {
        # [1] 히스토리 (History)
        (AssetType.KR_STOCK, "HISTORY"): {
            "url": "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            "tr_id": "FHKST03010200", "pg_method": "TIME"
        },
        (AssetType.OS_STOCK, "HISTORY"): {
            "url": "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice",
            "tr_id": "HHDFS76950200", "pg_method": "KEY"
        },
        (AssetType.KR_FUTOPT, "HISTORY"): {
            "url": "/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice",
            "tr_id": "FHKIF03020200", "pg_method": "TIME"
        },
        (AssetType.OS_FUTOPT, "HISTORY"): {
            "url": "/uapi/overseas-futureoption/v1/quotations/inquire-time-futurechartprice",
            "tr_id": "HHDFC55020400", "pg_method": "KEY"
        },
        (AssetType.KR_BOND, "HISTORY"): {
            "url": "/uapi/domestic-bond/v1/quotations/inquire-daily-itemchartprice",
            "tr_id": "FHKBJ773701C0", "pg_method": "NONE"
        },

        # [2] 주문 (Order) - 매수/매도 TR이 분리된 경우와 통합된 경우
        (AssetType.KR_STOCK, "ORDER_BUY"): {
            "url": "/uapi/domestic-stock/v1/trading/order-cash",
            "tr_id": ("TTTC0802U", "VTTC0802U")
        },
        (AssetType.KR_STOCK, "ORDER_SELL"): {
            "url": "/uapi/domestic-stock/v1/trading/order-cash",
            "tr_id": ("TTTC0801U", "VTTC0801U")
        },
        (AssetType.OS_STOCK, "ORDER_BUY"): {
            "url": "/uapi/overseas-stock/v1/trading/order",
            "tr_id": ("TTTT1002U", "VTTT1002U") # [주의] 일부 실전계좌 TR JTTT1002U 확인 필요
        },
        (AssetType.OS_STOCK, "ORDER_SELL"): {
            "url": "/uapi/overseas-stock/v1/trading/order",
            "tr_id": ("TTTT1006U", "VTTT1006U")
        },
        (AssetType.KR_FUTOPT, "ORDER"): {
            "url": "/uapi/domestic-futureoption/v1/trading/order",
            "tr_id": ("TTTO1101U", "VTTO1101U") 
        },
        (AssetType.OS_FUTOPT, "ORDER"): {
            "url": "/uapi/overseas-futureoption/v1/trading/order",
            "tr_id": ("OTFM3001U", None)
        },
        (AssetType.KR_BOND, "ORDER_BUY"): {
            "url": "/uapi/domestic-bond/v1/trading/buy",
            "tr_id": ("TTTC0952U", None)
        },
        
        # [3] 정정/취소 (Modify/Cancel)
        (AssetType.KR_STOCK, "ORDER_MODIFY"): {
             "url": "/uapi/domestic-stock/v1/trading/order-rvsecncl",
             "tr_id": ("TTTC0803U", "VTTC0803U")
        },
        (AssetType.OS_STOCK, "ORDER_MODIFY"): {
             "url": "/uapi/overseas-stock/v1/trading/order-rvsecncl",
             "tr_id": ("TTTT1004U", "VTTT1004U")
        },

        # [4] 잔고 (Balance)
        (AssetType.KR_STOCK, "BALANCE"): {
            "url": "/uapi/domestic-stock/v1/trading/inquire-balance",
            "tr_id": ("TTTC8434R", "VTTC8434R")
        },
        (AssetType.OS_STOCK, "BALANCE"): {
            "url": "/uapi/overseas-stock/v1/trading/inquire-balance",
            "tr_id": ("TTTS3012R", "VTTS3012R")
        },
        (AssetType.KR_FUTOPT, "BALANCE"): {
            "url": "/uapi/domestic-futureoption/v1/trading/inquire-balance",
            "tr_id": ("CTFO6118R", "VTFO6118R")
        },
        (AssetType.OS_FUTOPT, "BALANCE"): {
            "url": "/uapi/overseas-futureoption/v1/trading/inquire-deposit",
            "tr_id": ("OTFM1411R", None)
        }
    }

    @classmethod
    def get_tr_config(cls, asset_type: str, action: str, is_vts: bool = False) -> Optional[dict]:
        """API 설정 반환 (실전/모의 자동 분기)"""
        # 1. 1차 조회
        config = cls.TR_REGISTRY.get((asset_type, action))
        
        # 2. 통합 TR 조회 (매수/매도 구분 없이 ORDER 사용 시 Fallback)
        if not config:
            if action in ["ORDER_BUY", "ORDER_SELL"] and (asset_type in [AssetType.KR_FUTOPT, AssetType.OS_FUTOPT]):
                config = cls.TR_REGISTRY.get((asset_type, "ORDER"))
            elif action == "ORDER_CANCEL":
                config = cls.TR_REGISTRY.get((asset_type, "ORDER_MODIFY"))

        if not config:
            return None
        
        # 3. 실전/모의 분기 (Tuple 처리)
        tr_val = config["tr_id"]
        if isinstance(tr_val, tuple):
            final_tr_id = tr_val[1] if is_vts else tr_val[0]
        else:
            final_tr_id = tr_val

        if not final_tr_id:
            return None

        result = config.copy()
        result["tr_id"] = final_tr_id
        return result

    # ----------------------------------------------------------------
    # 4. 파라미터 빌더 (Request -> JSON Params)
    # ----------------------------------------------------------------
    
    @classmethod
    def build_history_params(cls, req: HistoryRequest, config: dict, end_dt: datetime, next_key: str = "") -> dict:
        """히스토리 파라미터 (시세용 코드 사용)"""
        date_str = end_dt.strftime("%Y%m%d")
        time_str = end_dt.strftime("%H%M%S")
        
        asset_type = cls.get_asset_type(req.exchange, req.symbol)
        
        if asset_type == AssetType.KR_STOCK:
            return {
                "FID_ETC_CLS_CODE": "",
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": req.symbol,
                "FID_INPUT_DATE_1": date_str,
                "FID_INPUT_HOUR_1": time_str,
                "FID_PW_DATA_INCU_YN": "Y"
            }
        elif asset_type == AssetType.KR_FUTOPT:
            return {
                "FID_COND_MRKT_DIV_CODE": "",
                "FID_INPUT_ISCD": req.symbol,
                "FID_INPUT_DATE_1": date_str,
                "FID_INPUT_HOUR_1": time_str,
                "FID_PW_DATA_INCU_YN": "Y"
            }
        elif asset_type == AssetType.KR_BOND:
            return {
                "FID_COND_MRKT_DIV_CODE": "B",
                "FID_INPUT_ISCD": req.symbol,
                "FID_INPUT_DATE_1": req.start.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": date_str,
                "FID_PERIOD_DIV_CODE": "D"
            }
        elif asset_type == AssetType.OS_STOCK:
            # [매핑 적용] 시세 조회용 3자리 코드 (EXCD)
            excd = cls.get_kis_exchange_code(req.exchange, is_order=False)
            
            return {
                "AUTH": "",
                "EXCD": excd,
                "SYMB": req.symbol,
                "NMIN": "1",
                "PINC": "1",
                "NEXT": "1" if next_key else "",
                "NREC": "120",
                "KEYB": next_key
            }
        elif asset_type == AssetType.OS_FUTOPT:
            return {
                "SRS_CD": req.symbol,
                "CNT": "120",
                "QRY_TP": "P" if next_key else "Q",
                "INDEX_KEY": next_key
            }
        return {}

    @classmethod
    def build_order_params(cls, req: OrderRequest, asset_type: str, account_no: str) -> dict:
        """주문 파라미터 (주문용 코드 사용)"""
        cano = account_no[:8]
        acnt_prdt_cd = account_no[8:] if len(account_no) > 8 else "01"
        
        qty_str = str(int(req.volume))
        price_str = str(req.price) if req.price else "0"
        
        # KIS 주문구분: 00(지정가), 01(시장가)
        ord_dvsn = "00" 
        if req.type == OrderType.MARKET:
            ord_dvsn = "01" 

        if asset_type == AssetType.KR_STOCK:
            return {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "PDNO": req.symbol,
                "ORD_DVSN": ord_dvsn,
                "ORD_QTY": qty_str,
                "ORD_UNPR": price_str
            }

        elif asset_type == AssetType.KR_FUTOPT:
            sll_buy_dvsn = "02" if req.direction == Direction.LONG else "01"
            return {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "PDNO": req.symbol,
                "ORD_DVSN_CD": ord_dvsn,
                "SLL_BUY_DVSN_CD": sll_buy_dvsn,
                "ORD_QTY": qty_str,
                "ORD_UNPR": price_str
            }

        elif asset_type == AssetType.OS_STOCK:
            # [매핑 적용] 주문용 4자리 코드 (OVRS_EXCG_CD)
            ovrs_excg = cls.get_kis_exchange_code(req.exchange, is_order=True)
            
            return {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "OVRS_EXCG_CD": ovrs_excg,
                "PDNO": req.symbol,
                "ORD_QTY": qty_str,
                "OVRS_ORD_UNPR": price_str,
                "ORD_SVR_DVSN_CD": "0",
                "ORD_DVSN": ord_dvsn
            }

        elif asset_type == AssetType.OS_FUTOPT:
            sll_buy_dvsn = "02" if req.direction == Direction.LONG else "01"
            return {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "OVRS_FUTR_FX_PDNO": req.symbol,
                "SLL_BUY_DVSN_CD": sll_buy_dvsn,
                "FM_ORD_QTY": qty_str,
                "FM_ORD_PRIC": price_str,
                "ORD_DVSN_CD": ord_dvsn,
                "FM_LQD_LMT_ORD_PRIC": "",
                "CCLD_CNDT_CD": "6"
            }

        elif asset_type == AssetType.KR_BOND:
            return {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "PDNO": req.symbol,
                "ORD_QTY2": qty_str,
                "BOND_ORD_UNPR": price_str,
                "SAMT_MKET_PTCI_YN": "N",
                "BOND_RTL_MKET_YN": "N",
                "ORD_SVR_DVSN_CD": "0"
            }

        return {}

    @classmethod
    def build_cancel_params(cls, req: CancelRequest, asset_type: str, account_no: str) -> dict:
        """주문 정정/취소 파라미터"""
        cano = account_no[:8]
        acnt_prdt_cd = account_no[8:] if len(account_no) > 8 else "01"
        org_no = str(req.orderid)
        
        params = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "ORGN_ODNO": org_no,
            "RVSE_CNCL_DVSN_CD": "02", # 01:정정, 02:취소
            "ORD_QTY": "0",
            "ORD_UNPR": "0", 
            "QTY_ALL_ORD_YN": "Y"
        }

        if asset_type == AssetType.OS_STOCK:
            # [매핑 적용] 정정/취소 시에도 주문용 코드 사용
            params["OVRS_EXCG_CD"] = cls.get_kis_exchange_code(req.exchange, is_order=True)
            params["PDNO"] = req.symbol
            params["OVRS_ORD_UNPR"] = "0"
            del params["ORD_UNPR"]
            
        return params

    @classmethod
    def build_balance_params(cls, asset_type: str, account_no: str) -> dict:
        """잔고 조회 파라미터"""
        cano = account_no[:8]
        acnt_prdt_cd = account_no[8:] if len(account_no) > 8 else "01"

        if asset_type == AssetType.KR_STOCK:
            return {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "N",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "00",
                "CTX_AREA_FK": "",
                "CTX_AREA_NK": ""
            }
        elif asset_type == AssetType.OS_STOCK:
             return {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "WCRC_FRCR_DVSN_CD": "02",
                "NATN_CD": "840", # 미국 기준 기본값 (필요시 확장)
                "TR_MKET_CD": "00",
                "CTX_AREA_FK": "",
                "CTX_AREA_NK": ""
            }
        elif asset_type == AssetType.KR_FUTOPT:
            return {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "FUTR_OPT_GTFO_DVSN_CD": "1",
                "CTX_AREA_FK": "",
                "CTX_AREA_NK": ""
            }
        elif asset_type == AssetType.OS_FUTOPT:
            return {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "CRCY_CD": "USD" # 기본값 USD
            }
        elif asset_type == AssetType.KR_BOND:
            return {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "INQR_DVSN": "01",
                "CTX_AREA_FK": "",
                "CTX_AREA_NK": ""
            }
        return {}