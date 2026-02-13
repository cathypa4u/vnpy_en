"""
KIS API Helper & Config (Final Refined Version)
- FileName: kis_api_helper.py
- Features:
  1. Centralized TR ID Management (Registry Pattern)
  2. Asset-Specific Logic Encapsulation (Stock, FutOpt, Bond, Overseas)
  3. Dynamic Parameter Construction (Order, Query, History, Contract Search)
  4. Intelligent Asset Type Inference

MCP Reference (한국투자 코딩도우미 MCP):
  - TR/URL 검증·추가 시: search_*_api 로 공식 API 검색 후 read_source_code(url_main) 로 샘플 확인
  - AssetType ↔ MCP category:
      KR_STOCK   → domestic_stock      (search_domestic_stock_api)
      KR_FUTOPT  → domestic_futureoption (search_domestic_futureoption_api)
      KR_BOND    → domestic_bond      (search_domestic_bond_api)
      OS_STOCK   → overseas_stock     (search_overseas_stock_api)
      OS_FUTOPT  → overseas_futureoption (search_overseas_futureoption_api)
  - 액션 ↔ 공식 function_name 예: balance→inquire_balance, modify/cancel→order_rvsecncl, daily→inquire_daily_itemchartprice
"""

from typing import Dict, Any, Tuple, Optional, Union, List
from datetime import datetime
from vnpy.trader.constant import Direction, Exchange, Product, OrderType, Status, Offset, Interval
from vnpy.trader.object import OrderRequest, CancelRequest, HistoryRequest

# =============================================================================
# [상수 정의] 자산 타입 구분 (Refined)
# =============================================================================
class AssetType:
    KR_STOCK = "KR_STOCK"       # 국내 주식 (NXT, SOR, ISA 포함)
    KR_FUTOPT = "KR_FUTOPT"     # 국내 선물/옵션
    KR_BOND = "KR_BOND"         # 국내 장내채권
    KR_INDEX = "KR_INDEX"       # 국내 업종/지수
    
    OS_STOCK = "OS_STOCK"       # 해외 주식
    OS_FUTOPT = "OS_FUTOPT"     # 해외 선물/옵션
    
    NIGHT_FUT = "NIGHT_FUT"     # 야간 선물/옵션 (Eurex) - 별도 TR 구조 유지

# =============================================================================
# [통합 설정] KIS API 레지스트리
# =============================================================================
class KisConfig:
    
    ASIA_EXCHANGES = {
        Exchange.HKFE, Exchange.TSE, Exchange.SSE, Exchange.SZSE, 
        Exchange.HOSE, Exchange.OSE
    }
    
    # vn.py Exchange -> KIS Code
    VN_TO_KIS_EXCHANGE = {
        Exchange.KRX: "", Exchange.NXT: "", Exchange.SOR: "",
        Exchange.NASDAQ: "NASD", Exchange.NYSE: "NYSE", Exchange.AMEX: "AMEX",
        Exchange.HKFE: "SEHK", Exchange.TSE: "TKSE", Exchange.SSE: "SHAA", Exchange.SZSE: "SZAA",
        Exchange.HOSE: "VNSE", Exchange.OSE: "HASE",
        Exchange.CME: "CME", Exchange.CBOT: "CBOT", Exchange.NYMEX: "NYMEX", Exchange.COMEX: "COMEX",
        Exchange.EUREX: "EUREX", Exchange.SGX: "SGX", Exchange.ICE: "ICE", Exchange.LME: "LME"
    }
    
    # 시세 조회용 (3자리 코드)
    VN_TO_KIS_QUOTE_EXCHANGE = {
        Exchange.NASDAQ: "NAS", Exchange.NYSE: "NYS", Exchange.AMEX: "AMS",
        Exchange.HKFE: "HKS", Exchange.TSE: "TSE", Exchange.SSE: "SHS", Exchange.SZSE: "SZS",
        Exchange.HOSE: "HSX", Exchange.OSE: "HNX"
    }
    
    KIS_TO_VN_EXCHANGE = {v: k for k, v in VN_TO_KIS_EXCHANGE.items()}
    KIS_TO_VN_EXCHANGE.update({v: k for k, v in VN_TO_KIS_QUOTE_EXCHANGE.items()})

    STATUS_MAP = {
        "01": Status.SUBMITTING, "02": Status.NOTTRADED, "03": Status.REJECTED,
        "04": Status.NOTTRADED,  "05": Status.NOTTRADED, "11": Status.ALLTRADED,
        "12": Status.NOTTRADED,  "13": Status.CANCELLED
    }

    # 에러 코드 분류
    RETRYABLE_ERRORS = ["E00001", "OPS", "500", "502", "504", "TIME_OUT"]
    FATAL_ERRORS = ["IGW00121", "IGW00201", "E10000", "E00002"]

    # 주문 구분 코드 (국내주식 ORD_DVSN 등)
    ORD_DVSN_LIMIT = "00"
    ORD_DVSN_MARKET = "01"
    ORD_DVSN_FAK = "10"
    ORD_DVSN_FOK = "11"
    # 선옵 주문유형
    ORD_TP_LIMIT = "01"
    ORD_TP_MARKET = "02"
    SLL_BUY_SELL = "01"
    SLL_BUY_BUY = "02"
    # 취소 구분
    RVSE_CNCL_DVSN = "02"
    QTY_ALL_ORD_YN = "Y"

    ASSET_REGISTRY = {
        # [A] 국내 주식 — MCP: domestic_stock (inquire_balance, order_rvsecncl, inquire_daily_itemchartprice)
        AssetType.KR_STOCK: {
            "REAL": { 
                # 기본 KRX TR
                # "buy": "TTTC0012U", "sell": "TTTC0011U", 
                "buy": "TTTC0802U", "sell": "TTTC0801U", "modify": "TTTC0013U", "cancel": "TTTC0013U", 
                "daily": "FHKST03010100", "min": "FHKST03010200", 
                "deposit": "CTRP6548R", "balance": "TTTC8434R", "nccs": "TTTC8001R", "ccnl": "TTTC8001R",
                "search_info": "FHKST01010100",
                "tick": "H0STCNT0", "hoka": "H0STASP0",
                
                # Nextrade 전용 TR
                "tick_nxt": "H0NXCNT0", "hoka_nxt": "H0NXASP0",
                
                # SOR 전용 TR
                "tick_sor": "H0UNCNT0", "hoka_sor": "H0UNASP0"
            },
            "DEMO": { 
                "buy": "VTTC0012U", "sell": "VTTC0011U", "modify": "VTTC0013U", "cancel": "VTTC0013U", 
                "daily": "FHKST03010100", "min": "FHKST03010200", 
                "deposit": "VTTC8434R", "balance": "VTTC8434R", "nccs": "VTTC8001R", "ccnl": "VTTC8001R",
                "search_info": "FHKST01010100",
                "tick": "H0STCNT0", "hoka": "H0STASP0",
                "tick_nxt": "H0NXCNT0", "hoka_nxt": "H0NXASP0",
                "tick_sor": "H0UNCNT0", "hoka_sor": "H0UNASP0"
            },
            "URL": { 
                "buy": "/uapi/domestic-stock/v1/trading/order-cash", 
                "sell": "/uapi/domestic-stock/v1/trading/order-cash", 
                "modify": "/uapi/domestic-stock/v1/trading/order-rvsecncl", 
                "cancel": "/uapi/domestic-stock/v1/trading/order-rvsecncl", 
                "daily": "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", 
                "min": "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice", 
                "deposit": "/uapi/domestic-stock/v1/trading/inquire-account-balance", 
                "balance": "/uapi/domestic-stock/v1/trading/inquire-balance", 
                "nccs": "/uapi/domestic-stock/v1/trading/inquire-daily-ccld", 
                "ccnl": "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
                "search_info": "/uapi/domestic-stock/v1/quotations/inquire-price"
            }
        },
        # [B] 국내 선물/옵션 — MCP: domestic_futureoption (order, order_rvsecncl, inquire_deposit, inquire_balance)
        AssetType.KR_FUTOPT: {
            "REAL": { 
                "order": "TTTO1101U", "modify": "TTTO1103U", "cancel": "TTTO1103U", 
                "daily": "FHKIF03020100", "min": "FHKIF03020200", 
                "deposit": "CTRP6550R", "balance": "CTFO6118R", "nccs": "TTTO5301R", "ccnl": "TTTO5201R",
                "search_info": "FHMIF10000000",
                "tick": "H0IFCNT0", "hoka": "H0IFASP0", "tick_opt": "H0IOCNT0", "hoka_opt": "H0IOASP0"
            },
            "DEMO": { 
                "order": "VTTO1101U", "modify": "VTTO1103U", "cancel": "VTTO1103U", 
                "daily": "FHKIF03020100", "min": "FHKIF03020200", 
                "deposit": "VTFO6118R", "balance": "VTFO6118R", "nccs": "VTTO5301R", "ccnl": "VTTO5201R",
                "search_info": "FHMIF10000000",
                "tick": "H0IFCNT0", "hoka": "H0IFASP0", "tick_opt": "H0IOCNT0", "hoka_opt": "H0IOASP0"
            },
            "URL": { 
                "order": "/uapi/domestic-futureoption/v1/trading/order", 
                "modify": "/uapi/domestic-futureoption/v1/trading/order-rvsecncl", 
                "cancel": "/uapi/domestic-futureoption/v1/trading/order-rvsecncl", 
                "daily": "/uapi/domestic-futureoption/v1/quotations/inquire-daily-fuopchartprice", 
                "min": "/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice", 
                "deposit": "/uapi/domestic-futureoption/v1/trading/inquire-deposit", 
                "balance": "/uapi/domestic-futureoption/v1/trading/inquire-balance", 
                "nccs": "/uapi/domestic-futureoption/v1/trading/inquire-nccs", 
                "ccnl": "/uapi/domestic-futureoption/v1/trading/inquire-ccnl",
                "search_info": "/uapi/domestic-futureoption/v1/quotations/inquire-price"
            }
        },
        # [C] 국내 장내채권 — MCP: domestic_bond (buy, sell, order_rvsecncl, inquire_balance, inquire_price)
        AssetType.KR_BOND: {
            "REAL": { 
                "buy": "TTTC0952U", "sell": "TTTC0958U", "modify": "TTTC0953U", "cancel": "TTTC0953U", 
                "daily": "FHKBJ773701C0", "min": "", 
                "deposit": "CTSC8407R", "balance": "CTSC8407R", "nccs": "CTSC8035R", "ccnl": "CTSC8013R",
                "search_info": "FHKBJ773400C0",
                "tick": "H0BJCNT0", "hoka": "H0BJASP0"
            },
            "DEMO": { 
                "buy": "", "sell": "", "modify": "", "cancel": "", 
                "daily": "", "min": "", "deposit": "", "balance": "", "nccs": "", "ccnl": "",
                "search_info": "", "tick": "", "hoka": ""
            },
            "URL": { 
                "buy": "/uapi/domestic-bond/v1/trading/buy", 
                "sell": "/uapi/domestic-bond/v1/trading/sell", 
                "modify": "/uapi/domestic-bond/v1/trading/order-rvsecncl", 
                "cancel": "/uapi/domestic-bond/v1/trading/order-rvsecncl", 
                "daily": "/uapi/domestic-bond/v1/quotations/inquire-daily-itemchartprice", 
                "deposit": "/uapi/domestic-bond/v1/trading/inquire-balance", 
                "balance": "/uapi/domestic-bond/v1/trading/inquire-balance", 
                "nccs": "/uapi/domestic-bond/v1/trading/inquire-psbl-rvsecncl", 
                "ccnl": "/uapi/domestic-bond/v1/trading/inquire-daily-ccld",
                "search_info": "/uapi/domestic-bond/v1/quotations/inquire-price"
            }
        },
        # [D] 국내 업종/지수 — 실시간만 (tick)
        AssetType.KR_INDEX: {
            "REAL": { "tick": "H0UPCNT0", "hoka": "" },
            "DEMO": { "tick": "H0UPCNT0", "hoka": "" }
        },
        # [E] 해외 주식 — MCP: overseas_stock (inquire_balance, order_rvsecncl, inquire_psamount)
        AssetType.OS_STOCK: {
            "REAL": { 
                "US_buy": "TTTT1002U", "US_sell": "TTTT1006U", "US_modify": "TTTT1004U", "US_cancel": "TTTT1004U", 
                "ASIA_buy": "TTTS1002U", "ASIA_sell": "TTTS1001U", "ASIA_modify": "TTTS1003U", "ASIA_cancel": "TTTS1003U", 
                "daily": "HHDFS76240000", "min": "HHDFS76950200", 
                "deposit": "TTTS3007R", "balance": "TTTS3012R", "nccs": "TTTS3018R", "ccnl": "TTTS3035R",
                "search_info": "CTPF1702R",
                "tick": "HDFSCNT0", "hoka": "HDFSASP0"
            },
            "DEMO": { 
                "US_buy": "VTTT1002U", "US_sell": "VTTT1006U", "US_modify": "VTTT1004U", "US_cancel": "VTTT1004U", 
                "ASIA_buy": "VTTS1002U", "ASIA_sell": "VTTS1001U", "ASIA_modify": "VTTS1003U", "ASIA_cancel": "VTTS1003U", 
                "daily": "HHDFS76240000", "min": "HHDFS76950200", 
                "deposit": "VTTS3007R", "balance": "VTTS3012R", "nccs": "", "ccnl": "VTTS3035R",
                "search_info": "CTPF1702R",
                "tick": "HDFSCNT0", "hoka": "HDFSASP0"
            },
            "URL": { 
                "buy": "/uapi/overseas-stock/v1/trading/order", 
                "sell": "/uapi/overseas-stock/v1/trading/order", 
                "modify": "/uapi/overseas-stock/v1/trading/order-rvsecncl", 
                "cancel": "/uapi/overseas-stock/v1/trading/order-rvsecncl", 
                "daily": "/uapi/overseas-price/v1/quotations/dailyprice", 
                "min": "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice", 
                "deposit": "/uapi/overseas-stock/v1/trading/inquire-psamount", 
                "balance": "/uapi/overseas-stock/v1/trading/inquire-balance", 
                "nccs": "/uapi/overseas-stock/v1/trading/inquire-nccs", 
                "ccnl": "/uapi/overseas-stock/v1/trading/inquire-ccnl",
                "search_info": "/uapi/overseas-price/v1/quotations/search-info"
            }
        },
        # [F] 해외 선물/옵션 — MCP: overseas_futureoption
        AssetType.OS_FUTOPT: {
            "REAL": { 
                "order": "OTFM3001U", "modify": "OTFM3002U", "cancel": "OTFM3003U", 
                "daily": "HHDFC55020100", "min": "HHDFC55020400", 
                "deposit": "OTFM1411R", "balance": "OTFM1412R", "nccs": "OTFM3116R", "ccnl": "OTFM3122R",
                "search_info": "HHDFC55010100", 
                "search_info_opt": "HHDFO55010100",
                "tick": "HDFFF020", "hoka": "HDFFF010"
            },
            "DEMO": { 
                "order": "", "modify": "", "cancel": "", "daily": "", "min": "", 
                "deposit": "", "balance": "", "nccs": "", "ccnl": "",
                "search_info": "HHDFC55010100",
                "search_info_opt": "HHDFO55010100",
                "tick": "HDFFF020", "hoka": "HDFFF010"
            },
            "URL": { 
                "order": "/uapi/overseas-futureoption/v1/trading/order", 
                "modify": "/uapi/overseas-futureoption/v1/trading/order-rvsecncl", 
                "cancel": "/uapi/overseas-futureoption/v1/trading/order-rvsecncl", 
                "daily": "/uapi/overseas-futureoption/v1/quotations/daily-ccnl", 
                "min": "/uapi/overseas-futureoption/v1/quotations/inquire-time-futurechartprice", 
                "deposit": "/uapi/overseas-futureoption/v1/trading/inquire-deposit", 
                "balance": "/uapi/overseas-futureoption/v1/trading/inquire-unpd", 
                "nccs": "/uapi/overseas-futureoption/v1/trading/inquire-ccld", 
                "ccnl": "/uapi/overseas-futureoption/v1/trading/inquire-daily-ccld",
                "search_info": "/uapi/overseas-futureoption/v1/quotations/stock-detail",
                "search_info_opt": "/uapi/overseas-futureoption/v1/quotations/opt-detail"
            }
        },
        # [G] 야간 선물/옵션 (Eurex) — 실시간 tick/hoka 전용
        AssetType.NIGHT_FUT: {
            "REAL": { 
                "tick": "H0MFCNT0", "hoka": "H0MFASP0", # 야간 선물
                "tick_opt": "H0EUCNT0", "hoka_opt": "H0EUASP0" # 야간 옵션
            },
            "DEMO": { 
                "tick": "H0MFCNT0", "hoka": "H0MFASP0",
                "tick_opt": "H0EUCNT0", "hoka_opt": "H0EUASP0"
            }
        }
    }

# =============================================================================
# [Helper Class] API 파라미터 빌더 및 유틸리티
# =============================================================================
class KisApiHelper:
    """
    Gateway에서 사용할 API 관련 정적 유틸리티 메서드 집합
    """

    @staticmethod
    def check_response(data: dict) -> Tuple[bool, str]:
        if not data: return False, "Empty Response"
        rt_cd = data.get("rt_cd", "")
        msg1 = data.get("msg1", "")
        msg_cd = data.get("msg_cd", "")
        if rt_cd == "0": return True, msg1
        else: return False, f"[{msg_cd}] {msg1}"

    @classmethod
    def check_retryable_error(cls, msg_cd: str) -> bool:
        """재시도 가능한 에러 판별"""
        if not msg_cd:
            return False
        for code in KisConfig.FATAL_ERRORS:
            if code in msg_cd:
                return False
        for code in KisConfig.RETRYABLE_ERRORS:
            if code in msg_cd:
                return True
        return False

    @staticmethod
    def get_tr_id(asset_type: str, action: str, is_real: bool, exchange: Exchange = None) -> str:
        env_key = "REAL" if is_real else "DEMO"
        registry = KisConfig.ASSET_REGISTRY.get(asset_type, {}).get(env_key, {})
        
        # [New] 국내 주식 특수 거래소(NXT, SOR) 처리
        if asset_type == AssetType.KR_STOCK and action in ["tick", "hoka"]:
            if exchange == Exchange.NXT:
                return registry.get(f"{action}_nxt", "")
            elif exchange == Exchange.SOR:
                return registry.get(f"{action}_sor", "")
            # Default KRX fallthrough

        # 해외주식 매매 TR 분기 (미국/아시아)
        if asset_type == AssetType.OS_STOCK and exchange and action in ["buy", "sell", "modify", "cancel"]:
            prefix = "ASIA_" if exchange in KisConfig.ASIA_EXCHANGES else "US_"
            return registry.get(f"{prefix}{action}", "")
        
        # 선옵 통합 주문 TR
        if asset_type in [AssetType.KR_FUTOPT, AssetType.OS_FUTOPT] and action in ["buy", "sell"]:
            return registry.get("order", "")
        
        # 야간 선물/옵션 분기
        if asset_type == AssetType.NIGHT_FUT and action in ["tick", "hoka"]:
            # 기본적으로 선물을 반환하지만, 옵션인 경우 'tick_opt', 'hoka_opt'를 호출자가 요청해야 할 수도 있음.
            # 여기서는 편의상 기본값 반환. Gateway에서 Product 타입 확인 필요 시 분기 로직 추가 가능.
            return registry.get(action, "")

        return registry.get(action, "")

    @staticmethod
    def get_url_path(asset_type: str, action: str) -> str:
        url_map = KisConfig.ASSET_REGISTRY.get(asset_type, {}).get("URL", {})
        if "order" in url_map and action in ["buy", "sell"]:
            return url_map["order"]
        return url_map.get(action, "")

    @staticmethod
    def get_kis_exchange_code(asset_type: str, exchange: Exchange, is_order: bool = False) -> str:
        if asset_type in [AssetType.KR_STOCK, AssetType.KR_FUTOPT, AssetType.KR_BOND]: return ""
        if asset_type == AssetType.OS_STOCK and not is_order:
            return KisConfig.VN_TO_KIS_QUOTE_EXCHANGE.get(exchange, "")
        return KisConfig.VN_TO_KIS_EXCHANGE.get(exchange, "")

    @staticmethod
    def get_kis_nation_code(exchange: Exchange) -> str:
        if exchange in {Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX}: return "512" # 미국
        if exchange in {Exchange.HKFE}: return "501" # 홍콩
        if exchange in {Exchange.SSE, Exchange.SZSE}: return "502" # 중국
        if exchange in {Exchange.TSE, Exchange.OSE}: return "515" # 일본
        if exchange in {Exchange.HOSE}: return "529" # 베트남
        return "512" # 기본값

    @staticmethod
    def get_vnpy_exchange(kis_exch_code: str) -> Exchange:
        return KisConfig.KIS_TO_VN_EXCHANGE.get(kis_exch_code, Exchange.LOCAL)

    @staticmethod
    def determine_asset_type(exchange: Exchange, symbol: str) -> str:
        # [Refactored] NXT, SOR -> KR_STOCK 통합, EUREX -> NIGHT_FUT
        if exchange in [Exchange.NXT, Exchange.SOR]: 
            return AssetType.KR_STOCK
        
        if exchange == Exchange.EUREX: 
            return AssetType.NIGHT_FUT
        
        if exchange == Exchange.KRX:
            if len(symbol) == 12 and symbol.startswith("KR"): return AssetType.KR_BOND
            if len(symbol) == 6 and symbol.isdigit(): return AssetType.KR_STOCK
            return AssetType.KR_FUTOPT
        elif exchange in [Exchange.NASDAQ, Exchange.NYSE, Exchange.AMEX, Exchange.HKFE, Exchange.TSE, Exchange.HOSE, Exchange.OSE, Exchange.SSE, Exchange.SZSE]:
            return AssetType.OS_STOCK
        elif exchange in [Exchange.CME, Exchange.CBOT, Exchange.NYMEX, Exchange.COMEX, Exchange.SGX, Exchange.ICE, Exchange.LME]:
            return AssetType.OS_FUTOPT
        return ""

    @staticmethod
    def get_contract_search_params(asset_type: str, symbol: str, exchange: Exchange, is_real: bool) -> Tuple[str, str, dict]:
        tr_id = KisApiHelper.get_tr_id(asset_type, "search_info", is_real)
        url = KisApiHelper.get_url_path(asset_type, "search_info")
        params = {}

        if asset_type == AssetType.KR_STOCK:
            params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        elif asset_type == AssetType.KR_FUTOPT:
            params = {"FID_COND_MRKT_DIV_CODE": "F", "FID_INPUT_ISCD": symbol}
        elif asset_type == AssetType.KR_BOND:
            params = {"FID_COND_MRKT_DIV_CODE": "B", "FID_INPUT_ISCD": symbol}
        elif asset_type == AssetType.OS_STOCK:
            nation_code = KisApiHelper.get_kis_nation_code(exchange)
            params = {"PRDT_TYPE_CD": nation_code, "PDNO": symbol}
        elif asset_type == AssetType.OS_FUTOPT:
            is_option = False
            if " " in symbol or len(symbol) > 10: is_option = True
            if is_option:
                tr_id = KisApiHelper.get_tr_id(asset_type, "search_info_opt", is_real)
                url = KisApiHelper.get_url_path(asset_type, "search_info_opt")
            params = {"SRS_CD": symbol}
            
        return tr_id, url, params

    @staticmethod
    def build_query_params(asset_type: str, account_no: str, action: str = "balance") -> Dict[str, Any]:
        cano, acnt_prdt_cd = account_no[:8], account_no[8:] if len(account_no)>8 else "01"
        params = {"CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd}
        
        if asset_type == AssetType.KR_STOCK:
            params.update({"AFHR_FLPR_YN": "N", "OFL_YN": "N", "INQR_DVSN": "01", "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""})
        elif asset_type == AssetType.OS_STOCK:
            if action == "deposit":
                 params.update({"OVRS_EXCG_CD": "NASD", "OVRS_ORD_UNPR": "0", "ITEM_CD": ""})
            elif action == "balance":
                params.update({"WCRC_FRCR_DVSN_CD": "02", "NATN_CD": "840", "TR_MKET_CD": "00", "INQR_DVSN_CD": "00"})
        elif asset_type == AssetType.OS_FUTOPT:
            params["CRCY_CD"] = "USD"
        return params

    @staticmethod
    def build_order_params(req: OrderRequest, asset_type: str, account_no: str) -> Dict[str, Any]:
        cano, acnt_prdt_cd = account_no[:8], account_no[8:] if len(account_no)>8 else "01"
        qty_str = str(int(req.volume))
        price_str = str(req.price)
        if asset_type in [AssetType.KR_STOCK, AssetType.KR_BOND]: price_str = str(int(req.price))

        params = {"CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "PDNO": req.symbol}

        if asset_type == AssetType.KR_STOCK:
            ord_dvsn = KisConfig.ORD_DVSN_LIMIT
            if req.type == OrderType.MARKET:
                ord_dvsn = KisConfig.ORD_DVSN_MARKET
            elif req.type == OrderType.FAK:
                ord_dvsn = KisConfig.ORD_DVSN_FAK
            elif req.type == OrderType.FOK:
                ord_dvsn = KisConfig.ORD_DVSN_FOK
            params.update({"ORD_QTY": qty_str, "ORD_DVSN": ord_dvsn, "ORD_UNPR": price_str})
            if req.type == OrderType.MARKET:
                params["ORD_UNPR"] = "0"

        elif asset_type == AssetType.KR_FUTOPT:
            op_code = KisConfig.ORD_TP_LIMIT
            if req.type == OrderType.MARKET:
                op_code = KisConfig.ORD_TP_MARKET
            sll_buy = KisConfig.SLL_BUY_SELL if req.direction == Direction.SHORT else KisConfig.SLL_BUY_BUY
            params.update({"ORD_QTY": qty_str, "ORD_TP_CODE": op_code, "ORD_PRC": price_str, "SLL_BUY_DVSN_CD": sll_buy})

        elif asset_type == AssetType.KR_BOND:
            params["ORD_QTY2"] = qty_str
            params["BOND_ORD_UNPR"] = price_str
            params["SAMT_MKET_PTCI_YN"] = "N" 
            params["BOND_RTL_MKET_YN"] = "N" 
            params["ORD_SVR_DVSN_CD"] = "0"

        elif asset_type == AssetType.OS_STOCK:
            excg_cd = KisApiHelper.get_kis_exchange_code(asset_type, req.exchange, is_order=True)
            params.update({"OVRS_EXCG_CD": excg_cd, "ORD_QTY": qty_str, "OVRS_ORD_UNPR": price_str, "ORD_SVR_DVSN_CD": "0"})
            
        elif asset_type == AssetType.OS_FUTOPT:
            od_code = "00"
            if req.type == OrderType.MARKET:
                od_code = "01"
            elif req.type == OrderType.STOP:
                od_code = "03"
            sll_buy = KisConfig.SLL_BUY_SELL if req.direction == Direction.SHORT else KisConfig.SLL_BUY_BUY
            params.update({
                "FM_ORD_QTY": qty_str, "FM_ORD_PRIC": price_str,
                "SLL_BUY_DVSN_CD": sll_buy,
                "ORD_DVSN_CD": od_code, "OVRS_FUTR_FX_PDNO": req.symbol
            })
            del params["PDNO"]
            
        return params

    @staticmethod
    def build_cancel_params(req: CancelRequest, org_order_no: str, asset_type: str, account_no: str) -> Dict[str, Any]:
        cano, acnt_prdt_cd = account_no[:8], account_no[8:] if len(account_no)>8 else "01"
        qty_str = "0"
        
        params = {"CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "ORGN_ODNO": org_order_no, "RVSE_CNCL_DVSN_CD": KisConfig.RVSE_CNCL_DVSN}

        if asset_type == AssetType.KR_STOCK:
            params.update({"ORD_QTY": qty_str, "ORD_UNPR": "0", "QTY_ALL_ORD_YN": KisConfig.QTY_ALL_ORD_YN, "ORD_DVSN": KisConfig.ORD_DVSN_LIMIT})
        elif asset_type == AssetType.KR_FUTOPT:
            params.update({"ORD_DVSN_CD": KisConfig.ORD_TP_LIMIT, "RMN_QTY_YN": KisConfig.QTY_ALL_ORD_YN})
        elif asset_type == AssetType.KR_BOND:
            params.update({"ORD_QTY2": qty_str, "BOND_ORD_UNPR": "0", "QTY_ALL_ORD_YN": KisConfig.QTY_ALL_ORD_YN})
        elif asset_type == AssetType.OS_STOCK:
            excg_cd = KisApiHelper.get_kis_exchange_code(asset_type, req.exchange, is_order=True)
            params.update({"OVRS_EXCG_CD": excg_cd, "PDNO": req.symbol, "ORD_QTY": qty_str, "OVRS_ORD_UNPR": "0"})
        elif asset_type == AssetType.OS_FUTOPT:
            params.update({"OVRS_FUTR_FX_PDNO": req.symbol, "FM_ORD_QTY": qty_str, "FM_ORD_PRIC": "0", "ORD_DVSN_CD": "00"})
            
        return params

    @staticmethod
    def build_history_params(req: HistoryRequest, asset_type: str, next_ctx: dict = None, interval_num: int = 1) -> Dict[str, Any]:
        """
        차트(과거 데이터) 조회용 파라미터 빌더
        [Modified] interval_num: 분봉 조회 시 n분 데이터 요청 (기본 1)
        """
        params = {}
        start_dt = req.start.strftime("%Y%m%d")
        end_dt = req.end.strftime("%Y%m%d")
        end_tm = req.end.strftime("%H%M%S")
        
        # Interval 체크
        is_daily = req.interval == Interval.DAILY
        is_weekly = req.interval == Interval.WEEKLY
        is_monthly = req.interval == Interval.MONTHLY
        is_period = is_daily or is_weekly or is_monthly
        
        # [A] 국내 주식
        if asset_type == AssetType.KR_STOCK:
            if is_period:
                # ... (일/주/월봉 로직 동일) ...
                period_code = "D"
                if is_weekly: period_code = "W"
                elif is_monthly: period_code = "M"

                params = {
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": req.symbol,
                    "FID_INPUT_DATE_1": start_dt,
                    "FID_INPUT_DATE_2": end_dt,
                    "FID_PERIOD_DIV_CODE": period_code,
                    "FID_ORG_ADJ_PRC": "0"
                }
            else:
                # [국내주식 분봉]
                # 공식적으로 n분봉 파라미터가 없으나, 미래 확장을 위해 구조 유지
                # TR: FHKST03010200 은 기본 1분봉
                params = {
                    "FID_ETC_CLS_CODE": "",
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": req.symbol,
                    "FID_INPUT_HOUR_1": end_tm,
                    "FID_PW_DATA_INCU_YN": "Y"
                }

        # [B] 국내 선물/옵션
        elif asset_type == AssetType.KR_FUTOPT:
            if is_period:
                # ... (일/주/월봉 로직 동일) ...
                period_code = "D"
                if is_weekly: period_code = "W"
                elif is_monthly: period_code = "M"
                params = {
                    "FID_COND_MRKT_DIV_CODE": "F",
                    "FID_INPUT_ISCD": req.symbol,
                    "FID_INPUT_DATE_1": start_dt,
                    "FID_INPUT_DATE_2": end_dt,
                    "FID_PERIOD_DIV_CODE": period_code
                }
            else:
                params = {
                    "FID_ETC_CLS_CODE": "",
                    "FID_COND_MRKT_DIV_CODE": "F",
                    "FID_INPUT_ISCD": req.symbol,
                    "FID_INPUT_HOUR_1": end_tm,
                    "FID_PW_DATA_INCU_YN": "Y"
                }

        # [C] 해외 주식
        elif asset_type == AssetType.OS_STOCK:
            excd = KisApiHelper.get_kis_exchange_code(asset_type, req.exchange, is_order=False)
            if is_period:
                # ... (일/주/월봉 로직 동일) ...
                gubn = "0"
                if is_weekly: gubn = "1"
                elif is_monthly: gubn = "2"
                
                params = {
                    "EXCD": excd,
                    "SYMB": req.symbol,
                    "GUBN": gubn,
                    "BYMD": end_dt,
                    "MODP": "1"
                }
            else:
                # [해외주식 분봉] interval_num 적용 (30분봉 지원)
                # Next/Keyb 로직 적용
                next_val = next_ctx.get("NEXT", "") if next_ctx else ""
                keyb_val = next_ctx.get("KEYB", "") if next_ctx else ""

                params = {
                    "EXCD": excd,
                    "SYMB": req.symbol,
                    "NMIN": str(interval_num), # [수정] 1 -> interval_num (예: 30)
                    "PINC": "1",
                    "NEXT": next_val,
                    "KEYB": keyb_val
                }

        # [D] 해외 선물
        elif asset_type == AssetType.OS_FUTOPT:
             if is_period:
                 # ... (일/주/월봉 로직 동일) ...
                 gubn = "0"
                 if is_weekly: gubn = "1"
                 elif is_monthly: gubn = "2"
                 params = {
                     "SRS_CD": req.symbol,
                     "START_DATE": start_dt,
                     "END_DATE": end_dt,
                     "GUBN": gubn 
                 }
             else:
                 # [해외선물 분봉] TM_DV가 타임 interval
                 params = {
                     "SRS_CD": req.symbol,
                     "CNT": "100",
                     "TM_DV": str(interval_num) # [수정] 1 -> interval_num
                 }
        
        # Pagination Context 병합 (국내주식용 ctx_area 등)
        if next_ctx:
            params.update(next_ctx)
            
        return params