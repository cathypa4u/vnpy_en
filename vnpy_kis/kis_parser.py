# kis_parser.py
# KIS API Data Parser (Optimized Version)
# - Features: Direct Mapping to vnpy Constants (Status, Direction, OrderType)
# - Updates: Removed redundant if-else conversions, aligned with AssetType

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional, Any
import traceback

from vnpy.trader.object import BarData, ContractData
from vnpy.trader.constant import (
    Exchange, Interval, Status, Direction, OrderType, Product
)
from vnpy_kis.kis_api_helper import AssetType, KisApiHelper

# KST Timezone
KST = ZoneInfo("Asia/Seoul")

# =========================================================
# [CONFIG] Constant Mappings (Direct Lookup)
# =========================================================

# 1. Order Status Mapping
KIS_STATUS_MAP = {
    "01": Status.NOTTRADED, "02": Status.NOTTRADED,
    "03": "CHECK_QTY",      # Partial/All Check Needed
    "04": Status.NOTTRADED, "05": Status.CANCELLED, "06": Status.REJECTED,
    "ALLTRADED": Status.ALLTRADED, "PARTTRADED": Status.PARTTRADED,
    "CANCELLED": Status.CANCELLED, "REJECTED": Status.REJECTED
}

# 2. Direction Mapping
KIS_DIRECTION_MAP = {
    "1": Direction.SHORT, "01": Direction.SHORT,
    "2": Direction.LONG,  "02": Direction.LONG,
    "매도": Direction.SHORT, "매수": Direction.LONG
}

# 3. Order Type Mapping
KIS_ORDER_TYPE_MAP = {
    "00": OrderType.LIMIT, "01": OrderType.MARKET,
    "LIMIT": OrderType.LIMIT, "MARKET": OrderType.MARKET
}

# 4. Field Mapping for Bar Data
FIELD_MAPPING = {
    (AssetType.KR_STOCK, "D"): {
        "close": "stck_clpr", "open": "stck_oprc", "high": "stck_hgpr", "low": "stck_lwpr",
        "vol": "acml_vol", "turnover": "acml_tr_pbmn"
    },
    (AssetType.KR_STOCK, "M"): {
        "close": "stck_prpr", "open": "stck_oprc", "high": "stck_hgpr", "low": "stck_lwpr",
        "vol": "cntg_vol", "turnover": "acml_tr_pbmn"
    },
    (AssetType.OS_STOCK, "D"): {
        "close": "clos", "open": "open", "high": "high", "low": "low",
        "vol": "ovrs_nstock_vol", "turnover": "ovrs_nstock_tr_pbmn"
    },
    (AssetType.OS_STOCK, "M"): {
        "close": "last", "open": "open", "high": "high", "low": "low",
        "vol": ["evol", "tvol", "vol"], "turnover": "tamt"
    },
    (AssetType.KR_FUTOPT, "D"): {
        "close": "futs_prpr", "open": "futs_oprc", "high": "futs_hgpr", "low": "futs_lwpr",
        "vol": "acml_vol", "turnover": "acml_tr_pbmn"
    },
    (AssetType.KR_FUTOPT, "M"): {
        "close": "futs_prpr", "open": "futs_oprc", "high": "futs_hgpr", "low": "futs_lwpr",
        "vol": "cntg_vol", "turnover": "acml_tr_pbmn"
    },
    (AssetType.KR_BOND, "D"): {
        "close": "bond_prpr", "open": "bond_oprc", "high": "bond_hgpr", "low": "bond_lwpr",
        "vol": "acml_vol", "turnover": "acml_tr_pbmn"
    }
}

# --- Utility Functions ---
def aes_cbc_base64_dec(key, iv, cipher_text):
    try:
        cipher = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
        return bytes.decode(unpad(cipher.decrypt(b64decode(cipher_text)), AES.block_size))
    except Exception:
        return None

def _to_float(val):
    try: 
        if val is None: return 0.0
        s_val = str(val).strip().replace(",", "")
        return float(s_val) if s_val else 0.0
    except: return 0.0

def _to_int(val):
    try: 
        if val is None: return 0
        s_val = str(val).strip().replace(",", "")
        return int(s_val) if s_val else 0
    except: return 0

def _get_val(vals, idx, default=""):
    try: return vals[idx]
    except IndexError: return default

def _find_first_valid_float(candidates: list, default=0.0) -> float:
    for val in candidates:
        if val is not None and str(val).strip() != "":
            return _to_float(val)
    return default

# =========================================================
# 1. Real-time WebSocket Data Parser (All Assets)
# =========================================================
def parse_ws_realtime(tr_id, body):
    """실시간 체결/시세 데이터 정밀 파싱"""
    vals = body.split('^')
    data = {"tr_id": tr_id, "valid": True}

    try:
        # 1.1 Domestic Stock (KRX/NXT/SOR)
        if tr_id in ["H0STCNT0", "H0UNCNT0", "H0NXCNT0"]:
            data.update({
                "type": "STOCK", "market": "KR", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 1),
                "price": _to_float(_get_val(vals, 2)),
                "diff": _to_float(_get_val(vals, 4)),
                "rate": _to_float(_get_val(vals, 5)),
                "open": _to_float(_get_val(vals, 7)), "high": _to_float(_get_val(vals, 8)), "low": _to_float(_get_val(vals, 9)),
                "volume": _to_float(_get_val(vals, 12)), "acc_volume": _to_float(_get_val(vals, 13)),
                "turnover": _to_float(_get_val(vals, 14)),  
                "ask_1": _to_float(_get_val(vals, 10)), "bid_1": _to_float(_get_val(vals, 11)),
                "market_cap": _to_float(_get_val(vals, 16)) 
            })
        
        # 1.2 Domestic Futures
        elif tr_id == "H0IFCNT0":
            data.update({
                "type": "FUTURE", "market": "KR", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 1),
                "price": _to_float(_get_val(vals, 5)),
                "open": _to_float(_get_val(vals, 6)), "high": _to_float(_get_val(vals, 7)), "low": _to_float(_get_val(vals, 8)),
                "volume": _to_float(_get_val(vals, 9)), "acc_volume": _to_float(_get_val(vals, 10)),
                "turnover": _to_float(_get_val(vals, 11)), "open_interest": _to_float(_get_val(vals, 18))
            })

        # 1.3 Domestic Index (업종/지수) [Added]
        elif tr_id == "H0UPCNT0":
            data.update({
                "type": "INDEX", "market": "KR", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 1),
                "price": _to_float(_get_val(vals, 2)),
                "diff": _to_float(_get_val(vals, 4)), "rate": _to_float(_get_val(vals,9)),
                "acc_volume": _to_float(_get_val(vals, 5)), "turnover": _to_float(_get_val(vals, 6))
            })

        # 1.4 Domestic Bonds (장내채권) [Added]
        elif tr_id == "H0BJCNT0":
            data.update({
                "type": "BOND", "market": "KR", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 2),
                "price": _to_float(_get_val(vals, 6)), 
                "diff": _to_float(_get_val(vals, 4)), "rate": _to_float(_get_val(vals, 5)),
                "acc_volume": _to_float(_get_val(vals, 16)),
                "yield": _to_float(_get_val(vals, 12))  # 채권 수익률
            })

        # 1.5 Overseas Stock
        elif tr_id == "HDFSCNT0":
            data.update({
                "type": "STOCK", "market": "OS", "currency": "USD", 
                "code": _get_val(vals, 1), 
                "time": _get_val(vals, 5), "localtime": f"{_get_val(vals, 4)} {_get_val(vals, 5)}",
                "price": _to_float(_get_val(vals, 11)), "diff": _to_float(_get_val(vals, 13)), "rate": _to_float(_get_val(vals, 14)),
                "volume": _to_float(_get_val(vals, 19)), "acc_volume": _to_float(_get_val(vals, 20)), "turnover": _to_float(_get_val(vals, 21)),
                "open": _to_float(_get_val(vals, 8)), "high": _to_float(_get_val(vals, 9)), "low": _to_float(_get_val(vals, 10))
            })

        # 1.6 Overseas Futures
        elif tr_id == "HDFFF020":
            data.update({
                "type": "FUTURE", "market": "OS", "currency": "USD",
                "code": _get_val(vals, 0), "time": _get_val(vals, 3),
                "price": _to_float(_get_val(vals, 10)), "volume": _to_float(_get_val(vals, 11)),
                "acc_volume": _to_float(_get_val(vals, 17)),
                "open": _to_float(_get_val(vals, 14)), "high": _to_float(_get_val(vals, 15)), "low": _to_float(_get_val(vals, 16))
            })

        # 1.7 Night Futures/Options (Eurex) [Added]
        elif tr_id in ["ECEUCNT0", "H0EUCNT0"]:
            data.update({
                "type": "NIGHT_FUTOPT", "market": "KR_NIGHT", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 1),
                "price": _to_float(_get_val(vals, 2)), 
                "diff": _to_float(_get_val(vals, 4)), "rate": _to_float(_get_val(vals, 5)),
                "acc_volume": _to_float(_get_val(vals, 10))
            })

        else:
            data["valid"] = False

    except Exception:
        data["valid"] = False

    return data

def parse_ws_hoka(tr_id, body):
    """호가(Depth) 데이터 파싱"""
    vals = body.split('^')
    data = {"tr_id": tr_id, "asks": [], "bids": []}

    try:
        # 2.1 Domestic Stock (10단계)
        if tr_id in ["H0STASP0", "H0UNASP0", "H0NXASP0"]:
            data["code"] = _get_val(vals, 0)
            # 국내주식 호가 데이터 구조 (인덱스 기준)
            # 3~12: 매도호가1~10, 13~22: 매수호가1~10
            # 23~32: 매도잔량1~10, 33~42: 매수잔량1~10
            for i in range(10):
                data["asks"].append((_to_float(_get_val(vals, 3+i)), _to_float(_get_val(vals, 23+i))))
                data["bids"].append((_to_float(_get_val(vals, 13+i)), _to_float(_get_val(vals, 33+i))))
        
        # 2.2 Domestic Fut/Opt (5단계)
        elif tr_id in ["H0IFASP0", "H0IOASP0"]:
            data["code"] = _get_val(vals, 0)
            for i in range(5):
                data["asks"].append((_to_float(_get_val(vals, 2+i)), _to_float(_get_val(vals, 22+i))))
                data["bids"].append((_to_float(_get_val(vals, 7+i)), _to_float(_get_val(vals, 27+i))))
        
        # 2.3 Domestic Bond (3단계 예상/일반적으로 3~5단계) [Added]
        # 장내 채권 호가의 경우 포맷이 주식과 유사하나 TR_ID H0BJASP0 사용
        elif tr_id == "H0BJASP0":
            data["code"] = _get_val(vals, 0)
            for i in range(5):
                # 문서 구조: 수익률(Ask) -> 수익률(Bid) -> 가격(Ask) -> 가격(Bid) -> 잔량(Ask) -> 잔량(Bid)
                base_idx = i * 6
                # Ask: Price(Idx 4), Vol(Idx 6)
                ask_p = _to_float(_get_val(vals, 4 + base_idx))
                ask_v = _to_float(_get_val(vals, 6 + base_idx))
                data["asks"].append((ask_p, ask_v))
                
                # Bid: Price(Idx 5), Vol(Idx 7)
                bid_p = _to_float(_get_val(vals, 5 + base_idx))
                bid_v = _to_float(_get_val(vals, 7 + base_idx))
                data["bids"].append((bid_p, bid_v))
            
        # 2.4 Overseas Stock (1단계)
        elif tr_id == "HDFSASP0":
            data["code"] = _get_val(vals, 0) # 종목코드
            # vals[1]은 수신시간            
            for i in range(10):
                base_idx = 2 + (i * 6)
                # 구조: 매도호가(0) -> 매수호가(1) -> 매도잔량(2) -> 매수잔량(3) -> 매수대비(4) -> 매도대비(5)
                # Ask: Price(idx+0), Vol(idx+2)
                ask_p = _to_float(_get_val(vals, base_idx + 0))
                ask_v = _to_float(_get_val(vals, base_idx + 2))
                data["asks"].append((ask_p, ask_v))
                
                # Bid: Price(idx+1), Vol(idx+3)
                bid_p = _to_float(_get_val(vals, base_idx + 1))
                bid_v = _to_float(_get_val(vals, base_idx + 3))
                data["bids"].append((bid_p, bid_v))
            
        # 2.5 Overseas Future (5단계)
        elif tr_id == "HDFFF010":
            data["code"] = _get_val(vals, 0) # 종목코드 (SERIES_CD)
            # vals[1]:일자, vals[2]:시간, vals[3]:전일종가            
            for i in range(5):
                base_idx = 4 + (i * 6)
                # 구조: 매수잔량(0) -> 매수건수(1) -> 매수호가(2) -> 매도잔량(3) -> 매도건수(4) -> 매도호가(5)
                
                # Ask: Price(idx+5), Vol(idx+3)
                ask_p = _to_float(_get_val(vals, base_idx + 5))
                ask_v = _to_float(_get_val(vals, base_idx + 3))
                data["asks"].append((ask_p, ask_v))

                # Bid: Price(idx+2), Vol(idx+0)
                bid_p = _to_float(_get_val(vals, base_idx + 2))
                bid_v = _to_float(_get_val(vals, base_idx + 0))
                data["bids"].append((bid_p, bid_v))
            
    except Exception as e: 
        # [FIX] 에러 무시하지 않고 출력
        print(f"❌ [Parser] Hoka Parse Error ({tr_id}): {e}")
        # traceback.print_exc() # 필요시 주석 해제
    return data

# =========================================================
# 2. WebSocket Notice Parser (Execution/Order)
# =========================================================

def parse_ws_notice(tr_id, body, key, iv):
    # (앞선 최적화된 코드와 동일)
    dec_str = aes_cbc_base64_dec(key, iv, body)
    if not dec_str: return {"valid": False, "msg": "Decrypt Failed"}
    
    vals = dec_str.split('^')
    res = {
        "valid": True, "tr_id": tr_id, "raw_list": vals,
        "order_status": Status.NOTTRADED, "direction": Direction.NET, "order_type": OrderType.LIMIT,
        "account": "", "order_no": "", "code": "", 
        "order_qty": 0, "order_price": 0.0, "filled_qty": 0, "filled_price": 0.0, "unfilled_qty": 0, "msg": ""
    }

    try:
        # A. Domestic Notice (Stock/Fut/Opt)
        if tr_id in ["H0STCNI0", "H0STCNI9", "H0IFCNI0", "H0IFCNI9"]:
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["code"] = _get_val(vals, 4)
            res["direction"] = KIS_DIRECTION_MAP.get(_get_val(vals, 15), Direction.NET)
            res["order_type"] = KIS_ORDER_TYPE_MAP.get(_get_val(vals, 14), OrderType.LIMIT)
            res["order_qty"] = _to_int(_get_val(vals, 9))
            res["order_price"] = _to_float(_get_val(vals, 10))
            res["filled_qty"] = _to_int(_get_val(vals, 11))
            res["filled_price"] = _to_float(_get_val(vals, 12))
            res["unfilled_qty"] = _to_int(_get_val(vals, 13))
            
            method_code = _get_val(vals, 16) 
            mapped = KIS_STATUS_MAP.get(method_code, Status.NOTTRADED)
            if mapped == "CHECK_QTY":
                res["order_status"] = Status.ALLTRADED if res["unfilled_qty"] == 0 else Status.PARTTRADED
            else:
                res["order_status"] = mapped

        # B. Overseas Notice
        elif tr_id in ["H0GSCNI0", "H0GSCNI9"]:
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["code"] = _get_val(vals, 7)
            res["direction"] = KIS_DIRECTION_MAP.get(_get_val(vals, 4), Direction.NET)
            res["order_type"] = KIS_ORDER_TYPE_MAP.get(_get_val(vals, 13), OrderType.LIMIT)
            res["order_qty"] = _to_int(_get_val(vals, 8))
            res["order_price"] = _to_float(_get_val(vals, 9))
            res["unfilled_qty"] = _to_int(_get_val(vals, 10))
            
            notice_type = _get_val(vals, 12)
            if _get_val(vals, 11) != "00000": 
                res["order_status"] = Status.REJECTED
            elif notice_type == '2':
                res["filled_qty"] = res["order_qty"]
                res["order_status"] = Status.ALLTRADED if res["unfilled_qty"] == 0 else Status.PARTTRADED
            elif _get_val(vals, 5) == '2': 
                res["order_status"] = Status.CANCELLED
            else: 
                res["order_status"] = Status.NOTTRADED
            
    except Exception as e:
        res["valid"] = False
        res["msg"] = str(e)

    return res

# =========================================================
# 3. REST API Response Parser (Common)
# =========================================================

def parse_order_response(data: dict) -> dict:
    """주문 응답 파싱 (국내/해외 구분 처리)"""
    if not data: return {}
    output = data.get('output', {})
    # 1. 주문번호 파싱
    # 국내주식은 'KRX_FWDG_ORD_ORGNO'(주문조직번호)가 있지만, 해외주식/선물은 없음
    org_no = output.get('KRX_FWDG_ORD_ORGNO', '')
    odno = output.get("ODNO", "")
    orderid = f"{org_no}-{odno}" if org_no else odno
    return { "orderid": orderid, "odno": odno, "msg": data.get("msg1", "") }
    
def parse_cancel_response(data: dict) -> dict:
    if not data: return {}
    output = data.get('output', {})
    if isinstance(output, list) and output: output = output[0]
    return {"odno": output.get("ODNO", ""), "orgn_odno": output.get("ORGN_ODNO", ""), "msg": data.get("msg1", ""), "status": data.get("rt_cd", "")}
    
def parse_balance(data: dict, asset_type: str) -> dict:
    """
    자산별 잔고 조회 파싱
    asset_type: kis_api_helper.AssetType 상수 사용
    """
    info = {"balance": 0.0, "available": 0.0, "net_pnl": 0.0}
    if not data: return info
    
    row = data.get('output2', {}) or data.get('output', {})
    if isinstance(row, list) and row: row = row[0]
    if not isinstance(row, dict): row = {}

    # 1. 국내 주식 (주식잔고조회_실현손익 TTTC8494R 등)
    if asset_type == AssetType.KR_STOCK: 
        info["balance"] = _to_float(row.get("tot_evlu_amt", 0))       # 총평가금액
        info["available"] = _to_float(row.get("dnca_tot_amt", 0))     # 예수금총금액 (주문가능금액은 별도 API이나 여기선 예수금으로 대체)
        info["net_pnl"] = _to_float(row.get("evlu_pfls_smt_tluj_amt", 0)) # 평가손익합계

    # 2. 해외 주식 (해외주식 잔고 v1_해외주식-006)
    elif asset_type == AssetType.OS_STOCK:
        # output2: 외화평가총액, 주문가능금액 확인
        info["balance"] = _to_float(row.get("tot_asst_amt", 0))       # 자산총액 (또는 ovrs_tot_pfls + frcr_pchs_amt1)
        if info["balance"] == 0:
             info["balance"] = _to_float(row.get("tot_evlu_amt", 0))   # 필드명 다를 경우 대비

        info["available"] = _to_float(row.get("frcr_ord_psbl_amt1", 0)) # 외화주문가능금액
        info["net_pnl"] = _to_float(row.get("ovrs_tot_pfls", 0))        # 해외총손익

    # 3. 국내 선물/옵션 (선물옵션 잔고현황 CTFO6118R)
    elif asset_type == AssetType.KR_FUTOPT:
        info["balance"] = _to_float(row.get("prsm_dpast_amt", 0))     # 추정예탁금액
        info["available"] = _to_float(row.get("ord_psbl_tota", 0))    # 주문가능총액
        # [수정] 명세서상 총평가손익 필드명: evlu_pfls_amt_smtl
        info["net_pnl"] = _to_float(row.get("evlu_pfls_amt_smtl", 0)) 

    # 4. 해외 선물/옵션 (해외선물옵션 잔고현황 OTFM1412R - output2)
    elif asset_type == AssetType.OS_FUTOPT: 
        info["balance"] = _to_float(row.get("fm_tot_asst_evlu_amt", 0)) # FM총자산평가금액
        info["available"] = _to_float(row.get("fm_ord_psbl_amt", 0))    # FM주문가능금액
        info["net_pnl"] = _to_float(row.get("fm_lqd_pfls_amt", 0))      # FM청산손익금액 (평가손익 합계가 별도로 없으면 개별 합산 필요할 수 있음)

    # 5. 장내 채권 (장내채권 잔고조회 CTSC8407R)
    elif asset_type == AssetType.KR_BOND:
        # 채권은 'output2'(계좌총괄)가 없는 경우가 많아 output1의 합산이나 예수금 API 별도 필요
        # 여기서는 예수금(dnca_tot_amt)이 있으면 사용
        info["balance"] = _to_float(row.get("dnca_tot_amt", 0))
        info["available"] = _to_float(row.get("dnca_tot_amt", 0))
        
    return info

def parse_position(data: dict, asset_type: str) -> list:
    """
    보유 포지션 파싱 (vnpy Direction/Exchange 상수 적용)
    """
    positions = []
    rows = data.get('output1', []) if 'output1' in data else data.get('output', [])
    if not isinstance(rows, list): rows = [rows]

    for row in rows:
        if not row: continue
        try:
            pos = {}
            # 1. 국내 주식
            if asset_type == AssetType.KR_STOCK:
                pos["symbol"] = row["pdno"]
                pos["quantity"] = _to_int(row.get("hldg_qty", 0))
                pos["price"] = _to_float(row.get("pchs_avg_pric", 0))
                pos["pnl"] = _to_float(row.get("evlu_pfls_amt", 0))
                pos["direction"] = Direction.LONG

            # 2. 해외 주식
            elif asset_type == AssetType.OS_STOCK:
                pos["symbol"] = row["ovrs_pdno"]
                pos["quantity"] = _to_int(row.get("ovrs_cblc_qty", 0)) # 해외잔고수량
                pos["price"] = _to_float(row.get("pchs_avg_pric", 0))
                pos["pnl"] = _to_float(row.get("frcr_evlu_pfls_amt", 0)) # 외화평가손익
                pos["direction"] = Direction.LONG

            # 3. 국내 선물/옵션
            elif asset_type == AssetType.KR_FUTOPT:
                pos["symbol"] = row["pdno"]
                dvsn = row.get("trad_dvsn_name", "")
                pos["direction"] = Direction.SHORT if "매도" in dvsn else Direction.LONG
                pos["quantity"] = _to_int(row.get("cblc_qty", 0))
                pos["price"] = _to_float(row.get("pchs_avg_pric", 0))
                pos["pnl"] = _to_float(row.get("evlu_pfls_amt", 0))

            # 4. 해외 선물/옵션
            elif asset_type == AssetType.OS_FUTOPT: 
                pos["symbol"] = row["ovrs_futr_fx_pdno"]
                dvsn = row.get("sll_buy_dvsn_cd", "")
                pos["direction"] = Direction.SHORT if dvsn == "01" else Direction.LONG
                pos["quantity"] = _to_int(row.get("fm_ustl_qty", 0))  # 미결제수량
                pos["price"] = _to_float(row.get("fm_ccld_avg_pric", 0)) 
                pos["pnl"] = _to_float(row.get("fm_evlu_pfls_amt", 0)) 
            
            # 5. 장내 채권 [수정] 필드명 보정
            elif asset_type == AssetType.KR_BOND:
                pos["symbol"] = row["pdno"]
                pos["quantity"] = _to_int(row.get("cblc_qty", 0))
                
                # 채권은 'pchs_avg_pric' 대신 'buy_unpr'(매입단가) 사용 가능성 높음
                price = _to_float(row.get("pchs_avg_pric", 0))
                if price == 0: price = _to_float(row.get("buy_unpr", 0))
                pos["price"] = price
                
                # 평가손익 필드가 없으면 (평가금액 - 매입금액)
                pnl = _to_float(row.get("evlu_pfls_amt", 0))
                if pnl == 0:
                    buy_amt = _to_float(row.get("buy_amt", 0))
                    eval_amt = _to_float(row.get("evlu_amt", 0))
                    if buy_amt > 0 and eval_amt > 0:
                        pnl = eval_amt - buy_amt
                pos["pnl"] = pnl
                pos["direction"] = Direction.LONG
                
            if "symbol" in pos and pos["quantity"] > 0:
                positions.append(pos)
        except:
            continue
            
    return positions

def parse_kis_bar_data(
    data_list: List[Dict], 
    req_symbol: str, 
    req_exchange: Exchange, 
    req_interval: Interval, 
    gateway_name: str = "KIS",
    asset_type: str = AssetType.KR_STOCK
) -> List[BarData]:
    """
    KIS API 응답 리스트를 BarData 리스트로 변환 (AssetType Constant 사용)
    """
    bars: List[BarData] = []
    
    # 1. 매핑 모드 결정 (Daily vs Minute)
    interval_group = "D" if req_interval in [Interval.DAILY, Interval.WEEKLY, Interval.MONTHLY] else "M"
    
    # 2. 명시적 필드맵 가져오기
    field_map = FIELD_MAPPING.get((asset_type, interval_group))
    
    for item in data_list:
        try:
            # --- Date/Time Parsing ---
            date_str = (item.get("stck_bsop_date") or item.get("kymd") or item.get("data_date") or item.get("tymd") or item.get("xymd"))
            time_str = (item.get("stck_cntg_hour") or item.get("khms") or item.get("data_time") or item.get("xhms") or "000000")
            if not date_str: continue

            if len(time_str) == 6:
                dt = datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H%M%S")
            else:
                dt = datetime.strptime(date_str, "%Y%m%d")
            dt = dt.replace(tzinfo=KST)

            # --- Explicit Value Parsing ---
            if field_map:
                close_val = _to_float(item.get(field_map["close"]))
                open_val  = _to_float(item.get(field_map["open"]))
                high_val  = _to_float(item.get(field_map["high"]))
                low_val   = _to_float(item.get(field_map["low"]))
                
                vol_key = field_map["vol"]
                if isinstance(vol_key, list):
                    vol_val = _find_first_valid_float([item.get(k) for k in vol_key])
                else:
                    vol_val = _to_float(item.get(vol_key))
                                    
                turn_val  = _to_float(item.get(field_map["turnover"]))
            else:
                # [Fallback]
                close_val = _find_first_valid_float([item.get("stck_clpr"), item.get("stck_prpr"), item.get("last"), item.get("clos")])
                open_val  = _find_first_valid_float([item.get("stck_oprc"), item.get("open")], default=close_val)
                high_val  = _find_first_valid_float([item.get("stck_hgpr"), item.get("high")], default=close_val)
                low_val   = _find_first_valid_float([item.get("stck_lwpr"), item.get("low")], default=close_val)
                vol_val   = _find_first_valid_float([item.get("acml_vol"), item.get("cntg_vol"), item.get("vol")])
                turn_val  = _find_first_valid_float([item.get("acml_tr_pbmn"), item.get("tamt")])

            bar = BarData(
                symbol=req_symbol, exchange=req_exchange, datetime=dt, interval=req_interval,
                volume=vol_val, turnover=turn_val,
                open_price=open_val, high_price=high_val, low_price=low_val, close_price=close_val,
                gateway_name=gateway_name
            )
            
            if bar.turnover == 0 and bar.volume > 0 and bar.close_price > 0:
                bar.turnover = bar.close_price * bar.volume
                
            if bar.close_price > 0:
                if bar.open_price == 0: bar.open_price = bar.close_price
                if bar.high_price == 0: bar.high_price = bar.close_price
                if bar.low_price == 0: bar.low_price = bar.close_price

            bars.append(bar)
        except Exception:
            continue

    bars.sort(key=lambda x: x.datetime)
    return bars

def parse_contract(data: dict, asset_type, contract: ContractData) -> ContractData:
    """
    REST API 응답(현재가/Quote TR)을 파싱하여 ContractData 생성
    """
    output = data.get("output", {})
    if not output:
        return None

    try:
        # ---------------------------------------------------------
        # A. 국내 주식 (KR_STOCK)
        # ---------------------------------------------------------
        if asset_type == AssetType.KR_STOCK:
            contract.name = output.get("hts_kor_isnm", "") or output.get("rprs_mrkt_kor_name", contract.symbol)
            current_price = float(output.get("stck_prpr", "0"))
            contract.pricetick = get_kr_stock_pricetick(current_price, "KOSPI") # 시장구분 로직 추가 권장

        # ---------------------------------------------------------
        # B. 국내 선물/옵션 (KR_FUTOPT)
        # ---------------------------------------------------------
        elif asset_type == AssetType.KR_FUTOPT:
            contract.name = output.get("hts_kor_isnm", contract.symbol)
            
            if contract.product == Product.FUTURES:
                contract.pricetick = 0.05
                contract.size = 250000 
            elif contract.product == Product.OPTION:
                contract.pricetick = 0.01
                contract.size = 250000

        # ---------------------------------------------------------
        # C. 해외 주식 (OS_STOCK)
        # ---------------------------------------------------------
        elif asset_type == AssetType.OS_STOCK:
            contract.name = output.get("rsym", contract.symbol) # 영문명 (e.g., APPLE INC)
            
            # Response에 있는 거래소 코드로 vnpy Exchange 역변환 (검증용)
            res_ex_code = output.get("excd", "") # KIS Code (NAS, NYS...)
            if res_ex_code:
                contract.exchange = KisApiHelper.get_vnpy_exchange_from_kis(asset_type, res_ex_code)
            
            # 호가단위
            zdiv = int(output.get("zdiv", 2))
            contract.pricetick = 1 / (10 ** zdiv) if zdiv > 0 else 1

        # ---------------------------------------------------------
        # D. 국내 채권 (KR_BOND)
        # ---------------------------------------------------------
        elif asset_type == AssetType.KR_BOND:
            contract.name = output.get("hts_kor_isnm", contract.symbol)
            contract.pricetick = 1

    except Exception as e:
        print(f"Contract Parsing Error for {contract.symbol}: {e}")

    return contract

def get_kr_stock_pricetick(price: float, market_type: str = "KOSPI") -> float:
    if market_type == "KOSDAQ":
        if price < 1000: return 1
        if price < 5000: return 5
        if price < 10000: return 10
        if price < 50000: return 50
        return 100
    else: # KOSPI
        if price < 1000: return 1
        if price < 5000: return 5
        if price < 10000: return 10
        if price < 50000: return 50
        if price < 100000: return 100
        if price < 500000: return 500
        return 1000