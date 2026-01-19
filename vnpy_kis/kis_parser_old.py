# kis_parser.py
# KIS API Data Parser (Universal & Independent)
# Supports: Domestic/Overseas Stocks, Futures, Options, Bonds, Indices, Night Market
# Features: Precise Field Mapping, Multi-currency Balance

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional, Any, Union  # <--- [중요] List, Dict 임포트 추가
import traceback

from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange, Interval


# KST Timezone for Domestic Market
KST = ZoneInfo("Asia/Seoul")

# --- Utility Functions ---
# --- Utility Functions ---
def aes_cbc_base64_dec(key, iv, cipher_text):
    try:
        cipher = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
        return bytes.decode(unpad(cipher.decrypt(b64decode(cipher_text)), AES.block_size))
    except Exception:
        return None

def _to_float(val):
    try: return float(str(val).replace(",", "")) if val and str(val).strip() != "" else 0.0
    except: return 0.0

def _to_int(val):
    try: return int(str(val).replace(",", "")) if val and str(val).strip() != "" else 0
    except: return 0

def _get_val(vals, idx, default=""):
    try: return vals[idx]
    except IndexError: return default
   
# =========================================================
# [CORE] Explicit Status Mapping Constants
# =========================================================

# 1. 국내 시장 공통 (주식/선물/옵션/채권)
# API 필드: '처리구분' (Method Code)
# 01:접수, 02:확인, 03:체결(부분포함), 04:정정, 05:취소, 06:거부
KIS_DOMESTIC_STATUS_MAP = {
    "01": Status.NOTTRADED, # 접수 (미체결)
    "02": Status.NOTTRADED, # 확인 (접수 후 확인 단계)
    "03": "CHECK_QTY",      # 체결 (전량/부분 판단 로직 필요)
    "04": Status.NOTTRADED, # 정정 (상태는 미체결 유지)
    "05": Status.CANCELLED, # 취소
    "06": Status.REJECTED   # 거부
}

# =========================================================
# 1. Real-time WebSocket Data Parser (Market Data)
# =========================================================

def parse_ws_realtime(tr_id, body):
    """
    실시간 체결/시세 데이터 정밀 파싱
    Return: Dictionary (Standardized Keys)
    """
    vals = body.split('^')
    data = {"tr_id": tr_id, "valid": True}

    try:
        # 1.1 Domestic Stock (국내주식) - H0STCNT0
        if tr_id == "H0STCNT0":
            data.update({
                "type": "STOCK", "market": "KR", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 1),
                "price": _to_float(_get_val(vals, 2)),
                "diff": _to_float(_get_val(vals, 4)),
                "rate": _to_float(_get_val(vals, 5)),
                "open": _to_float(_get_val(vals, 7)), "high": _to_float(_get_val(vals, 8)), "low": _to_float(_get_val(vals, 9)),
                "volume": _to_float(_get_val(vals, 12)),    
                "acc_volume": _to_float(_get_val(vals, 13)),
                "turnover": _to_float(_get_val(vals, 14)),  
                "ask_1": _to_float(_get_val(vals, 10)), "bid_1": _to_float(_get_val(vals, 11)),
                "market_cap": _to_float(_get_val(vals, 16)) 
            })

        # 1.2 Domestic Futures (국내선물) - H0IFCNT0
        elif tr_id == "H0IFCNT0":
            data.update({
                "type": "FUTURE", "market": "KR", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 1),
                "price": _to_float(_get_val(vals, 5)),
                "open": _to_float(_get_val(vals, 6)), "high": _to_float(_get_val(vals, 7)), "low": _to_float(_get_val(vals, 8)),
                "volume": _to_float(_get_val(vals, 9)),
                "acc_volume": _to_float(_get_val(vals, 10)),
                "turnover": _to_float(_get_val(vals, 11)),
                "open_interest": _to_float(_get_val(vals, 18))
            })

        # 1.3 Domestic Options (국내옵션) - H0IOCNT0
        elif tr_id == "H0IOCNT0":
            data.update({
                "type": "OPTION", "market": "KR", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 1),
                "price": _to_float(_get_val(vals, 2)),
                "diff": _to_float(_get_val(vals, 4)),
                "open": _to_float(_get_val(vals, 6)), "high": _to_float(_get_val(vals, 7)), "low": _to_float(_get_val(vals, 8)),
                "volume": _to_float(_get_val(vals, 9)),
                "acc_volume": _to_float(_get_val(vals, 10)),
                "turnover": _to_float(_get_val(vals, 11)),
                "open_interest": _to_float(_get_val(vals, 13))
            })

        # 1.4 Domestic Index (업종/지수) - H0UPCNT0
        elif tr_id == "H0UPCNT0":
            data.update({
                "type": "INDEX", "market": "KR", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 1),
                "price": _to_float(_get_val(vals, 2)),
                "diff": _to_float(_get_val(vals, 4)),
                "rate": _to_float(_get_val(vals, 5)),
                "acc_volume": _to_float(_get_val(vals, 8)),
                "turnover": _to_float(_get_val(vals, 9))
            })

        # 1.5 Bonds (장내채권) - H0BJCNT0
        elif tr_id == "H0BJCNT0":
            data.update({
                "type": "BOND", "market": "KR", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 2),
                "price": _to_float(_get_val(vals, 6)),
                "acc_volume": _to_float(_get_val(vals, 16)),
                "yield": _to_float(_get_val(vals, 12)) 
            })

        # 1.6 Overseas Stock (해외주식) - HDFSCNT0
        elif tr_id == "HDFSCNT0":
            data.update({
                "type": "STOCK", "market": "OVRS", "currency": "USD", 
                "code": _get_val(vals, 1), 
                "time": _get_val(vals, 5), "localtime": f"{_get_val(vals, 4)} {_get_val(vals, 5)}",
                "price": _to_float(_get_val(vals, 11)),
                "diff": _to_float(_get_val(vals, 13)),
                "rate": _to_float(_get_val(vals, 14)),
                "volume": _to_float(_get_val(vals, 19)),
                "acc_volume": _to_float(_get_val(vals, 20)),
                "turnover": _to_float(_get_val(vals, 21)),
                "open": _to_float(_get_val(vals, 8)), "high": _to_float(_get_val(vals, 9)), "low": _to_float(_get_val(vals, 10))
            })

        # 1.7 Overseas Futures (해외선물) - HDFFF020
        elif tr_id == "HDFFF020":
            data.update({
                "type": "FUTURE", "market": "OVRS", "currency": "USD",
                "code": _get_val(vals, 0), "time": _get_val(vals, 3),
                "price": _to_float(_get_val(vals, 10)),
                "volume": _to_float(_get_val(vals, 11)),
                "acc_volume": _to_float(_get_val(vals, 17)),
                "open": _to_float(_get_val(vals, 14)), "high": _to_float(_get_val(vals, 15)), "low": _to_float(_get_val(vals, 16))
            })

        # 1.8 Night Market (야간선물옵션/Eurex) - H0EUCNT0 / ECEUCNT0
        elif tr_id in ["H0EUCNT0", "ECEUCNT0"]:
            data.update({
                "type": "NIGHT_FUTOPT", "market": "KR_NIGHT", "currency": "KRW",
                "code": _get_val(vals, 0), "time": _get_val(vals, 1),
                "price": _to_float(_get_val(vals, 2)),
                "acc_volume": _to_float(_get_val(vals, 7))
            })
            
        else:
            data["valid"] = False

    except Exception:
        data["valid"] = False

    return data


def parse_ws_hoka(tr_id, body):
    """
    호가(Depth) 데이터 파싱
    """
    vals = body.split('^')
    data = {"tr_id": tr_id, "asks": [], "bids": []}

    try:
        # 2.1 Domestic Stock (10단계)
        if tr_id == "H0STASP0":
            data["code"] = _get_val(vals, 0)
            for i in range(10):
                data["asks"].append((_to_float(_get_val(vals, 3+i)), _to_float(_get_val(vals, 23+i))))
                data["bids"].append((_to_float(_get_val(vals, 13+i)), _to_float(_get_val(vals, 33+i))))
        
        # 2.2 Domestic Fut/Opt (5단계)
        elif tr_id in ["H0IFASP0", "H0IOASP0"]:
            data["code"] = _get_val(vals, 0)
            for i in range(5):
                data["asks"].append((_to_float(_get_val(vals, 2+i)), _to_float(_get_val(vals, 22+i))))
                data["bids"].append((_to_float(_get_val(vals, 7+i)), _to_float(_get_val(vals, 27+i))))

        # 2.3 Overseas Stock (1단계 - Best Quote)
        elif tr_id.startswith("HDFSASP"):
            data["code"] = _get_val(vals, 1)
            data["asks"].append((_to_float(_get_val(vals, 12)), _to_float(_get_val(vals, 14))))
            data["bids"].append((_to_float(_get_val(vals, 11)), _to_float(_get_val(vals, 13))))
            
        # 2.4 Overseas Future (5단계)
        elif tr_id == "HDFFF010":
            data["code"] = _get_val(vals, 0)
            # 해외선물 호가 필드 매핑 (API 문서 기준 근사치)
            # Ask: 9, 7 / Bid: 6, 4 (Best)
            data["asks"].append((_to_float(_get_val(vals, 9)), _to_float(_get_val(vals, 7))))
            data["bids"].append((_to_float(_get_val(vals, 6)), _to_float(_get_val(vals, 4))))
            
    except Exception:
        pass

    return data

# --- WebSocket Notice Parser (Execution/Order) ---
def parse_ws_notice(tr_id, body, key, iv):
    """
    웹소켓 체결/주문 통보 정밀 파싱 (AES 복호화 포함)
    지원: 국내주식(H0STCNI0), 국내선물옵션(H0IFCNI0), 해외주식(H0GSCNI0), 해외선물(H0GSCNI0-유사)
    """
    dec_str = aes_cbc_base64_dec(key, iv, body)
    if not dec_str: return {"valid": False, "msg": "Decrypt Failed"}
    
    vals = dec_str.split('^')
    res = {
        "valid": True, "tr_id": tr_id, "raw_list": vals,
        "order_status": "UNKNOWN", "account": "", "order_no": "", "org_order_no": "",
        "code": "", "direction": None, "order_type": None,
        "order_qty": 0, "order_price": 0.0, 
        "filled_qty": 0, "filled_price": 0.0, "unfilled_qty": 0,
        "msg": ""
    }

    try:
        # 1. 국내 주식 (H0STCNI0, H0STCNI9)
        if tr_id in ["H0STCNI0", "H0STCNI9"]:
            # idx: 0:CustID, 1:Acnt, 2:OrdNo, 3:OrgOrdNo, 4:Code, 
            # 9:OrdQty, 10:OrdPrice, 11:FilledQty, 12:FilledPrice, 13:Unfilled
            # 14:OrdGubun(00:Limit, 01:Market...), 15:Side(1:Sell, 2:Buy), 16:Method(1:New, 2:Mod, 3:Cancel)
            
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["org_order_no"] = _get_val(vals, 3)
            res["code"] = _get_val(vals, 4)
            
            # Direction
            side = _get_val(vals, 15) # 매도수구분
            if side == '1': res["direction"] = "SHORT"
            elif side == '2': res["direction"] = "LONG"
            
            # Order Type
            ord_tp = _get_val(vals, 14)
            res["order_type"] = "MARKET" if ord_tp == "01" else "LIMIT"

            # Quantities & Prices
            res["order_qty"] = _to_int(_get_val(vals, 9))
            res["order_price"] = _to_float(_get_val(vals, 10))
            res["filled_qty"] = _to_int(_get_val(vals, 11))
            res["filled_price"] = _to_float(_get_val(vals, 12))
            res["unfilled_qty"] = _to_int(_get_val(vals, 13))
            
            # Status Logic
            method = _get_val(vals, 16) # 1:접수, 2:체결, 3:확인?? (문서별 상이, 보통 통보구분)
            notice_type = _get_val(vals, 18) # 접수/체결 구분값 확인 필요. (보통 맨앞이나 뒤에 있음)
            # *실제 KIS 데이터 패턴에 맞춘 로직:
            # filled_qty > 0 이면 체결로 간주
            if res["filled_qty"] > 0:
                res["order_status"] = "ALLTRADED" if res["unfilled_qty"] == 0 else "PARTTRADED"
            else:
                # 접수 단계
                if _get_val(vals, 14) == "03": # 취소? (확인 필요)
                     res["order_status"] = "CANCELLED"
                else:
                     res["order_status"] = "NOTTRADED"

        # 2. 국내 선물/옵션 (H0IFCNI0, H0IFCNI9)
        elif tr_id in ["H0IFCNI0", "H0IFCNI9"]:
            # idx: 1:Acnt, 2:OrdNo, 3:OrgOrdNo, 4:Code
            # 9:OrdQty, 10:OrdPrice, 11:FilledQty, 12:FilledPrice, 13:Unfilled
            # 15:Side(1:Sell, 2:Buy), 16:OrdType
            
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["org_order_no"] = _get_val(vals, 3)
            res["code"] = _get_val(vals, 4)
            
            side = _get_val(vals, 15)
            res["direction"] = "SHORT" if side == '1' else "LONG"
            
            # 선물옵션 가격은 소수점 처리 중요
            res["order_qty"] = _to_int(_get_val(vals, 9))
            res["order_price"] = _to_float(_get_val(vals, 10))
            res["filled_qty"] = _to_int(_get_val(vals, 11))
            res["filled_price"] = _to_float(_get_val(vals, 12))
            res["unfilled_qty"] = _to_int(_get_val(vals, 13))
            
            if res["filled_qty"] > 0:
                res["order_status"] = "ALLTRADED" if res["unfilled_qty"] == 0 else "PARTTRADED"
            else:
                res["order_status"] = "NOTTRADED" # Or Cancelled logic needed based on 'method' code

        # 3. 해외 주식/선물 (H0GSCNI0, H0GSCNI9)
        elif tr_id in ["H0GSCNI0", "H0GSCNI9"]:
            # idx: 1:Acnt, 2:OrdNo, 3:OrgOrdNo, 4:Side(1:Sell, 2:Buy), 7:Code
            # 8:Qty(Contextual), 9:Price, 10:Unfilled, 11:RejectCode, 12:NoticeType(1:Accept, 2:Fill)
            # 13:OrdType(00:Limit, 01:Market)
            
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["org_order_no"] = _get_val(vals, 3)
            res["code"] = _get_val(vals, 7)
            
            side = _get_val(vals, 4)
            res["direction"] = "SHORT" if side == '1' else "LONG"
            
            otp = _get_val(vals, 13)
            res["order_type"] = "MARKET" if otp == "01" else "LIMIT"
            
            qty_val = _to_int(_get_val(vals, 8))
            price_val = _to_float(_get_val(vals, 9))
            unfilled = _to_int(_get_val(vals, 10))
            res["unfilled_qty"] = unfilled
            
            notice_type = _get_val(vals, 12) # 1: 접수, 2: 체결
            
            if notice_type == '2': # Filled
                res["filled_qty"] = qty_val
                res["filled_price"] = price_val
                res["order_status"] = "ALLTRADED" if unfilled == 0 else "PARTTRADED"
            else: # Accepted
                res["order_qty"] = qty_val
                res["order_price"] = price_val
                
                # Check Cancel/Modify via vals[5] (TradType: 1:Correct, 2:Cancel) or Reject(11)
                sub_type = _get_val(vals, 5)
                reject = _get_val(vals, 11)
                
                if reject and reject != '00000': res["order_status"] = "REJECTED"
                elif sub_type == '2': res["order_status"] = "CANCELLED"
                else: res["order_status"] = "NOTTRADED"

    except Exception as e:
        res["valid"] = False
        res["msg"] = f"Parse Error: {e}"

    return res

# =========================================================
# 2. REST API Response Parser (대폭 개선)
# =========================================================

def parse_order_response(data: dict) -> dict:
    """
    주문 접수 결과 파싱 (공통)
    """
    if not data:
        return {}
    
    output = data.get('output', {})
    
    # 국내/해외/선물 공통적으로 KRX_FWDG_ORD_ORGNO(주문조직), ODNO(주문번호) 사용
    # 일부 해외선물의 경우 필드명이 다를 수 있어 체크
    return {
        "orderid": output.get("KRX_FWDG_ORD_ORGNO", "") + "-" + output.get("ODNO", ""),
        "odno": output.get("ODNO", ""),
        "msg": data.get("msg1", "")
    }

def parse_cancel_response(data: dict) -> dict:
    """
    주문 취소/정정 응답 파싱
    
    KIS API Cancel/Revise Response Structure:
    {
        "rt_cd": "0",
        "msg1": "정상처리되었습니다.",
        "output": {
            "KRX_FWDG_ORD_ORGNO": "06010",  # 주문조직(지점)
            "ODNO": "00001234",             # 이번 취소/정정 요청의 주문번호
            "ORGN_ODNO": "00001111"         # (중요) 취소 대상 원주문번호
        }
    }
    """
    if not data:
        return {}
        
    output = data.get('output', {})
    
    # 일부 해외선물 등은 output이 리스트일 수 있으므로 예외처리
    if isinstance(output, list) and len(output) > 0:
        output = output[0]
    
    # 필드 추출
    branch = output.get("KRX_FWDG_ORD_ORGNO", "")
    new_odno = output.get("ODNO", "")       # 취소 접수 번호
    org_odno = output.get("ORGN_ODNO", "")  # 원주문 번호
    
    # Gateway에서 주문 식별에 사용하는 ID 포맷 (지점-주문번호) 생성
    # 참고: cancel_orderid는 이번 "취소 요청" 자체의 ID이고,
    #       original_orderid는 사용자가 취소하려고 했던 원래 주문의 ID입니다.
    
    return {
        "cancel_orderid": f"{branch}-{new_odno}",    # 취소접수 ID
        "original_orderid": f"{branch}-{org_odno}",  # 원주문 ID
        "odno": new_odno,
        "orgn_odno": org_odno,
        "msg": data.get("msg1", ""),
        "status": data.get("rt_cd", "")
    }
    
# --- Balance & Position Parsing (Enhanced) ---
def parse_balance(data: dict, asset_type: str) -> dict:
    info = {"balance": 0.0, "available": 0.0, "net_pnl": 0.0}
    if not data: return info
    
    # TR별 Output 위치가 다를 수 있음 (일관성 확보)
    row = data.get('output2', {}) 
    if isinstance(row, list) and row: row = row[0]
    if not isinstance(row, dict): row = data.get('output', {}) # 해외선물 예수금 등

    if asset_type == "KR_STOCK": # 국내 주식
        info["balance"] = _to_float(row.get("tot_evlu_amt", 0))
        info["available"] = _to_float(row.get("dnca_tot_amt", 0))
        info["net_pnl"] = _to_float(row.get("evlu_pfls_smt_tluj_amt", 0))
        
    elif asset_type == "OS_STOCK": # 해외 주식 (TTTS3012R Output2)
        info["balance"] = _to_float(row.get("tot_asst_amt", 0)) # 자산총액
        info["available"] = _to_float(row.get("frcr_ord_psbl_amt1", 0)) # 외화주문가능
        info["net_pnl"] = _to_float(row.get("ovrs_tot_pfls", 0))
        
    elif asset_type == "KR_FUTOPT": # 국내 선물옵션 (CTFO6118R Output2)
        info["balance"] = _to_float(row.get("prsm_dpast_amt", 0)) # 추정예탁자산
        info["available"] = _to_float(row.get("ord_psbl_tota", 0)) # 주문가능총액
        info["net_pnl"] = _to_float(row.get("tot_pnl_amt", 0)) # (별도계산 필요할 수 있음)
        
    elif asset_type == "OS_FUTOPT": # 해외 선물옵션 (OTFM1411R Output)
        # 해외선물은 output이 dict 형태
        info["balance"] = _to_float(row.get("fm_tot_asst_evlu_amt", 0)) # 총자산평가금액
        info["available"] = _to_float(row.get("fm_ord_psbl_amt", 0)) # 주문가능금액
        info["net_pnl"] = _to_float(row.get("fm_lqd_pfls_amt", 0)) # 청산손익 (평가손익은 잔고에서 합산해야 함)

    return info

def parse_position(data: dict, asset_type: str) -> list:
    positions = []
    rows = data.get('output1', []) if 'output1' in data else data.get('output', [])
    if not isinstance(rows, list): rows = [rows] # 단일 객체일 경우 리스트화

    for row in rows:
        if not row: continue
        try:
            pos = {}
            if asset_type == "KR_STOCK":
                pos["symbol"] = row["pdno"]
                pos["direction"] = "long" # 국내주식 현물은 Long Only
                pos["quantity"] = _to_int(row.get("hldg_qty", 0))
                pos["price"] = _to_float(row.get("pchs_avg_pric", 0))
                pos["pnl"] = _to_float(row.get("evlu_pfls_amt", 0))
                
            elif asset_type == "OS_STOCK":
                pos["symbol"] = row["ovrs_pdno"]
                pos["direction"] = "long"
                pos["quantity"] = _to_int(row.get("ovrs_cblc_qty", 0))
                pos["price"] = _to_float(row.get("pchs_avg_pric", 0))
                pos["pnl"] = _to_float(row.get("frcr_evlu_pfls_amt", 0))
                
            elif asset_type == "KR_FUTOPT":
                # pdno(상품코드), trad_dvsn_name(매매구분: 매수/매도)
                pos["symbol"] = row["pdno"]
                dvsn = row.get("trad_dvsn_name", "")
                pos["direction"] = "short" if "매도" in dvsn else "long"
                pos["quantity"] = _to_int(row.get("cblc_qty", 0))
                pos["price"] = _to_float(row.get("pchs_avg_pric", 0)) # or cblc_amt / qty
                pos["pnl"] = _to_float(row.get("evlu_pfls_amt", 0))
                
            elif asset_type == "OS_FUTOPT": # OTFM1412R Output
                pos["symbol"] = row["ovrs_futr_fx_pdno"]
                dvsn = row.get("sll_buy_dvsn_cd", "")
                pos["direction"] = "short" if dvsn == "01" else "long" # 01매도, 02매수
                pos["quantity"] = _to_int(row.get("fm_ustl_qty", 0)) # 미결제수량
                pos["price"] = _to_float(row.get("fm_ccld_avg_pric", 0)) # 체결평균가
                pos["pnl"] = _to_float(row.get("fm_evlu_pfls_amt", 0)) # 평가손익

            if "symbol" in pos and pos["quantity"] > 0:
                positions.append(pos)
        except Exception:
            continue
            
    return positions

def parse_kis_bar_data(
    data_list: List[Dict], 
    req_symbol: str, 
    req_exchange: Exchange, 
    req_interval: Interval, 
    gateway_name: str = "KIS"
) -> List[BarData]:
    """
    KIS API 응답 리스트(output2 등)를 BarData 리스트로 변환
    """
    bars: List[BarData] = []
    
    for item in data_list:
        try:
            # 1. 날짜/시간 파싱 (자산별 키값 자동 대응)
            # stck_bsop_date: 국내주식/선물
            # kymd: 해외주식 (한국시간)
            # data_date: 해외선물 (현지일자) -> 주의: 해외선물은 data_date + data_time 조합
            date_str = (
                item.get("stck_bsop_date") or 
                item.get("kymd") or 
                item.get("data_date") or 
                item.get("tymd")
            )
            
            # 시간 문자열 (일봉인 경우 없을 수 있음)
            time_str = (
                item.get("stck_cntg_hour") or 
                item.get("khms") or 
                item.get("data_time") or 
                item.get("xhms") or 
                "000000"
            )
            
            if not date_str:
                continue

            # 포맷팅 (YYYYMMDD or YYYYMMDDHHMMSS)
            if len(time_str) == 6:
                dt_str = f"{date_str} {time_str}"
                dt_fmt = "%Y%m%d %H%M%S"
            else:
                dt_str = date_str
                dt_fmt = "%Y%m%d"

            dt = datetime.strptime(dt_str, dt_fmt)
            dt = dt.replace(tzinfo=KST) # 기본 KST 설정 (필요시 UTC 변환 로직 추가 가능)

            # 2. 가격/거래량 파싱 (우선순위 큐 방식)
            # 종가
            close_price = _to_float(
                item.get("stck_prpr") or item.get("last") or item.get("close") or 
                item.get("futs_prpr") or item.get("bond_prpr") or item.get("last_price")
            )
            
            # 시가
            open_price = _to_float(
                item.get("stck_oprc") or item.get("open") or item.get("futs_oprc") or 
                item.get("bond_oprc") or item.get("open_price")
            )
            
            # 고가
            high_price = _to_float(
                item.get("stck_hgpr") or item.get("high") or item.get("futs_hgpr") or 
                item.get("bond_hgpr") or item.get("high_price")
            )
            
            # 저가
            low_price = _to_float(
                item.get("stck_lwpr") or item.get("low") or item.get("futs_lwpr") or 
                item.get("bond_lwpr") or item.get("low_price")
            )
            
            # 거래량
            volume = _to_float(
                item.get("cntg_vol") or item.get("evol") or item.get("vol") or 
                item.get("acml_vol") or item.get("last_qntt")
            )
            
            # 거래대금 (옵션)
            turnover = _to_float(
                item.get("acml_tr_pbmn") or item.get("eamt") or item.get("tamt")
            )
            
            # 데이터 보정 (틱 데이터 등에서 시고저가 없는 경우 종가로 채움)
            if open_price == 0: open_price = close_price
            if high_price == 0: high_price = close_price
            if low_price == 0: low_price = close_price

            bar = BarData(
                symbol=req_symbol,
                exchange=req_exchange,
                datetime=dt,
                interval=req_interval,
                volume=volume,
                turnover=turnover,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
                gateway_name=gateway_name
            )
            bars.append(bar)
            
        except Exception:
            # 파싱 에러 발생 시 해당 Bar 건너뜀
            continue

    # 날짜 오름차순 정렬 (과거 -> 현재)
    bars.sort(key=lambda x: x.datetime)
    return bars

def parse_history_data(data: any) -> list:
    """
    통합 차트 데이터 파싱
    Input: API Response(Dict) 또는 output List
    Output: [{'datetime':..., 'open':..., ...}, ...] (Standardized Dict List)
    """
    candles = []
    
    # 1. 데이터 타입 정합성 처리 (Dict vs List)
    raw_list = []
    if isinstance(data, dict):
        # output2가 우선 (보통 연속조회 데이터), 없으면 output
        raw_list = data.get('output2') or data.get('output') or []
    elif isinstance(data, list):
        raw_list = data
    else:
        return candles # None or Invalid type

    if not raw_list:
        return candles

    # 2. 필드 매핑 및 파싱 (공통 로직)
    for item in raw_list:
        try:
            bar = {}
            
            # --- DateTime Parsing ---
            # Date Part
            d_part = (item.get("stck_bsop_date") or item.get("kymd") or 
                      item.get("data_date") or item.get("xymd") or "")
            
            # Time Part (분봉일 경우)
            t_part = (item.get("stck_cntg_hour") or item.get("khms") or 
                      item.get("data_time") or item.get("xhms") or "000000")

            if not d_part: continue

            full_dt = f"{d_part}{t_part}"
            # 포맷에 따라 파싱 (YYYYMMDDHHMMSS or YYYYMMDD)
            if len(full_dt) >= 14:
                bar["datetime"] = datetime.strptime(full_dt[:14], "%Y%m%d%H%M%S")
            else:
                bar["datetime"] = datetime.strptime(d_part, "%Y%m%d")

            # --- OHLCV Parsing (Universal Mapping) ---
            # 값이 존재하는 첫 번째 키를 사용
            
            # Close
            bar["close"] = _find_first_valid_float([
                item.get("stck_clpr"), item.get("stck_prpr"), # 국내 주식 (일/분)
                item.get("futs_prpr"), item.get("optn_prpr"), # 국내 선옵
                item.get("bond_prpr"),                        # 채권
                item.get("last"), item.get("last_price"), item.get("clos") # 해외
            ])
            
            # Open (없으면 Close 사용)
            bar["open"] = _find_first_valid_float([
                item.get("stck_oprc"), item.get("futs_oprc"), item.get("optn_oprc"),
                item.get("bond_oprc"), item.get("open"), item.get("open_price")
            ], default=bar["close"])

            # High
            bar["high"] = _find_first_valid_float([
                item.get("stck_hgpr"), item.get("futs_hgpr"), item.get("optn_hgpr"),
                item.get("bond_hgpr"), item.get("high"), item.get("high_price")
            ], default=bar["close"])

            # Low
            bar["low"] = _find_first_valid_float([
                item.get("stck_lwpr"), item.get("futs_lwpr"), item.get("optn_lwpr"),
                item.get("bond_lwpr"), item.get("low"), item.get("low_price")
            ], default=bar["close"])

            # Volume
            bar["volume"] = _find_first_valid_float([
                item.get("acml_vol"), item.get("cntg_vol"),
                item.get("vol"), item.get("evol"), item.get("tvol")
            ])
            
            # Turnover
            bar["turnover"] = _find_first_valid_float([
                item.get("acml_tr_pbmn"), item.get("tamt"), item.get("eamt")
            ])

            candles.append(bar)

        except Exception:
            continue
            
    return candles

def _find_first_valid_float(candidates, default=0.0):
    for val in candidates:
        if val is not None and str(val).strip() != "":
            return _to_float_(val)
    return default


def parse_interest_group(data):
    """
    Parse Interest Stock Group
    Returns: List of symbols
    """
    symbols = []
    items = data.get('output2', [])
    for item in items:
        code = item.get("pdno") # Product Number
        if code:
            symbols.append(code)
    return symbols