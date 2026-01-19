# kis_parser.py
# KIS API Data Parser (Improved with Universal Explicit Status Mapping)

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional, Any
import traceback

from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange, Interval, Status
from vnpy_kis.kis_api_helper import AssetType

# KST Timezone for Domestic Market
KST = ZoneInfo("Asia/Seoul")

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
            data["asks"].append((_to_float(_get_val(vals, 9)), _to_float(_get_val(vals, 7))))
            data["bids"].append((_to_float(_get_val(vals, 6)), _to_float(_get_val(vals, 4))))
            
    except Exception:
        pass

    return data

# =========================================================
# 2. WebSocket Notice Parser (Execution/Order)
#    [IMPROVED] Explicit Status Mapping for All Assets
# =========================================================

def parse_ws_notice(tr_id, body, key, iv):
    """
    웹소켓 체결/주문 통보 정밀 파싱 (AES 복호화 포함)
    지원: 국내주식/선물/채권, 해외주식/선물
    """
    dec_str = aes_cbc_base64_dec(key, iv, body)
    if not dec_str: return {"valid": False, "msg": "Decrypt Failed"}
    
    vals = dec_str.split('^')
    res = {
        "valid": True, "tr_id": tr_id, "raw_list": vals,
        "order_status": Status.NOTTRADED, 
        "account": "", "order_no": "", "org_order_no": "",
        "code": "", "direction": None, "order_type": None,
        "order_qty": 0, "order_price": 0.0, 
        "filled_qty": 0, "filled_price": 0.0, "unfilled_qty": 0,
        "msg": ""
    }

    try:
        # ------------------------------------------------------------------
        # [CASE A] 국내 시장 공통 (주식, 선물, 옵션, 채권)
        # TR IDs: H0STCNI0/9 (주식/채권), H0IFCNI0/9 (선물/옵션)
        # ------------------------------------------------------------------
        if tr_id in ["H0STCNI0", "H0STCNI9", "H0IFCNI0", "H0IFCNI9"]:
            # 필드 인덱스가 주식/선물 거의 동일함
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["org_order_no"] = _get_val(vals, 3)
            res["code"] = _get_val(vals, 4)
            
            # 매도수 구분 (15번)
            side = _get_val(vals, 15)
            if side == '1': res["direction"] = "SHORT"
            elif side == '2': res["direction"] = "LONG"
            
            # 주문 구분 (14번)
            ord_tp = _get_val(vals, 14)
            res["order_type"] = "MARKET" if ord_tp == "01" else "LIMIT"

            # 수량/가격
            res["order_qty"] = _to_int(_get_val(vals, 9))
            res["order_price"] = _to_float(_get_val(vals, 10))
            res["filled_qty"] = _to_int(_get_val(vals, 11))
            res["filled_price"] = _to_float(_get_val(vals, 12))
            res["unfilled_qty"] = _to_int(_get_val(vals, 13))
            
            # [핵심] 처리구분 코드를 이용한 명시적 상태 판별 (Index 16)
            method_code = _get_val(vals, 16) # 01:접수, 02:확인, 03:체결, ...
            
            mapped_status = KIS_DOMESTIC_STATUS_MAP.get(method_code, Status.NOTTRADED)
            
            if mapped_status == "CHECK_QTY":
                # '03'(체결)인 경우, 잔량을 확인하여 전량/부분 판별
                if res["unfilled_qty"] == 0:
                    res["order_status"] = "ALLTRADED" # Gateway 호환 문자열
                else:
                    res["order_status"] = "PARTTRADED"
            else:
                # Enum을 String으로 변환하여 Gateway로 전달 (Gateway가 String을 매핑)
                if mapped_status == Status.NOTTRADED: res["order_status"] = "NOTTRADED"
                elif mapped_status == Status.CANCELLED: res["order_status"] = "CANCELLED"
                elif mapped_status == Status.REJECTED: res["order_status"] = "REJECTED"

        # ------------------------------------------------------------------
        # [CASE B] 해외 시장 (주식, 선물)
        # TR IDs: H0GSCNI0/9 (해외주식/선물 통합 통보)
        # ------------------------------------------------------------------
        elif tr_id in ["H0GSCNI0", "H0GSCNI9"]:
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["org_order_no"] = _get_val(vals, 3)
            res["code"] = _get_val(vals, 7)
            
            side = _get_val(vals, 4) # 1:매도, 2:매수
            res["direction"] = "SHORT" if side == '1' else "LONG"
            
            otp = _get_val(vals, 13)
            res["order_type"] = "MARKET" if otp == "01" else "LIMIT"
            
            res["order_qty"] = _to_int(_get_val(vals, 8))
            res["order_price"] = _to_float(_get_val(vals, 9))
            res["unfilled_qty"] = _to_int(_get_val(vals, 10)) # 미체결수량
            
            # [핵심] 해외 상태 판별 로직
            # 필드: 12(통보구분), 11(거부코드), 5(업무구분)
            notice_type = _get_val(vals, 12) # 1:접수, 2:체결
            reject_code = _get_val(vals, 11) # "00000" 아니면 거부
            trad_type = _get_val(vals, 5)    # 1:입력(신규/정정), 2:취소
            
            if reject_code and reject_code != "00000":
                res["order_status"] = "REJECTED"
                res["msg"] = f"Reject Code: {reject_code}"
            
            elif notice_type == '2': # 체결 통보
                # 해외 통보는 체결 시 '주문수량' 필드에 '이번 체결 수량'이 담기기도 함 (주의)
                # KIS 문서상: 8번이 체결수량으로 올 때가 있음. 
                # 안전하게: Unfilled Qty를 기준으로 판단
                res["filled_qty"] = res["order_qty"] # 이번 이벤트의 수량
                res["filled_price"] = res["order_price"] # 이번 이벤트의 가격
                
                if res["unfilled_qty"] == 0:
                    res["order_status"] = "ALLTRADED"
                else:
                    res["order_status"] = "PARTTRADED"
                    
            else: # 접수 통보 (Notice Type == 1)
                if trad_type == '2':
                    res["order_status"] = "CANCELLED"
                else:
                    # 신규 접수 or 정정 접수
                    res["order_status"] = "NOTTRADED"

    except Exception as e:
        res["valid"] = False
        res["msg"] = f"Parse Error: {str(e)}"
        print(f"[KisParser] WS Notice Error: {e}")
        # traceback.print_exc()

    return res

# =========================================================
# 3. REST API Response Parser (Common)
# =========================================================

def parse_order_response(data: dict) -> dict:
    """
    주문 접수 결과 파싱 (공통)
    """
    if not data: return {}
    output = data.get('output', {})
    
    # 국내/해외 공통적으로 ODNO(주문번호) 사용
    return {
        "orderid": f"{output.get('KRX_FWDG_ORD_ORGNO', '')}-{output.get('ODNO', '')}",
        "odno": output.get("ODNO", ""),
        "msg": data.get("msg1", "")
    }

def parse_cancel_response(data: dict) -> dict:
    """
    주문 취소/정정 응답 파싱
    """
    if not data: return {}
    output = data.get('output', {})
    if isinstance(output, list) and output: output = output[0]
    
    return {
        "cancel_orderid": f"{output.get('KRX_FWDG_ORD_ORGNO', '')}-{output.get('ODNO', '')}",
        "original_orderid": f"{output.get('KRX_FWDG_ORD_ORGNO', '')}-{output.get('ORGN_ODNO', '')}",
        "odno": output.get("ODNO", ""),
        "orgn_odno": output.get("ORGN_ODNO", ""),
        "msg": data.get("msg1", ""),
        "status": data.get("rt_cd", "")
    }
    
def parse_balance(data: dict, asset_type: str) -> dict:
    info = {"balance": 0.0, "available": 0.0, "net_pnl": 0.0}
    if not data: return info
    
    row = data.get('output2', {}) 
    if isinstance(row, list) and row: row = row[0]
    if not isinstance(row, dict): row = data.get('output', {})

    if asset_type == AssetType.KR_STOCK: 
        info["balance"] = _to_float(row.get("tot_evlu_amt", 0))
        info["available"] = _to_float(row.get("dnca_tot_amt", 0))
        info["net_pnl"] = _to_float(row.get("evlu_pfls_smt_tluj_amt", 0))
        
    elif asset_type == AssetType.OS_STOCK:
        info["balance"] = _to_float(row.get("tot_asst_amt", 0))
        info["available"] = _to_float(row.get("frcr_ord_psbl_amt1", 0))
        info["net_pnl"] = _to_float(row.get("ovrs_tot_pfls", 0))
        
    elif asset_type == AssetType.KR_FUTOPT:
        info["balance"] = _to_float(row.get("prsm_dpast_amt", 0))
        info["available"] = _to_float(row.get("ord_psbl_tota", 0))
        info["net_pnl"] = _to_float(row.get("tot_pnl_amt", 0))
        
    elif asset_type == AssetType.OS_FUTOPT: 
        info["balance"] = _to_float(row.get("fm_tot_asst_evlu_amt", 0))
        info["available"] = _to_float(row.get("fm_ord_psbl_amt", 0))
        info["net_pnl"] = _to_float(row.get("fm_lqd_pfls_amt", 0))

    return info

def parse_position(data: dict, asset_type: AssetType) -> list:
    positions = []
    rows = data.get('output1', []) if 'output1' in data else data.get('output', [])
    if not isinstance(rows, list): rows = [rows]

    for row in rows:
        if not row: continue
        try:
            pos = {}
            if asset_type == AssetType.KR_STOCK:
                pos["symbol"] = row["pdno"]
                pos["direction"] = "long"
                pos["quantity"] = _to_int(row.get("hldg_qty", 0))
                pos["price"] = _to_float(row.get("pchs_avg_pric", 0))
                pos["pnl"] = _to_float(row.get("evlu_pfls_amt", 0))

            elif asset_type == AssetType.OS_STOCK:
                pos["symbol"] = row["ovrs_pdno"]
                pos["direction"] = "long"
                pos["quantity"] = _to_int(row.get("ovrs_cblc_qty", 0))
                pos["price"] = _to_float(row.get("pchs_avg_pric", 0))
                pos["pnl"] = _to_float(row.get("frcr_evlu_pfls_amt", 0))

            elif asset_type == AssetType.KR_FUTOPT:
                pos["symbol"] = row["pdno"]
                dvsn = row.get("trad_dvsn_name", "")
                pos["direction"] = "short" if "매도" in dvsn else "long"
                pos["quantity"] = _to_int(row.get("cblc_qty", 0))
                pos["price"] = _to_float(row.get("pchs_avg_pric", 0))
                pos["pnl"] = _to_float(row.get("evlu_pfls_amt", 0))

            elif asset_type == AssetType.OS_FUTOPT: 
                pos["symbol"] = row["ovrs_futr_fx_pdno"]
                dvsn = row.get("sll_buy_dvsn_cd", "")
                pos["direction"] = "short" if dvsn == "01" else "long" 
                pos["quantity"] = _to_int(row.get("fm_ustl_qty", 0)) 
                pos["price"] = _to_float(row.get("fm_ccld_avg_pric", 0)) 
                pos["pnl"] = _to_float(row.get("fm_evlu_pfls_amt", 0)) 

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
    KIS API 응답 리스트를 BarData 리스트로 변환
    """
    bars: List[BarData] = []
    
    for item in data_list:
        try:
            date_str = (
                item.get("stck_bsop_date") or 
                item.get("kymd") or 
                item.get("data_date") or 
                item.get("tymd")
            )
            
            time_str = (
                item.get("stck_cntg_hour") or 
                item.get("khms") or 
                item.get("data_time") or 
                item.get("xhms") or 
                "000000"
            )
            
            if not date_str: continue

            # 포맷팅
            if len(time_str) == 6:
                dt_str = f"{date_str} {time_str}"
                dt_fmt = "%Y%m%d %H%M%S"
            else:
                dt_str = date_str
                dt_fmt = "%Y%m%d"

            dt = datetime.strptime(dt_str, dt_fmt)
            dt = dt.replace(tzinfo=KST)

            bar = BarData(
                symbol=req_symbol,
                exchange=req_exchange,
                datetime=dt,
                interval=req_interval,
                volume=_to_float(item.get("cntg_vol") or item.get("evol") or item.get("vol") or item.get("acml_vol")),
                turnover=_to_float(item.get("acml_tr_pbmn") or item.get("eamt") or item.get("tamt")),
                open_price=_to_float(item.get("stck_oprc") or item.get("open") or item.get("futs_oprc") or item.get("bond_oprc") or item.get("open_price")),
                high_price=_to_float(item.get("stck_hgpr") or item.get("high") or item.get("futs_hgpr") or item.get("bond_hgpr") or item.get("high_price")),
                low_price=_to_float(item.get("stck_lwpr") or item.get("low") or item.get("futs_lwpr") or item.get("bond_lwpr") or item.get("low_price")),
                close_price=_to_float(item.get("stck_prpr") or item.get("last") or item.get("close") or item.get("futs_prpr") or item.get("bond_prpr") or item.get("last_price")),
                gateway_name=gateway_name
            )
            
            # 데이터 보정
            if bar.open_price == 0 and bar.close_price > 0: bar.open_price = bar.close_price
            if bar.high_price == 0 and bar.close_price > 0: bar.high_price = bar.close_price
            if bar.low_price == 0 and bar.close_price > 0: bar.low_price = bar.close_price

            bars.append(bar)
            
        except Exception:
            continue

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