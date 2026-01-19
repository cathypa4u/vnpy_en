# kis_parser.py
# KIS API Data Parser (Universal & Independent)
# Supports: Domestic/Overseas Stocks, Futures, Options, Bonds, Indices, Night Market
# Features: Precise Field Mapping, Multi-currency Balance

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode
from datetime import datetime
from zoneinfo import ZoneInfo

# KST Timezone for Domestic Market
KST = ZoneInfo("Asia/Seoul")

# --- Utility Functions ---

def aes_cbc_base64_dec(key, iv, cipher_text):
    """AES256 Decode for Notices"""
    try:
        cipher = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
        return bytes.decode(unpad(cipher.decrypt(b64decode(cipher_text)), AES.block_size))
    except Exception:
        return None

def _to_float(val):
    try: return float(val) if val and val.strip() != "" else 0.0
    except: return 0.0

def _to_int(val):
    try: return int(val) if val and val.strip() != "" else 0
    except: return 0

def _get_val(vals, idx, default=""):
    """Safe list access"""
    try:
        return vals[idx]
    except IndexError:
        return default

# =========================================================
# 1. Real-time WebSocket Data Parser
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


def parse_ws_notice(tr_id, body, key, iv):
    """
    체결 통보 데이터 정밀 파싱 (AES 복호화 포함)
    TR ID 별로 인덱스가 다르므로 분기 처리
    """
    dec_str = aes_cbc_base64_dec(key, iv, body)
    if not dec_str: return None
    
    vals = dec_str.split('^')
    res = {
        "valid": True, "tr_id": tr_id, "raw_list": vals,
        "order_status": "UNKNOWN", # NEW, MODIFY, CANCEL, FILLED, REJECT
        "account": "", "order_no": "", "org_order_no": "", "code": "",
        "order_qty": 0, "order_price": 0.0, "filled_qty": 0, "filled_price": 0.0,
        "msg": ""
    }

    try:
        # ---------------------------------------------------------
        # 1. Domestic Stock (국내주식) - H0STCNI0, H0STCNI9
        # ---------------------------------------------------------
        if tr_id in ["H0STCNI0", "H0STCNI9"]:
            # vals[13]: 통보구분 (1:접수, 2:체결)
            # vals[5]: 정정구분 (0:주문, 1:정정, 2:취소)
            # vals[11]: 거부여부 (0:정상, 1:거부)
            
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["org_order_no"] = _get_val(vals, 3)
            res["code"] = _get_val(vals, 8)
            res["time"] = _get_val(vals, 11) # 체결시간

            notice_type = _get_val(vals, 13) # 2: 체결
            modify_type = _get_val(vals, 5)  # 0:주문, 1:정정, 2:취소
            reject_type = _get_val(vals, 11) # 1: 거부

            if notice_type == '2': # 체결
                res["order_status"] = "FILLED"
                res["filled_qty"] = _to_int(_get_val(vals, 9))
                res["filled_price"] = _to_float(_get_val(vals, 10))
                res["order_qty"] = _to_int(_get_val(vals, 16)) # 전체주문수량
            else: # 접수 (주문/정정/취소)
                if reject_type == '1':
                    res["order_status"] = "REJECT"
                elif modify_type == '1':
                    res["order_status"] = "MODIFY"
                elif modify_type == '2':
                    res["order_status"] = "CANCEL"
                else:
                    res["order_status"] = "NEW"
                
                res["order_qty"] = _to_int(_get_val(vals, 9))
                res["order_price"] = _to_float(_get_val(vals, 10))

        # ---------------------------------------------------------
        # 2. Domestic Fut/Opt (국내선물옵션) - H0IFCNI0, H0IFCNI9
        # ---------------------------------------------------------
        elif tr_id in ["H0IFCNI0", "H0IFCNI9"]:
            # vals[6]: 통보구분 (0:체결, L:접수/확인) -> 확인필요 (문서에 따라 다름, 보통 0이 체결)
            # vals[5]: 정정취소구분 (1:정정, 2:취소)
            # vals[11]: 거부여부 (1:거부)
            
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["code"] = _get_val(vals, 7)
            
            notice_type = _get_val(vals, 6) # 0: 체결통보
            modify_type = _get_val(vals, 5) # 1:정정, 2:취소
            reject_type = _get_val(vals, 11)

            if notice_type == '0': # 체결
                res["order_status"] = "FILLED"
                res["filled_qty"] = _to_int(_get_val(vals, 8))
                res["filled_price"] = _to_float(_get_val(vals, 9))
            else: # 접수
                if reject_type == '1':
                    res["order_status"] = "REJECT"
                elif modify_type == '1':
                    res["order_status"] = "MODIFY"
                elif modify_type == '2':
                    res["order_status"] = "CANCEL"
                else:
                    res["order_status"] = "NEW"

                res["order_qty"] = _to_int(_get_val(vals, 8)) # or 15
                res["order_price"] = _to_float(_get_val(vals, 21))

        # ---------------------------------------------------------
        # 3. Overseas Stock (해외주식) - H0GSCNI0, H0GSCNI9
        # ---------------------------------------------------------
        elif tr_id in ["H0GSCNI0", "H0GSCNI9"]:
            # vals[12]: 1:접수, 2:체결
            # vals[5]: 1:정정, 2:취소
            # vals[11]: 1:거부
            
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["code"] = _get_val(vals, 7)
            
            notice_type = _get_val(vals, 12)
            modify_type = _get_val(vals, 5)
            reject_type = _get_val(vals, 11)

            if notice_type == '2': # 체결
                res["order_status"] = "FILLED"
                res["filled_qty"] = _to_int(_get_val(vals, 8))
                res["filled_price"] = _to_float(_get_val(vals, 9))
            else:
                if reject_type == '1':
                    res["order_status"] = "REJECT"
                elif modify_type == '1':
                    res["order_status"] = "MODIFY"
                elif modify_type == '2':
                    res["order_status"] = "CANCEL"
                else:
                    res["order_status"] = "NEW"
                
                res["order_qty"] = _to_int(_get_val(vals, 8))
                res["order_price"] = _to_float(_get_val(vals, 9))

        # ---------------------------------------------------------
        # 4. Overseas Future (해외선물) - HDFFF2C0
        # ---------------------------------------------------------
        elif tr_id == "HDFFF2C0":
            # 해외선물은 필드 구성이 다소 상이함
            # vals[15]: 체결수량, vals[16]: 체결단가
            # 통상 0번 인덱스 유저ID
            
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 3)
            res["code"] = _get_val(vals, 6)
            
            filled_qty = _to_int(_get_val(vals, 15))
            
            if filled_qty > 0:
                res["order_status"] = "FILLED"
                res["filled_qty"] = filled_qty
                res["filled_price"] = _to_float(_get_val(vals, 16))
            else:
                # 상태 구분 로직 추가 필요 (정정/취소 등)
                # 여기서는 단순 접수/체결로 이분화
                res["order_status"] = "NEW"
                res["order_qty"] = _to_int(_get_val(vals, 12))
                
        # ---------------------------------------------------------
        # 5. Night Market (야간선물옵션) - H0MFCNI0, H0EUCNI0
        # ---------------------------------------------------------
        elif tr_id in ["H0MFCNI0", "H0EUCNI0"]:
             # 국내선물옵션 포맷과 유사
            res["account"] = _get_val(vals, 1)
            res["order_no"] = _get_val(vals, 2)
            res["code"] = _get_val(vals, 7)
            
            notice_type = _get_val(vals, 6) # 0:체결
            
            if notice_type == '0':
                res["order_status"] = "FILLED"
                res["filled_qty"] = _to_int(_get_val(vals, 8))
                res["filled_price"] = _to_float(_get_val(vals, 9))
            else:
                res["order_status"] = "NEW" # 상세 구분 생략(국내선물과 유사)

    except Exception as e:
        res["valid"] = False
        res["msg"] = str(e)

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

def parse_balance(data: dict, asset_type: str = "KR_STOCK") -> dict:
    """
    예수금/잔고 현황 파싱 (Account Data)
    asset_type: 'stock'(국내주식), 'overseas_stock'(해외주식), 'future'(선물옵션)
    """
    balance_info = {
        "balance": 0.0,      # 총 평가금액 (Total Value)
        "available": 0.0,    # 주문가능금액 (Buying Power)
        "frozen": 0.0,       # 동결금액 (필요시 계산)
        "net_pnl": 0.0       # 순손익
    }

    if not data:
        return balance_info
        
    output2 = data.get('output2', [])
    if isinstance(output2, list) and len(output2) > 0:
        row = output2[0]
    elif isinstance(output2, dict):
        row = output2
    else:
        return balance_info

    # 1. 국내 주식 (dnca_tot_amt: 예수금, prvs_rcdl_excc_amt: 가수금 등)
    if asset_type == "KR_STOCK":
        balance_info["balance"] = _to_float(row.get("tot_evlu_amt", 0)) # 총평가금액
        balance_info["available"] = _to_float(row.get("dnca_tot_amt", 0)) # 예수금총액 (주문가능)
        balance_info["net_pnl"] = _to_float(row.get("evlu_pfls_smt_tluj_amt", 0)) # 평가손익합계

    # 2. 해외 주식 (frcr_dncl_amt_2: 외화예수금, ovrs_rlzt_pfls_amt: 실현손익)
    # API 문서 [해외주식 잔고] 참조:
    # frcr_ord_psbl_amt1: 외화주문가능금액 -> Available
    # tot_asst_amt: 자산총액 (예수금 + 평가금액) -> Balance
    # ovrs_tot_pfls: 해외총손익 -> Net PnL
    elif asset_type == "OS_STOCK":
        balance_info["balance"] = _to_float(row.get("tot_asst_amt", 0))        # 자산총액
        balance_info["available"] = _to_float(row.get("frcr_ord_psbl_amt1", 0)) # 외화주문가능금액
        balance_info["net_pnl"] = _to_float(row.get("ovrs_tot_pfls", 0))      # 해외총손익

    # 3. 국내 선물/옵션 (dnca_tot_amt: 예탁금총액, ord_psbl_amt: 주문가능액)
    elif asset_type == "KR_FUTOPT":
        balance_info["balance"] = _to_float(row.get("tot_asst_amt", 0)) # 총자산
        balance_info["available"] = _to_float(row.get("ord_psbl_amt", 0)) # 주문가능금액
        balance_info["net_pnl"] = _to_float(row.get("tot_pnl_amt", 0)) # 총손익

    # 4. 해외 선물
    elif asset_type == "OS_FUTOPT":
        balance_info["balance"] = _to_float(row.get("tot_asst_amt", 0))
        balance_info["available"] = _to_float(row.get("ord_psbl_amt", 0))
        # 해외선물은 output1에 상세가 있는 경우가 많음

    return balance_info

def parse_position(data: dict, asset_type: str = "KR_STOCK") -> list:
    """
    보유 종목(Position) 파싱
    지원: 국내주식, 해외주식, 국내선물옵션, 해외선물
    """
    positions = []
    if not data:
        return positions
        
    # 대부분의 잔고조회 API는 output1에 보유종목 리스트가 옴
    output1 = data.get('output1', [])
    if not isinstance(output1, list):
        # 일부 API는 dict로 오거나 output1이 아닐 수 있음
        return positions

    for item in output1:
        pos = {
            "symbol": "",
            "direction": "net", # net: 주식, long/short: 선물
            "quantity": 0,
            "price": 0.0,    # 평균단가
            "current_price": 0.0,
            "pnl": 0.0
        }

        # 1. 국내 주식
        if asset_type == "KR_STOCK":
            pos["symbol"] = item.get("pdno", "")
            pos["quantity"] = _to_int(item.get("hldg_qty", 0))
            pos["price"] = _to_float(item.get("pchs_avg_pric", 0))
            pos["current_price"] = _to_float(item.get("prpr", 0))
            pos["pnl"] = _to_float(item.get("evlu_pfls_amt", 0)) # 평가손익

        # 2. 해외 주식
        elif asset_type == "OS_STOCK":
            pos["symbol"] = item.get("ovrs_pdno", "")
            pos["quantity"] = _to_int(item.get("ovrs_cblc_qty", 0)) # 잔고수량
            pos["price"] = _to_float(item.get("pchs_avg_pric", 0))
            pos["current_price"] = _to_float(item.get("now_pric2", 0))
            pos["pnl"] = _to_float(item.get("frcr_evlu_pfls_amt", 0)) # 외화평가손익

        # 3. 국내 선물/옵션
        elif asset_type == "KR_FUTOPT":
            pos["symbol"] = item.get("pdno") or item.get("futs_prdt_cd") or item.get("optn_prdt_cd")
            
            # 매수/매도 구분 (API에 따라 buy_qty, sell_qty가 따로 오거나 gubun 코드가 있음)
            # 여기서는 순보유수량 기준으로 처리 예시
            buy_qty = _to_int(item.get("buy_qty", 0))
            sell_qty = _to_int(item.get("sell_qty", 0))
            
            if buy_qty > 0:
                pos["direction"] = "long"
                pos["quantity"] = buy_qty
            elif sell_qty > 0:
                pos["direction"] = "short"
                pos["quantity"] = sell_qty
            
            pos["price"] = _to_float(item.get("avr_unpr", 0)) # 평균단가
            pos["current_price"] = _to_float(item.get("futs_prpr", 0) or item.get("optn_prpr", 0))
            pos["pnl"] = _to_float(item.get("dlim_pnl_amt", 0)) # 일일손익

        # 4. 해외 선물
        elif asset_type == "OS_FUTOPT":
            pos["symbol"] = item.get("srs_cd", "")
            # 해외선물 잔고상세
            # pos_div: 1(매도), 2(매수) 등 코드 확인 필요
            type_code = item.get("futs_slby_cls_code", "") # 예시 필드
            pos["direction"] = "short" if type_code == "1" else "long"
            
            pos["quantity"] = _to_int(item.get("ccld_qty", 0)) # 체결수량(보유)
            pos["price"] = _to_float(item.get("avg_price", 0))
            pos["pnl"] = _to_float(item.get("ovrs_pfls_amt", 0))

        if pos["symbol"]:
            positions.append(pos)
            
    return positions

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

def _to_float_(val):
    try: return float(str(val).replace(",", ""))
    except: return 0.0

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