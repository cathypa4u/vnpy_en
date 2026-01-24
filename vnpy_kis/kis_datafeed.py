# kis_datafeed.py
# KIS Datafeed Module (Final Version)
# - Updates: Smart Date Adjustment for Pre-market/Weekend
# - Fixes: 0-bar issue when querying at midnight

import requests
import traceback
import time
from datetime import datetime, timedelta
from typing import List, Optional
from pytz import timezone

from vnpy.trader.setting import SETTINGS
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.datafeed import BaseDatafeed
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError

# Local Modules Import
try:
    from .kis_auth import KisAuthManager
    from .kis_parser import parse_kis_bar_data
    from .kis_api_helper import KisApiHelper, AssetType
except ImportError:
    from kis_auth import KisAuthManager
    from kis_parser import parse_kis_bar_data
    from kis_api_helper import KisApiHelper, AssetType

# 도메인 설정
REAL_DOMAIN = "https://openapi.koreainvestment.com:9443"
VIRTUAL_DOMAIN = "https://openapivts.koreainvestment.com:29443"

class KisDatafeed(BaseDatafeed):
    """
    KIS Datafeed: 통합 히스토리 데이터 조회 엔진
    """
    
    def __init__(self, app_key: str = "", app_secret: str = "", server: str = "REAL"):
        self.app_key: str = app_key or SETTINGS.get("context.kis.app_key", "")
        self.app_secret: str = app_secret or SETTINGS.get("context.kis.app_secret", "")
        
        if server in ["DEMO", "VIRTUAL"]:
            self.vts = True
        elif server == "REAL":
            self.vts = False
        else:
            self.vts = SETTINGS.get("context.kis.vts", False)
        
        self.base_url = VIRTUAL_DOMAIN if self.vts else REAL_DOMAIN
        self.auth_manager = KisAuthManager()
        
        if not self.app_key or not self.app_secret:
            print("⚠️ [Datafeed] app_key 또는 app_secret이 설정되지 않았습니다. SETTINGS를 확인하세요.")

    def query_bar_history(self, req: HistoryRequest, output_filepath: str = None) -> List[BarData]:
        """
        분봉/일봉 데이터 조회 (전체 기간 자동 Pagination)
        """
        
        # 1. 자산 타입 판별
        asset_type = KisApiHelper.get_asset_type(req.exchange, req.symbol)
        if not asset_type:
            print(f"❌ [Datafeed] 지원하지 않는 자산/거래소입니다: {req.exchange} {req.symbol}")
            return []

        # 2. API 설정(History TR) 가져오기
        config = KisApiHelper.get_tr_config(asset_type, "HISTORY", self.vts, req.interval)
        if not config:
            print(f"❌ [Datafeed] 설정 없음: {asset_type} {req.interval}")
            return []

        # 3. 토큰 발급
        server_name = "VIRTUAL" if self.vts else "REAL"
        token = self.auth_manager.get_token(self.app_key, self.app_secret, server_name)
        if not token:
            print("❌ [Datafeed] 인증 토큰 발급 실패")
            return []

        # 4. 조회 종료일 보정 [핵심 수정]
        # 새벽에 조회하더라도 전일 데이터를 가져오도록 시간 조정
        query_end_dt = self._adjust_business_day(req.end)
        
        all_bars: List[BarData] = []
        next_key: str = ""
        retry_count = 0
        MAX_RETRIES = 5  
        
        print(f"🚀 [Datafeed] 조회 시작: {req.symbol} ({asset_type}) | 범위: {req.start} ~ {query_end_dt} (보정됨)")

        while True:
            # 5. Rate Limit
            self.auth_manager.check_rate_limit(self.app_key)

            # 6. 파라미터 생성
            params = KisApiHelper.build_history_params(req, config, query_end_dt, next_key)
            
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appkey": self.app_key, "appsecret": self.app_secret,
                "tr_id": config["tr_id"], "custtype": "P"
            }

            try:
                # 7. 요청 전송
                url = f"{self.base_url}{config['url']}"
                resp = requests.get(url, headers=headers, params=params)
                
                # TPS 에러 핸들링
                if resp.status_code == 500:
                    try:
                        err_json = resp.json()
                        msg_cd = err_json.get("msg_cd", "")
                        if msg_cd == "EGW00201" or "초과" in err_json.get("msg1", ""):
                            print(f"⚠️ [Datafeed] TPS 초과(500). 1초 대기... ({retry_count+1}/{MAX_RETRIES})")
                            time.sleep(1)
                            retry_count += 1
                            if retry_count > MAX_RETRIES: break
                            continue
                    except: pass
                
                if resp.status_code != 200:
                    print(f"❌ [Datafeed] HTTP Error: {resp.status_code} {resp.text}")
                    break

                data = resp.json()
                rt_cd = data.get("rt_cd", "")
                if rt_cd != "0":
                    msg_cd = data.get("msg_cd", "")
                    if msg_cd == "EGW00201":
                        print(f"⚠️ [Datafeed] TPS 초과(Body). 1.5초 대기... ({retry_count+1}/{MAX_RETRIES})")
                        time.sleep(1.5)
                        retry_count += 1
                        if retry_count > MAX_RETRIES: break
                        continue
                    
                    print(f"❌ [Datafeed] API Error: {data.get('msg1')} ({msg_cd})")
                    break

                # 8. 데이터 추출
                items = data.get("output2") or data.get("output") or []
                if not items and "output1" in data and isinstance(data["output1"], list):
                     items = data["output1"]
                
                if not items:
                    break

                # [FIX] 국내주식 분봉의 경우 날짜가 output1에만 있고 output2(items)에는 없는 경우가 있음
                # 이를 보정하기 위해 output1에서 날짜를 가져와 주입
                if asset_type == AssetType.KR_STOCK and "stck_cntg_hour" in items[0] and "stck_bsop_date" not in items[0]:
                    base_date = ""
                    if "output1" in data and isinstance(data["output1"], dict):
                        base_date = data["output1"].get("stck_bsop_date", "")
                    
                    # output1에도 없으면 요청 파라미터(FID_INPUT_DATE_1) 사용 (역순조회 시 정확성 위해)
                    if not base_date:
                        base_date = params.get("FID_INPUT_DATE_1", "")

                    if base_date:
                        for item in items:
                            item["stck_bsop_date"] = base_date
                
                # 9. 파싱 (asset_type 전달)
                new_bars = parse_kis_bar_data(
                    items, req.symbol, req.exchange, req.interval, "KIS", 
                    asset_type=asset_type 
                )
                if not new_bars:
                    break
                
                all_bars.extend(new_bars)
                retry_count = 0 
                
                # 10. Pagination
                pg_method = config.get("pg_method", "NONE")
                
                if pg_method == "TIME":
                    earliest_bar = new_bars[0]
                    if earliest_bar.datetime <= req.start:
                        break 
                    query_end_dt = earliest_bar.datetime - timedelta(minutes=1)
                
                elif pg_method == "DATE":
                    earliest_bar = new_bars[0]
                    if earliest_bar.datetime <= req.start:
                        break
                    query_end_dt = earliest_bar.datetime - timedelta(days=1)

                elif pg_method == "KEY":
                    output1 = data.get("output1", {})
                    next_key = "" 
                    if asset_type == AssetType.OS_STOCK:
                        if isinstance(output1, dict) and output1.get("next") == "1":
                            next_key = output1.get("keyb")
                    elif asset_type == AssetType.OS_FUTOPT:
                        if isinstance(output1, dict):
                            next_key = output1.get("index_key")
                    
                    if not next_key or (new_bars and new_bars[0].datetime < req.start):
                        break
                else:
                    break

                time.sleep(0.2)
                
            except Exception as e:
                print(f"❌ [Datafeed] Exception: {e}")
                retry_count += 1
                if retry_count > MAX_RETRIES: break
                time.sleep(1.0)
                continue

        # 11. 최종 정리
        unique_bars = {b.datetime: b for b in all_bars}
        final_bars = sorted(unique_bars.values(), key=lambda x: x.datetime)
        result = [b for b in final_bars if req.start <= b.datetime <= req.end]
        
        print(f"✅ [Datafeed] 완료: 총 {len(result)}개 봉 데이터 수신.")
        return result

    def _adjust_business_day(self, dt: datetime) -> datetime:
        """
        [보정 로직 개선]
        1. 장 시작 전(09:00)이면 전일로 이동
        2. 주말이면 직전 금요일로 이동
        """
        # 1. 장 시작 전(09:00) 체크 -> 전일로 롤백
        if dt.hour < 9:
            dt = dt - timedelta(days=1)
            # 시간을 장 종료 시점 쯤으로 보정 (데이터 조회에는 날짜가 중요)
            dt = dt.replace(hour=15, minute=30)
            
        # 2. 주말 체크 -> 금요일로 롤백
        if dt.weekday() == 5: # 토요일
            dt = dt - timedelta(days=1)
            dt = dt.replace(hour=15, minute=30)
        elif dt.weekday() == 6: # 일요일
            dt = dt - timedelta(days=2)
            dt = dt.replace(hour=15, minute=30)
            
        return dt