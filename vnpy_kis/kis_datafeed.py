# kis_datafeed.py
import requests
import traceback
import time
from datetime import datetime, timedelta
from typing import List, Optional
from pytz import timezone

from vnpy.trader.setting import SETTINGS
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.datafeed import BaseDatafeed

# Local Modules
# kis_auth, kis_parser, kis_api_helper 파일이 vnpy_kis 패키지 내에 있다고 가정
try:
    from .kis_auth import KisAuthManager
    from .kis_parser import parse_kis_bar_data
    from .kis_api_helper import KisApiHelper, AssetType
except ImportError:
    # 패키지 경로가 다를 경우를 대비한 Fallback (개발 환경에 따라 조정)
    from kis_auth import KisAuthManager
    from kis_parser import parse_kis_bar_data
    from kis_api_helper import KisApiHelper, AssetType

# 도메인 설정
REAL_DOMAIN = "https://openapi.koreainvestment.com:9443"
VIRTUAL_DOMAIN = "https://openapivts.koreainvestment.com:29443"

class KisDatafeed(BaseDatafeed):
    """
    KIS Datafeed: 통합 히스토리 데이터 조회 엔진
    - KisApiHelper를 통해 국내/해외/선물/채권 등 모든 자산의 과거 데이터를 조회합니다.
    - Gateway의 query_history 요청을 처리하는 전담 모듈입니다.
    """
    
    def __init__(self):
        self.app_key: str = SETTINGS.get("context.kis.app_key", "")
        self.app_secret: str = SETTINGS.get("context.kis.app_secret", "")
        self.vts: bool = SETTINGS.get("context.kis.vts", False)
        
        self.base_url = VIRTUAL_DOMAIN if self.vts else REAL_DOMAIN
        
        # 인증 관리자 (Singleton - Gateway와 토큰 공유)
        self.auth_manager = KisAuthManager()

    def query_bar_history(self, req: HistoryRequest, output_filepath: str = None) -> List[BarData]:
        """
        분봉/일봉 데이터 조회 (전체 기간 자동 Pagination)
        Gateway에서 이 함수를 호출하여 데이터를 받아갑니다.
        """
        
        # 1. 자산 타입 판별 (Helper 위임)
        asset_type = KisApiHelper.get_asset_type(req.exchange, req.symbol)
        if not asset_type:
            print(f"❌ [Datafeed] 지원하지 않는 자산/거래소입니다: {req.exchange} {req.symbol}")
            return []

        # 2. API 설정(History TR) 가져오기
        config = KisApiHelper.get_tr_config(asset_type, "HISTORY", self.vts)
        if not config:
            print(f"❌ [Datafeed] 해당 자산의 히스토리 조회 설정을 찾을 수 없습니다: {asset_type}")
            return []

        # 3. 토큰 발급 (AuthManager 위임)
        server_name = "VIRTUAL" if self.vts else "REAL"
        token = self.auth_manager.get_token(self.app_key, self.app_secret, server_name)
        if not token:
            print("❌ [Datafeed] 인증 토큰 발급에 실패했습니다.")
            return []

        # 4. 조회 종료일 보정 (주말이면 직전 금요일로 변경)
        # 채권 등 일봉 데이터의 경우 주말 날짜 요청이 와도 API가 처리할 수 있으나, 안전을 위해 보정
        query_end_dt = self._adjust_business_day(req.end)
        
        all_bars: List[BarData] = []
        next_key: str = ""
        
        print(f"🚀 [Datafeed] 조회 시작: {req.symbol} ({asset_type}) | 범위: {req.start} ~ {query_end_dt}")

        while True:
            # 5. Rate Limit (API 제한 속도 준수)
            self.auth_manager.check_rate_limit(self.app_key)

            # 6. 파라미터 생성 (Helper 이용 - 채권의 경우 start/end date 모두 처리됨)
            params = KisApiHelper.build_history_params(req, config, query_end_dt, next_key)
            
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": config["tr_id"],
                "custtype": "P"
            }

            try:
                # 7. 요청 전송
                url = f"{self.base_url}{config['url']}"
                resp = requests.get(url, headers=headers, params=params)
                
                if resp.status_code != 200:
                    print(f"❌ [Datafeed] HTTP Error: {resp.status_code} {resp.text}")
                    break
                    
                data = resp.json()
                
                # API 응답 코드 확인
                rt_cd = data.get("rt_cd", "")
                if rt_cd != "0":
                    msg = data.get("msg1", "")
                    print(f"❌ [Datafeed] API Error: {msg} (Code: {data.get('msg_cd')})")
                    break

                # 8. 데이터 파싱 (Common Parser 이용)
                # API마다 output 필드명(output2, output 등)이 다를 수 있음
                items = data.get("output2") or data.get("output") or []
                
                # 해외주식 일부 TR 예외 처리 (output1에 리스트가 있는 경우)
                if not items and "output1" in data and isinstance(data["output1"], list):
                     items = data["output1"]
                
                if not items:
                    # 데이터가 없으면 종료
                    break

                # kis_parser를 통해 자산별 상이한 필드명을 BarData로 통일
                new_bars = parse_kis_bar_data(items, req.symbol, req.exchange, req.interval, "KIS")
                if not new_bars:
                    break
                
                all_bars.extend(new_bars)
                
                # 9. Pagination (다음 페이지 조회 로직)
                pg_method = config.get("pg_method", "NONE")
                
                # [Case A] 시간 기준 Pagination (국내 주식/선물)
                if pg_method == "TIME":
                    # 수신된 데이터 중 가장 과거 데이터 시간 확인 (오름차순 정렬 가정)
                    earliest_bar = new_bars[0] 
                    
                    # 요청한 시작 시간보다 더 과거 데이터까지 받았다면 종료
                    if earliest_bar.datetime <= req.start:
                        break 
                    
                    # 기준 시간을 '가장 과거 데이터 - 1분'으로 설정하여 다음 루프 실행
                    query_end_dt = earliest_bar.datetime - timedelta(minutes=1)
                
                # [Case B] Key 기준 Pagination (해외 주식/선물)
                elif pg_method == "KEY":
                    # 헤더나 output1 영역에서 다음 Key 확인
                    output1 = data.get("output1", {})
                    
                    # Key 초기화 (이번 응답에 다음 키가 없으면 종료)
                    next_key = "" 
                    
                    if asset_type == AssetType.OS_STOCK:
                        if isinstance(output1, dict) and output1.get("next") == "1":
                            next_key = output1.get("keyb")
                    elif asset_type == AssetType.OS_FUTOPT:
                        if isinstance(output1, dict):
                            next_key = output1.get("index_key")
                    
                    # 다음 키가 없거나, 수집된 데이터가 요청 시작일보다 과거에 도달했으면 종료
                    if not next_key or (new_bars and new_bars[0].datetime < req.start):
                        break
                
                # [Case C] 채권 및 단건 조회 (NONE)
                else:
                    # 채권(KR_BOND)은 '기간별시세(일)' API를 사용하며 
                    # build_history_params에서 이미 start~end 날짜를 지정해서 요청하므로
                    # 한 번의 요청으로 완료됩니다. (Pagination 불필요)
                    break

                # 루프 간 짧은 대기 (Rate Limit 보조)
                time.sleep(0.05)

            except Exception as e:
                print(f"❌ [Datafeed] Exception: {e}")
                traceback.print_exc()
                break

        # 10. 최종 정리 (중복 제거, 정렬, 기간 필터링)
        # 딕셔너리를 이용해 중복된 시간의 데이터를 제거
        unique_bars = {b.datetime: b for b in all_bars}
        final_bars = sorted(unique_bars.values(), key=lambda x: x.datetime)
        
        # 사용자가 요청한 start ~ end 기간만 정확히 잘라서 반환
        result = [b for b in final_bars if req.start <= b.datetime <= req.end]
        
        print(f"✅ [Datafeed] 완료: 총 {len(result)}개 봉 데이터 수신.")
        return result

    def _adjust_business_day(self, dt: datetime) -> datetime:
        """주말이면 직전 금요일로 조정"""
        if dt.weekday() == 5: # 토요일
            return dt - timedelta(days=1)
        elif dt.weekday() == 6: # 일요일
            return dt - timedelta(days=2)
        return dt