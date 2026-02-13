import sys
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# VNPY 모듈 임포트
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import HistoryRequest

# [중요] 테스트 대상 모듈 임포트 (경로에 따라 수정 필요)
# vnpy_kis 패키지가 설치되어 있거나 현재 경로에 있어야 함
try:
    from vnpy_kis.kis_datafeed import KisDatafeed
    from vnpy_kis.kis_api_helper import KisApiHelper, AssetType
except ImportError:
    # 현재 폴더에 파일들이 있는 경우
    from kis_datafeed import KisDatafeed
    from kis_api_helper import KisApiHelper, AssetType

# --- [설정] 테스트할 계좌 정보 입력 ---
# vt_setting.json이 없다면 아래에 직접 입력하세요.
SETTINGS["context.kis.app_key"] = "PSMMvjarlJG2X9kKvxrKccOGQyK8VKndIONW"
SETTINGS["context.kis.app_secret"] = "Ptq2bAhVFLKSRgVeJ9XzNe7KygFaYPuJ+h8fWzQ+1vynlzRfl6ALd28Csg2JXbyMxOr9PbFBlk/C8neMnyXonk9Ws3QhXcM4Xb+Y0hTAKuyll65aYaqY9V/kp2Xi5q20lCG1Fbr+ODbSxQLV3qYgVs8wr0Ilux8Q0MZqbu8c+fXdOrO+2d4="
SETTINGS["context.kis.vts"] = False  # 실전: False, 모의: True

SETTINGS["context.kis.app_key"] = "PSVhcHCkIY5dej5oD3i7OybncL61GRzNTJOl"
SETTINGS["context.kis.app_secret"] = "Z0FmcjWVe5xOJ9jI6d4kGqnOAQVqhfcoSz9EZae3+pCEiY35NABWv8oXxzaPWm1yxqAKdytXklvtWSyJbiaiQiDD8+h3dX7B88JyyLe6+kbEJ5gQRfTwjXaNiDJLF9ddVH7S39PiwWrAzTSDbGK/tWu/DxOQHtImxsm7c0A68/piLM0koT4="
SETTINGS["context.kis.vts"] = True  # 실전: False, 모의: True

def run_test():
    print("=" * 60)
    print("🚀 [TEST] KIS Datafeed Multi-Asset Integration Test")
    print("=" * 60)

    # 1. Datafeed 초기화
    try:
        datafeed = KisDatafeed()
        print("✅ Datafeed Initialized.")
    except Exception as e:
        print(f"❌ Datafeed Init Failed: {e}")
        return

    # 2. 공통 조회 기간 설정 (최근 3일)
    # 주말일 경우 자동으로 금요일로 보정되는지 확인하기 위해 오늘 날짜 사용
    end_dt = datetime.now(ZoneInfo("Asia/Seoul"))
    start_dt = end_dt - timedelta(days=3)

    # 3. 자산별 테스트 케이스 정의
    # [주의] 선물/옵션 코드는 만기가 지나면 조회가 안되므로, 현재 유효한 종목코드로 수정 필요할 수 있음
    test_cases = [
        {
            "name": "국내 주식 (Samsung Elec)",
            "symbol": "005930",
            "exchange": Exchange.KRX,
            "interval": Interval.MINUTE
        },
        {
            "name": "해외 주식 (Tesla - NAS)",
            "symbol": "TSLA",
            "exchange": Exchange.NASDAQ,
            "interval": Interval.DAILY
        },
        {
            "name": "국내 선물 (KOSPI200 Futures)",
            "symbol": "101V3000", # 예시: 101 + 월물코드 (유효한 코드로 변경 필요)
            # 만약 코드를 모른다면 최근월물 지수선물 코드로 교체하세요. (예: 101V3000 등)
            "exchange": Exchange.KRX, 
            "interval": Interval.MINUTE
        },
        {
            "name": "해외 선물 (Nasdaq 100 E-mini)",
            "symbol": "NQH25", # 예: 2025년 3월물 (유효한 코드로 변경 필요)
            "exchange": Exchange.CME,
            "interval": Interval.MINUTE
        },
        {
            "name": "장내 채권 (KTB)",
            "symbol": "KR103502G983", # 국고채권
            "exchange": Exchange.KRX,
            "interval": Interval.DAILY # 채권은 일봉만 가능
        }
    ]

    # 4. 테스트 실행 루프
    for case in test_cases:
        print(f"\n🧪 Testing: {case['name']} ...")
        
        req = HistoryRequest(
            symbol=case["symbol"],
            exchange=case["exchange"],
            start=start_dt,
            end=end_dt,
            interval=case["interval"]
        )
        
        # Helper가 자산 타입을 제대로 인식하는지 선검증
        detected_type = KisApiHelper.get_asset_type(req.exchange, req.symbol)
        print(f"   ℹ️  Detected Asset Type: {detected_type}")
        
        if not detected_type:
            print("   ⚠️  Asset Type Detection Failed! Skipping...")
            continue

        try:
            bars = datafeed.query_bar_history(req)
            
            if bars:
                print(f"   ✅ Success! Retrieved {len(bars)} bars.")
                print(f"      First: {bars[0].datetime} | O:{bars[0].open_price},H:{bars[0].high_price},L:{bars[0].low_price},C:{bars[0].close_price},V:{bars[0].volume}")
                print(f"      Last : {bars[-1].datetime} | O:{bars[-1].open_price},H:{bars[-1].high_price},L:{bars[-1].low_price},C:{bars[-1].close_price},V:{bars[-1].volume}")
            else:
                print("   ⚠️  No Data Returned (Check Market Open/Holiday or Symbol Validity)")
                
        except Exception as e:
            print(f"   ❌ Error during query: {e}")
            import traceback
            traceback.print_exc()
        
        # API 호출 제한 방지 (1초 대기)
        time.sleep(1)

    print("\n" + "=" * 60)
    print("🎉 All Tests Completed.")
    print("=" * 60)

if __name__ == "__main__":
    run_test()