import sys
import os
import time
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# vn.py Core
from vnpy.event import EventEngine, Event
from vnpy.trader.engine import MainEngine
from vnpy.trader.event import EVENT_TICK, EVENT_CONTRACT, EVENT_LOG, EVENT_ACCOUNT, EVENT_POSITION
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import SubscribeRequest, HistoryRequest
from vnpy.trader.utility import get_folder_path

# Local Imports (현재 폴더에 모듈이 있다고 가정)
try:
    from kis_gateway import KisGateway
    from kis_datafeed import KisDatafeed
    from kis_api_helper import KisApiHelper, AssetType
except ImportError:
    print("❌ 모듈 임포트 실패: kis_gateway.py 등의 파일이 현재 경로에 있는지 확인해주세요.")
    sys.exit(1)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --------------------------------------------------------------------------------
# 유틸리티: 계좌 설정 로드
# --------------------------------------------------------------------------------
def get_first_available_account():
    """kis_accounts.json에서 첫 번째 계좌 별칭을 가져옵니다."""
    kis_dir = get_folder_path("kis")
    account_file = os.path.join(kis_dir, "kis_accounts.json")
    
    if os.path.exists(account_file):
        try:
            with open(account_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data:
                    alias = list(data.keys())[0]
                    print(f"ℹ️  [설정] 감지된 계좌 별칭: {alias}")
                    return alias
        except Exception as e:
            print(f"⚠️ 설정 파일 읽기 오류: {e}")
    
    print("⚠️ kis_accounts.json 파일을 찾을 수 없거나 비어있습니다.")
    return None

# --------------------------------------------------------------------------------
# 1. Datafeed 독립 테스트
# --------------------------------------------------------------------------------
def test_datafeed_standalone():
    print("\n" + "=" * 60)
    print("🧪 [TEST 1] Datafeed 독립 실행 테스트 (과거 데이터 조회)")
    print("=" * 60)

    # Datafeed 초기화 (kis_key.json 또는 Gateway 인증 정보 활용)
    datafeed = KisDatafeed()
    
    # 만약 kis_key.json이 없다면 Gateway용 kis_accounts.json 내용을 참조하여
    # 수동으로 키를 주입해야 할 수도 있습니다. 여기선 자동 로드(init)에 맡깁니다.
    if not datafeed.init():
        print("❌ Datafeed 초기화 실패 (kis_key.json 확인 필요)")
        print("   참고: Gateway 테스트 단계에서 Gateway 인증 정보를 주입받아 조회하는 테스트가 수행됩니다.")
        return

    # 테스트 케이스: 국내 주식 (삼성전자) & 해외 주식 (테슬라)
    end_dt = datetime.now(ZoneInfo("Asia/Seoul"))
    start_dt = end_dt - timedelta(days=5)

    reqs = [
        HistoryRequest(
            symbol="005930", exchange=Exchange.KRX, 
            start=start_dt, end=end_dt, interval=Interval.MINUTE
        ),
        HistoryRequest(
            symbol="TSLA", exchange=Exchange.NASDAQ, 
            start=start_dt, end=end_dt, interval=Interval.DAILY
        )
    ]

    for req in reqs:
        print(f"\nQUERY: {req.symbol} ({req.exchange.value}) / {req.interval.value}")
        bars = datafeed.query_bar_history(req)
        if bars:
            print(f"✅ 수신 성공: {len(bars)}개 바 데이터")
            print(f"   첫 데이터: {bars[0].datetime} O:{bars[0].open_price} C:{bars[0].close_price}")
            print(f"   마지막 데이터: {bars[-1].datetime} O:{bars[-1].open_price} C:{bars[-1].close_price}")
        else:
            print("⚠️ 데이터 수신 실패 또는 데이터 없음")

# --------------------------------------------------------------------------------
# 2. Gateway 통합 테스트
# --------------------------------------------------------------------------------
def process_log_event(event: Event):
    log = event.data
    # 500 에러 등 긴 로그는 축약
    msg = log.msg if len(log.msg) < 100 else log.msg[:100] + "..."
    print(f"[GATEWAY LOG] {msg}")

def process_tick_event(event: Event):
    tick = event.data
    print(f"⚡ [TICK] {tick.vt_symbol} | {tick.datetime.strftime('%H:%M:%S')} | 현재가: {tick.last_price}")

def process_account_event(event: Event):
    acc = event.data
    print(f"💰 [ACCOUNT] {acc.accountid} | 잔고: {acc.balance:,.0f} | 동결: {acc.frozen}")

def test_gateway_integration():
    print("\n" + "=" * 60)
    print("🧪 [TEST 2] Gateway 통합 연결 및 구독 테스트")
    print("=" * 60)

    account_alias = get_first_available_account()
    if not account_alias:
        print("❌ 테스트 중단: 사용할 계좌 정보가 없습니다.")
        return

    # 엔진 초기화
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    
    # 이벤트 핸들러 등록
    event_engine.register(EVENT_LOG, process_log_event)
    event_engine.register(EVENT_TICK, process_tick_event)
    event_engine.register(EVENT_ACCOUNT, process_account_event)
    # EVENT_CONTRACT는 데이터가 너무 많으므로 생략하거나 개수만 체크 권장

    # 게이트웨이 추가
    gateway_name = "KIS"
    main_engine.add_gateway(KisGateway, gateway_name)

    # 연결 설정
    setting = {
        "사용계정": account_alias,
        "User ID": "swahn4u" # 필요 시 HTS ID 입력
    }

    print(f">>> Gateway Connect 요청: {account_alias}")
    main_engine.connect(setting, gateway_name)

    # 초기화 대기 (마스터 데이터 로드 등)
    print(">>> 초기화 대기 중 (10초)...")
    time.sleep(10)

    # 잔고 확인 (연결 시 자동 조회됨)
    print("\n>>> [상태 점검] 잔고 및 마스터 데이터 수신 확인 완료 대기")
    
    # 구독 테스트
    print("\n>>> [구독] 실시간 시세 구독 요청")
    
    # 1. 국내 주식 (삼성전자)
    req_kr = SubscribeRequest(symbol="005930", exchange=Exchange.KRX)
    main_engine.subscribe(req_kr, gateway_name)
    
    # 2. 해외 주식 (엔비디아) - 장 중이 아니면 시세가 안 올 수 있음
    req_os = SubscribeRequest(symbol="NVDA", exchange=Exchange.NASDAQ)
    main_engine.subscribe(req_os, gateway_name)

    print(">>> 30초간 데이터 수신 대기...")
    try:
        count = 0
        while count < 30:
            time.sleep(1)
            count += 1
            if count % 10 == 0:
                print(f"... {count}초 경과")
    except KeyboardInterrupt:
        print("사용자 중단")

    # Gateway를 통한 History Query 테스트 (Gateway의 인증정보 주입 확인)
    print("\n>>> [TEST 3] Gateway를 통한 History Query (인증 공유 확인)")
    req_hist = HistoryRequest(
        symbol="005930", exchange=Exchange.KRX,
        start=datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(days=7),
        end=datetime.now(ZoneInfo("Asia/Seoul")),
        interval=Interval.DAILY
    )
    bars = main_engine.query_history(req_hist, gateway_name)
    if bars:
        print(f"✅ Gateway History Query 성공: {len(bars)}개 데이터 수신")
    else:
        print("⚠️ Gateway History Query 데이터 없음")

    print("\n>>> 테스트 종료, 엔진 정지.")
    main_engine.close()

if __name__ == "__main__":
    # 1. Datafeed 단독 테스트
    # (참고: kis_key.json이 설정되어 있어야 성공합니다)
    test_datafeed_standalone()
    
    # 2. Gateway 통합 테스트
    # (참고: kis_accounts.json이 설정되어 있어야 성공합니다)
    test_gateway_integration()