import sys
import time
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# vn.py 코어 모듈 임포트
from vnpy.event import EventEngine, Event
from vnpy.trader.engine import MainEngine
from vnpy.trader.event import EVENT_TICK, EVENT_CONTRACT, EVENT_LOG
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import SubscribeRequest, HistoryRequest

# 작성하신 게이트웨이 임포트 (파일 경로에 따라 수정 필요)
# 예: vnpy_kis 폴더 안에 있다면 from vnpy_kis.kis_gateway import KisGateway
try:
    from vnpy_kis.kis_gateway import KisGateway
except ImportError:
    print("❌ kis_gateway.py를 찾을 수 없습니다. 파일 경로를 확인해주세요.")
    sys.exit(1)

# ----------------------------------------------------------------------
# 1. 설정 정보 (본인의 정보로 수정 필수)
# ----------------------------------------------------------------------
KIS_SETTING = {
    "usr_id": "swahn4u",           # HTS ID (모의투자는 불필요할 수 있으나 공란 가능)
    "app_key": "PSaYJYiqUO0CJfPD40nxeoehTa6ANiygCzWy",         # 발급받은 App Key
    "app_secret": "kWfOEtpRCpsGh06UGWkBNGF0gdjJ+jmAsMYPjWezsQxFpfsxPY1Nd8/Ys+p9iZBxHpJH6837LgqzYBq2UdeGNEso0UnpQC0Nl3MD2tR8xva7ELOqbfks7C++v3Xp0qtXY7R9mXe4Gvn8LUtQQUpGe9Q7KQUmdYgZoqjjTEZbVahTOwEBwSU=",   # 발급받은 App Secret
    "account_no": "50158896",          # 계좌번호 앞 8자리
    "account_code": "01",              # 계좌번호 뒤 2자리 (보통 01)
    "server": "DEMO"                   # 실전: "REAL", 모의: "DEMO"
}
KIS_SETTING = {
    "usr_id": "swahn4u",           # HTS ID (모의투자는 불필요할 수 있으나 공란 가능)
    "app_key": "PSMMvjarlJG2X9kKvxrKccOGQyK8VKndIONW",         # 발급받은 App Key
    "app_secret": "Ptq2bAhVFLKSRgVeJ9XzNe7KygFaYPuJ+h8fWzQ+1vynlzRfl6ALd28Csg2JXbyMxOr9PbFBlk/C8neMnyXonk9Ws3QhXcM4Xb+Y0hTAKuyll65aYaqY9V/kp2Xi5q20lCG1Fbr+ODbSxQLV3qYgVs8wr0Ilux8Q0MZqbu8c+fXdOrO+2d4=",   # 발급받은 App Secret
    "account_no": "43695919",          # 계좌번호 앞 8자리
    "account_code": "01",              # 계좌번호 뒤 2자리 (보통 01)
    "server": "REAL"                   # 실전: "REAL", 모의: "DEMO"
}

KIS_SETTING = {
    "User ID": "swahn4u",
    "사용계정": "종합계좌"
}
# ----------------------------------------------------------------------
# 2. 이벤트 핸들러 함수 정의
# ----------------------------------------------------------------------
def process_tick_event(event: Event):
    """실시간 체결(Tick) 데이터가 들어오면 출력"""
    tick = event.data
    print(f"\n[TICK] {tick.vt_symbol} | 시간: {tick.datetime.strftime('%H:%M:%S')}")
    print(f"   현재가: {tick.last_price} | 등락: {tick.last_price - tick.open_price if tick.open_price else 0}")
    print(f"   고가: {tick.high_price} | 저가: {tick.low_price}")
    print(f"   매수1호가: {tick.bid_price_1} ({tick.bid_volume_1})")
    print(f"   매도1호가: {tick.ask_price_1} ({tick.ask_volume_1})")

def process_log_event(event: Event):
    """게이트웨이 로그 출력"""
    log = event.data
    # 500 에러 로그는 너무 길어서 생략하거나 간단히 출력
    if "500" in log.msg:
        print(f"[LOG] 서버 응답 오류 (장 종료/점검 시간 가능성): {log.msg[:50]}...")
    else:
        print(f"[LOG] {log.msg}")
def process_contract_event(event: Event):
    """계약 정보(종목 마스터) 수신 확인"""
    contract = event.data
    #print(f"[CONTRACT] 종목 정보 수신: {contract.vt_symbol} ({contract.name}) [size:{contract.size}, pricetick:{contract.pricetick}]")

# ----------------------------------------------------------------------
# 3. 메인 실행 블록
# ----------------------------------------------------------------------
def main():
    # 1) 엔진 초기화
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    
    # 2) 이벤트 리스너 등록
    event_engine.register(EVENT_TICK, process_tick_event)
    event_engine.register(EVENT_LOG, process_log_event)
    event_engine.register(EVENT_CONTRACT, process_contract_event)
    
    # 3) 게이트웨이 추가
    gateway_name = "KIS"
    gateway: KisGateway = main_engine.add_gateway(KisGateway, gateway_name)
    gateway.set_main_engine(main_engine)

    print(">>> 게이트웨이 연결 시작...")
    
    # 4) 게이트웨이 연결 (로그인)
    # connect() 호출 시 내부적으로 Session 시작 -> Websocket 연결 -> 계좌 조회 등이 수행됨
    main_engine.connect(KIS_SETTING, gateway_name)

        
    # 연결 및 초기화 대기 (API 연결 속도 고려)
    time.sleep(5)
    
    if True:
        print("\n>>> 실시간 시세 구독 요청...")

        # 5) 시세 구독 요청 (삼성전자 예시)
        # 주의: 앞서 수정한 코드에 따라 SOR 종목은 Exchange.SOR 사용
        # 일반 KRX 종목: Exchange.KRX
        
        # Case A: 삼성전자 (KRX)
        req_krx = SubscribeRequest(
            symbol="005930",
            exchange=Exchange.KRX
        )
        main_engine.subscribe(req_krx, gateway_name)
        
        # Case B: 하이닉스 (만약 SOR 테스트가 필요하다면)
        req_sor = SubscribeRequest(
            symbol="000660",
            exchange=Exchange.SOR
        )
        main_engine.subscribe(req_sor, gateway_name)

        # Case C: NVDA (만약 OS 테스트가 필요하다면)
        req_os = SubscribeRequest(
            symbol="NVDA",
            exchange=Exchange.NASDAQ
        )
        main_engine.subscribe(req_os, gateway_name)

        print(">>> 데이터 수신 대기 중 (Ctrl+C로 종료)...")    
        
        # 6) 무한 루프로 프로그램 유지 (이벤트 엔진이 백그라운드에서 동작)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n>>> 프로그램 종료 요청")
            main_engine.close()
            sys.exit(0)
    else:
        # ---------------------------------------------------------
        # [검증] 장 종료 후에도 가능한 '과거 데이터(History)' 조회
        # ---------------------------------------------------------
        print("\n>>> [테스트] 삼성전자(005930) 최근 조회 요청...")
        
        req = HistoryRequest(
            symbol="005930",
            exchange=Exchange.KRX,
            start=datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(weeks=4), # 이틀 전부터
            end=datetime.now(ZoneInfo("Asia/Seoul")),
            interval=Interval.DAILY
        )
        history_data = main_engine.query_history(req, gateway_name)

        if history_data:
            print(f"\n✅ 히스토리 데이터 수신 성공! (총 {len(history_data)}개)")
            print("--- 최근 데이터 5개 ---")
            for bar in history_data[-200:]:
                print(f"Time:{str(bar.datetime):<25}, O:{bar.open_price:<10}, H:{bar.high_price:<10}, L:{bar.low_price:<10}, C:{bar.close_price:<10}, V:{bar.volume:<10}, TO:{bar.turnover}")
        else:
            print("\n❌ 히스토리 데이터 수신 실패 (설정이나 코드를 다시 확인해주세요)")    
            
        os_req = HistoryRequest(
            symbol="NVDA",
            exchange=Exchange.NASDAQ,
            start=datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(weeks=4), # 이틀 전부터
            end=datetime.now(ZoneInfo("Asia/Seoul")),
            interval=Interval.DAILY
        )        
        history_data = main_engine.query_history(os_req, gateway_name)
        
        if history_data:
            print(f"\n✅ 히스토리 데이터 수신 성공! (총 {len(history_data)}개)")
            print("--- 최근 데이터 5개 ---")
            for bar in history_data[-200:]:
                print(f"Time:{str(bar.datetime):<25}, O:{bar.open_price:<10}, H:{bar.high_price:<10}, L:{bar.low_price:<10}, C:{bar.close_price:<10}, V:{bar.volume:<10}, TO:{bar.turnover}")
        else:
            print("\n❌ 히스토리 데이터 수신 실패 (설정이나 코드를 다시 확인해주세요)")    
            
        print("\n>>> (참고) 현재 시간은 장 종료 시간이므로 실시간 TICK은 발생하지 않습니다.")
        print(">>> 프로그램 종료.")
        main_engine.close()
        sys.exit(0)

if __name__ == "__main__":
    main()