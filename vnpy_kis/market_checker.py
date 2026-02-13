import datetime
import pytz
import holidays
from enum import Enum

class MarketState(Enum):
    OFF = "off"   # 휴장 (주말, 공휴일, 운영시간 외)
    PRE = "pre"   # 장전 (프리마켓, 동시호가)
    ON = "on"     # 정규장 (본마켓)
    POST = "post" # 장후 (시간외 단일가, 애프터마켓)

class MarketScheduler:
    def __init__(self):
        # 휴일 데이터 로드 (한국, 미국 NYSE)
        self.kr_holidays = holidays.KR()
        
        # [수정] holidays.US(market="NYSE") 대신 holidays.NYSE() 사용
        # holidays 라이브러리 버전에 따라 financial 하위 모듈에 있을 수 있으나,
        # 최신 버전에서는 holidays.NYSE()로 바로 접근 가능합니다.
        try:
            self.us_holidays = holidays.NYSE() 
        except AttributeError:
            # 만약 아주 구버전이거나 구조가 다른 경우 financial 모듈 시도
            try:
                self.us_holidays = holidays.financial.NYSE()
            except:
                # 최악의 경우 US 공휴일로 대체하되 경고 (정확도 낮음)
                print("Warning: NYSE holiday calendar not found. Using US Federal holidays.")
                self.us_holidays = holidays.US()

        # 타임존 정의
        self.tz_kr = pytz.timezone('Asia/Seoul')
        self.tz_us = pytz.timezone('America/New_York') # 서머타임 자동 적용

    def get_market_state(self, exchange: str, dt: datetime.datetime = None) -> str:
        """
        거래소와 시간을 입력받아 시장 상태를 반환합니다.
        :param exchange: KRX, NXT, SOR, NASDAQ, NYSE, CME
        :param dt: 확인할 시간 (datetime 객체, None일 경우 현재시간)
        :return: 'pre', 'on', 'post', 'off'
        """
        exchange = exchange.upper()
        
        # 1. 시간 설정 (입력 없으면 현재 시간 UTC -> 각국 로컬 변환용)
        if dt is None:
            dt = datetime.datetime.now(datetime.timezone.utc)
        elif dt.tzinfo is None:
            # 타임존 정보가 없으면 로컬 시스템 시간(KST 가정)으로 간주하고 처리
            dt = self.tz_kr.localize(dt)

        # 2. 거래소별 로직 분기
        if exchange in ["KRX", "KOSPI", "KOSDAQ"]:
            return self._check_krx(dt)
        elif exchange == "NXT":
            return self._check_nxt(dt)
        elif exchange == "SOR":
            return self._check_sor(dt)
        elif exchange in ["NASDAQ", "NYSE", "AMEX"]:
            return self._check_us_equity(dt)
        elif exchange in ["CME", "FUTURES"]:
            return self._check_cme(dt)
        else:
            return "unknown"

    def _check_krx(self, utc_dt):
        """KRX (한국거래소) - 09:00~15:30"""
        local_dt = utc_dt.astimezone(self.tz_kr)
        
        # 주말 및 공휴일 체크
        if local_dt.weekday() >= 5 or local_dt.date() in self.kr_holidays:
            return MarketState.OFF.value

        t = local_dt.time()
        
        # 시간대별 상태 정의
        if datetime.time(8, 30) <= t < datetime.time(9, 0):
            return MarketState.PRE.value  # 장전 동시호가/시간외
        elif datetime.time(9, 0) <= t < datetime.time(15, 30):
            return MarketState.ON.value   # 정규장
        elif datetime.time(15, 30) <= t < datetime.time(18, 0):
            return MarketState.POST.value # 장후 시간외/단일가
        else:
            return MarketState.OFF.value

    def _check_nxt(self, utc_dt):
        """NXT (넥스트레이드 - 대체거래소) - 08:00~20:00 (예정)"""
        local_dt = utc_dt.astimezone(self.tz_kr)

        if local_dt.weekday() >= 5 or local_dt.date() in self.kr_holidays:
            return MarketState.OFF.value

        t = local_dt.time()

        # NXT 운영시간 (KRX 정규장 전후로 확장)
        if datetime.time(8, 0) <= t < datetime.time(9, 0):
            return MarketState.PRE.value  # 프리마켓 (NXT 기준)
        elif datetime.time(9, 0) <= t < datetime.time(15, 30):
            return MarketState.ON.value   # KRX와 겹치는 정규 시간
        elif datetime.time(15, 30) <= t < datetime.time(20, 0):
            return MarketState.POST.value # 애프터마켓 (NXT 기준)
        else:
            return MarketState.OFF.value

    def _check_sor(self, utc_dt):
        """SOR (Smart Order Routing) - KRX와 NXT 중 하나라도 열려있으면 ON"""
        # SOR은 라우팅 시스템이므로 가장 넓은 범위를 커버합니다.
        krx_state = self._check_krx(utc_dt)
        nxt_state = self._check_nxt(utc_dt)

        if krx_state == MarketState.ON.value or nxt_state == MarketState.ON.value:
            return MarketState.ON.value
        elif krx_state != MarketState.OFF.value or nxt_state != MarketState.OFF.value:
            # 둘 중 하나라도 Pre/Post 상태라면
            if nxt_state == MarketState.PRE.value: return MarketState.PRE.value
            if nxt_state == MarketState.POST.value: return MarketState.POST.value
            return krx_state # 기본 KRX 상태
        else:
            return MarketState.OFF.value

    def _check_us_equity(self, utc_dt):
        """NASDAQ/NYSE (미국 주식)"""
        local_dt = utc_dt.astimezone(self.tz_us)

        # 미국 주말 및 NYSE 휴일 체크
        if local_dt.weekday() >= 5 or local_dt.date() in self.us_holidays:
            return MarketState.OFF.value

        t = local_dt.time()

        # 미국 주식 시간 (현지 시간 기준)
        # Pre: 04:00 ~ 09:30
        # Main: 09:30 ~ 16:00
        # Post: 16:00 ~ 20:00
        if datetime.time(4, 0) <= t < datetime.time(9, 30):
            return MarketState.PRE.value
        elif datetime.time(9, 30) <= t < datetime.time(16, 0):
            return MarketState.ON.value
        elif datetime.time(16, 0) <= t < datetime.time(20, 0):
            return MarketState.POST.value
        else:
            return MarketState.OFF.value

    def _check_cme(self, utc_dt):
        """CME (미국 선물 - Globex)"""
        local_dt = utc_dt.astimezone(self.tz_us)
        
        # CME 휴장 규칙: 금요일 17:00 ET 종료 ~ 일요일 18:00 ET 개장
        weekday = local_dt.weekday() # 0:Mon, ..., 4:Fri, 5:Sat, 6:Sun
        t = local_dt.time()

        # 주말 휴장 체크
        if weekday == 4 and t >= datetime.time(17, 0): # 금요일 17:00 이후 OFF
            return MarketState.OFF.value
        if weekday == 5: # 토요일 전체 OFF
            return MarketState.OFF.value
        if weekday == 6 and t < datetime.time(18, 0): # 일요일 18:00 이전 OFF
            return MarketState.OFF.value
        
        # CME 공휴일은 보통 단축운영이므로 여기선 단순화하여 평일 로직 적용
        # Daily Break: 17:00 ~ 18:00 ET (유지보수 시간)
        if datetime.time(17, 0) <= t < datetime.time(18, 0):
            return MarketState.PRE.value # 혹은 Maintenance
        
        # 그 외 시간은 거의 ON (Pre-market 개념이 주식과 다름, 그냥 ON으로 봄)
        return MarketState.ON.value

# ==============================================================================
# 사용 편의를 위한 래퍼 함수
# ==============================================================================
_scheduler = MarketScheduler()

def market_state(exchange: str, dt: datetime.datetime = None) -> str:
    return _scheduler.get_market_state(exchange, dt)

if __name__ == "__main__":
    # 테스트용 시간 생성 (KST 기준)
    now_kst = datetime.datetime.now()
    
    # 1. 현재 시간 기준 조회
    print(f"현재 시간(KST): {now_kst}")
    print(f"KRX 상태: {market_state('KRX')}")
    print(f"NASDAQ 상태: {market_state('NASDAQ')}")
    
    print("-" * 30)

    # 2. 특정 시간 시뮬레이션
    # 예: 한국 화요일 오전 8:40 (KRX 장전, NXT 거래중, 미국 야간)
    test_dt = datetime.datetime(2024, 5, 21, 8, 40) # 화요일
    print(f"테스트 시간: {test_dt}")
    print(f"KRX (08:40): {market_state('KRX', test_dt)}") # PRE
    print(f"NXT (08:40): {market_state('NXT', test_dt)}") # PRE (넥스트레이드 기준으로는 거래 가능)
    
    # 예: 한국 화요일 밤 11:00 (미국 서머타임 시 오전 10:00 -> 본장)
    test_dt_night = datetime.datetime(2024, 5, 21, 23, 00)
    print(f"테스트 시간: {test_dt_night}")
    print(f"NASDAQ (23:00 KST): {market_state('NASDAQ', test_dt_night)}") # ON
    
    # 예: CME 선물 (일요일 오전 -> 휴장)
    test_sunday = datetime.datetime(2024, 5, 19, 10, 00) 
    print(f"테스트 시간: {test_sunday}")
    print(f"CME (일요일): {market_state('CME', test_sunday)}") # OFF
    
    test_dt = datetime.datetime.now() # 화요일
    print(f"테스트 시간: {test_dt}")
    print(f"KRX (08:40): {market_state('KRX', test_dt)}") # PRE
    print(f"NXT (08:40): {market_state('NXT', test_dt)}") # PRE (넥스트레이드 기준으로는 거래 가능)    
    print(f"SOR (08:40): {market_state('SOR', test_dt)}") # PRE (넥스트레이드 기준으로는 거래 가능)