from datetime import datetime, timedelta
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import HistoryRequest
from vnpy_kis.kis_datafeed import KisDatafeed
# 필요시 timezone 설정
from vnpy_kis.kis_parser import KIS_TZ

def test_overseas_minute():
    datafeed = KisDatafeed()
    if not datafeed.init(app_key="PSMMvjarlJG2X9kKvxrKccOGQyK8VKndIONW",sec_key="Ptq2bAhVFLKSRgVeJ9XzNe7KygFaYPuJ+h8fWzQ+1vynlzRfl6ALd28Csg2JXbyMxOr9PbFBlk/C8neMnyXonk9Ws3QhXcM4Xb+Y0hTAKuyll65aYaqY9V/kp2Xi5q20lCG1Fbr+ODbSxQLV3qYgVs8wr0Ilux8Q0MZqbu8c+fXdOrO+2d4="):
        print("데이터피드 초기화 실패")
        return

    # [중요] 해외주식 분봉은 최근 1개월(약 30일) 데이터만 제공됩니다.
    # 테스트를 위해 '어제'부터 '오늘'까지의 데이터를 요청합니다.
    end_dt = datetime.now(KIS_TZ)
    start_dt = end_dt - timedelta(days=2) # 2일 전 데이터 요청

    print(f"Requesting Minute Data: {start_dt} ~ {end_dt}")

    # 1. 1분봉 요청
    req = HistoryRequest(
        symbol="TSLA",          # 테슬라
        exchange=Exchange.NASDAQ,
        start=start_dt,
        end=end_dt,
        interval=Interval.MINUTE
    )

    bars = datafeed.query_bar_history(req)
    print(f"1분봉 수신 개수: {len(bars)}")
    
    if bars:
        print(f"First Bar: {bars[0].datetime} Price: {bars[0].close_price}")
        print(f"Last Bar : {bars[-1].datetime} Price: {bars[-1].close_price}")
    else:
        print("데이터 수신 실패 (장 운영시간이나 휴장일 확인 필요)")

    # 2. 1시간봉 (1분봉 합성) 요청
    req_hour = HistoryRequest(
        symbol="TSLA",
        exchange=Exchange.NASDAQ,
        start=start_dt,
        end=end_dt,
        interval=Interval.HOUR
    )
    
    hour_bars = datafeed.query_bar_history(req_hour)
    print(f"1시간봉 수신 개수: {len(hour_bars)}")

if __name__ == "__main__":
    test_overseas_minute()