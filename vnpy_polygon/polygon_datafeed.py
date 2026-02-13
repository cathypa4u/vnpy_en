from datetime import datetime
from typing import Any
from collections.abc import Iterator, Callable

from polygon import RESTClient
from polygon.rest.aggs import Agg

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.setting import SETTINGS
from vnpy.trader.database import DB_TZ


INTERVAL_VT2POLYGON = {
    Interval.MINUTE: "minute",
    Interval.HOUR: "hour",
    Interval.DAILY: "day",
}

POLYGON_API_KEY: str = "xenclI6MsDyhaq7KZ32CaFYVmzZF899M"

class PolygonDatafeed(BaseDatafeed):
    """Polygon.io Data service interface"""

    def __init__(self) -> None:
        """"""
        self.api_key: str = SETTINGS["datafeed.password"]
        if not self.api_key:
            self.api_key = POLYGON_API_KEY

        self.client: RESTClient
        self.inited: bool = False

    def init(self, output: Callable[[str], Any] = print) -> bool:
        """초기화"""
        if self.inited:
            return True

        if not self.api_key:
            output("Polygon.io 데이터서비스 초기화실패. API 키가 비어 있습니다!")
            return False

        try:
            self.client = RESTClient(self.api_key)

            self.client.get_exchanges(asset_class='options')
        except Exception as e:
            output(f"Polygon.io 데이터서비스 초기화실패: {e}")
            return False

        self.inited = True
        return True

    def query_bar_history(self, req: HistoryRequest, output: Callable[[str], Any] = print) -> list[BarData]:
        """查询K线数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval = req.interval
        start: datetime = req.start
        end: datetime = req.end

        polygon_interval: str | None = INTERVAL_VT2POLYGON.get(interval)
        if not polygon_interval:
            output(f"Polygon.io 캔들스틱 데이터 조회실패：지원되지 않는 기간{interval.value}")
            return []

        if len(symbol) > 10:
            symbol = "O:" + symbol  # Polygon모델은 옵션 코드 앞에 "O:"를 붙여야 합니다.

        # polygon클라이언트의 list_aggs 메서드는 페이지네이션 처리를 위한 이터레이터를 반환합니다
        aggs: Iterator[Agg] = self.client.list_aggs(
            ticker=symbol,
            multiplier=1,
            timespan=polygon_interval,
            from_=start,
            to=end,
            limit=5000      # 매번 5000개 조회
        )

        bars: list[BarData] = []
        for agg in aggs:
            # Polygon타임스탬프는 밀리초 단위이므로 datetime으로 변환하십시오.
            dt: datetime = datetime.fromtimestamp(agg.timestamp / 1000)

            # list_aggs요청된 범위를 벗어난 데이터가 반환될 수 있으므로 필터링이 필요합니다.
            if not (start <= dt <= end):
                continue

            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=exchange,
                datetime=dt.replace(tzinfo=DB_TZ),
                interval=interval,
                volume=agg.volume,
                open_price=agg.open,
                high_price=agg.high,
                low_price=agg.low,
                close_price=agg.close,
                turnover=agg.vwap * agg.volume,
                gateway_name="POLYGON"
            )
            bars.append(bar)

        return bars

    def query_tick_history(self, req: HistoryRequest, output: Callable[[str], Any] = print) -> list[TickData]:
        """查询Tick数据"""
        return []
