from datetime import datetime, timedelta
from typing import cast
from collections.abc import Callable

from numpy import ndarray
from pandas import DataFrame, Timestamp
from rqdatac import init
from rqdatac.services.get_price import get_price
from rqdatac.services.future import get_dominant_price
from rqdatac.services.basic import all_instruments
from rqdatac.services.calendar import get_next_trading_date
from rqdatac.share.errors import RQDataError

from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.utility import round_to, ZoneInfo
from vnpy.trader.datafeed import BaseDatafeed


INTERVAL_VT2RQ: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "60m",
    Interval.DAILY: "1d",
}

INTERVAL_ADJUSTMENT_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()         # no need to adjust for daily bar
}

FUTURES_EXCHANGES: set[Exchange] = {
    Exchange.CFFEX,
    Exchange.SHFE,
    Exchange.CZCE,
    Exchange.DCE,
    Exchange.INE,
    Exchange.GFEX
}

CHINA_TZ = ZoneInfo("Asia/Shanghai")


def to_rq_symbol(symbol: str, exchange: Exchange, all_symbols: ndarray) -> str:
    """Convert exchange code to MiKang code"""
    #Stock
    if exchange in {Exchange.SSE, Exchange.SZSE}:
        if exchange == Exchange.SSE:
            rq_symbol: str = f"{symbol}.XSHG"
        else:
            rq_symbol = f"{symbol}.XSHE"
    #Gold Exchange spot
    elif exchange == Exchange.SGE:
        for char in ["(", ")", "+"]:
            symbol = symbol.replace(char, "")
        symbol = symbol.upper()
        rq_symbol = f"{symbol}.SGEX"
    #Futures and options
    elif exchange in {
        Exchange.CFFEX,
        Exchange.SHFE,
        Exchange.DCE,
        Exchange.CZCE,
        Exchange.INE,
        Exchange.GFEX
    }:
        for count, word in enumerate(symbol):  # noqa: B007
            if word.isdigit():
                break

        product: str = symbol[:count]
        time_str: str = symbol[count:]

        #Futures
        if time_str.isdigit():
            #Only Zhengzhou Commercial Office needs special treatment
            if exchange is not Exchange.CZCE:
                return symbol.upper()

            #Check whether it is a continuous contract or an index contract
            if time_str in ["88", "888", "99", "889"]:
                return symbol

            #Extract year and month
            year: str = symbol[count]
            month: str = symbol[count + 1:]

            guess_1: str = f"{product}1{year}{month}".upper()
            guess_2: str = f"{product}2{year}{month}".upper()

            #Give priority to contracts after 20 years
            if guess_2 in all_symbols:
                rq_symbol = guess_2
            else:
                rq_symbol = guess_1
        #Options and futures sub-main continuous contracts
        else:
            if time_str == "88A2":
                return symbol.upper()

            if exchange in {
                Exchange.CFFEX,
                Exchange.DCE,
                Exchange.SHFE,
                Exchange.INE,
                Exchange.GFEX
            }:
                rq_symbol = symbol.replace("-", "").upper()
            elif exchange == Exchange.CZCE:
                year = symbol[count]
                suffix: str = symbol[count + 1:]

                guess_1 = f"{product}1{year}{suffix}".upper()
                guess_2 = f"{product}2{year}{suffix}".upper()

                #Give priority to contracts after 20 years
                if guess_2 in all_symbols:
                    rq_symbol = guess_2
                else:
                    rq_symbol = guess_1
    else:
        rq_symbol = f"{symbol}.{exchange.value}"

    return rq_symbol


class RqdataDatafeed(BaseDatafeed):
    """MiKang RQData data service interface"""

    def __init__(self) -> None:
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

        self.inited: bool = False

    def init(self, output: Callable = print) -> bool:
        """Initialization"""
        if self.inited:
            return True

        if not self.username:
            output("RQData data service initialization failed: user name is empty!")
            return False

        if not self.password:
            output("RQData data service initialization failed: password is empty!")
            return False

        try:
            init(
                self.username,
                self.password,
                ("rqdatad-pro.ricequant.com", 16011),
                use_pool=True,
                max_pool_size=1,
                auto_load_plugins=False
            )

            df: DataFrame = all_instruments()
            self.symbols: ndarray = df["order_book_id"].to_numpy()
        except RQDataError as ex:
            output(f"RQData data service initialization failed: {ex}")
            return False
        except RuntimeError as ex:
            output(f"A runtime error occurred: {ex}")
            return False
        except Exception as ex:
            output(f"An unknown exception occurred: {ex}")
            return False

        self.inited = True
        return True

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData] | None:
        """Query K line data"""
        #If there is a futures type and there is no number in the code (not a specific contract), then query the main force continuously
        if req.exchange in FUTURES_EXCHANGES and req.symbol.isalpha():
            return self._query_dominant_history(req, output)
        else:
            return self._query_bar_history(req, output)

    def _query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData] | None:
        """Query K line data"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval = req.interval
        start: datetime = req.start
        end: datetime = req.end

        #Stock options do not add exchange suffix
        if exchange in [Exchange.SSE, Exchange.SZSE] and symbol in self.symbols:
            rq_symbol: str = symbol
        else:
            rq_symbol = to_rq_symbol(symbol, exchange, self.symbols)

        #Check that the queried code is within the range
        if rq_symbol not in self.symbols:
            output(f"RQData failed to query K-line data: Unsupported contract code {req.vt_symbol}")
            return []

        rq_interval: str | None = INTERVAL_VT2RQ.get(interval, None)
        if not rq_interval:
            output(f"RQData failed to query K-line data: Unsupported time period {req.interval.value}")
            return []

        #In order to convert the Mi basket timestamp (the end time of the K line) to the VeighNa timestamp (the start time of the K line)
        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[interval]

        #Query position data only for derivatives contracts
        fields: list = ["open", "high", "low", "close", "volume", "total_turnover"]
        if not symbol.isdigit():
            fields.append("open_interest")

        #For stock query, K-line data before re-righting
        if rq_symbol.endswith(".XSHG") or rq_symbol.endswith(".XSHE"):
            adjust_type: str = "pre_volume"
        else:
            adjust_type = "none"

        df: DataFrame = get_price(
            rq_symbol,
            frequency=rq_interval,
            fields=fields,
            start_date=start,
            end_date=get_next_trading_date(end),        #In order to query the night trading data
            adjust_type=adjust_type
        )

        data: list[BarData] = []

        if df is not None:
            #Fill NaN with 0
            df.fillna(0, inplace=True)

            for row in df.itertuples():
                row_index: tuple[str, Timestamp] = cast(tuple[str, Timestamp], row.Index)
                dt: datetime = row_index[1].to_pydatetime() - adjustment
                dt = dt.replace(tzinfo=CHINA_TZ)

                if dt >= end:
                    break

                bar: BarData = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=round_to(row.open, 0.000001),
                    high_price=round_to(row.high, 0.000001),
                    low_price=round_to(row.low, 0.000001),
                    close_price=round_to(row.close, 0.000001),
                    volume=row.volume,
                    turnover=row.total_turnover,
                    open_interest=getattr(row, "open_interest", 0),
                    gateway_name="RQ"
                )

                data.append(bar)

        return data

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> list[TickData] | None:
        """Query Tick data"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        start: datetime = req.start
        end: datetime = req.end

        #Stock options do not add exchange suffix
        if exchange in [Exchange.SSE, Exchange.SZSE] and symbol in self.symbols:
            rq_symbol: str = symbol
        else:
            rq_symbol = to_rq_symbol(symbol, exchange, self.symbols)

        if rq_symbol not in self.symbols:
            output(f"RQData failed to query Tick data: Unsupported contract code {req.vt_symbol}")
            return []

        #Query position data only for derivatives contracts
        fields: list = [
            "open",
            "high",
            "low",
            "last",
            "prev_close",
            "volume",
            "total_turnover",
            "limit_up",
            "limit_down",
            "b1",
            "b2",
            "b3",
            "b4",
            "b5",
            "a1",
            "a2",
            "a3",
            "a4",
            "a5",
            "b1_v",
            "b2_v",
            "b3_v",
            "b4_v",
            "b5_v",
            "a1_v",
            "a2_v",
            "a3_v",
            "a4_v",
            "a5_v",
        ]
        if not symbol.isdigit():
            fields.append("open_interest")

        df: DataFrame = get_price(
            rq_symbol,
            frequency="tick",
            fields=fields,
            start_date=start,
            end_date=get_next_trading_date(end),        #In order to query the night trading data
            adjust_type="none"
        )

        data: list[TickData] = []

        if df is not None:
            #Fill NaN with 0
            df.fillna(0, inplace=True)

            for row in df.itertuples():
                row_index: tuple[str, Timestamp] = cast(tuple[str, Timestamp], row.Index)
                dt: datetime = row_index[1].to_pydatetime()
                dt = dt.replace(tzinfo=CHINA_TZ)

                if dt >= end:
                    break

                tick: TickData = TickData(
                    symbol=symbol,
                    exchange=exchange,
                    datetime=dt,
                    open_price=row.open,
                    high_price=row.high,
                    low_price=row.low,
                    pre_close=row.prev_close,
                    last_price=row.last,
                    volume=row.volume,
                    turnover=row.total_turnover,
                    open_interest=getattr(row, "open_interest", 0),
                    limit_up=row.limit_up,
                    limit_down=row.limit_down,
                    bid_price_1=row.b1,
                    bid_price_2=row.b2,
                    bid_price_3=row.b3,
                    bid_price_4=row.b4,
                    bid_price_5=row.b5,
                    ask_price_1=row.a1,
                    ask_price_2=row.a2,
                    ask_price_3=row.a3,
                    ask_price_4=row.a4,
                    ask_price_5=row.a5,
                    bid_volume_1=row.b1_v,
                    bid_volume_2=row.b2_v,
                    bid_volume_3=row.b3_v,
                    bid_volume_4=row.b4_v,
                    bid_volume_5=row.b5_v,
                    ask_volume_1=row.a1_v,
                    ask_volume_2=row.a2_v,
                    ask_volume_3=row.a3_v,
                    ask_volume_4=row.a4_v,
                    ask_volume_5=row.a5_v,
                    gateway_name="RQ"
                )

                data.append(tick)

        return data

    def _query_dominant_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData] | None:
        """Query futures main K-line data"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval = req.interval
        start: datetime = req.start
        end: datetime = req.end

        rq_interval: str | None = INTERVAL_VT2RQ.get(interval, None)
        if not rq_interval:
            output(f"RQData failed to query K-line data: Unsupported time period {req.interval.value}")
            return []

        #In order to convert the Mi basket timestamp (the end time of the K line) to the VeighNa timestamp (the start time of the K line)
        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[interval]

        #Query position data only for derivatives contracts
        fields: list = ["open", "high", "low", "close", "volume", "total_turnover"]
        if not symbol.isdigit():
            fields.append("open_interest")

        df: DataFrame = get_dominant_price(
            symbol.upper(),                         #Use uppercase letters for contract codes
            frequency=rq_interval,
            fields=fields,
            start_date=start,
            end_date=get_next_trading_date(end),    #In order to query the night trading data
            adjust_type="pre",                      #Former restoration of rights
            adjust_method="prev_close_ratio"        #Switch the previous day's closing price proportional restoration
        )

        data: list[BarData] = []

        if df is not None:
            #Fill NaN with 0
            df.fillna(0, inplace=True)

            for row in df.itertuples():
                row_index: tuple[str, Timestamp] = cast(tuple[str, Timestamp], row.Index)
                dt: datetime = row_index[1].to_pydatetime() - adjustment
                dt = dt.replace(tzinfo=CHINA_TZ)

                if dt >= end:
                    break

                bar: BarData = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=round_to(row.open, 0.000001),
                    high_price=round_to(row.high, 0.000001),
                    low_price=round_to(row.low, 0.000001),
                    close_price=round_to(row.close, 0.000001),
                    volume=row.volume,
                    turnover=row.total_turnover,
                    open_interest=getattr(row, "open_interest", 0),
                    gateway_name="RQ"
                )

                data.append(bar)

        return data
