from datetime import datetime, timedelta, date
from collections.abc import Callable
from functools import lru_cache

from icetcore import TCoreAPI, BarType

from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest, TickData
from vnpy.trader.utility import ZoneInfo, extract_vt_symbol
from vnpy.trader.datafeed import BaseDatafeed


#Time period mapping
INTERVAL_VT2MC: dict[Interval, tuple] = {
    Interval.MINUTE: (BarType.MINUTE, 1),
    Interval.HOUR: (BarType.MINUTE, 60),
    Interval.DAILY: (BarType.DK, 1)
}

#Time adjustment mapping
INTERVAL_ADJUSTMENT_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()
}

#Exchange mapping
EXCHANGE_MC2VT: dict[str, Exchange] = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE,
    "GFEX": Exchange.GFEX,
    "SSE": Exchange.SSE,
    "SZSE": Exchange.SZSE,
}

#Time zone constants
CHINA_TZ = ZoneInfo("Asia/Shanghai")


class McdataDatafeed(BaseDatafeed):
    """Data service interface of MultiCharts"""

    def __init__(self) -> None:
        """Constructor"""
        self.apppath: str = SETTINGS["datafeed.username"]       #The field name used for passing parameters
        if not self.apppath:
            self.apppath = "C:/MCTrader14/APPs"                 #Default program path

        self.inited: bool = False                               #Initialization state

        self.api: TCoreAPI = None                               #API examples

    def init(self, output: Callable = print) -> bool:
        """Initialization"""
        #Disable repeated initialization
        if self.inited:
            return True

        #Create API instance and connect
        self.api = TCoreAPI(apppath=self.apppath)
        self.api.connect()

        #Return to initialization state
        self.inited = True
        return True

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData]:
        """Query K line data"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        #Check contract code
        mc_symbol: str = to_mc_symbol(req.vt_symbol)
        if not mc_symbol:
            output(f"Failed to query K-line data: Unsupported contract code {req.vt_symbol}")
            return []

        #Check K-line cycle
        mc_interval, mc_window = INTERVAL_VT2MC.get(req.interval, ("", ""))
        if not mc_interval:
            output(f"Failed to query K-line data: Unsupported time period {req.interval.value}")
            return []

        #Check end time
        if not req.end:
            req.end = datetime.now(CHINA_TZ)

        #Get timestamp translation amplitude
        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[req.interval]

        #Initialize query data cache
        all_quote_history: list[dict] = []

        #Daily direct full query
        if req.interval == Interval.DAILY:
            quote_history: list[dict] | None = self.api.getquotehistory(
                mc_interval,
                mc_window,
                mc_symbol,
                req.start.strftime("%Y%m%d%H"),
                req.end.strftime("%Y%m%d%H")
            )

            if quote_history:
                all_quote_history.extend(quote_history)
        #Minute and hour K-lines are queried daily
        else:
            query_start: date = req.start.date()
            query_end: date = req.end.date()
            d: date = query_start

            while d <= query_end:
                #Skip weekend
                if d.weekday() not in {5, 6}:
                    #Initiate K-line query
                    quote_history = self.api.getquotehistory(
                        mc_interval,
                        mc_window,
                        mc_symbol,
                        d.strftime("%Y%m%d00"),
                        (d + timedelta(days=1)).strftime("%Y%m%d00")
                    )

                    #Save query results
                    if quote_history:
                        all_quote_history.extend(quote_history)

                d += timedelta(days=1)

            #Update query start time
            query_start = query_end

        #If it fails, return directly
        if not all_quote_history:
            output(f"Failed to obtain {req.symbol} contract {req.start}-{req.end} historical data")
            return []

        #Convert data format
        bars: dict[datetime, BarData] = {}

        for history in all_quote_history:
            #Adjust the timestamp to the beginning of the K line
            dt: datetime = (history["DateTime"] - adjustment).replace(tzinfo=CHINA_TZ)

            #Minutes and seconds removed from daily line
            if req.interval == Interval.DAILY:
                dt = dt.replace(hour=0, minute=0)

            #Create a K-line object and cache it
            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                interval=req.interval,
                datetime=dt,
                open_price=history["Open"],
                high_price=history["High"],
                low_price=history["Low"],
                close_price=history["Close"],
                volume=history["Volume"],
                open_interest=history["OpenInterest"],
                gateway_name="MCDATA"
            )

            bars[bar.datetime] = bar

        dts: list[datetime] = sorted(bars.keys())
        result: list[BarData] = [bars[dt] for dt in dts]

        return result

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> list[TickData]:
        """Query Tick data"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        #Check contract code
        mc_symbol: str = to_mc_symbol(req.vt_symbol)
        if not mc_symbol:
            output(f"Failed to query K-line data: Unsupported contract code {req.vt_symbol}")
            return []

        #Check end time
        if not req.end:
            req.end = datetime.now(CHINA_TZ)

        #Initialize query data cache
        all_quote_history: list[dict] = []

        query_start: date = req.start.date()
        query_end: date = req.end.date()
        d: date = query_start

        while d <= query_end:
            #Skip weekend
            if d.weekday() not in {5, 6}:
                #Initiate K-line query
                quote_history = self.api.getquotehistory(
                    BarType.TICK,
                    1,
                    mc_symbol,
                    d.strftime("%Y%m%d00"),
                    (d + timedelta(days=1)).strftime("%Y%m%d00")
                )

                #Save query results
                if quote_history:
                    all_quote_history.extend(quote_history)

            d += timedelta(days=1)

        #Update query start time
        query_start = query_end

        #If it fails, return directly
        if not all_quote_history:
            output(f"Failed to obtain {req.symbol} contract {req.start}-{req.end} historical data")
            return []

        #Convert data format
        ticks: dict[datetime, TickData] = {}

        for history in all_quote_history:
            dt: datetime = history["DateTime"].replace(tzinfo=CHINA_TZ)

            #Create Tick object and cache
            tick: TickData = TickData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=dt,
                name=req.symbol,
                last_price=history["Last"],
                last_volume=history["Quantity"],
                volume=history["Volume"],
                open_interest=history["OpenInterest"],
                bid_price_1=history["Bid"],
                ask_price_1=history["Ask"],
                gateway_name="MCDATA"
            )

            ticks[tick.datetime] = tick

        dts: list[datetime] = sorted(ticks.keys())
        result: list[TickData] = [ticks[dt] for dt in dts]

        return result


@lru_cache(maxsize=10000)
def to_mc_symbol(vt_symbol: str) -> str:
    """Convert to MC contract code"""
    symbol, exchange = extract_vt_symbol(vt_symbol)

    #Currently only futures exchange contracts are supported
    if exchange in {
        Exchange.CFFEX,
        Exchange.SHFE,
        Exchange.CZCE,
        Exchange.DCE,
        Exchange.INE,
        Exchange.GFEX,
    }:
        #Futures contract
        if len(symbol) <= 8:
            suffix: str = check_perpetual(symbol)

            #Continuous Contract
            if suffix:
                product: str = symbol.replace(suffix, "")
                return f"TC.F.{exchange.value}.{product}.{suffix}"
            #Trading contract
            else:
                #Get product code
                product = get_product(symbol)

                #Get the contract month
                month: str = symbol[-2:]

                #Get the contract year
                year: str = symbol.replace(product, "").replace(month, "")
                if len(year) == 1:      #Special treatment by Zhengzhou Commercial Exchange
                    if int(year) <= 6:
                        year = "2" + year
                    else:
                        year = "1" + year

                return f"TC.F.{exchange.value}.{product}.20{year}{month}"
        #Futures options contracts
        else:
            product = get_product(symbol)
            left: str = symbol.replace(product, "")

            #CICC, Dalian Commodity Exchange, Guangzhou Futures Exchange
            if "-" in left:
                if "-C-" in left:
                    option_type: str = "C"
                elif "-P-" in left:
                    option_type = "P"

                time_end: int = left.index("-") - 1
                strike_start: int = time_end + 4
            #SHFE, Energy Exchange, Zhengzhou Commodity Exchange
            else:
                if "C" in left:
                    option_type = "C"
                    time_end = left.index("C") - 1
                elif "P" in left:
                    option_type = "P"
                    time_end = left.index("P") - 1

                strike_start = time_end + 2

            #Get key information
            strike: str = left[strike_start:]
            time_str: str = left[:time_end + 1]

            if "MS" in symbol:
                time_str = time_str.replace("MS", "")
                product = product + "_MS"

            month = time_str[-2:]
            year = time_str.replace(month, "")

            #Special treatment by Zhengzhou Commercial Exchange
            if len(year) == 1:
                if int(year) <= 6:
                    year = "2" + year
                else:
                    year = "1" + year

            return f"TC.O.{exchange.value}.{product}.20{year}{month}.{option_type}.{strike}"

    return ""


def get_product(symbol: str) -> str:
    """Get futures product code"""
    buf: list[str] = []

    for w in symbol:
        if w.isdigit():
            break
        buf.append(w)

    return "".join(buf)


def check_perpetual(symbol: str) -> str:
    """Determine whether it is a continuous contract"""
    for suffix in [
        "HOT",      #The main force continues
        "HOT/Q",    #The main force regains power
        "HOT/H",    #The main force regains power after
        "000000"    #Exponentially continuous
    ]:
        if symbol.endswith(suffix):
            return suffix

    return ""
