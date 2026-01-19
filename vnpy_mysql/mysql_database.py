from datetime import datetime

from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    DoubleField,
    IntegerField,
    Model,
    MySQLDatabase as PeeweeMySQLDatabase,
    ModelSelect,
    ModelDelete,
    chunked,
    fn,
    Asc,
    Desc
)
from playhouse.shortcuts import ReconnectMixin

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS


class ReconnectMySQLDatabase(ReconnectMixin, PeeweeMySQLDatabase):
    """MySQL database class with reconnect mixin"""
    pass

db = ReconnectMySQLDatabase(
    database=SETTINGS["database.database"],
    user=SETTINGS["database.user"],
    password=SETTINGS["database.password"],
    host=SETTINGS["database.host"],
    port=SETTINGS["database.port"]
)


class DateTimeMillisecondField(DateTimeField):
    """Supports date timestamp fields in milliseconds"""

    def get_modifiers(self) -> list:
        """Millisecond support"""
        return [3]


class DbBarData(Model):
    """K-line data table mapping object"""

    id: AutoField = AutoField()

    symbol: CharField = CharField()
    exchange: CharField = CharField()
    datetime: DateTimeField = DateTimeField()
    interval: CharField = CharField()

    volume: DoubleField = DoubleField()
    turnover: DoubleField = DoubleField()
    open_interest: DoubleField = DoubleField()
    open_price: DoubleField = DoubleField()
    high_price: DoubleField = DoubleField()
    low_price: DoubleField = DoubleField()
    close_price: DoubleField = DoubleField()

    class Meta:
        database: PeeweeMySQLDatabase = db
        indexes: tuple = ((("symbol", "exchange", "interval", "datetime"), True),)


class DbTickData(Model):
    """TICK data table mapping object"""

    id: AutoField = AutoField()

    symbol: CharField = CharField()
    exchange: CharField = CharField()
    datetime: DateTimeField = DateTimeMillisecondField()

    name: CharField = CharField()
    volume: DoubleField = DoubleField()
    turnover: DoubleField = DoubleField()
    open_interest: DoubleField = DoubleField()
    last_price: DoubleField = DoubleField()
    last_volume: DoubleField = DoubleField()
    limit_up: DoubleField = DoubleField()
    limit_down: DoubleField = DoubleField()

    open_price: DoubleField = DoubleField()
    high_price: DoubleField = DoubleField()
    low_price: DoubleField = DoubleField()
    pre_close: DoubleField = DoubleField()

    bid_price_1: DoubleField = DoubleField()
    bid_price_2: DoubleField = DoubleField(null=True)
    bid_price_3: DoubleField = DoubleField(null=True)
    bid_price_4: DoubleField = DoubleField(null=True)
    bid_price_5: DoubleField = DoubleField(null=True)

    ask_price_1: DoubleField = DoubleField()
    ask_price_2: DoubleField = DoubleField(null=True)
    ask_price_3: DoubleField = DoubleField(null=True)
    ask_price_4: DoubleField = DoubleField(null=True)
    ask_price_5: DoubleField = DoubleField(null=True)

    bid_volume_1: DoubleField = DoubleField()
    bid_volume_2: DoubleField = DoubleField(null=True)
    bid_volume_3: DoubleField = DoubleField(null=True)
    bid_volume_4: DoubleField = DoubleField(null=True)
    bid_volume_5: DoubleField = DoubleField(null=True)

    ask_volume_1: DoubleField = DoubleField()
    ask_volume_2: DoubleField = DoubleField(null=True)
    ask_volume_3: DoubleField = DoubleField(null=True)
    ask_volume_4: DoubleField = DoubleField(null=True)
    ask_volume_5: DoubleField = DoubleField(null=True)

    localtime: DateTimeField = DateTimeMillisecondField(null=True)

    class Meta:
        database: PeeweeMySQLDatabase = db
        indexes: tuple = ((("symbol", "exchange", "datetime"), True),)


class DbBarOverview(Model):
    """K-line summary data table mapping object"""

    id: AutoField = AutoField()

    symbol: CharField = CharField()
    exchange: CharField = CharField()
    interval: CharField = CharField()
    count: IntegerField = IntegerField()
    start: DateTimeField = DateTimeField()
    end: DateTimeField = DateTimeField()

    class Meta:
        database: PeeweeMySQLDatabase = db
        indexes: tuple = ((("symbol", "exchange", "interval"), True),)


class DbTickOverview(Model):
    """Tick ​​summary data table mapping object"""

    id: AutoField = AutoField()

    symbol: CharField = CharField()
    exchange: CharField = CharField()
    count: IntegerField = IntegerField()
    start: DateTimeField = DateTimeField()
    end: DateTimeField = DateTimeField()

    class Meta:
        database: PeeweeMySQLDatabase = db
        indexes: tuple = ((("symbol", "exchange"), True),)


class MysqlDatabase(BaseDatabase):
    """Mysql database interface"""

    def __init__(self) -> None:
        """"""
        self.db: PeeweeMySQLDatabase = db
        self.db.connect()

        #If the data table does not exist, perform creation initialization
        if not DbBarData.table_exists():
            self.db.create_tables([DbBarData, DbTickData, DbBarOverview, DbTickOverview])

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """Save K-line data"""
        #Read primary key parameters
        bar: BarData = bars[0]
        symbol: str = bar.symbol
        exchange: Exchange = bar.exchange
        interval: Interval = bar.interval

        #Convert BarData data to dictionary and adjust time zone
        data: list = []

        for bar in bars:
            bar.datetime = convert_tz(bar.datetime)

            d: dict = bar.__dict__
            d["exchange"] = d["exchange"].value
            d["interval"] = d["interval"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            d.pop("extra", None)
            data.append(d)

        #Use the upsert operation to update data into the database
        with self.db.atomic():
            for c in chunked(data, 50):
                DbBarData.insert_many(c).on_conflict_replace().execute()

        #Update K-line summary data
        overview: DbBarOverview = DbBarOverview.get_or_none(
            DbBarOverview.symbol == symbol,
            DbBarOverview.exchange == exchange.value,
            DbBarOverview.interval == interval.value,
        )

        if not overview:
            overview = DbBarOverview()
            overview.symbol = symbol
            overview.exchange = exchange.value
            overview.interval = interval.value
            overview.start = bars[0].datetime
            overview.end = bars[-1].datetime
            overview.count = len(bars)
        elif stream:
            overview.end = bars[-1].datetime
            overview.count += len(bars)
        else:
            overview.start = min(bars[0].datetime, overview.start)
            overview.end = max(bars[-1].datetime, overview.end)

            s: ModelSelect = DbBarData.select().where(
                (DbBarData.symbol == symbol)
                & (DbBarData.exchange == exchange.value)
                & (DbBarData.interval == interval.value)
            )
            overview.count = s.count()

        overview.save()

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """Save TICK data"""
        #Read primary key parameters
        tick: TickData = ticks[0]
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange

        #Convert TickData data to dictionary and adjust time zone
        data: list = []

        for tick in ticks:
            tick.datetime = convert_tz(tick.datetime)

            d: dict = tick.__dict__
            d["exchange"] = d["exchange"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            d.pop("extra", None)
            data.append(d)

        #Use the upsert operation to update data into the database
        with self.db.atomic():
            for c in chunked(data, 50):
                DbTickData.insert_many(c).on_conflict_replace().execute()

        #Update Tick summary data
        overview: DbTickOverview = DbTickOverview.get_or_none(
            DbTickOverview.symbol == symbol,
            DbTickOverview.exchange == exchange.value,
        )

        if not overview:
            overview = DbTickOverview()
            overview.symbol = symbol
            overview.exchange = exchange.value
            overview.start = ticks[0].datetime
            overview.end = ticks[-1].datetime
            overview.count = len(ticks)
        elif stream:
            overview.end = ticks[-1].datetime
            overview.count += len(ticks)
        else:
            overview.start = min(ticks[0].datetime, overview.start)
            overview.end = max(ticks[-1].datetime, overview.end)

            s: ModelSelect = DbTickData.select().where(
                (DbTickData.symbol == symbol)
                & (DbTickData.exchange == exchange.value)
            )
            overview.count = s.count()

        overview.save()

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """"""
        s: ModelSelect = (
            DbBarData.select().where(
                (DbBarData.symbol == symbol)
                & (DbBarData.exchange == exchange.value)
                & (DbBarData.interval == interval.value)
                & (DbBarData.datetime >= start)
                & (DbBarData.datetime <= end)
            ).order_by(DbBarData.datetime)
        )

        bars: list[BarData] = []
        for db_bar in s:
            bar: BarData = BarData(
                symbol=db_bar.symbol,
                exchange=Exchange(db_bar.exchange),
                datetime=datetime.fromtimestamp(db_bar.datetime.timestamp(), DB_TZ),
                interval=Interval(db_bar.interval),
                volume=db_bar.volume,
                turnover=db_bar.turnover,
                open_interest=db_bar.open_interest,
                open_price=db_bar.open_price,
                high_price=db_bar.high_price,
                low_price=db_bar.low_price,
                close_price=db_bar.close_price,
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[TickData]:
        """Read TICK data"""
        s: ModelSelect = (
            DbTickData.select().where(
                (DbTickData.symbol == symbol)
                & (DbTickData.exchange == exchange.value)
                & (DbTickData.datetime >= start)
                & (DbTickData.datetime <= end)
            ).order_by(DbTickData.datetime)
        )

        ticks: list[TickData] = []
        for db_tick in s:
            tick: TickData = TickData(
                symbol=db_tick.symbol,
                exchange=Exchange(db_tick.exchange),
                datetime=datetime.fromtimestamp(db_tick.datetime.timestamp(), DB_TZ),
                name=db_tick.name,
                volume=db_tick.volume,
                turnover=db_tick.turnover,
                open_interest=db_tick.open_interest,
                last_price=db_tick.last_price,
                last_volume=db_tick.last_volume,
                limit_up=db_tick.limit_up,
                limit_down=db_tick.limit_down,
                open_price=db_tick.open_price,
                high_price=db_tick.high_price,
                low_price=db_tick.low_price,
                pre_close=db_tick.pre_close,
                bid_price_1=db_tick.bid_price_1,
                bid_price_2=db_tick.bid_price_2,
                bid_price_3=db_tick.bid_price_3,
                bid_price_4=db_tick.bid_price_4,
                bid_price_5=db_tick.bid_price_5,
                ask_price_1=db_tick.ask_price_1,
                ask_price_2=db_tick.ask_price_2,
                ask_price_3=db_tick.ask_price_3,
                ask_price_4=db_tick.ask_price_4,
                ask_price_5=db_tick.ask_price_5,
                bid_volume_1=db_tick.bid_volume_1,
                bid_volume_2=db_tick.bid_volume_2,
                bid_volume_3=db_tick.bid_volume_3,
                bid_volume_4=db_tick.bid_volume_4,
                bid_volume_5=db_tick.bid_volume_5,
                ask_volume_1=db_tick.ask_volume_1,
                ask_volume_2=db_tick.ask_volume_2,
                ask_volume_3=db_tick.ask_volume_3,
                ask_volume_4=db_tick.ask_volume_4,
                ask_volume_5=db_tick.ask_volume_5,
                localtime=db_tick.localtime,
                gateway_name="DB"
            )
            ticks.append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """Delete K line data"""
        d: ModelDelete = DbBarData.delete().where(
            (DbBarData.symbol == symbol)
            & (DbBarData.exchange == exchange.value)
            & (DbBarData.interval == interval.value)
        )
        count: int = d.execute()

        #Delete K-line summary data
        d2: ModelDelete = DbBarOverview.delete().where(
            (DbBarOverview.symbol == symbol)
            & (DbBarOverview.exchange == exchange.value)
            & (DbBarOverview.interval == interval.value)
        )
        d2.execute()
        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """Delete TICK data"""
        d: ModelDelete = DbTickData.delete().where(
            (DbTickData.symbol == symbol)
            & (DbTickData.exchange == exchange.value)
        )

        count: int = d.execute()

        #Delete Tick summary data
        d2: ModelDelete = DbTickOverview.delete().where(
            (DbTickOverview.symbol == symbol)
            & (DbTickOverview.exchange == exchange.value)
        )
        d2.execute()
        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """Query the K-line summary information in the database"""
        #If there is a K line but the summary information is missing, initialization is performed
        data_count: int = DbBarData.select().count()
        overview_count: int = DbBarOverview.select().count()
        if data_count and not overview_count:
            self.init_bar_overview()

        s: ModelSelect = DbBarOverview.select()
        overviews: list[BarOverview] = []
        for overview in s:
            overview.exchange = Exchange(overview.exchange)
            overview.interval = Interval(overview.interval)
            overviews.append(overview)
        return overviews

    def get_tick_overview(self) -> list[TickOverview]:
        """Query Tick summary information in the database"""
        s: ModelSelect = DbTickOverview.select()
        overviews: list = []
        for overview in s:
            overview.exchange = Exchange(overview.exchange)
            overviews.append(overview)
        return overviews

    def init_bar_overview(self) -> None:
        """Initialize K-line summary information in the database"""
        s: ModelSelect = (
            DbBarData.select(
                DbBarData.symbol,
                DbBarData.exchange,
                DbBarData.interval,
                fn.COUNT(DbBarData.id).alias("count")
            ).group_by(
                DbBarData.symbol,
                DbBarData.exchange,
                DbBarData.interval
            )
        )

        for data in s:
            overview: DbBarOverview = DbBarOverview()
            overview.symbol = data.symbol
            overview.exchange = data.exchange
            overview.interval = data.interval
            overview.count = data.count

            start_bar: DbBarData = (
                DbBarData.select()
                .where(
                    (DbBarData.symbol == data.symbol)
                    & (DbBarData.exchange == data.exchange)
                    & (DbBarData.interval == data.interval)
                )
                .order_by(Asc(DbBarData.datetime))
                .first()
            )
            overview.start = start_bar.datetime

            end_bar: DbBarData = (
                DbBarData.select()
                .where(
                    (DbBarData.symbol == data.symbol)
                    & (DbBarData.exchange == data.exchange)
                    & (DbBarData.interval == data.interval)
                )
                .order_by(Desc(DbBarData.datetime))
                .first()
            )
            overview.end = end_bar.datetime

            overview.save()
