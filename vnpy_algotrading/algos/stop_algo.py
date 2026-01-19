from vnpy.trader.constant import Direction
from vnpy.trader.object import OrderData, TickData, TradeData
from vnpy.trader.engine import BaseEngine

from ..template import AlgoTemplate


class StopAlgo(AlgoTemplate):
    """Conditional Order Algorithm Class"""

    display_name: str = "Stop Conditional Order"

    default_setting: dict = {
        "price_add": 0.0
    }

    variables: list = [
        "vt_orderid",
        "order_status",
    ]

    def __init__(
        self,
        algo_engine: BaseEngine,
        algo_name: str,
        vt_symbol: str,
        direction: str,
        offset: str,
        price: float,
        volume: float,
        setting: dict
    ) -> None:
        """Constructor"""
        super().__init__(algo_engine, algo_name, vt_symbol, direction, offset, price, volume, setting)

        # Parameters
        self.price_add: float = setting["price_add"]

        # Variables
        self.vt_orderid: str = ""
        self.order_status: str = ""

        self.put_event()

    def on_tick(self, tick: TickData) -> None:
        """Tick Market Data Callback"""
        if self.vt_orderid:
            return

        if self.direction == Direction.LONG:
            if tick.last_price >= self.price:
                price: float = self.price + self.price_add

                if tick.limit_up:
                    price = min(price, tick.limit_up)

                self.vt_orderid = self.buy(
                    price,
                    self.volume,
                    offset=self.offset
                )
                self.write_log(
                    f"Stop Order triggered, Code: {self.vt_symbol}, Direction: {self.direction}, Price: {self.price}, Volume: {self.volume}, Offset: {self.offset}")

        else:
            if tick.last_price <= self.price:
                price = self.price - self.price_add

                if tick.limit_down:
                    price = max(price, tick.limit_down)

                self.vt_orderid = self.sell(
                    price,
                    self.volume,
                    offset=self.offset
                )
                self.write_log(
                    f"Stop Order triggered, Code: {self.vt_symbol}, Direction: {self.direction}, Price: {self.price}, Volume: {self.volume}, Offset: {self.offset}")

        self.put_event()

    def on_order(self, order: OrderData) -> None:
        """Order Callback"""
        self.order_status = order.status
        self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        """Trade Callback"""
        if self.traded == self.volume:
            self.finish()
        self.put_event()