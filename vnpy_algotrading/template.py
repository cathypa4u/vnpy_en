from typing import TYPE_CHECKING

from vnpy.trader.engine import BaseEngine
from vnpy.trader.object import TickData, OrderData, TradeData, ContractData
from vnpy.trader.constant import OrderType, Offset, Direction

from .base import AlgoStatus

if TYPE_CHECKING:
    from .engine import AlgoEngine


class AlgoTemplate:
    """Algorithm Template"""

    _count: int = 0                 # Instance count

    display_name: str = ""          # Display name
    default_setting: dict = {}      # Default parameters
    variables: list = []            # Variable names

    def __init__(
        self,
        algo_engine: "AlgoEngine",
        algo_name: str,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        setting: dict
    ) -> None:
        """Constructor"""
        self.algo_engine: BaseEngine = algo_engine
        self.algo_name: str = algo_name

        self.vt_symbol: str = vt_symbol
        self.direction: Direction = direction
        self.offset: Offset = offset
        self.price: float = price
        self.volume: float = volume

        self.status: AlgoStatus = AlgoStatus.PAUSED
        self.traded: float = 0
        self.traded_price: float = 0

        self.active_orders: dict[str, OrderData] = {}  # vt_orderid:order

    def update_tick(self, tick: TickData) -> None:
        """Tick data update"""
        if self.status == AlgoStatus.RUNNING:
            self.on_tick(tick)

    def update_order(self, order: OrderData) -> None:
        """Order data update"""
        if order.is_active():
            self.active_orders[order.vt_orderid] = order
        elif order.vt_orderid in self.active_orders:
            self.active_orders.pop(order.vt_orderid)

        self.on_order(order)

    def update_trade(self, trade: TradeData) -> None:
        """Trade data update"""
        cost: float = self.traded_price * self.traded + trade.price * trade.volume
        self.traded += trade.volume
        self.traded_price = cost / self.traded

        self.on_trade(trade)

    def update_timer(self) -> None:
        """Timer update per second"""
        if self.status == AlgoStatus.RUNNING:
            self.on_timer()

    def on_tick(self, tick: TickData) -> None:
        """Tick callback"""
        return

    def on_order(self, order: OrderData) -> None:
        """Order callback"""
        return

    def on_trade(self, trade: TradeData) -> None:
        """Trade callback"""
        return

    def on_timer(self) -> None:
        """Timer callback"""
        return

    def start(self) -> None:
        """Start"""
        self.status = AlgoStatus.RUNNING
        self.put_event()

        self.write_log("Algorithm Started")

    def stop(self) -> None:
        """Stop"""
        self.status = AlgoStatus.STOPPED
        self.cancel_all()
        self.put_event()

        self.write_log("Algorithm Stopped")

    def finish(self) -> None:
        """Finish"""
        self.status = AlgoStatus.FINISHED
        self.cancel_all()
        self.put_event()

        self.write_log("Algorithm Finished")

    def pause(self) -> None:
        """Pause"""
        self.status = AlgoStatus.PAUSED
        self.put_event()

        self.write_log("Algorithm Paused")

    def resume(self) -> None:
        """Resume"""
        self.status = AlgoStatus.RUNNING
        self.put_event()

        self.write_log("Algorithm Resumed")

    def buy(
        self,
        price: float,
        volume: float,
        order_type: OrderType = OrderType.LIMIT,
        offset: Offset = Offset.NONE
    ) -> str:
        """Buy"""
        if self.status != AlgoStatus.RUNNING:
            return ""

        msg: str = f"{self.vt_symbol}, Order Buy {order_type.value}, {volume}@{price}"
        self.write_log(msg)

        vt_orderid: str = self.algo_engine.send_order(
            self,
            Direction.LONG,
            price,
            volume,
            order_type,
            offset
        )

        return vt_orderid

    def sell(
        self,
        price: float,
        volume: float,
        order_type: OrderType = OrderType.LIMIT,
        offset: Offset = Offset.NONE
    ) -> str:
        """Sell"""
        if self.status != AlgoStatus.RUNNING:
            return ""

        msg: str = f"{self.vt_symbol} Order Sell {order_type.value}, {volume}@{price}"
        self.write_log(msg)

        vt_orderid: str = self.algo_engine.send_order(
            self,
            Direction.SHORT,
            price,
            volume,
            order_type,
            offset
        )

        return vt_orderid

    def cancel_order(self, vt_orderid: str) -> None:
        """Cancel Order"""
        self.algo_engine.cancel_order(self, vt_orderid)

    def cancel_all(self) -> None:
        """Cancel All Orders"""
        if not self.active_orders:
            return

        for vt_orderid in self.active_orders.keys():
            self.cancel_order(vt_orderid)

    def get_tick(self) -> TickData | None:
        """Query Tick"""
        return self.algo_engine.get_tick(self)

    def get_contract(self) -> ContractData | None:
        """Query Contract"""
        return self.algo_engine.get_contract(self)

    def get_parameters(self) -> dict:
        """Get Algorithm Parameters"""
        strategy_parameters: dict = {}
        for name in self.default_setting.keys():
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self) -> dict:
        """Get Algorithm Variables"""
        strategy_variables: dict = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self) -> dict:
        """Get Algorithm Information"""
        algo_data: dict = {
            "algo_name": self.algo_name,
            "vt_symbol": self.vt_symbol,
            "direction": self.direction,
            "offset": self.offset,
            "price": self.price,
            "volume": self.volume,
            "status": self.status,
            "traded": self.traded,
            "left": self.volume - self.traded,
            "traded_price": self.traded_price,
            "parameters": self.get_parameters(),
            "variables": self.get_variables()
        }
        return algo_data

    def write_log(self, msg: str) -> None:
        """Output Log"""
        self.algo_engine.write_log(msg, self)

    def put_event(self) -> None:
        """Push Update"""
        data: dict = self.get_data()
        self.algo_engine.put_algo_event(self, data)