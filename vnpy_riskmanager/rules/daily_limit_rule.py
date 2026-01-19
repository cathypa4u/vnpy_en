from collections import defaultdict

from vnpy.trader.object import OrderRequest, OrderData, TradeData
from vnpy.trader.constant import Status

from ..template import RuleTemplate


class DailyLimitRule(RuleTemplate):
    """Daily Limit Check Risk Management Rule"""

    name: str = "Daily Limit Check"

    parameters: dict[str, str] = {
        "total_order_limit": "Total Order Limit",
        "total_cancel_limit": "Total Cancel Limit",
        "total_trade_limit": "Total Trade Limit",
        "contract_order_limit": "Contract Order Limit",
        "contract_cancel_limit": "Contract Cancel Limit",
        "contract_trade_limit": "Contract Trade Limit"
    }

    variables: dict[str, str] = {
        "total_order_count": "Total Order Count",
        "total_cancel_count": "Total Cancel Count",
        "total_trade_count": "Total Trade Count",
        "contract_order_count": "Contract Order Count",
        "contract_cancel_count": "Contract Cancel Count",
        "contract_trade_count": "Contract Trade Count"
    }

    def on_init(self) -> None:
        """Initialization"""
        # Default Parameters
        self.total_order_limit: int = 20_000
        self.total_cancel_limit: int = 10_000
        self.total_trade_limit: int = 10_000
        self.contract_order_limit: int = 2_000
        self.contract_cancel_limit: int = 1_000
        self.contract_trade_limit: int = 1_000

        # Order ID Records
        self.all_orderids: set[str] = set()
        self.cancel_orderids: set[str] = set()

        # Trade ID Records
        self.all_tradeids: set[str] = set()

        # Count Statistics
        self.total_order_count: int = 0
        self.total_cancel_count: int = 0
        self.total_trade_count: int = 0

        self.contract_order_count: dict[str, int] = defaultdict(int)
        self.contract_cancel_count: dict[str, int] = defaultdict(int)
        self.contract_trade_count: dict[str, int] = defaultdict(int)

    def check_allowed(self, req: OrderRequest, gateway_name: str) -> bool:
        """Check if order submission is allowed"""
        contract_order_count: int = self.contract_order_count[req.vt_symbol]
        if contract_order_count >= self.contract_order_limit:
            self.write_log(f"Contract Order Count {contract_order_count} reached limit {self.contract_order_limit}：{req}")
            return False

        contract_cancel_count: int = self.contract_cancel_count[req.vt_symbol]
        if contract_cancel_count >= self.contract_cancel_limit:
            self.write_log(f"Contract Cancel Count {contract_cancel_count} reached limit {self.contract_cancel_limit}：{req}")
            return False

        contract_trade_count: int = self.contract_trade_count[req.vt_symbol]
        if contract_trade_count >= self.contract_trade_limit:
            self.write_log(f"Contract Trade Count {contract_trade_count} reached limit {self.contract_trade_limit}：{req}")
            return False

        if self.total_order_count >= self.total_order_limit:
            self.write_log(f"Total Order Count {self.total_order_count} reached limit {self.total_order_limit}：{req}")
            return False

        if self.total_cancel_count >= self.total_cancel_limit:
            self.write_log(f"Total Cancel Count {self.total_cancel_count} reached limit {self.total_cancel_limit}：{req}")
            return False

        if self.total_trade_count >= self.total_trade_limit:
            self.write_log(f"Total Trade Count {self.total_trade_count} reached limit {self.total_trade_limit}：{req}")
            return False

        return True

    def on_order(self, order: OrderData) -> None:
        """Order Push"""
        if order.vt_orderid not in self.all_orderids:
            self.all_orderids.add(order.vt_orderid)
            self.total_order_count += 1

            self.contract_order_count[order.vt_symbol] += 1

            self.put_event()
        elif (
            order.status == Status.CANCELLED
            and order.vt_orderid not in self.cancel_orderids
        ):
            self.cancel_orderids.add(order.vt_orderid)
            self.total_cancel_count += 1

            self.contract_cancel_count[order.vt_symbol] += 1

            self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        """Trade Push"""
        if trade.vt_tradeid in self.all_tradeids:
            return

        self.all_tradeids.add(trade.vt_tradeid)
        self.total_trade_count += 1

        self.contract_trade_count[trade.vt_symbol] += 1

        self.put_event()