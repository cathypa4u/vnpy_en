from vnpy.trader.object import OrderRequest, OrderData

from ..template import RuleTemplate


class ActiveOrderRule(RuleTemplate):
    """Active Order Count Check Risk Management Rule"""

    name: str = "Active Order Check"

    parameters: dict[str, str] = {
        "active_order_limit": "Active Order Limit"
    }

    variables: dict[str, str] = {
        "active_order_count": "Active Order Count"
    }

    def on_init(self) -> None:
        """Initialization"""
        # Default Parameters
        self.active_order_limit: int = 50

        # Active Orders
        self.active_orders: dict[str, OrderData] = {}

        # Count Statistics
        self.active_order_count: int = 0

    def check_allowed(self, req: OrderRequest, gateway_name: str) -> bool:
        """Check if order submission is allowed"""
        if self.active_order_count >= self.active_order_limit:
            self.write_log(f"Active Order Count {self.active_order_count} reached limit {self.active_order_limit}：{req}")
            return False

        return True

    def on_order(self, order: OrderData) -> None:
        """Order Push"""
        if order.is_active():
            self.active_orders[order.vt_orderid] = order
        elif order.vt_orderid in self.active_orders:
            self.active_orders.pop(order.vt_orderid)

        self.active_order_count = len(self.active_orders)

        self.put_event()