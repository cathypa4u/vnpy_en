from vnpy.trader.object import OrderRequest, ContractData

from ..template import RuleTemplate


class OrderSizeRule(RuleTemplate):
    """Order Size Check Risk Management Rule"""

    name: str = "Order Size Check"

    parameters: dict[str, str] = {
        "order_volume_limit": "Order Volume Limit",
        "order_value_limit": "Order Value Limit",
    }

    def on_init(self) -> None:
        """Initialization"""
        self.order_volume_limit: int = 500
        self.order_value_limit: float = 1_000_000

    def check_allowed(self, req: OrderRequest, gateway_name: str) -> bool:
        """Check if order submission is allowed"""
        if req.volume > self.order_volume_limit:
            self.write_log(f"Order Volume {req.volume} exceeded limit {self.order_volume_limit}：{req}")
            return False

        contract: ContractData | None = self.get_contract(req.vt_symbol)
        if contract and req.price:      # Only consider limit orders
            order_value: float = req.volume * req.price * contract.size
            if order_value > self.order_value_limit:
                self.write_log(f"Order Value {order_value} exceeded limit {self.order_value_limit}：{req}")
                return False

        return True