from collections import defaultdict

from vnpy.trader.object import OrderRequest

from ..template import RuleTemplate


class DuplicateOrderRule(RuleTemplate):
    """Duplicate Order Submission Check Risk Management Rule"""

    name: str = "Duplicate Order Check"

    parameters: dict[str, str] = {
        "duplicate_order_limit": "Duplicate Order Limit",
    }

    variables: dict[str, str] = {
        "duplicate_order_count": "Duplicate Order Count"
    }

    def on_init(self) -> None:
        """Initialization"""
        # Default Parameters
        self.duplicate_order_limit: int = 10

        # Duplicate Submission Statistics
        self.duplicate_order_count: dict[str, int] = defaultdict(int)

    def check_allowed(self, req: OrderRequest, gateway_name: str) -> bool:
        """Check if order submission is allowed"""
        req_str: str = self.format_req(req)
        self.duplicate_order_count[req_str] += 1
        self.put_event()

        duplicate_order_count: int = self.duplicate_order_count[req_str]
        if duplicate_order_count >= self.duplicate_order_limit:
            self.write_log(f"Duplicate Order Count {duplicate_order_count} reached limit {self.duplicate_order_limit}：{req}")
            return False

        return True

    def format_req(self, req: OrderRequest) -> str:
        """Convert Order Request to String"""
        return f"{req.vt_symbol}|{req.type.value}|{req.direction.value}|{req.offset.value}|{req.volume}@{req.price}"