# cython: language_level=3
from vnpy_riskmanager.template cimport RuleTemplate


cdef class OrderSizeRuleCy(RuleTemplate):
    """Order Size Check Risk Management Rule (Cython Version)"""

    cdef public int order_volume_limit
    cdef public float order_value_limit

    cpdef void on_init(self):
        """Initialization"""
        self.order_volume_limit = 500
        self.order_value_limit = 1_000_000

    cpdef bint check_allowed(self, object req, str gateway_name):
        """Check if order submission is allowed"""
        cdef object contract
        cdef float order_value

        if req.volume > self.order_volume_limit:
            self.write_log(f"Order Volume {req.volume} exceeded limit {self.order_volume_limit}：{req}")
            return False

        contract = self.get_contract(req.vt_symbol)
        if contract and req.price:      # Only consider limit orders
            order_value = req.volume * req.price * contract.size
            if order_value > self.order_value_limit:
                self.write_log(f"Order Value {order_value} exceeded limit {self.order_value_limit}：{req}")
                return False

        return True


class OrderSizeRule(OrderSizeRuleCy):
    """Order Size Check Rule Python Wrapper Class"""

    name: str = "Order Size Check"

    parameters: dict[str, str] = {
        "order_volume_limit": "Order Volume Limit",
        "order_value_limit": "Order Value Limit",
    }