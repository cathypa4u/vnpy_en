# cython: language_level=3
from vnpy.trader.object import OrderRequest, OrderData

# 使用cimport导入Cython扩展类型
from vnpy_riskmanager.template cimport RuleTemplate


cdef class ActiveOrderRuleCy(RuleTemplate):
    """Active Order Count Check Risk Management Rule (Cython Version)"""

    # 实例属性声明
    cdef public int active_order_limit
    cdef public int active_order_count
    cdef dict active_orders

    cpdef void on_init(self):
        """Initialization"""
        # Default Parameters
        self.active_order_limit = 50

        # Active Orders
        self.active_orders = {}

        # Count Statistics
        self.active_order_count = 0

    cpdef bint check_allowed(self, object req, str gateway_name):
        """Check if order submission is allowed"""
        if self.active_order_count >= self.active_order_limit:
            msg = f"Active Order Count {self.active_order_count} reached limit {self.active_order_limit}：{req}"
            self.write_log(msg)
            return False

        return True

    cpdef void on_order(self, object order):
        """Order Push"""
        cdef str vt_orderid = order.vt_orderid
        
        if order.is_active():
            self.active_orders[vt_orderid] = order
        elif vt_orderid in self.active_orders:
            self.active_orders.pop(vt_orderid)

        self.active_order_count = len(self.active_orders)

        self.put_event()


# Python wrapper类，用于提供类属性（engine.py需要）
class ActiveOrderRule(ActiveOrderRuleCy):
    """Active Order Check Rule Python Wrapper Class"""
    
    name: str = "Active Order Check"
    
    parameters: dict[str, str] = {
        "active_order_limit": "Active Order Limit"
    }
    
    variables: dict[str, str] = {
        "active_order_count": "Active Order Count"
    }