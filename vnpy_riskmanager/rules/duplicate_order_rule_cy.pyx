# cython: language_level=3
from collections import defaultdict

from vnpy_riskmanager.template cimport RuleTemplate


cdef class DuplicateOrderRuleCy(RuleTemplate):
    """Duplicate Order Submission Check Risk Management Rule (Cython Version)"""

    cdef public int duplicate_order_limit
    cdef public object duplicate_order_count

    cpdef void on_init(self):
        """Initialization"""
        # Default Parameters
        self.duplicate_order_limit = 10

        # Duplicate Submission Statistics
        self.duplicate_order_count = defaultdict(int)

    cpdef bint check_allowed(self, object req, str gateway_name):
        """Check if order submission is allowed"""
        cdef str req_str = self.format_req(req)
        self.duplicate_order_count[req_str] += 1
        self.put_event()

        cdef int duplicate_order_count = self.duplicate_order_count[req_str]
        if duplicate_order_count >= self.duplicate_order_limit:
            self.write_log(f"Duplicate Order Count {duplicate_order_count} reached limit {self.duplicate_order_limit}：{req}")
            return False

        return True

    cpdef str format_req(self, object req):
        """Convert Order Request to String"""
        return f"{req.vt_symbol}|{req.type.value}|{req.direction.value}|{req.offset.value}|{req.volume}@{req.price}"


class DuplicateOrderRule(DuplicateOrderRuleCy):
    """Duplicate Order Check Rule Python Wrapper Class"""

    name: str = "Duplicate Order Check"

    parameters: dict[str, str] = {
        "duplicate_order_limit": "Duplicate Order Limit",
    }

    variables: dict[str, str] = {
        "duplicate_order_count": "Duplicate Order Count"
    }