# cython: language_level=3
from collections import defaultdict

from vnpy.trader.constant import Status

from vnpy_riskmanager.template cimport RuleTemplate


cdef class DailyLimitRuleCy(RuleTemplate):
    """Daily Limit Check Risk Management Rule (Cython Version)"""

    cdef public int total_order_limit
    cdef public int total_cancel_limit
    cdef public int total_trade_limit
    cdef public int contract_order_limit
    cdef public int contract_cancel_limit
    cdef public int contract_trade_limit

    cdef set all_orderids
    cdef set cancel_orderids
    cdef set all_tradeids

    cdef public int total_order_count
    cdef public int total_cancel_count
    cdef public int total_trade_count

    cdef public object contract_order_count
    cdef public object contract_cancel_count
    cdef public object contract_trade_count

    cpdef void on_init(self):
        """Initialization"""
        # Default Parameters
        self.total_order_limit = 20_000
        self.total_cancel_limit = 10_000
        self.total_trade_limit = 10_000
        self.contract_order_limit = 2_000
        self.contract_cancel_limit = 1_000
        self.contract_trade_limit = 1_000

        # Order ID Records
        self.all_orderids = set()
        self.cancel_orderids = set()

        # Trade ID Records
        self.all_tradeids = set()

        # Count Statistics
        self.total_order_count = 0
        self.total_cancel_count = 0
        self.total_trade_count = 0

        self.contract_order_count = defaultdict(int)
        self.contract_cancel_count = defaultdict(int)
        self.contract_trade_count = defaultdict(int)

    cpdef bint check_allowed(self, object req, str gateway_name):
        """Check if order submission is allowed"""
        cdef str vt_symbol = req.vt_symbol

        cdef int contract_order_count = self.contract_order_count[vt_symbol]
        if contract_order_count >= self.contract_order_limit:
            self.write_log(f"Contract Order Count {contract_order_count} reached limit {self.contract_order_limit}：{req}")
            return False

        cdef int contract_cancel_count = self.contract_cancel_count[vt_symbol]
        if contract_cancel_count >= self.contract_cancel_limit:
            self.write_log(f"Contract Cancel Count {contract_cancel_count} reached limit {self.contract_cancel_limit}：{req}")
            return False

        cdef int contract_trade_count = self.contract_trade_count[vt_symbol]
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

    cpdef void on_order(self, object order):
        """Order Push"""
        cdef str vt_orderid = order.vt_orderid
        cdef str vt_symbol = order.vt_symbol

        if vt_orderid not in self.all_orderids:
            self.all_orderids.add(vt_orderid)
            self.total_order_count += 1
            self.contract_order_count[vt_symbol] += 1
            self.put_event()
        elif (
            order.status == Status.CANCELLED
            and vt_orderid not in self.cancel_orderids
        ):
            self.cancel_orderids.add(vt_orderid)
            self.total_cancel_count += 1
            self.contract_cancel_count[vt_symbol] += 1
            self.put_event()

    cpdef void on_trade(self, object trade):
        """Trade Push"""
        cdef str vt_tradeid = trade.vt_tradeid

        if vt_tradeid in self.all_tradeids:
            return

        self.all_tradeids.add(vt_tradeid)
        self.total_trade_count += 1
        self.contract_trade_count[trade.vt_symbol] += 1
        self.put_event()


class DailyLimitRule(DailyLimitRuleCy):
    """Daily Limit Check Risk Management Rule (Python Wrapper Class)"""

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