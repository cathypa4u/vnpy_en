# cython: language_level=3
from vnpy_riskmanager.template cimport RuleTemplate


cdef class OrderValidityRuleCy(RuleTemplate):
    """Order Validity Check Risk Management Rule (Cython Version)"""

    cpdef bint check_allowed(self, object req, str gateway_name):
        """Check if order submission is allowed"""
        cdef double pricetick
        cdef double remainder

        # Check if contract exists
        cdef object contract = self.get_contract(req.vt_symbol)
        if not contract:
            self.write_log(f"Contract Code {req.vt_symbol} does not exist：{req}")
            return False

        # Check minimum price change (pricetick)
        if contract.pricetick > 0:
            pricetick = contract.pricetick

            # Calculate remainder of price divided by min tick size
            remainder = req.price % pricetick

            # Check remainder, ensuring the price is an integer multiple of pricetick (allow minute error for float precision)
            if abs(remainder) > 1e-6 and abs(remainder - pricetick) > 1e-6:
                self.write_log(f"Price {req.price} is not an integer multiple of contract minimum tick size {pricetick}：{req}")
                return False

        # Check maximum order volume
        if contract.max_volume and req.volume > contract.max_volume:
            self.write_log(f"Order Volume {req.volume} is greater than contract max volume limit {contract.max_volume}：{req}")
            return False

        # Check minimum order volume
        if req.volume < contract.min_volume:
            self.write_log(f"Order Volume {req.volume} is less than contract min volume limit {contract.min_volume}：{req}")
            return False

        return True


class OrderValidityRule(OrderValidityRuleCy):
    """Order Validity Check Rule Python Wrapper Class"""

    name: str = "Order Validity Check"