from vnpy.trader.object import OrderRequest, ContractData

from ..template import RuleTemplate


class OrderValidityRule(RuleTemplate):
    """Order Validity Check Rule"""

    name: str = "Order Validity Check"

    def check_allowed(self, req: OrderRequest, gateway_name: str) -> bool:
        """Check if order is allowed"""
        # Check if contract exists
        contract: ContractData | None = self.get_contract(req.vt_symbol)
        if not contract:
            self.write_log(f"Contract {req.vt_symbol} not found: {req}")
            return False

        # Check price tick
        if contract.pricetick > 0:
            pricetick: float = contract.pricetick

            # Calculate remainder of price divided by price tick
            remainder: float = req.price % pricetick

            # Check if price is multiple of price tick (allowing small error for float precision)
            if abs(remainder) > 1e-6 and abs(remainder - pricetick) > 1e-6:
                self.write_log(f"Price {req.price} is not a multiple of price tick {pricetick}: {req}")
                return False

        # Check max order volume
        if contract.max_volume and req.volume > contract.max_volume:
            self.write_log(f"Order volume {req.volume} exceeds max limit {contract.max_volume}: {req}")
            return False

        # Check min order volume
        if req.volume < contract.min_volume:
            self.write_log(f"Order volume {req.volume} below min limit {contract.min_volume}: {req}")
            return False

        return True