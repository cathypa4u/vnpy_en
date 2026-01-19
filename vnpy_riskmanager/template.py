from typing import TYPE_CHECKING, Any

from vnpy.trader.object import OrderRequest, TickData, OrderData, TradeData, ContractData

if TYPE_CHECKING:
    from .engine import RiskEngine


class RuleTemplate:
    """Risk Rule Template"""

    # Risk Rule Name
    name: str = ""

    # Parameter fields and names
    parameters: dict[str, str] = {}

    # Variable fields and names
    variables: dict[str, str] = {}

    def __init__(self, risk_engine: "RiskEngine", setting: dict) -> None:
        """Constructor"""
        # Bind Risk Engine object
        self.risk_engine: RiskEngine = risk_engine

        # Add rule activation status parameter
        self.active: bool = True

        parameters: dict[str, str] = {
            "active": "Activate Rule"
        }
        parameters.update(self.parameters)
        self.parameters = parameters

        # Initialize rule
        self.on_init()

        # Update rule parameters
        self.update_setting(setting)

    def write_log(self, msg: str) -> None:
        """Output Risk Log"""
        self.risk_engine.write_log(msg)

    def update_setting(self, rule_setting: dict) -> None:
        """Update Risk Rule parameters"""
        for name in self.parameters.keys():
            if name in rule_setting:
                value = rule_setting[name]
                setattr(self, name, value)

    def check_allowed(self, req: OrderRequest, gateway_name: str) -> bool:
        """Check if order submission is allowed"""
        return True

    def on_init(self) -> None:
        """Initialization"""
        pass

    def on_tick(self, tick: TickData) -> None:
        """Tick Push"""
        pass

    def on_order(self, order: OrderData) -> None:
        """Order Push"""
        pass

    def on_trade(self, trade: TradeData) -> None:
        """Trade Push"""
        pass

    def on_timer(self) -> None:
        """Timer Push (triggered every second)"""
        pass

    def get_contract(self, vt_symbol: str) -> ContractData | None:
        """Query contract information"""
        return self.risk_engine.get_contract(vt_symbol)

    def put_event(self) -> None:
        """Push Data Update Event"""
        self.risk_engine.put_rule_event(self)

    def get_data(self) -> dict[str, Any]:
        """Get data"""
        parameters: dict[str, Any] = {}
        for name in self.parameters.keys():
            value: Any = getattr(self, name)
            parameters[name] = value

        variables: dict[str, Any] = {}
        for name in self.variables.keys():
            value = getattr(self, name)
            variables[name] = value

        data: dict[str, Any] = {
            "name": self.name,
            "class_name": self.__class__.__name__,
            "parameters": parameters,
            "variables": variables
        }
        return data