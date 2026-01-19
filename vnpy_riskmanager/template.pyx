# cython: language_level=3
from typing import TYPE_CHECKING, Any

from vnpy.trader.object import (
    OrderRequest,
    CancelRequest,
    TickData,
    OrderData,
    TradeData,
    ContractData
)

if TYPE_CHECKING:
    from .engine import RiskEngine


cdef class RuleTemplate:
    """Risk Rule Template (Cython Version)"""

    def __init__(self, risk_engine: "RiskEngine", setting: dict) -> None:
        """Constructor"""
        # Bind Risk Engine object
        self.risk_engine = risk_engine

        # Initialize basic attributes
        self.name = ""
        self.parameters = {}
        self.variables = {}
        
        # Add rule activation status parameter
        self.active = True

        # Attempt to retrieve metadata from class attributes (for Python-style subclasses)
        if hasattr(self.__class__, 'name') and isinstance(self.__class__.name, str):
            self.name = self.__class__.name

        if hasattr(self.__class__, 'parameters') and isinstance(self.__class__.parameters, dict):
            self.parameters.update(self.__class__.parameters)

        if hasattr(self.__class__, 'variables') and isinstance(self.__class__.variables, dict):
            self.variables.update(self.__class__.variables)

        # Initialize rule (subclasses set metadata and initial values here)
        self.on_init()

        # Build complete parameters dictionary (after on_init, add "active" field)
        parameters = {
            "active": "Activate Rule"
        }
        parameters.update(self.parameters)
        self.parameters = parameters

        # Update rule parameters
        self.update_setting(setting)

    cpdef void write_log(self, str msg):
        """Output Risk Log"""
        self.risk_engine.write_log(msg)

    cpdef void update_setting(self, dict rule_setting):
        """Update Risk Rule parameters"""
        cdef str name
        cdef object value
        
        for name in self.parameters.keys():
            if name in rule_setting:
                value = rule_setting[name]
                setattr(self, name, value)

    cpdef bint check_allowed(self, object req, str gateway_name):
        """Check if order submission is allowed"""
        return True

    cpdef void on_init(self):
        """Initialization (Subclasses override)"""
        pass

    cpdef void on_tick(self, object tick):
        """Tick Push"""
        pass

    cpdef void on_order(self, object order):
        """Order Push"""
        pass

    cpdef void on_trade(self, object trade):
        """Trade Push"""
        pass

    cpdef void on_timer(self):
        """Timer Push (triggered every second)"""
        pass

    cpdef object get_contract(self, str vt_symbol):
        """Query contract information"""
        return self.risk_engine.get_contract(vt_symbol)

    cpdef void put_event(self):
        """Push Data Update Event"""
        self.risk_engine.put_rule_event(self)

    cpdef dict get_data(self):
        """Get data"""
        cdef dict parameters_data = {}
        cdef dict variables_data = {}
        cdef str name
        cdef object value
        
        # Collect values for all parameters
        for name in self.parameters.keys():
            value = getattr(self, name, None)
            parameters_data[name] = value

        # Collect values for all variables
        for name in self.variables.keys():
            value = getattr(self, name, None)
            variables_data[name] = value

        data = {
            "name": self.name,
            "class_name": self.__class__.__name__,
            "parameters": parameters_data,
            "variables": variables_data
        }
        return data