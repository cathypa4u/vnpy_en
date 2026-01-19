from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..gateway import BaseGateway

from ..utility import load_json, save_json
from ..gateways import get_gateway_class
from ..gateway import BaseGateway
from .setting import load_gateway_type


def load_gateway_setting(gateway_type: str) -> dict[str, Any]:
    """Load the connection settings for the specified gateway"""
    filename: str = f"connect_{gateway_type.lower()}.json"
    return load_json(filename)


def save_gateway_setting(gateway_type: str, setting: dict[str, Any]) -> None:
    """Save the connection settings for the specified gateway"""
    filename: str = f"connect_{gateway_type.lower()}.json"
    save_json(filename, setting)


def create_gateway() -> BaseGateway:
    """Create an AI service interface instance based on the current configuration"""
    #Load the currently selected gateway type
    gateway_type: str = load_gateway_type()

    #Load connection settings
    gateway_cls: type[BaseGateway] = get_gateway_class(gateway_type)
    setting: dict[str, Any] = load_gateway_setting(gateway_type)

    #Create an instance and initialize it
    gateway: BaseGateway = gateway_cls()
    gateway.init(setting)

    return gateway
