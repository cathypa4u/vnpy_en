"""Gateway registry"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vnag.gateway import BaseGateway

from .openai_gateway import OpenaiGateway
from .anthropic_gateway import AnthropicGateway
from .dashscope_gateway import DashscopeGateway
from .deepseek_gateway import DeepseekGateway
from .minimax_gateway import MinimaxGateway
from .bailian_gateway import BailianGateway
from .openrouter_gateway import OpenrouterGateway


#Gateway type name to class mapping
GATEWAY_CLASSES: dict[str, type["BaseGateway"]] = {
    OpenaiGateway.default_name: OpenaiGateway,
    AnthropicGateway.default_name: AnthropicGateway,
    DashscopeGateway.default_name: DashscopeGateway,
    DeepseekGateway.default_name: DeepseekGateway,
    MinimaxGateway.default_name: MinimaxGateway,
    BailianGateway.default_name: BailianGateway,
    OpenrouterGateway.default_name: OpenrouterGateway,
}


def get_gateway_names() -> list[str]:
    """Get a list of all available gateway names"""
    return list(GATEWAY_CLASSES.keys())


def get_gateway_class(name: str) -> type["BaseGateway"]:
    """Get the gateway class by name, or OpenaiGateway if the name does not exist (most versatile)"""
    return GATEWAY_CLASSES.get(name, OpenaiGateway)
