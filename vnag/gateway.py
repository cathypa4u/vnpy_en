from abc import ABC, abstractmethod
from typing import Any
from collections.abc import Generator

from .object import Request, Response, Delta


class BaseGateway(ABC):
    """Gateway base class: only responsible for sending prepared messages to the model and returning streaming results"""

    default_name: str = ""

    default_setting: dict = {}

    @abstractmethod
    def init(self, setting: dict[str, Any]) -> bool:
        """Initialize client"""
        pass

    @abstractmethod
    def invoke(self, request: Request) -> Response:
        """Blocking call interface"""
        pass

    @abstractmethod
    def stream(self, request: Request) -> Generator[Delta, None, None]:
        """Streaming call interface returns a StreamChunk generator"""
        pass

    @abstractmethod
    def list_models(self) -> list[str]:
        """Query the list of available models"""
        pass

    def write_log(self, text: str) -> None:
        """Write log"""
        print(text)
