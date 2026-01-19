from enum import Enum


class Role(str, Enum):
    """Message role"""
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"


class FinishReason(str, Enum):
    """Streaming response end reason"""
    STOP = "stop"
    LENGTH = "length"
    TOOL_CALLS = "tool_calls"
    UNKNOWN = "unknown"
    ERROR = "error"
