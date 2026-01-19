from typing import Any
import json

from .openai_gateway import OpenaiGateway
from vnag.object import Message
from vnag.constant import Role


class OpenrouterGateway(OpenaiGateway):
    """
    OpenRouter 网关

    继承自 OpenaiGateway，覆盖钩子方法以支持：
    - reasoning_details 格式的 thinking 提取
    - 请求中启用 reasoning 参数
    - 回传 thinking 内容到后续请求
    """

    default_name: str = "OpenRouter"

    default_setting: dict = {
        "base_url": "https://openrouter.ai/api/v1",
        "api_key": "",
        "reasoning_effort": ["high", "medium", "low"]
    }

    def init(self, setting: dict[str, Any]) -> bool:
        """Initialize the connection and internal service components, and return whether it is successful"""
        self.reasoning_effort: str = setting.get("reasoning_effort", "medium")
        return super().init(setting)

    def _get_reasoning_data(self, obj: Any) -> list[dict[str, Any]] | None:
        """Get reasoning_details data from object"""
        if hasattr(obj, "reasoning_details") and obj.reasoning_details:
            data: list[dict[str, Any]] = list(obj.reasoning_details)
            return data
        return None

    def _extract_thinking(self, message: Any) -> str:
        """Extract thinking content from message object"""
        reasoning_data = self._get_reasoning_data(message)
        if not reasoning_data:
            return ""

        thinking: str = ""
        for detail in reasoning_data:
            if isinstance(detail, dict) and detail.get("text"):
                thinking += detail["text"]
        return thinking

    def _extract_reasoning(self, message: Any) -> list[dict[str, Any]]:
        """Extract reasoning data from message object"""
        data: list[dict[str, Any]] | None = self._get_reasoning_data(message)
        return data if data else []

    def _extract_thinking_delta(self, delta: Any) -> str:
        """Extract thinking delta from streaming delta object"""
        reasoning_data = self._get_reasoning_data(delta)
        if not reasoning_data:
            return ""

        thinking: str = ""
        for detail in reasoning_data:
            if isinstance(detail, dict) and detail.get("text"):
                thinking += detail["text"]
        return thinking

    def _extract_reasoning_delta(self, delta: Any) -> list[dict[str, Any]]:
        """Extract reasoning delta data from streaming delta objects"""
        data: list[dict[str, Any]] | None = self._get_reasoning_data(delta)
        return data if data else []

    def _get_extra_body(self) -> dict[str, Any]:
        """Get additional parameters of the request and enable OpenRouter’s reasoning function"""
        return {"reasoning": {"effort": self.reasoning_effort}}

    def _convert_messages(self, messages: list[Message]) -> list[dict[str, Any]]:
        """
        将内部 Message 格式转换为 OpenRouter API 格式

        同时支持：
        - reasoning_details 格式（Gemini 需要 thought_signature）
        - content 数组格式（Claude extended thinking）
        """
        openai_messages: list[dict[str, Any]] = []

        for msg in messages:
            #Process tool results: split into multiple tool messages
            if msg.tool_results:
                for tool_result in msg.tool_results:
                    openai_messages.append({
                        "role": "tool",
                        "tool_call_id": tool_result.id,
                        "content": tool_result.content
                    })
                continue

            message_dict: dict[str, Any] = {"role": msg.role.value}

            #For assistant messages
            if msg.role == Role.ASSISTANT:
                #When reasoning_details is present, use OpenRouter format
                if msg.reasoning:
                    if msg.content:
                        message_dict["content"] = msg.content
                    message_dict["reasoning_details"] = msg.reasoning
                else:
                    #Downgrade: use content array format (Claude extended thinking)
                    content_parts: list[dict[str, Any]] = []
                    content_parts.append({
                        "type": "thinking",
                        "thinking": msg.thinking or ""
                    })
                    if msg.content:
                        content_parts.append({
                            "type": "text",
                            "text": msg.content
                        })
                    message_dict["content"] = content_parts
            elif msg.content:
                message_dict["content"] = msg.content

            #Handling tool_calls
            if msg.tool_calls:
                message_dict["tool_calls"] = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": json.dumps(tc.arguments)
                        }
                    }
                    for tc in msg.tool_calls
                ]

            openai_messages.append(message_dict)

        return openai_messages
