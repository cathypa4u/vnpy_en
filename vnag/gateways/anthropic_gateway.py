import json
from typing import Any
from collections.abc import Generator

from anthropic import Anthropic, Stream
from anthropic.types import Message as AnthropicMessage, MessageStreamEvent

from vnag.constant import FinishReason, Role
from vnag.gateway import BaseGateway
from vnag.object import Request, Response, Delta, Usage, Message, ToolCall


ANTHROPIC_FINISH_REASON_MAP = {
    "end_turn": FinishReason.STOP,
    "max_tokens": FinishReason.LENGTH,
    "stop_sequence": FinishReason.STOP,
    "tool_use": FinishReason.TOOL_CALLS,
}


class AnthropicGateway(BaseGateway):
    """Gateway to connect to Anthropic official SDK, providing a unified interface"""

    default_name: str = "Anthropic"

    default_setting: dict = {
        "base_url": "",
        "api_key": "",
    }

    def __init__(self, gateway_name: str = "") -> None:
        """Constructor"""
        if not gateway_name:
            gateway_name = self.default_name
        self.gateway_name = gateway_name
        self.client: Anthropic | None = None

    def _convert_messages(self, messages: list[Message]) -> tuple[str, list[dict[str, Any]]]:
        """Convert internal format to Anthropic format"""
        system_prompt: str = ""
        anthropic_messages: list[dict[str, Any]] = []

        for msg in messages:
            #Extract system messages
            if msg.role == Role.SYSTEM:
                system_prompt = msg.content
                continue

            #Processing tool results: merged into one user message
            if msg.tool_results:
                content_blocks: list[dict[str, Any]] = [
                    {
                        "type": "tool_result",
                        "tool_use_id": tr.id,
                        "content": tr.content,
                        "is_error": tr.is_error
                    }
                    for tr in msg.tool_results
                ]
                anthropic_messages.append({
                    "role": "user",
                    "content": content_blocks
                })

            #Handle assistant's tool calls
            elif msg.tool_calls:
                content_blocks = []

                #If there is text content, add the text block first
                if msg.content:
                    content_blocks.append({
                        "type": "text",
                        "text": msg.content
                    })

                #Add tool call block
                for tc in msg.tool_calls:
                    content_blocks.append({
                        "type": "tool_use",
                        "id": tc.id,
                        "name": tc.name,
                        "input": tc.arguments
                    })

                anthropic_messages.append({
                    "role": "assistant",
                    "content": content_blocks
                })

            #General news
            else:
                anthropic_messages.append({
                    "role": msg.role.value,
                    "content": msg.content
                })

        return system_prompt, anthropic_messages

    def init(self, setting: dict[str, Any]) -> bool:
        """Initialize the connection and internal service components, and return whether it is successful"""
        base_url: str | None = setting.get("base_url", None)
        api_key: str = setting.get("api_key", "")

        if not api_key:
            self.write_log("The configuration is incomplete, please check the following configuration items:")
            self.write_log("- api_key: API key not set")
            return False

        self.client = Anthropic(api_key=api_key, base_url=base_url)

        return True

    def invoke(self, request: Request) -> Response:
        """Conventional calling interface: Send the prepared message to the model and output the text in one go"""
        if not self.client:
            self.write_log("LLM client is not initialized, please check the configuration")
            return Response(id="", content="LLM client not initialized", usage=Usage())

        if not request.max_tokens:
            self.write_log("Max_tokens is a required parameter for Anthropic")
            return Response(id="", content="Max_tokens cannot be empty", usage=Usage())

        #Use new message conversion methods
        system_prompt, anthropic_messages = self._convert_messages(request.messages)

        #Prepare request parameters
        create_params: dict[str, Any] = {
            "model": request.model,
            "messages": anthropic_messages,
            "max_tokens": request.max_tokens,
            "system": system_prompt,
            "temperature": request.temperature,
        }

        #Add tool definition (if any)
        if request.tool_schemas:
            #Convert to Anthropic format
            tools: list[dict[str, Any]] = [
                {
                    "name": schema.name,
                    "description": schema.description,
                    "input_schema": schema.parameters
                }
                for schema in request.tool_schemas
            ]
            create_params["tools"] = tools

        response: AnthropicMessage = self.client.messages.create(**create_params)

        usage: Usage = Usage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        )

        #Extract text content and tool calls
        content: str = ""
        tool_calls: list[ToolCall] = []

        if response.content:
            for block in response.content:
                if hasattr(block, "text"):
                    content += block.text
                elif hasattr(block, "type") and block.type == "tool_use":
                    arguments: dict[str, Any] = {}
                    if isinstance(block.input, dict):
                        arguments = block.input

                    tool_call: ToolCall = ToolCall(
                        id=block.id,
                        name=block.name,
                        arguments=arguments
                    )
                    tool_calls.append(tool_call)

        #Determine the reason for ending
        finish_reason: FinishReason = FinishReason.UNKNOWN
        if response.stop_reason:
            finish_reason = ANTHROPIC_FINISH_REASON_MAP.get(
                response.stop_reason, FinishReason.UNKNOWN
            )

        #Construct the returned message object
        message = Message(
            role=Role.ASSISTANT,
            content=content,
            tool_calls=tool_calls
        )

        return Response(
            id=response.id,
            content=content,
            usage=usage,
            finish_reason=finish_reason,
            message=message
        )

    def stream(self, request: Request) -> Generator[Delta, None, None]:
        """Streaming call interface"""
        if not self.client:
            self.write_log("LLM client is not initialized, please check the configuration")
            return

        if not request.max_tokens:
            self.write_log("Max_tokens is a required parameter for Anthropic")
            return

        #Use new message conversion methods
        system_prompt, anthropic_messages = self._convert_messages(request.messages)

        #Prepare request parameters
        create_params: dict[str, Any] = {
            "model": request.model,
            "messages": anthropic_messages,
            "max_tokens": request.max_tokens,
            "stream": True,
            "system": system_prompt,
            "temperature": request.temperature,
        }

        #Add tool definition (if any)
        if request.tool_schemas:
            #Convert to Anthropic format
            tools: list[dict[str, Any]] = [
                {
                    "name": schema.name,
                    "description": schema.description,
                    "input_schema": schema.parameters
                }
                for schema in request.tool_schemas
            ]
            create_params["tools"] = tools

        stream: Stream[MessageStreamEvent] = self.client.messages.create(**create_params)

        response_id: str = ""
        input_tokens: int = 0
        #For cumulative tool calls
        accumulated_tool_calls: dict[int, dict[str, Any]] = {}
        current_block_index: int = -1

        for stream_event in stream:
            if stream_event.type == "message_start":
                response_id = stream_event.message.id
                input_tokens = stream_event.message.usage.input_tokens

            elif stream_event.type == "content_block_start":
                #Record the index of the current content block
                current_block_index = stream_event.index
                if stream_event.content_block.type == "tool_use":
                    accumulated_tool_calls[current_block_index] = {
                        "id": stream_event.content_block.id,
                        "name": stream_event.content_block.name,
                        "input": ""
                    }

            elif stream_event.type == "content_block_delta":
                if stream_event.delta.type == "text_delta":
                    yield Delta(
                        id=response_id,
                        content=stream_event.delta.text,
                    )
                elif stream_event.delta.type == "input_json_delta":
                    #Accumulate parameters for tool calls
                    if stream_event.index in accumulated_tool_calls:
                        accumulated_tool_calls[stream_event.index]["input"] += stream_event.delta.partial_json

            elif stream_event.type == "message_delta":
                finish_reason: FinishReason = FinishReason.UNKNOWN
                if stream_event.delta.stop_reason:
                    finish_reason = ANTHROPIC_FINISH_REASON_MAP.get(
                        stream_event.delta.stop_reason, FinishReason.UNKNOWN
                    )

                delta = Delta(
                    id=response_id,
                    finish_reason=finish_reason,
                    usage=Usage(
                        input_tokens=input_tokens,
                        output_tokens=stream_event.usage.output_tokens,
                    ),
                )

                #If the tool call ends, convert the accumulated tool calls
                if finish_reason == FinishReason.TOOL_CALLS and accumulated_tool_calls:
                    tool_calls: list[ToolCall] = []
                    for tc_data in accumulated_tool_calls.values():
                        try:
                            arguments: dict[str, Any] = json.loads(tc_data["input"])
                        except json.JSONDecodeError:
                            arguments = {}

                        tool_calls.append(ToolCall(
                            id=tc_data["id"],
                            name=tc_data["name"],
                            arguments=arguments
                        ))

                    delta.calls = tool_calls

                yield delta

    def list_models(self) -> list[str]:
        """Query the list of available models"""
        self.write_log("Anthropic API does not support querying model lists")
        return []
