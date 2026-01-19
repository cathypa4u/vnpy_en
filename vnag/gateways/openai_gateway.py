from typing import Any
from collections.abc import Generator
import json

from openai import OpenAI, Stream
from openai.types.chat import ChatCompletion, ChatCompletionChunk
from openai.types.chat.chat_completion import Choice
from openai.types.chat.chat_completion_chunk import Choice as ChunkChoice

from vnag.gateway import BaseGateway
from vnag.object import FinishReason, Request, Response, Delta, Usage, Message, ToolCall
from vnag.object import Role


FINISH_REASON_MAP = {
    "stop": FinishReason.STOP,
    "length": FinishReason.LENGTH,
    "tool_calls": FinishReason.TOOL_CALLS,
}


class OpenaiGateway(BaseGateway):
    """
    OpenAI 兼容的 AI 大模型网关基类

    标准 OpenAI API 不返回 thinking/reasoning 内容。
    如需支持 thinking，请继承此类并覆盖相关钩子方法。
    """

    default_name: str = "OpenAI"

    default_setting: dict = {
        "base_url": "",
        "api_key": "",
    }

    def __init__(self, gateway_name: str = "") -> None:
        """Constructor"""
        if not gateway_name:
            gateway_name = self.default_name
        self.gateway_name = gateway_name

        self.client: OpenAI | None = None

    def _extract_thinking(self, message: Any) -> str:
        """
        从消息对象中提取 thinking 内容（子类可覆盖）

        标准 OpenAI API 不返回 thinking 内容，返回空字符串。
        """
        return ""

    def _extract_reasoning(self, message: Any) -> list[dict[str, Any]]:
        """
        从消息对象中提取 reasoning 数据（子类可覆盖）

        标准 OpenAI API 不返回 reasoning_details，返回空列表。
        """
        return []

    def _extract_thinking_delta(self, delta: Any) -> str:
        """
        从流式 delta 对象中提取 thinking 增量（子类可覆盖）

        标准 OpenAI API 不返回 thinking 内容，返回空字符串。
        """
        return ""

    def _extract_reasoning_delta(self, delta: Any) -> list[dict[str, Any]]:
        """
        从流式 delta 对象中提取 reasoning 增量数据（子类可覆盖）

        标准 OpenAI API 不返回 reasoning 内容，返回空列表。
        """
        return []

    def _get_extra_body(self) -> dict[str, Any] | None:
        """
        获取请求的额外参数（子类可覆盖）

        标准 OpenAI API 不需要额外参数，返回 None。
        """
        return None

    def _convert_thinking_for_request(self, thinking: str) -> dict[str, Any] | None:
        """
        将 thinking 转换为请求格式（子类可覆盖）

        标准 OpenAI API 不支持回传 thinking，返回 None。
        """
        return None

    def _convert_messages(self, messages: list[Message]) -> list[dict[str, Any]]:
        """
        将内部 Message 格式转换为 OpenAI API 格式

        内部格式支持 tool_results，需要拆分为多条 tool 角色消息
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

            #Process normal messages or messages with tool_calls
            else:
                message_dict: dict[str, Any] = {"role": msg.role.value}

                if msg.content:
                    message_dict["content"] = msg.content

                if msg.tool_calls:
                    #Convert tool_calls to OpenAI format
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

                #Return thinking content (through hook method, subclass can be customized)
                thinking_data: dict[str, Any] | None = self._convert_thinking_for_request(msg.thinking)
                if thinking_data:
                    message_dict.update(thinking_data)

                openai_messages.append(message_dict)

        return openai_messages

    def init(self, setting: dict[str, Any]) -> bool:
        """Initialize the connection and internal service components, and return whether it is successful"""
        base_url: str = setting.get("base_url", "")
        api_key: str = setting.get("api_key", "")

        if not base_url or not api_key:
            self.write_log("The configuration is incomplete, please check the following configuration items:")
            if not base_url:
                self.write_log("- base_url: API address not set")
            if not api_key:
                self.write_log("- api_key: API key not set")
            return False

        self.client = OpenAI(api_key=api_key, base_url=base_url)

        return True

    def invoke(self, request: Request) -> Response:
        """Conventional calling interface: Send the prepared message to the model and output the text in one go"""
        if not self.client:
            self.write_log("LLM client is not initialized, please check the configuration")
            return Response(id="", content="", usage=Usage())

        #Convert message format
        openai_messages: list[dict[str, Any]] = self._convert_messages(request.messages)

        #Prepare request parameters
        create_params: dict[str, Any] = {
            "model": request.model,
            "messages": openai_messages,
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
        }

        #Add additional parameters (via hook methods, customizable by subclasses)
        extra_body: dict[str, Any] | None = self._get_extra_body()
        if extra_body:
            create_params["extra_body"] = extra_body

        #Add tool definition (if any)
        if request.tool_schemas:
            create_params["tools"] = [t.get_schema() for t in request.tool_schemas]

        #Make a request and get a response
        response: ChatCompletion = self.client.chat.completions.create(**create_params)

        #Extract usage information
        usage: Usage = Usage()
        if response.usage:
            usage.input_tokens = response.usage.prompt_tokens
            usage.output_tokens = response.usage.completion_tokens

        #Extract response content and end reason
        choice: Choice = response.choices[0]
        finish_reason: FinishReason = FINISH_REASON_MAP.get(
            choice.finish_reason, FinishReason.UNKNOWN
        )

        #Extract thinking content (through hook method, subclass can be customized)
        thinking: str = self._extract_thinking(choice.message)

        #Extract reasoning data (through hook method, subclass can be customized)
        reasoning: list[dict[str, Any]] = self._extract_reasoning(choice.message)

        #Extraction tool call
        tool_calls: list[ToolCall] = []
        if choice.message.tool_calls:
            for tc in choice.message.tool_calls:
                try:
                    if hasattr(tc, "function"):
                        arguments: dict[str, Any] = json.loads(tc.function.arguments)
                        tool_calls.append(ToolCall(
                            id=tc.id,
                            name=tc.function.name,
                            arguments=arguments
                        ))
                except json.JSONDecodeError:
                    pass

        #Construct the returned message object
        message = Message(
            role=Role.ASSISTANT,
            content=choice.message.content or "",
            thinking=thinking,
            reasoning=reasoning,
            tool_calls=tool_calls
        )

        return Response(
            id=response.id,
            content=choice.message.content or "",
            thinking=thinking,
            usage=usage,
            finish_reason=finish_reason,
            message=message
        )

    def stream(self, request: Request) -> Generator[Delta, None, None]:
        """Streaming call interface"""
        if not self.client:
            self.write_log("LLM client is not initialized, please check the configuration")
            return

        #Convert message format
        openai_messages: list[dict[str, Any]] = self._convert_messages(request.messages)

        #Prepare request parameters
        create_params: dict[str, Any] = {
            "model": request.model,
            "messages": openai_messages,
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
            "stream": True,
        }

        #Add additional parameters (via hook methods, customizable by subclasses)
        extra_body: dict[str, Any] | None = self._get_extra_body()
        if extra_body:
            create_params["extra_body"] = extra_body

        #Add tool definition (if any)
        if request.tool_schemas:
            create_params["tools"] = [t.get_schema() for t in request.tool_schemas]

        stream: Stream[ChatCompletionChunk] = self.client.chat.completions.create(**create_params)

        response_id: str = ""
        #Used to accumulate tool_calls (OpenAI streaming may return multiple times)
        accumulated_tool_calls: dict[int, dict[str, Any]] = {}

        for chuck in stream:
            if not response_id:
                response_id = chuck.id

            delta: Delta = Delta(id=response_id)
            should_yield: bool = False

            choice: ChunkChoice = chuck.choices[0]

            #Check thinking increment (via hook method, customizable by subclasses)
            thinking_delta: str = self._extract_thinking_delta(choice.delta)
            if thinking_delta:
                delta.thinking = thinking_delta
                should_yield = True

            #Check reasoning increment (via hook method, customizable by subclasses)
            reasoning_data: list[dict[str, Any]] = self._extract_reasoning_delta(choice.delta)
            if reasoning_data:
                delta.reasoning = reasoning_data
                should_yield = True

            #Check content delta
            delta_content: str | None = choice.delta.content
            if delta_content:
                delta.content = delta_content
                should_yield = True

            #Check tool_calls increment
            if choice.delta.tool_calls:
                for tc_chunk in choice.delta.tool_calls:
                    idx: int = tc_chunk.index

                    #Initialize or update accumulated tool_calls
                    if idx not in accumulated_tool_calls:
                        accumulated_tool_calls[idx] = {
                            "id": "",
                            "name": "",
                            "arguments": ""
                        }

                    if tc_chunk.id:
                        accumulated_tool_calls[idx]["id"] = tc_chunk.id

                    if tc_chunk.function:
                        if tc_chunk.function.name:
                            accumulated_tool_calls[idx]["name"] = tc_chunk.function.name
                        if tc_chunk.function.arguments:
                            accumulated_tool_calls[idx]["arguments"] += tc_chunk.function.arguments

            #Check end reason
            openai_finish_reason = choice.finish_reason
            if openai_finish_reason:
                vnag_finish_reason: FinishReason = FINISH_REASON_MAP.get(
                    openai_finish_reason, FinishReason.UNKNOWN
                )
                delta.finish_reason = vnag_finish_reason
                should_yield = True

                #If tool_calls ends, convert the accumulated tool_calls
                if vnag_finish_reason == FinishReason.TOOL_CALLS and accumulated_tool_calls:
                    tool_calls: list[ToolCall] = []
                    for tc_data in accumulated_tool_calls.values():
                        try:
                            arguments: dict[str, Any] = json.loads(tc_data["arguments"])
                        except json.JSONDecodeError:
                            arguments = {}

                        tool_calls.append(ToolCall(
                            id=tc_data["id"],
                            name=tc_data["name"],
                            arguments=arguments
                        ))

                    delta.calls = tool_calls

            #Check usage information (usually in the last data block)
            if chuck.usage:
                delta.usage = Usage(
                    input_tokens=chuck.usage.prompt_tokens or 0,
                    output_tokens=chuck.usage.completion_tokens or 0,
                )
                should_yield = True

            if should_yield:
                yield delta

    def list_models(self) -> list[str]:
        """Query the list of available models"""
        if not self.client:
            self.write_log("LLM client is not initialized, please check the configuration")
            return []

        models = self.client.models.list()
        return sorted([model.id for model in models])
