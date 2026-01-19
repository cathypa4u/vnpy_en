from typing import Any
from collections.abc import Generator

import dashscope
from dashscope import Generation, Models
from dashscope.api_entities.dashscope_response import (
    DashScopeAPIResponse,
    GenerationResponse,
    Choice
)

from vnag.gateway import BaseGateway
from vnag.object import FinishReason, Request, Response, Delta, Usage, Message
from vnag.object import Role


FINISH_REASON_MAP = {
    "stop": FinishReason.STOP,
    "length": FinishReason.LENGTH,
}


class DashscopeGateway(BaseGateway):
    """Gateway to connect to DashScope SDK, providing a unified interface"""

    default_name: str = "DashScope"

    default_setting: dict = {
        "api_key": "",
    }

    def __init__(self, gateway_name: str = "") -> None:
        """Constructor"""
        if not gateway_name:
            gateway_name = self.default_name
        self.gateway_name = gateway_name

        self.api_key: str = ""

    def _convert_messages(self, messages: list[Message]) -> list[dict[str, Any]]:
        """Convert internal format to Dashscope (OpenAI compatible) format"""
        dashscope_messages = []

        for msg in messages:
            message_dict: dict[str, Any] = {"role": msg.role.value}

            if msg.content:
                message_dict["content"] = msg.content

            dashscope_messages.append(message_dict)

        return dashscope_messages

    def init(self, setting: dict[str, Any]) -> bool:
        """Initialize the connection and internal service components, and return whether it is successful"""
        self.api_key = setting.get("api_key", "")

        if not self.api_key:
            self.write_log("The configuration is incomplete, please check the following configuration items:")
            self.write_log("- api_key: API key not set")
            return False

        dashscope.api_key = self.api_key
        return True

    def invoke(self, request: Request) -> Response:
        """Conventional calling interface: Send the prepared message to the model and output the text in one go"""
        if not self.api_key:
            self.write_log("LLM client is not initialized, please check the configuration")
            return Response(id="", content="LLM client not initialized", usage=Usage())

        #Use new message conversion methods
        dashscope_messages = self._convert_messages(request.messages)

        #Prepare request parameters
        call_params: dict[str, Any] = {
            "model": request.model,
            "messages": dashscope_messages,
            "result_format": "message",
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
            "top_p": request.top_p,
        }

        response: GenerationResponse = Generation.call(**call_params)

        if response.status_code != 200:
            return Response(
                id=response.request_id,
                content=f"Request failed: {response.message}",
                usage=Usage(),
            )

        usage: Usage = Usage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        )
        print(response)
        choice: Choice = response.output.choices[0]
        finish_reason: FinishReason = FINISH_REASON_MAP.get(
            choice.finish_reason, FinishReason.UNKNOWN
        )
        content: str = choice.message.content or ""

        #Construct the returned message object
        message: Message = Message(
            role=Role.ASSISTANT,
            content=content
        )

        return Response(
            id=response.request_id,
            content=content,
            usage=usage,
            finish_reason=finish_reason,
            message=message
        )

    def stream(self, request: Request) -> Generator[Delta, None, None]:
        """Streaming call interface"""
        if not self.api_key:
            self.write_log("LLM client is not initialized, please check the configuration")
            return

        #Use new message conversion methods
        dashscope_messages = self._convert_messages(request.messages)

        #Prepare request parameters
        call_params: dict[str, Any] = {
            "model": request.model,
            "messages": dashscope_messages,
            "result_format": "message",
            "stream": True,
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
            "top_p": request.top_p,
        }

        stream: Generator[GenerationResponse, None, None] = Generation.call(**call_params)

        response_id: str = ""
        full_content: str = ""

        for response in stream:
            if response.status_code != 200:
                self.write_log(f"An error occurred with the request: {response.message}")
                yield Delta(
                    id=response.request_id,
                    content=f"An error occurred with the request: {response.message}",
                    finish_reason=FinishReason.ERROR,
                )
                break

            if not response_id:
                response_id = response.request_id

            delta: Delta = Delta(id=response_id)
            should_yield: bool = False
            choice: Choice = response.output.choices[0]

            #Check content delta
            new_content = choice.message.content or ""
            delta_content = new_content[len(full_content):]
            if delta_content:
                full_content = new_content
                delta.content = delta_content
                should_yield = True

            #Check end reason
            if choice.finish_reason != "null":
                finish_reason: FinishReason = FINISH_REASON_MAP.get(
                    choice.finish_reason, FinishReason.UNKNOWN
                )
                delta.finish_reason = finish_reason
                delta.usage = Usage(
                    input_tokens=response.usage.input_tokens,
                    output_tokens=response.usage.output_tokens,
                )
                should_yield = True

            if should_yield:
                yield delta

    def list_models(self) -> list[str]:
        """Query the list of available models"""
        if not self.api_key:
            self.write_log("LLM client is not initialized, please check the configuration")
            return []

        try:
            response: DashScopeAPIResponse = Models.list(
                page_size=100,
                api_key=self.api_key
            )
        except Exception as err:
            self.write_log(f"Failed to query model list: {err}")
            return []

        if response.status_code != 200:
            self.write_log(f"Failed to query model list: {response.message}")
            return []

        print(response)

        model_names: list[str] = [d["name"] for d in response.output["models"]]
        return model_names
