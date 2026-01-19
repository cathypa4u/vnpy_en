import json
from pathlib import Path
from uuid import uuid4
from typing import TYPE_CHECKING, Any
from collections.abc import Generator

from .object import (
    Session, Profile, Delta, Request, Response, Message,
    Usage, ToolCall, ToolResult, ToolSchema
)
from .constant import Role, FinishReason
from .utility import SESSION_DIR
from .tracer import LogTracer

if TYPE_CHECKING:
    from .engine import AgentEngine


#Construct prompt words that summarize the request
TITLE_PROMPT: str = """
Based on the above dialogue content, generate a concise title summarizing the main topic of this conversation.

Requirements:
1. The title should accurately reflect the core content and primary issues discussed in the dialogue.
2. The title length must not exceed {max_length} characters.
3. Use concise, professional, and easily understandable language.
4. Return the title text directly without quotation marks, punctuation, or additional explanations.
5. If the dialogue involves multiple topics, extract the most significant theme.
"""


class TaskAgent:
    """
    标准的、可直接使用的任务智能体。
    """

    def __init__(
        self,
        engine: "AgentEngine",
        profile: Profile,
        session: Session,
        save: bool = False
    ) -> None:
        """Constructor"""
        self.engine: AgentEngine = engine
        self.profile: Profile = profile
        self.session: Session = session
        self.save: bool = save

        self.tracer: LogTracer = LogTracer(
            session_id=self.session.id,
            profile_name=self.profile.name
        )

        #Content accumulated during streaming generation
        self.collected_content: str = ""
        self.collected_tool_calls: list[ToolCall] = []

        #New conversations automatically add system prompt words and save them
        if not self.session.messages:
            system_message: Message = Message(
                role=Role.SYSTEM,
                content=self.profile.prompt
            )
            self.session.messages.append(system_message)

            self._save_session()

    def _save_session(self) -> None:
        """Save session state to file"""
        if not self.save:
            return

        data: dict = self.session.model_dump()
        file_path: Path = SESSION_DIR.joinpath(f"{self.session.id}.json")

        with open(file_path, mode="w+", encoding="UTF-8") as f:
            json.dump(
                data,
                f,
                indent=4,
                ensure_ascii=False
            )

    @property
    def id(self) -> str:
        """Task ID"""
        return self.session.id

    @property
    def name(self) -> str:
        """Task name"""
        return self.session.name

    @property
    def model(self) -> str:
        """Model name"""
        return self.session.model

    @property
    def messages(self) -> list[Message]:
        """Conversation message"""
        return self.session.messages

    def stream(self, prompt: str) -> Generator[Delta, None, None]:
        """Streaming generation"""
        #Add user input to session
        user_message: Message = Message(
            role=Role.USER,
            content=prompt
        )
        self.session.messages.append(user_message)

        #Initialize variables
        iteration: int = 0                                  #Number of iterations
        response_id: str = ""                               #Response ID

        #Query tool definition
        tool_schemas: list[ToolSchema] = self.engine.get_tool_schemas(self.profile.tools)

        #The main loop, which is responsible for handling multiple tool calls
        while iteration < self.profile.max_iterations:
            #Reset collected content
            self.collected_content = ""
            self.collected_thinking = ""
            self.collected_reasoning: list[dict[str, Any]] = []
            self.collected_tool_calls = []

            #Add 1 to the number of iterations
            iteration += 1

            #Prepare request
            request: Request = Request(
                model=self.session.model,
                messages=self.session.messages,
                tool_schemas=tool_schemas,
                temperature=self.profile.temperature,
                max_tokens=self.profile.max_tokens
            )

            #Call tracer: log request sent
            self.tracer.on_llm_start(request)

            #Data caching in this cycle
            finish_reason: FinishReason | None = None       #Ending reason for accumulation received

            #Send a request to the AI ​​server and collect the response
            for delta in self.engine.stream(request):
                #Record response ID
                if delta.id and not response_id:
                    response_id = delta.id

                #Accumulate received text content
                if delta.content:
                    self.collected_content += delta.content

                #Accumulate received thinking content
                if delta.thinking:
                    self.collected_thinking += delta.thinking

                #Accumulate received reasoning data (preserving original structure for postback)
                if delta.reasoning:
                    for new_item in delta.reasoning:
                        #If there is no index, append directly
                        if "index" not in new_item:
                            self.collected_reasoning.append(new_item)
                            continue

                        #Find if an item with the same index exists
                        existing_item = next(
                            (item for item in self.collected_reasoning if item.get("index") == new_item["index"]),
                            None
                        )

                        if existing_item:
                            #Merge fields
                            for key, value in new_item.items():
                                #String type fields are spliced ​​(signature is not spliced)
                                if key in ["text", "data", "summary"] and isinstance(value, str):
                                    existing_item[key] = existing_item.get(key, "") + value
                                #Other fields are directly covered (such as type, format, id, signature, etc.)
                                else:
                                    existing_item[key] = value
                        else:
                            #Append if it does not exist
                            self.collected_reasoning.append(new_item)

                #Cumulative received tool call requests
                if delta.calls:
                    self.collected_tool_calls.extend(delta.calls)

                #Record end reason
                if delta.finish_reason:
                    finish_reason = delta.finish_reason

                #Call tracer: record the received data block
                self.tracer.on_llm_delta(delta)

                #Forward the original Delta object directly to the caller to achieve real-time streaming effect
                yield delta

            #Add the AI's response (including thought process and tool call request) as a message to the conversation
            assistant_msg: Message = Message(
                role=Role.ASSISTANT,
                content=self.collected_content,
                thinking=self.collected_thinking,
                reasoning=self.collected_reasoning,
                tool_calls=self.collected_tool_calls
            )

            self.session.messages.append(assistant_msg)

            #Call tracer: record response received
            self.tracer.on_llm_end(assistant_msg)

            #If it ends normally, exit the loop directly
            if finish_reason == FinishReason.STOP:
                break
            #Model requires calling tool
            elif (
                finish_reason == FinishReason.TOOL_CALLS
                and self.collected_tool_calls    #And received a specific tool call request
            ):
                #Execute all tool calls in batches
                tool_results: list[ToolResult] = []

                for tool_call in self.collected_tool_calls:
                    #Before execution, send a notification through yield to tell the upper application "which tool is being executed"
                    yield Delta(
                        id=response_id or str(uuid4()),
                        content=f"\n\n[Execution tool: {tool_call.name}]\n\n"
                    )

                    #Call tracer: the logging tool starts execution
                    self.tracer.on_tool_start(tool_call)

                    #Execute a single tool call and log the results
                    result: ToolResult = self.engine.execute_tool(tool_call)
                    tool_results.append(result)

                    #Call tracer: the recording tool is executed
                    self.tracer.on_tool_end(result)

                #Package the execution results of all tools into a message and add it to the work list
                user_message = Message(
                    role=Role.USER,
                    tool_results=tool_results
                )
                self.session.messages.append(user_message)

                #Continue to the next cycle
                continue
            #In other abnormal situations, exit directly
            else:
                break

        #If the number of loops reaches the upper limit, send a warning message
        if iteration >= self.profile.max_iterations:
            yield Delta(
                id=response_id or str(uuid4()),
                content="\n[Warning: Maximum tool invocation limit reached]\n"
            )

        #Save latest session to file
        self._save_session()

    def abort_stream(self) -> None:
        """Stop streaming generation and save part of the generated content"""
        #Check if there is any content that needs to be saved
        if not self.collected_content:
            return

        #Save partially generated content
        assistant_msg = Message(
            role=Role.ASSISTANT,
            content=self.collected_content,
            tool_calls=self.collected_tool_calls
        )
        self.session.messages.append(assistant_msg)

        self._save_session()

    def invoke(self, prompt: str) -> Response:
        """Blocking generation"""
        full_content: str = ""
        response_id: str = ""
        total_usage: Usage = Usage()

        #Traverse the generator returned by the stream method and consume all Delta data
        for delta in self.stream(prompt):
            if delta.id:
                response_id = delta.id

            #Splice complete text content
            if delta.content:
                full_content += delta.content

            #Accumulated Token usage
            if delta.usage:
                total_usage.input_tokens += delta.usage.input_tokens
                total_usage.output_tokens += delta.usage.output_tokens

        #Assemble all collected information into a Response object and return
        return Response(
            id=response_id,
            content=full_content,
            usage=total_usage
        )

    def rename(self, name: str) -> None:
        """Rename task"""
        self.session.name = name

        self._save_session()

    def delete_round(self) -> None:
        """删除最后一轮对话

        一轮对话包含：用户prompt -> [助手回复 -> 工具结果 -> ...] -> 最终助手回复
        删除时需要回溯到用户发送的真正prompt（content非空的USER消息）
        """
        #There must be a conversation history, and the last one is an assistant message
        if (
            not self.messages
            or self.messages[-1].role != Role.ASSISTANT
        ):
            return

        #Delete from back to front until the real prompt sent by the user is deleted
        while self.messages:
            message: Message = self.messages.pop()

            #If you encounter a system message, you need to resume and stop
            if message.role == Role.SYSTEM:
                self.messages.append(message)
                break

            #If it is a real prompt sent by the user (with content), stop deleting it
            if message.role == Role.USER and message.content:
                break

        #Save session state
        self._save_session()

    def resend_round(self) -> str:
        """重新发送最后一轮对话

        一轮对话包含：用户prompt -> [助手回复 -> 工具结果 -> ...] -> 最终助手回复
        删除时需要回溯到用户发送的真正prompt（content非空的USER消息）
        """
        #There must be a conversation history, and the last one is an assistant message
        if (
            not self.messages
            or self.messages[-1].role != Role.ASSISTANT
        ):
            return ""

        user_prompt: str = ""

        #Delete from back to front until the real prompt sent by the user is deleted
        while self.messages:
            message: Message = self.messages.pop()

            #If you encounter a system message, you need to resume and stop
            if message.role == Role.SYSTEM:
                self.messages.append(message)
                break

            #If it is a real prompt sent by the user (with content), stop deleting it
            if message.role == Role.USER and message.content:
                user_prompt = message.content
                break

        #Save session state
        self._save_session()

        #Return user message content
        return user_prompt

    def set_model(self, model: str) -> None:
        """Set up the model"""
        self.session.model = model

        self._save_session()

    def generate_title(self, max_length: int = 20) -> str:
        """Generate session title"""
        #Copy conversation message and add summary request
        messages: list[Message] = self.session.messages.copy()
        messages.append(Message(role=Role.USER, content=TITLE_PROMPT.format(max_length=max_length)))

        #Construct requests (use lower temperatures for more stable results)
        request: Request = Request(
            model=self.session.model,
            messages=messages,
            tool_schemas=[],
            temperature=0.5,
            max_tokens=self.profile.max_tokens
        )

        #Call LLM to generate the title
        full_content: str = ""
        for delta in self.engine.stream(request):
            if delta.content:
                full_content += delta.content

        #Return the generated title (with leading and trailing whitespace and possible quotes removed)
        title: str = full_content.strip()

        #Remove possible quotes
        for quote in ['"', "'", '"', '"', ''', ''']:
            if title.startswith(quote) and title.endswith(quote):
                title = title[1:-1]
                break

        return title


class AgentTool:
    """
    智能体工具：将 Profile 封装为可调用的工具。

    每次调用时创建新的 TaskAgent 实例，不保留对话历史。
    """

    def __init__(
        self,
        engine: "AgentEngine",
        profile: Profile,
        model: str,
        name: str = "",
        description: str = "",
    ) -> None:
        """Constructor"""
        if not name:
            name = profile.name

        if not description:
            description = f"Call the agent [{profile.name}] to handle the task"

        #Use "-" to replace "_", consistent with other tools
        name = name.replace("_", "-")
        self.name: str = f"agent_{name}"

        self.description: str = description
        self.engine: AgentEngine = engine
        self.profile: Profile = profile
        self.model: str = model

        self.parameters: dict[str, Any] = {
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "Prompt word sent to the agent"
                }
            },
            "required": ["prompt"]
        }

    def get_schema(self) -> ToolSchema:
        """Get the Schema of the tool"""
        return ToolSchema(
            name=self.name,
            description=self.description,
            parameters=self.parameters
        )

    def execute(self, prompt: str) -> str:
        """Execution tool"""
        #Create a new TaskAgent instance
        agent: TaskAgent = self.engine.create_agent(
            self.profile,
            save=False      #The session is not saved because each call is a new session
        )

        #Set up the model
        agent.set_model(self.model)

        #Use the invoke method to perform tasks
        response: Response = agent.invoke(prompt)

        #Return the response content of the AI ​​model
        return response.content
