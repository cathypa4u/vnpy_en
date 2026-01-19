from typing import Any

from pydantic import BaseModel, Field

from .constant import Role, FinishReason


class Segment(BaseModel):
    """Unify document fragment structure"""
    text: str
    metadata: dict[str, str]
    score: float = 0


class Message(BaseModel):
    """Standardized message object"""
    role: Role
    content: str = ""
    thinking: str = ""
    reasoning: list[dict[str, Any]] = Field(default_factory=list)
    tool_calls: list["ToolCall"] = Field(default_factory=list)
    tool_results: list["ToolResult"] = Field(default_factory=list)


class Usage(BaseModel):
    """Standardized large model usage statistics"""
    input_tokens: int = 0
    output_tokens: int = 0


class Request(BaseModel):
    """Standardized LLM request object"""
    model: str
    messages: list[Message]
    tool_schemas: list["ToolSchema"] = Field(default_factory=list)
    temperature: float | None = Field(default=None, ge=0.0, le=2.0)
    top_p: float | None = Field(default=None, ge=0.0, le=1.0)
    max_tokens: int | None = Field(default=None, gt=0)


class Response(BaseModel):
    """Standardized LLM blocking response object"""
    id: str
    content: str
    thinking: str = ""
    usage: Usage
    finish_reason: FinishReason | None = None
    message: Message | None = None


class Delta(BaseModel):
    """Standardized LLM streaming response block"""

    id: str
    content: str | None = None
    thinking: str | None = None
    reasoning: list[dict[str, Any]] = Field(default_factory=list)
    calls: list["ToolCall"] | None = None
    finish_reason: FinishReason | None = None
    usage: Usage | None = None


class ToolSchema(BaseModel):
    """Unified tool class (pure description, no execution logic)"""
    name: str
    description: str
    parameters: dict[str, Any] = Field(default_factory=dict)

    def get_schema(self) -> dict[str, Any]:
        """Returns the Schema definition of the tool"""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters
            }
        }


class ToolCall(BaseModel):
    """Tool call request"""
    id: str
    name: str
    arguments: dict[str, Any]


class ToolResult(BaseModel):
    """Tool execution results (general format)"""
    id: str  #Tool call ID, used to associate the original tool call
    name: str  #Tool name, required by some APIs (such as Gemini)
    content: str  #Tool execution result content
    is_error: bool = False  #Identifies whether the result is an error (Anthropic support)


class Profile(BaseModel):
    """Agent configuration data"""
    name: str
    prompt: str
    tools: list[str]
    temperature: float | None = None
    max_tokens: int | None = None
    max_iterations: int = 10


class Session(BaseModel):
    """Chat interaction session history"""
    id: str
    profile: str
    name: str
    model: str = ""
    messages: list[Message] = Field(default_factory=list)
