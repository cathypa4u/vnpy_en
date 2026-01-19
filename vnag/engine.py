import json
from pathlib import Path
from collections.abc import Generator
from datetime import datetime

from .gateway import BaseGateway
from .object import (
    Request,
    Delta,
    ToolCall,
    ToolResult,
    ToolSchema,
    Session
)
from .mcp import McpManager
from .local import LocalManager, LocalTool
from .agent import Profile, TaskAgent, AgentTool
from .utility import PROFILE_DIR, SESSION_DIR


#Default agent configuration
default_profile: Profile = Profile(
    name="Chat assistant",
    prompt="You are a helpful chat assistant who responds to users' questions",
    tools=[]
)


class AgentEngine:
    """
    智能体引擎：负责智能体类的发现和注册，并提供智能体实例创建的工厂方法。
    """

    def __init__(self, gateway: BaseGateway) -> None:
        """Constructor"""
        self.gateway: BaseGateway = gateway

        self._local_manager: LocalManager = LocalManager()
        self._mcp_manager: McpManager = McpManager()

        self._local_schemas: dict[str, ToolSchema] = {}
        self._mcp_schemas: dict[str, ToolSchema] = {}
        self._agent_tools: dict[str, AgentTool] = {}

        self._profiles: dict[str, Profile] = {}
        self._agents: dict[str, TaskAgent] = {}
        self._models: list[str] = []

    def init(self) -> None:
        """Initialize engine"""
        self._load_local_schemas()
        self._load_mcp_schemas()

        self._load_profiles()
        self._load_agents()

    def _load_local_schemas(self) -> None:
        """Load local tools"""
        for schema in self._local_manager.list_tools():
            self._local_schemas[schema.name] = schema

    def _load_mcp_schemas(self) -> None:
        """Load MCP tools"""
        for schema in self._mcp_manager.list_tools():
            self._mcp_schemas[schema.name] = schema

    def _load_profiles(self) -> None:
        """Load agent configuration"""
        #Add default agent configuration
        self._profiles[default_profile.name] = default_profile

        #Load user-defined configuration
        for file_path in PROFILE_DIR.glob("*.json"):
            with open(file_path, encoding="UTF-8") as f:
                data: dict = json.load(f)
                profile: Profile = Profile.model_validate(data)
                self._profiles[profile.name] = profile

    def _save_profile(self, profile: Profile) -> None:
        """Save agent configuration to JSON file"""
        profile_path: Path = PROFILE_DIR.joinpath(f"{profile.name}.json")
        with open(profile_path, "w", encoding="UTF-8") as f:
            json.dump(profile.model_dump(), f, indent=4, ensure_ascii=False)

    def _load_agents(self) -> None:
        """Load all agents from JSON file"""
        for file_path in SESSION_DIR.glob("*.json"):
            with open(file_path, encoding="UTF-8") as f:
                data: dict = json.load(f)
                session: Session = Session.model_validate(data)
                profile: Profile = self._profiles[session.profile]
                agent: TaskAgent = TaskAgent(self, profile, session, save=True)
                self._agents[session.id] = agent

    def get_local_schemas(self) -> dict[str, ToolSchema]:
        """Get the Schema of the local tool"""
        return self._local_schemas

    def get_mcp_schemas(self) -> dict[str, ToolSchema]:
        """Get the Schema of the MCP tool"""
        return self._mcp_schemas

    def add_profile(self, profile: Profile) -> bool:
        """Add agent configuration"""
        if profile.name in self._profiles:
            return False

        self._profiles[profile.name] = profile

        self._save_profile(profile)

        return True

    def update_profile(self, profile: Profile) -> bool:
        """Update agent configuration"""
        if profile.name not in self._profiles:
            return False

        self._profiles[profile.name] = profile

        self._save_profile(profile)

        return True

    def delete_profile(self, name: str) -> bool:
        """Delete agent configuration"""
        if name not in self._profiles:
            return False

        self._profiles.pop(name)

        profile_path: Path = PROFILE_DIR.joinpath(f"{name}.json")
        profile_path.unlink()

        return True

    def get_profile(self, name: str) -> Profile | None:
        """Get agent configuration"""
        return self._profiles.get(name)

    def get_all_profiles(self) -> list[Profile]:
        """Get all agent configurations"""
        return list(self._profiles.values())

    def create_agent(self, profile: Profile, save: bool = False) -> TaskAgent:
        """Create a new agent"""
        #Use timestamp as session number
        now: datetime = datetime.now()
        session_id: str = now.strftime("%Y%m%d_%H%M%S_%f")

        #Create session
        session: Session = Session(
            id=session_id,
            profile=profile.name,
            name="Default session"
        )

        #Create an agent
        agent: TaskAgent = TaskAgent(self, profile, session, save=save)

        #Save session
        self._agents[session.id] = agent

        return agent

    def delete_agent(self, session_id: str) -> bool:
        """Delete agent"""
        if session_id not in self._agents:
            return False

        self._agents.pop(session_id)

        session_path: Path = SESSION_DIR.joinpath(f"{session_id}.json")
        session_path.unlink()

        return True

    def get_agent(self, session_id: str) -> TaskAgent | None:
        """Get the agent"""
        return self._agents.get(session_id)

    def get_all_agents(self) -> list[TaskAgent]:
        """Get all agents"""
        return list(self._agents.values())

    def register_tool(self, tool: LocalTool | AgentTool) -> None:
        """Registration tool"""
        if isinstance(tool, LocalTool):
            self._local_manager.register_tool(tool)
            self._local_schemas[tool.name] = tool.get_schema()
        elif isinstance(tool, AgentTool):
            self._agent_tools[tool.name] = tool

    def get_tool_schemas(self, tools: list[str] | None = None) -> list[ToolSchema]:
        """Get the Schema of all tools"""
        local_schemas: list[ToolSchema] = list(self._local_schemas.values())
        mcp_schemas: list[ToolSchema] = list(self._mcp_schemas.values())
        agent_schemas: list[ToolSchema] = [t.get_schema() for t in self._agent_tools.values()]
        all_schemas: list[ToolSchema] = local_schemas + mcp_schemas + agent_schemas

        if tools is not None:
            tool_schemas: list[ToolSchema] = []
            for schema in all_schemas:
                if schema.name in tools:
                    tool_schemas.append(schema)
            return tool_schemas
        else:
            return all_schemas

    def list_models(self) -> list[str]:
        """Query the list of available models"""
        if not self._models:
            self._models = self.gateway.list_models()

        return self._models

    def execute_tool(self, tool_call: ToolCall) -> ToolResult:
        """Execute a single tool and return the results"""
        if tool_call.name in self._local_schemas:
            result_content: str = self._local_manager.execute_tool(
                tool_call.name,
                tool_call.arguments
            )
        elif tool_call.name in self._mcp_schemas:
            result_content = self._mcp_manager.execute_tool(
                tool_call.name,
                tool_call.arguments
            )
        elif tool_call.name in self._agent_tools:
            agent_tool: AgentTool = self._agent_tools[tool_call.name]
            prompt: str = tool_call.arguments.get("prompt", "")
            result_content = agent_tool.execute(prompt)
        else:
            result_content = ""

        return ToolResult(
            id=tool_call.id,
            name=tool_call.name,
            content=result_content,
            is_error=bool(result_content)
        )

    def stream(self, request: Request) -> Generator[Delta, None, None]:
        """
        流式对话接口，通过生成器（Generator）实时返回 AI 的思考和回复。

        Args:
            request (Request): 请求对象。

        Yields:
            Generator[Delta, None, None]: 一个增量数据（Delta）的生成器。
        """
        return self.gateway.stream(request)
