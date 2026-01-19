import asyncio
from concurrent.futures import Future
from typing import Any
from threading import Event, Thread

from mcp.types import Tool as McpToolType
from fastmcp import Client
from fastmcp.client.client import MCPConfig, CallToolResult

from .utility import load_json
from .object import ToolSchema


class McpManager:
    """MCP Manager: Responsible for MCP tool management and execution"""

    config_path: str = "mcp_config.json"

    def __init__(self) -> None:
        """Constructor"""
        self.client: Client | None = None

        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: Thread | None = None

        self.shutdown_future: asyncio.Future | None = None
        self.started_event: Event = Event()

        self.server_name: str = ""      #When only one MCP service is configured, you need to splice the server name prefix for the tool

        config_data: dict[str, Any] = load_json(self.config_path)

        if config_data:
            mcp_config: MCPConfig = MCPConfig.from_dict(config_data)

            if len(mcp_config.mcpServers) == 1:
                self.server_name = list(mcp_config.mcpServers.keys())[0]

            self.client = Client(mcp_config)

            self.thread = Thread(target=self._run_loop, daemon=True)
            self.thread.start()
        else:
            self.started_event.set()

    def _run_loop(self) -> None:
        """Run event loop in background thread"""
        #Create a new event loop
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        #Create a future that closes the event loop
        self.shutdown_future = self.loop.create_future()

        #Run main loop
        async def main_loop() -> None:
            """Main loop"""
            if not self.client:
                return

            #Start MCP service
            async with self.client:
                #Set startup event
                self.started_event.set()

                #Wait for closing event
                if self.shutdown_future:
                    await self.shutdown_future

        self.loop.run_until_complete(main_loop())

    def __del__(self) -> None:
        """Safely shut down background threads and event loops"""
        if self.loop and self.shutdown_future:
            self.loop.call_soon_threadsafe(self.shutdown_future.set_result, None)

        if self.thread:
            self.thread.join()

        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)

    def list_tools(self) -> list[ToolSchema]:
        """List all available MCP tools"""
        #Wait for the background service to start
        self.started_event.wait()

        #If the client does not exist (no configuration file), an empty list is returned
        if not self.client or not self.loop:
            return []

        #Execute tasks in the background event loop
        async def _async_list_tools() -> list[ToolSchema]:
            """Asynchronous functions for running in an event loop"""
            try:
                assert self.client is not None

                #List all MCP tools
                mcp_tools: list[McpToolType] = await self.client.list_tools()

                #Convert data format and return
                tool_schemas: list[ToolSchema] = []

                for mcp_tool in mcp_tools:
                    if self.server_name:
                        name: str = f"{self.server_name}_{mcp_tool.name}"
                    else:
                        name = mcp_tool.name

                    tool_schema: ToolSchema = ToolSchema(
                        name=name,
                        description=mcp_tool.description or "",
                        parameters=mcp_tool.inputSchema
                    )
                    tool_schemas.append(tool_schema)

                return tool_schemas
            except Exception as e:
                print(f"Failed to list MCP tools: {e}")
                return []

        future: Future[list[ToolSchema]] = asyncio.run_coroutine_threadsafe(
            coro=_async_list_tools(),
            loop=self.loop
        )
        return future.result()

    def execute_tool(self, tool_name: str, arguments: dict[str, Any]) -> str:
        """Execute MCP tool"""
        #If the client does not exist (no configuration file), an error message is returned
        if not self.client or not self.loop:
            return ""

        #Wait for the background service to start
        self.started_event.wait()

        #If the tool name contains a server name prefix, remove the prefix
        if self.server_name:
            tool_name = tool_name.replace(self.server_name + "_", "")

        #Execute tasks in the background event loop
        async def _async_execute_tool() -> str:
            """Asynchronous functions for running in an event loop"""
            try:
                assert self.client is not None

                #Execute MCP tool call
                result: CallToolResult = await self.client.call_tool(
                    tool_name, arguments
                )

                return str(result)
            except Exception as e:
                return f"Error executing MCP tool '{tool_name}': {str(e)}"

        future: Future[str] = asyncio.run_coroutine_threadsafe(
            coro=_async_execute_tool(),
            loop=self.loop
        )
        return future.result()
