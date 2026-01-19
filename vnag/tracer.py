import sys
from pathlib import Path

from loguru import logger

from .object import Request, Delta, Message, ToolCall, ToolResult
from .utility import get_folder_path


#Use module-level variables to record whether it is configured
_logger_configured = False


def _configure_logger() -> None:
    """Configure vnag-specific logger and only execute it once"""
    global _logger_configured

    if _logger_configured:
        return

    #Remove loguru's default handler (ID=0) to avoid DEBUG log output to stderr
    try:
        logger.remove(0)
    except ValueError:
        pass

    #Add stdout handler to only process logs marked with vnag_module
    logger.add(
        sys.stdout,
        level="INFO",
        filter=lambda record: record["extra"].get("vnag_module") is True,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{extra[profile_name]}</cyan> | "
            "<level>{message}</level>"
        )
    )

    _logger_configured = True


class LogTracer:
    """
    使用 loguru 库记录日志信息的追踪器。
    """

    def __init__(self, session_id: str, profile_name: str) -> None:
        """Initialize and configure logger"""
        self.session_id: str = session_id
        self.profile_name: str = profile_name

        #Configure global handler
        _configure_logger()

        #Bind context, add vnag_module tag for isolation
        self.logger = logger.bind(
            session_id=self.session_id,
            profile_name=self.profile_name,
            vnag_module=True  #Used for filter recognition to avoid conflicts with other libraries
        )

        log_path: Path = get_folder_path("log")
        file_name: str = f"{self.session_id}.log"
        file_path: Path = log_path.joinpath(file_name)

        logger.add(
            file_path,
            level="DEBUG",
            filter=lambda record: (
                record["extra"].get("vnag_module") is True                  #Make sure it is the log of the vnag module
                and record["extra"].get("session_id") == self.session_id    #Make sure it is the log of the current session
            ),
            format=(
                "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
                "{level: <8} | "
                "{extra[profile_name]} | "
                "{message}"
            )
        )

    def on_llm_start(self, request: Request) -> None:
        """Logging LLM call start event"""
        self.logger.info(f"LLM -> Request sent (model: {request.model})")
        self.logger.debug(f"LLM -> Complete request data: {request.model_dump_json(indent=4)}")

    def on_llm_delta(self, delta: Delta) -> None:
        """Runs when LLM returns a streaming chunk (Delta)"""
        #Note: Frequent calls may generate a large amount of logs. The default is TRACE level
        self.logger.trace(f"LLM -> Received data block: {delta.model_dump_json(indent=4)}")

    def on_llm_end(self, message: Message) -> None:
        """Logging the LLM call end event"""
        self.logger.info("LLM <- response received")
        self.logger.debug(f"LLM <- Complete response data: {message.model_dump_json(indent=4)}")

    def on_tool_start(self, tool_call: ToolCall) -> None:
        """Logging tool call start event"""
        self.logger.info(f"Tool -> Start execution: {tool_call.name}")
        self.logger.debug(f"Tool -> Call parameters: {tool_call.arguments}")

    def on_tool_end(self, result: ToolResult) -> None:
        """Record tool call end event"""
        self.logger.info(f"Tool <- Execution completed: {result.name}")
        self.logger.debug(f"Tools <- Return results: {result.content}")
