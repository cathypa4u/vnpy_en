import traceback
from typing import Any

from ..agent import TaskAgent
from ..constant import Role
from .qt import QtCore


class StreamSignals(QtCore.QObject):
    """
    定义StreamWorker可以发出的信号
    """
    #Streaming response block (content content)
    delta: QtCore.Signal = QtCore.Signal(str)

    #Streaming response blocks (thinking content)
    thinking: QtCore.Signal = QtCore.Signal(str)

    #End of streaming response
    finished: QtCore.Signal = QtCore.Signal()

    #Streaming response error
    error: QtCore.Signal = QtCore.Signal(str)

    #Title generation completed
    title: QtCore.Signal = QtCore.Signal(str)


class StreamWorker(QtCore.QRunnable):
    """
    在线程池中处理流式网关请求的Worker
    """
    def __init__(self, agent: TaskAgent, prompt: str) -> None:
        """Constructor"""
        super().__init__()

        self.agent: TaskAgent = agent
        self.prompt: str = prompt
        self.signals: StreamSignals = StreamSignals()
        self.stopped: bool = False

    def stop(self) -> None:
        """Stop streaming request"""
        self.stopped = True

    def _safe_emit(self, signal: QtCore.SignalInstance, *args: Any) -> None:
        """Safely signal that the object has been deleted"""
        try:
            signal.emit(*args)
        except RuntimeError:
            #Signal object has been deleted (window closed), ignored
            pass

    def run(self) -> None:
        """Process data streams"""
        try:
            for delta in self.agent.stream(self.prompt):
                #User manual stop
                if self.stopped:
                    #Stop streaming generation and save part of the generated content
                    self.agent.abort_stream()
                    break
                #Received thinking data block
                if delta.thinking:
                    self._safe_emit(self.signals.thinking, delta.thinking)
                #Receive content data block
                if delta.content:
                    self._safe_emit(self.signals.delta, delta.content)

        except Exception:
            #Stop streaming generation and save part of the generated content
            self.agent.abort_stream()

            error_msg: str = traceback.format_exc()
            self._safe_emit(self.signals.error, error_msg)
        finally:
            self._safe_emit(self.signals.finished)

        #After the streaming response is complete, check if you need to automatically generate headers
        if not self.stopped and self._should_generate_title():
            try:
                title: str = self.agent.generate_title(max_length=10)
                if title:
                    self._safe_emit(self.signals.title, title)
            except Exception:
                error_msg = traceback.format_exc()
                self._safe_emit(self.signals.error, error_msg)

    def _should_generate_title(self) -> bool:
        """Determine whether you need to automatically generate a title"""
        #Check if it is still the default name
        if self.agent.name != "Default session":
            return False

        #Check whether the first conversation is completed (system message + user message + assistant message = 3)
        if len(self.agent.messages) < 3:
            return False

        #Make sure the last one is an assistant message
        if self.agent.messages[-1].role != Role.ASSISTANT:
            return False

        return True
