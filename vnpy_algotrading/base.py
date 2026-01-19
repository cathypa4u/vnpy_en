from enum import Enum


EVENT_ALGO_LOG = "eAlgoLog"
EVENT_ALGO_UPDATE = "eAlgoUpdate"


APP_NAME = "AlgoTrading"


class AlgoStatus(Enum):
    """Algorithm status"""

    RUNNING = "Run"
    PAUSED = "Pause"
    STOPPED = "Stop"
    FINISHED = "Finish"
