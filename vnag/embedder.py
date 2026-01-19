from abc import ABC, abstractmethod

import numpy as np
from numpy.typing import NDArray


class BaseEmbedder(ABC):
    """Abstract base class for Embedding"""

    @abstractmethod
    def encode(self, texts: list[str]) -> NDArray[np.float32]:
        """Encode text list into vector"""
        pass
