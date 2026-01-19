import numpy as np
from numpy.typing import NDArray

from sentence_transformers import SentenceTransformer

from vnag.embedder import BaseEmbedder


class SentenceEmbedder(BaseEmbedder):
    """SentenceTransformer local model adapter"""

    def __init__(self, model_name: str = "BAAI/bge-large-en-v1.5") -> None:
        """Initialize the SentenceTransformer model"""
        self.model: SentenceTransformer = SentenceTransformer(model_name)

    def encode(self, texts: list[str]) -> NDArray[np.float32]:
        """Encode text as vector"""
        return self.model.encode(texts)
