import numpy as np
from numpy.typing import NDArray

from openai import OpenAI
from openai.types.create_embedding_response import CreateEmbeddingResponse

from vnag.embedder import BaseEmbedder


class OpenaiEmbedder(BaseEmbedder):
    """OpenAI Embedding API Adapter"""

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model_name: str = "qwen/qwen3-embedding-8b",
        batch_size: int = 100,
    ) -> None:
        """初始化 OpenAI Embedding

        参数:
            api_key: OpenAI API Key
            base_url: API 基础 URL
            model_name: 模型名称（默认 qwen/qwen3-embedding-8b）
            batch_size: 批量大小（建议不超过 100）
        """
        #Set model name
        self.model_name: str = model_name
        #Set batch size
        self.batch_size: int = batch_size

        #Create an OpenAI client (retries are handled automatically by the SDK)
        self.client: OpenAI = OpenAI(
            api_key=api_key,
            base_url=base_url
        )

    def encode(self, texts: list[str]) -> NDArray[np.float32]:
        """Encode text as vector"""
        embeddings: list[list[float]] = []

        #Batch encoding
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            batch_embeddings = self._encode_batch(batch)
            embeddings.extend(batch_embeddings)

        return np.array(embeddings, dtype=np.float32)

    def _encode_batch(self, batch: list[str]) -> list[list[float]]:
        """Batch encoding (automatically retried by OpenAI SDK)"""
        #Call embeddings API using OpenAI SDK
        response: CreateEmbeddingResponse = self.client.embeddings.create(
            model=self.model_name,
            input=batch
        )

        #Extract embedding vector
        return [item.embedding for item in response.data]
