import time

import dashscope
from dashscope import TextEmbedding
import numpy as np
from numpy.typing import NDArray

from vnag.embedder import BaseEmbedder


class DashscopeEmbedder(BaseEmbedder):
    """Alibaba Cloud DashScope Embedding API Adapter"""

    def __init__(
        self,
        api_key: str,
        model_name: str = "text-embedding-v3",
        batch_size: int = 10,
        max_retries: int = 3
    ) -> None:
        """初始化 DashScope Embedding

        参数:
            api_key: DashScope API Key
            model_name: 模型名称
            batch_size: 批量大小（DashScope 限制最大 10）
            max_retries: 最大重试次数
        """
        #Set API Key
        dashscope.api_key = api_key
        #Set model name
        self.model_name: str = model_name
        #Set batch size
        self.batch_size: int = min(batch_size, 10)
        #Set the maximum number of retries
        self.max_retries: int = max_retries

    def encode(self, texts: list) -> NDArray[np.float32]:
        """Encode text as vector"""
        embeddings: list = []

        #Batch encoding
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            batch_embeddings = self._encode_batch_with_retry(batch)
            embeddings.extend(batch_embeddings)

        return np.array(embeddings, dtype=np.float32)

    def _encode_batch_with_retry(self, batch: list[str]) -> list[list[float]]:
        """Batch encoding with retries"""
        last_error: str = ""

        #Retry encoding
        for attempt in range(self.max_retries):
            try:
                response: dashscope.TextEmbeddingResponse = TextEmbedding.call(
                    model=self.model_name,
                    input=batch
                )

                if response.status_code == 200:
                    return [
                        item['embedding']
                        for item in response.output['embeddings']
                    ]
                else:
                    last_error = (
                        f"status_code={response.status_code}, "
                        f"message={response.message}"
                    )

            except Exception as e:
                last_error = str(e)

            if attempt < self.max_retries - 1:
                wait_time = 2 ** attempt
                time.sleep(wait_time)

        raise RuntimeError(
            f"DashScope API call failed (retries {self.max_retries} times):"
            f"{last_error}"
        )
