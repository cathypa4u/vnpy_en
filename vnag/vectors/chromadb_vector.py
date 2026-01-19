from pathlib import Path
from typing import Any
from collections.abc import Mapping

import numpy as np
from numpy.typing import NDArray
import chromadb
from chromadb.api import ClientAPI
from chromadb.api.models.Collection import Collection
from chromadb.api.types import GetResult, QueryResult
from chromadb.config import Settings as ChromaSettings

from vnag.object import Segment
from vnag.utility import get_folder_path
from vnag.vector import BaseVector
from vnag.embedder import BaseEmbedder


class ChromaVector(BaseVector):
    """Vector storage implemented based on ChromaDB"""

    def __init__(
        self,
        name: str,
        embedder: BaseEmbedder
    ) -> None:
        """Initialize the ChromaDB vector store"""
        self.persist_dir: Path = get_folder_path("chroma_db").joinpath(name)
        self.embedder: BaseEmbedder = embedder

        self.client: ClientAPI = chromadb.PersistentClient(
            path=str(self.persist_dir),
            settings=ChromaSettings(anonymized_telemetry=False),
        )

        self.collection: Collection = self.client.get_or_create_collection(
            name="segments", metadata={"hnsw:space": "cosine"}
        )

    def add_segments(self, segments: list[Segment]) -> list[str]:
        """Add a batch of document chunks to ChromaDB"""
        if not segments:
            return []

        texts: list[str] = [seg.text for seg in segments]
        metadatas: list[Mapping[str, Any]] = [seg.metadata for seg in segments]

        embeddings_np: NDArray[np.float32] = self.embedder.encode(
            texts
        )

        #Generate a unique ID using a combination of source (absolute path) and chunk_index
        ids: list[str] = [
            f"{seg.metadata['source']}_{seg.metadata['chunk_index']}"
            for seg in segments
        ]

        #Write in batches to avoid triggering Chroma's single batch limit (approximately 5461)
        db_batch_size: int = 3000
        for i in range(0, len(ids), db_batch_size):
            j = i + db_batch_size
            self.collection.upsert(
                embeddings=embeddings_np[i:j],
                documents=texts[i:j],
                metadatas=metadatas[i:j],
                ids=ids[i:j],
            )

        return ids

    def retrieve(self, query_text: str, k: int = 5) -> list[Segment]:
        """Retrieve similar document chunks from ChromaDB based on query text"""
        if self.count == 0:
            return []

        query_embedding_np: NDArray[np.float32] = self.embedder.encode(
            [query_text]
        )

        results: QueryResult = self.collection.query(
            query_embeddings=query_embedding_np, n_results=k
        )

        documents: list[list[str]] | None = results.get("documents")
        metadatas: list[list[Mapping[str, Any]]] | None = results.get("metadatas")
        distances: list[list[float]] | None = results.get("distances")

        if not (documents and metadatas and distances and documents[0]):
            return []

        retrieved_results: list[Segment] = []
        for text, meta, dist in zip(
            documents[0], metadatas[0], distances[0], strict=True
        ):
            #The metadata dictionary returned by ChromaDB may contain non-string values,
            #And Segment requires dict[str, str]. Conversions are done here to ensure type safety
            safe_meta: dict[str, str] = {
                str(key): str(value) for key, value in meta.items()
            }

            segment: Segment = Segment(text=text, metadata=safe_meta, score=dist)
            retrieved_results.append(segment)

        return retrieved_results

    def delete_segments(self, segment_ids: list[str]) -> bool:
        """Delete one or more documents from ChromaDB based on a list of IDs"""
        if not segment_ids:
            return True
        try:
            self.collection.delete(ids=segment_ids)
            return True
        except Exception:
            #Consider adding logging here
            return False

    def get_segments(self, segment_ids: list[str]) -> list[Segment]:
        """Get the original document chunk directly from ChromaDB based on the ID list"""
        if not segment_ids:
            return []

        results: GetResult = self.collection.get(ids=segment_ids)

        documents: list[str] | None = results.get("documents")
        metadatas: list[Mapping[str, Any]] | None = results.get("metadatas")

        if not (documents and metadatas):
            return []

        return [
            Segment(
                text=text,
                metadata={str(key): str(value) for key, value in meta.items()},
            )
            for text, meta in zip(documents, metadatas, strict=True)
        ]

    @property
    def count(self) -> int:
        """Get the total number of documents in the vector store"""
        count: int = self.collection.count()
        return count
