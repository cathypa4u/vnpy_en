from pathlib import Path
from typing import Any
from uuid import uuid5, NAMESPACE_DNS

import numpy as np
from numpy.typing import NDArray
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    CollectionDescription
)

from vnag.object import Segment
from vnag.utility import get_folder_path
from vnag.vector import BaseVector
from vnag.embedder import BaseEmbedder


class QdrantVector(BaseVector):
    """Vector storage implemented based on Qdrant"""

    def __init__(
        self,
        name: str,
        embedder: BaseEmbedder
    ) -> None:
        """Initialize Qdrant vector storage"""
        self.persist_dir: Path = get_folder_path("qdrant_db")
        self.embedder: BaseEmbedder = embedder
        self.collection_name: str = name

        #Get actual dimensions by encoding sample
        self.dimension: int = embedder.encode(["qdrant"]).shape[1]

        self.client: QdrantClient = QdrantClient(
            path=str(self.persist_dir)
        )

        #Create or get a collection
        self._init_collection()

    def _init_collection(self) -> None:
        """Initialize or get a collection"""
        collections: list[CollectionDescription] = self.client.get_collections().collections
        collection_names: list[str] = [col.name for col in collections]

        if self.collection_name not in collection_names:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=self.dimension,
                    distance=Distance.COSINE
                )
            )

    def add_segments(self, segments: list[Segment]) -> list[str]:
        """Add a batch of docblocks to Qdrant"""
        if not segments:
            return []

        texts: list[str] = [seg.text for seg in segments]

        embeddings_np: NDArray[np.float32] = self.embedder.encode(texts)

        #Generate a unique ID (as a string, for return)
        string_ids: list[str] = [
            f"{seg.metadata['source']}_{seg.metadata['chunk_index']}"
            for seg in segments
        ]

        #Build Qdrant Points
        points: list[PointStruct] = []
        for string_id, segment, embedding in zip(
            string_ids,
            segments,
            embeddings_np,
            strict=True
        ):
            #Convert string ID to UUID (required by Qdrant)
            uuid_id: str = str(uuid5(NAMESPACE_DNS, string_id))

            #Build payload (contains text, metadata, and raw string ID)
            payload: dict[str, Any] = segment.metadata.copy()
            payload["text"] = segment.text
            payload["string_id"] = string_id

            point: PointStruct = PointStruct(
                id=uuid_id,
                vector=embedding.tolist(),
                payload=payload
            )
            points.append(point)

        #Insert in batches to avoid timeout caused by excessive data in a single batch
        db_batch_size: int = 1000
        for i in range(0, len(points), db_batch_size):
            self.client.upsert(
                collection_name=self.collection_name,
                points=points[i:i + db_batch_size]
            )

        return string_ids

    def retrieve(self, query_text: str, k: int = 5) -> list[Segment]:
        """Retrieve similar document chunks from Qdrant based on query text"""
        if self.count == 0:
            return []

        query_embedding_np: NDArray[np.float32] = self.embedder.encode(
            [query_text]
        )

        #Perform a search
        search_result = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_embedding_np[0].tolist(),
            limit=k
        )

        #Build return results
        retrieved_results: list[Segment] = []
        for point in search_result:
            if point.payload:
                payload: dict[str, Any] = point.payload
            else:
                payload = {}
            text: str = payload.pop("text", "")

            #Convert payload to metadata
            safe_meta: dict[str, str] = {
                str(key): str(value) for key, value in payload.items()
            }

            #Qdrant returns score (cosine similarity, the greater the similarity, the greater the similarity)
            #ChromaDB returns distance (cosine distance, the smaller the more similar)
            #For consistency, convert Qdrant score to distance
            distance: float = 1.0 - point.score
            segment: Segment = Segment(
                text=text,
                metadata=safe_meta,
                score=distance
            )
            retrieved_results.append(segment)

        return retrieved_results

    def delete_segments(self, segment_ids: list[str]) -> bool:
        """Delete one or more documents from Qdrant based on a list of IDs"""
        if not segment_ids:
            return True

        try:
            #Convert string ID to UUID
            uuid_ids: list[str] = [
                str(uuid5(NAMESPACE_DNS, sid)) for sid in segment_ids
            ]
            self.client.delete(
                collection_name=self.collection_name,
                points_selector=uuid_ids
            )
            return True
        except Exception:
            return False

    def get_segments(self, segment_ids: list[str]) -> list[Segment]:
        """Get the original document block directly from Qdrant based on the ID list"""
        if not segment_ids:
            return []

        #Convert string ID to UUID
        uuid_ids: list[str] = [
            str(uuid5(NAMESPACE_DNS, sid)) for sid in segment_ids
        ]

        points = self.client.retrieve(
            collection_name=self.collection_name,
            ids=uuid_ids
        )

        results: list[Segment] = []
        for point in points:
            if point.payload:
                payload: dict[str, Any] = point.payload
            else:
                payload = {}
            text: str = payload.pop("text", "")

            safe_meta: dict[str, str] = {
                str(key): str(value) for key, value in payload.items()
            }

            segment: Segment = Segment(text=text, metadata=safe_meta)
            results.append(segment)

        return results

    @property
    def count(self) -> int:
        """Get the total number of documents in the vector store"""
        collection_info = self.client.get_collection(self.collection_name)

        if collection_info.points_count:
            count: int = collection_info.points_count
        else:
            count = 0

        return count
