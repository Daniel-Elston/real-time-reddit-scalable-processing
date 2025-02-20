from __future__ import annotations

import json

from config.pipeline_context import PipelineContext
from config.settings import Params

from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_huggingface import HuggingFaceEmbeddings



class Chunker:
    def __init__(
        self, ctx: PipelineContext,
    ):
        self.ctx = ctx
        self.params: Params = ctx.settings.params

    def _chunk_partition(self, partition):
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.params.chunk_size,
            chunk_overlap=self.params.chunk_overlap
        )
        partition['chunks'] = partition['body_lowercase'].apply(
            lambda text: json.dumps(text_splitter.split_text(text)) if text else "[]"
        )
        return partition

    def transform(self, ddf):
        return ddf.map_partitions(
            self._chunk_partition,
            meta={
                "comment_id": "string",
                "body_lowercase": "string",
                "chunks": "string"
            }
        )


class Embedder:
    def __init__(
        self, ctx: PipelineContext
    ):
        self.ctx = ctx
        self.params: Params = ctx.settings.params

    def _embed_partition(self, partition):
        embedding_model = HuggingFaceEmbeddings(
            model_name=self.params.model_name
        )
        partition['embeddings'] = partition['chunks'].apply(
            lambda chunks: json.dumps(embedding_model.embed_documents(json.loads(chunks)))
        )
        return partition

    def transform(self, ddf):
        return ddf.map_partitions(
            self._embed_partition,
            meta={
                "comment_id": "string",
                "body_lowercase": "string",
                "chunks": "string",
                "embeddings": "string"
            }
        )
