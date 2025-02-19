from __future__ import annotations

import logging
from config.pipeline_context import PipelineContext
from config.settings import Params

from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_huggingface import HuggingFaceEmbeddings
import json


class Chunker:
    def __init__(self, ctx: PipelineContext, text_splitter: RecursiveCharacterTextSplitter):
        self.ctx = ctx
        self.params: Params = ctx.settings.params
        self.text_splitter = text_splitter

    def _chunk_partition(self, partition):
        partition['chunks'] = partition['body_lowercase'].apply(
            lambda text: json.dumps(self.text_splitter.split_text(text)) if text else "[]"
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
    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx
        self.params: Params = ctx.settings.params

    def _embed_partition(self, partition):
        embedding_model = HuggingFaceEmbeddings(model_name=self.params.model_name)
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




# class Chunker:
#     def __init__(
#         self, ctx: PipelineContext,
#         text_splitter: RecursiveCharacterTextSplitter
#     ):
#         self.ctx = ctx
#         self.params: Params = ctx.settings.params
#         self.text_splitter = text_splitter
    
#     def _chunk_partition(self, partition):
#         partition['chunks'] = partition['body_lowercase'].apply(
#             lambda text: self.text_splitter.split_text(text) if text else []
#             # lambda text: text_splitter.split_text(text) if text else []
#         )
#         return partition
    
#     def transform(self, ddf):
#         # meta = ddf._meta.copy()
#         # meta['chunks'] = object
#         return ddf.map_partitions(
#             self._chunk_partition,
#             meta={
#                 "comment_id": "object",
#                 "body_lowercase": "object",
#                 "chunks": "string"
#             }
#             # meta=meta
#         )


class Embedder:
    def __init__(
        self, ctx: PipelineContext,
        # embedding_model: HuggingFaceEmbeddings
    ):
        self.ctx = ctx
        self.params: Params = ctx.settings.params
        # self.embedding_model = embedding_model
    
    def _embed_partition(self, partition):
        embedding_model = HuggingFaceEmbeddings(
            model_name=self.params.model_name
        )
        partition['embeddings'] = partition['chunks'].apply(
            # lambda chunks: self.embedding_model.embed_documents(chunks)
            lambda chunks: embedding_model.embed_documents(chunks)
        )
        return partition
    
    def transform(self, ddf):
        return ddf.map_partitions(
            self._embed_partition,
            meta={
                "comment_id": "object",
                "body_lowercase": "object",
                "chunks": "string",
                "embeddings": "string"
            }
        )