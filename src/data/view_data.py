from __future__ import annotations
import logging
import os
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from langchain.text_splitter import RecursiveCharacterTextSplitter
from config.pipeline_context import PipelineContext
from config.settings import Config, Params
from config.paths import Paths
from src.base.base_pipeline import BasePipeline
from src.data.nlp import Chunker, Embedder
import pyarrow as pa
import json
import pandas as pd


class ViewData(BasePipeline):
    def __init__(self, ctx: PipelineContext):
        super().__init__(ctx)
        self.ctx = ctx
        self.config: Config = ctx.settings.config
        self.params: Params = ctx.settings.params
        self.paths: Paths = ctx.paths


    # def read_partitions(self):
    #     # file_store = os.path.join(self.paths.get_path("dask-read"), '*.parquet')
    #     file_store = os.path.join("/home/delst-wsl/wsl-workspace/live-reddit-sentiment/data/dask/", '*.parquet')
    #     ddf = dd.read_parquet(
    #         file_store,
    #         engine="pyarrow",
    #         gather_statistics=False
    #     )
    #     print(ddf)
    #     return ddf


    def read_partitions(self):
        df = pd.read_parquet('/home/delst-wsl/wsl-workspace/live-reddit-sentiment/data/dask/ddf_p.parquet/part.1.parquet', engine='pyarrow')
        print(df)
        df.to_excel('test.xlsx', index=False)
        # file_store = os.path.join(self.paths.get_path("dask-read"), '*.parquet')
        # for partition in dd.read_parquet(
        #     file_store,
        #     engine="pyarrow",
        #     gather_statistics=False
        # ).compute():
        #     print(partition)
            
    def run(self):
        self.read_partitions()