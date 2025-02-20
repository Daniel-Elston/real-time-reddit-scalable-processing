from __future__ import annotations

import logging
import os
import dask.dataframe as dd
import pandas as pd

from config.pipeline_context import PipelineContext
from config.settings import Config, Params
from config.paths import Paths


class DaskDataHandler:
    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx
        self.config: Config = ctx.settings.config
        self.params: Params = ctx.settings.params
        self.paths: Paths = ctx.paths


    def _read_partitions(self, input_path):
        file_store = os.path.join(input_path, '*.parquet')
        ddf = dd.read_parquet(
            file_store,
            engine="pyarrow",
            gather_statistics=False
        )
        return ddf
    
    def _write_partitions(self, ddf, output_path):
        ddf.to_parquet(
            path=output_path,
            overwrite=True,
        )
    
    def _log_ddf_comparison(self, ddf_raw, ddf_processed):
        # Compute basic stats
        raw_rows, processed_rows = ddf_raw.shape[0].compute(), ddf_processed.shape[0].compute()
        raw_partitions, processed_partitions = ddf_raw.npartitions, ddf_processed.npartitions

        # Schema difference
        raw_schema = set(ddf_raw.dtypes.to_dict().keys())
        processed_schema = set(ddf_processed.dtypes.to_dict().keys())

        # Memory Estimates
        raw_mem = ddf_raw.memory_usage(deep=True).sum().compute() / 1e6
        processed_mem = ddf_processed.memory_usage(deep=True).sum().compute() / 1e6

        # Log
        logging.info(f"{"=" * 20} DATAFRAME COMPARISON {"=" * 20}")
        logging.info(f"Partitions: Before = {raw_partitions} | After = {processed_partitions}")
        logging.info(f"Schema Changes: Raw Schema: {raw_schema} | Processed Schema: {processed_schema}")
        logging.info(f"Raw Row Count: {raw_rows} | Processed Row Count: {processed_rows}")
        logging.info(f"Memory Usage Estimate: Before = {raw_mem:.2f} MB | After = {processed_mem:.2f} MB")
        logging.info(f"{"=" * 62}\n")


    def read_result(self, part_n):
        try:
            read_path = f'/home/delst-wsl/wsl-workspace/live-reddit-sentiment/data/dask/ddf_p.parquet/part.{part_n}.parquet'
            result_path = 'reports/result.xlsx'
            df = pd.read_parquet(
                read_path,
                engine='pyarrow'
            )
            df.to_excel(
                result_path,
                index=False
            )
            logging.info(f"Saving reult to ``{result_path}``. Result sample data:\n{df}")
        except Exception as e:
            logging.error(f"Failed to read result. Ensure part_n is within {range((self.params.npartitions-1))}. Error {e}")