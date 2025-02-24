from __future__ import annotations

import logging

from dask.distributed import Client
from dask.distributed import LocalCluster

from config.paths import Paths
from config.pipeline_context import PipelineContext
from config.settings import Config
from config.settings import Params
from src.base.base_pipeline import BasePipeline
from src.data.dask_data_handler import DaskDataHandler
from src.data.nlp import Chunker
from src.data.nlp import Embedder


class DaskPipeline(BasePipeline):
    def __init__(self, ctx: PipelineContext):
        """
        Summary
        ----------
        Dask-based efficient distributed computation pipeline for parallel text processing and embedding generation.

        Extended Summary
        ----------
        This pipeline is optimized for distributed execution, minimising memory overhead.
        Pipeline can be adapted for different text processing and embedding models.

        1. **Initialize Dask Cluster & Client**
        - A `LocalCluster` is created with `n_workers` and `threads_per_worker` to manage parallel processing.
        - A `Client` is used to distribute computation across workers dynamically.

        2. **Read & Partition Input Data**
        - Input data is read from Parquet files.
        - Data is split into `self.params.npartitions` partitions to optimize performance.

        3. **Text Processing Pipeline**
        - **Chunking**: The `Chunker` class splits text into smaller segments.
        - **Embedding**: The `Embedder` class generates vector embeddings from the chunks.

        4. **Schema Normalization & Storage**
        - **Schema Fix**: Ensures `chunks` and `embeddings` are stored as JSON strings to avoid Parquet serialisation errors.
        - **Repartitioning**: Data is evenly distributed across partitions before saving.
        - **Save to Parquet**: Processed data is written to `self.output_path` in Parquet format.

        Outputs
        ----------
        - A processed Parquet file, containing:
            - `comment_id`: Unique identifier for each comment.
            - `body_lowercase`: The original comment in lowercase.
            - `chunks`: Text split into smaller segments.
            - `embeddings`: Vector representations of the text chunks.

        Parameters
        ----------
        ctx : PipelineContext
            Context object containing global configs, paths, and runtime parameters.
        """

        super().__init__(ctx)
        self.ctx = ctx
        self.config: Config = ctx.settings.config
        self.params: Params = ctx.settings.params
        self.paths: Paths = ctx.paths

        self.dd_handler = DaskDataHandler(ctx=self.ctx)

        self.input_path = str(self.paths.get_path("dask-input"))
        self.output_path = str(self.paths.get_path("dask-output"))

    def distributed_process_and_store(self):
        # Form dask cluster and client
        cluster = LocalCluster(
            n_workers=self.params.n_workers,
            threads_per_worker=self.params.threads_per_worker
        )
        client = Client(cluster)
        logging.info(f"Cluster initialised with {self.params.n_workers} workers and {self.params.threads_per_worker} threads per worker.")
        with cluster, client:
            logging.info("Distributed processing started...\n")
            # Read from parquet
            ddf_raw = self.dd_handler._read_partitions(self.input_path)
            ddf_to_process = ddf_raw[["comment_id", "body_lowercase"]]

            # Define transformations
            chunker = Chunker(ctx=self.ctx)
            embedder = Embedder(ctx=self.ctx)

            # Apply transformations
            ddf_chunked = chunker.transform(ddf_to_process)
            ddf_embedded = embedder.transform(ddf_chunked)

            # Persist
            ddf_processed = ddf_embedded.persist()

            # Write to Parquet
            ddf_processed = ddf_processed.repartition(
                npartitions=self.params.npartitions
            )
            self.dd_handler._write_partitions(ddf_processed, self.output_path)
            self.dd_handler._log_ddf_comparison(ddf_to_process, ddf_processed)

        # View resultant dataset
        self.dd_handler.read_result(part_n=2)
