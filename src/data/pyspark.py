from __future__ import annotations

import logging

from config.pipeline_context import PipelineContext
from config.settings import Config
from config.paths import Paths

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

class PySparkProcessor:
    def __init__(
        self, ctx: PipelineContext,
        app_name: str,
    ):
        self.paths: Paths = ctx.paths
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    @staticmethod
    def define_schema():
        return StructType([
            StructField("comment_id", StringType(), True),
            StructField("body", StringType(), True),
            StructField("author", StringType(), True),
            StructField("subreddit", StringType(), True),
            StructField("created_utc", TimestampType(), True),
            StructField("score", IntegerType(), True),
            StructField("parent_id", StringType(), True)
        ])

    def process_and_save(self, batch_data):
        """Process batch data with PySpark and save to Parquet."""
        schema = self.define_schema()
        df = self.spark.createDataFrame(
            batch_data,
            schema=schema
        )
        self._show_metadata(df)
        df.write.mode("overwrite").parquet(str(self.paths.get_path("spark-processed")))

    def _show_metadata(self, df):
        df.show(truncate=False)
        df.printSchema()
        
        # logging.info(f"DataFrame Partition Count: {df.rdd.getNumPartitions()}")
        # logging.info(f"DataFrame Memory Usage: {df.storageLevel}")
        # logging.debug(f"Saving data to Parquet. N rows: {df.count()}")

