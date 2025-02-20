from __future__ import annotations

import logging

from config.pipeline_context import PipelineContext
from config.settings import Config
from config.paths import Paths

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F


class PySparkProcessor:
    """Summary: Handles PySpark DataFrame schema definition and processing"""
    def __init__(
        self, ctx: PipelineContext,
        app_name: str,
    ):
        self.config: Config = ctx.settings.config
        self.paths: Paths = ctx.paths
        self.output_dir = self.paths.get_path("spark-processed")
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.host", "10.255.255.254") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .getOrCreate()


    def process_and_save(self, batch_data):
        """Process batch data with PySpark and save to Parquet."""
        schema = self.define_schema()
        df = self.spark.createDataFrame(
            batch_data,
            schema=schema
        )
        processed_df = self.preprocess(df)
        processed_df.show(truncate=True)
        if processed_df.count() == 0:
            self._log_dataframe_info(df)
            return
        else:
            processed_df.write.mode(self.config.spark_write_mode).parquet(str(self.output_dir))
            processed_df.unpersist()
            self._log_dataframe_info(df)

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
        
    def preprocess(self, df):
        return (
            df.withColumn(
                "body_lowercase",
                F.lower(F.col("body"))
            )
            .withColumn(
                "comment_length",
                F.length(F.col("body_lowercase"))
            )
            .withColumn(
                "is_short_comment", 
                F.when(F.length(F.col("body_lowercase")) < self.config.short_comment_threshold, True).otherwise(False)
            )
        )

    def _log_dataframe_info(self, df):
        """Enhanced logging for DataFrame details"""
        logging.info(f"DataFrame Schema: {df.printSchema()}")
        logging.info(f"DataFrame Partition Count: {df.rdd.getNumPartitions()}")
        logging.info(f"Number of Rows: {df.count()}")
        logging.info(f"Estimated Memory Usage: {df.storageLevel}")
