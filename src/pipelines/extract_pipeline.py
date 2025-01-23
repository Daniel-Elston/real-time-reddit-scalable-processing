from __future__ import annotations

import logging

import threading

from config.pipeline_context import PipelineContext
from utils.execution import TaskExecutor
from config.settings import Config
from config.paths import Paths


from src.etl.extract import Extractor
from src.etl.produce import Producer
from src.etl.consume import Consumer

from src.data.pyspark import PySparkProcessor
from src.pipelines.life_cycle_manager import LifeCycleManager


class ExtractionPipeline:
    def __init__(
        self, ctx: PipelineContext,
        exe: TaskExecutor,
    ):
        self.ctx = ctx
        self.exe = exe
        self.config: Config = ctx.settings.config
        self.paths: Paths = ctx.paths

        self.producer = Producer(ctx=ctx)
        self.consumer_instance = Consumer(ctx=ctx)
        self.extractor = Extractor(
            ctx=ctx,
            producer=self.producer,
        )
        self.consumer_thread = threading.Thread(
            target=self.consumer_instance.process_messages,
            daemon=True
        )
        self.pyspark_processor = PySparkProcessor(
            ctx=ctx,
            app_name="RedditPipeline",
        )
        self.lifecycle_manager = LifeCycleManager(
            spark_processor=self.pyspark_processor,
            consumer=self.consumer_instance,
            producer=self.producer
        )

    def extract_real_time_data(self):
        """Process real-time data extraction and processing."""
        send_batch = []
        
        def process_batch(batch_data): 
            """Callback for processing individual comments."""
            send_batch.append(batch_data)
            return self._process_streaming_batch(send_batch)

        steps = [
            self.consumer_thread.start,
            lambda: self.extractor.stream_comments(callback=process_batch),
            self.consumer_thread.join,
        ]
        
        try:
            self.exe._execute_steps(steps, stage="parent")
        except Exception as e:
            logging.error(f"Error in data extraction: {e}")
            self.lifecycle_manager.shutdown_event.set()
        finally:
            self.lifecycle_manager._cleanup()

    def _process_streaming_batch(self, send_batch):
        """Manage batch processing for streaming data."""
        if len(send_batch) >= self.config.batch_size:
            logging.info(f"Sending batch data to {PySparkProcessor.__name__}...")
            self.pyspark_processor.process_and_save(send_batch)
            send_batch.clear()
        return send_batch