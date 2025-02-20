from __future__ import annotations

import logging

import threading

from config.pipeline_context import PipelineContext
from config.settings import Config


from src.etl.extract import Extractor
from src.etl.produce import Producer
from src.etl.consume import Consumer

from src.data.pyspark import PySparkProcessor
from src.pipelines.life_cycle_manager import LifeCycleManager
from src.base.base_pipeline import BasePipeline


class ExtractionPipeline(BasePipeline):
    def __init__(
        self, ctx: PipelineContext,
    ):
        super().__init__(ctx)
        self.ctx = ctx
        self.config: Config = ctx.settings.config

        self.producer = Producer(ctx=ctx)
        self.consumer_instance = Consumer(ctx=ctx)
        self.extractor = Extractor(ctx=ctx)
        self.consumer_thread = threading.Thread(
            target=self.consumer_instance.process_messages,
            daemon=True
        )
        self.pyspark_processor = PySparkProcessor(
            ctx=ctx,
            app_name=self.config.app_name,
        )
        self.lifecycle_manager = LifeCycleManager(
            spark_processor=self.pyspark_processor,
            consumer=self.consumer_instance,
            producer=self.producer
        )
        

    def extract_real_time_data(self):
        """Process real-time data extraction and processing."""
        send_batch = []
        batch_lock = threading.Lock()
        
        # def process_batch(batch_data): 
        #     """Callback for processing individual comments."""
        #     send_batch.append(batch_data)
        #     return self._process_streaming_batch(send_batch)
        def process_batch(batch_data): 
            """Callback for processing individual comments."""
            with batch_lock:
                send_batch.append(batch_data)
                # Process batch when reaching the configured size
                if len(send_batch) >= self.config.batch_size:
                    logging.info(f"Batch size reached. Processing batch with {len(send_batch)} items.")
                    self.pyspark_processor.process_and_save(send_batch)
                    send_batch.clear()
            return send_batch

        steps = [
            self.consumer_thread.start,
            lambda: self.extractor.stream_comments(callback=process_batch),
            self.consumer_thread.join,
        ]
        
        try:
            self._execute_steps(steps, stage="extraction")
        except Exception as e:
            logging.error(f"Error in data extraction: {e}")
            self.lifecycle_manager.shutdown_event.set()
        finally:
            self.lifecycle_manager._cleanup()

    def _process_streaming_batch(self, send_batch):
        """Manage batch processing for streaming data."""
        if len(send_batch) >= self.config.batch_size:
            logging.info(
                f"Sending batch data to {self.pyspark_processor.__class__.__name__}...")
            self.pyspark_processor.process_and_save(send_batch)
            send_batch.clear()
        return send_batch