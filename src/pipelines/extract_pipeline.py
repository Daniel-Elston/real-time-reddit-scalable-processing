from __future__ import annotations

import logging

from config.pipeline_context import PipelineContext
from utils.execution import TaskExecutor
from config.settings import Config
from config.paths import Paths

import signal

from src.etl.extract import Extractor
from src.etl.produce import Producer
from src.etl.consume import Consumer

from src.data.pyspark import PySparkProcessor

import threading

import threading
import signal
import logging
import sys


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
        self.shutdown_event = threading.Event()
        self._setup_signal_handling()


    def extract_real_time_data(self):
        """Process real-time data extraction and processing."""
        send_batch = []
        
        def process_batch(batch_data): 
            """Pass batch data to PySpark processor."""
            send_batch.append(batch_data)

            if len(send_batch) >= self.config.batch_size:
                logging.info(f"Sending batch data to PySpark processor. Batch size: {len(send_batch)}")
                self.pyspark_processor.process_and_save(send_batch)
                send_batch.clear()

        steps = [
            self.consumer_thread.start,
            lambda: self.extractor.stream_comments(callback=process_batch),
            self.consumer_thread.join,
        ]
        self.exe._execute_steps(steps, stage="parent")

    def _setup_signal_handling(self) -> None:
        """Setup graceful shutdown on SIGINT/SIGTERM"""
        def signal_handler(signum, frame):
            logging.warning("Shutdown signal received. Cleaning up resources...")
            self.shutdown_event.set()  # Signal threads to stop
            self._cleanup()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _cleanup(self):
        """Clean up resources before shutting down."""
        try:
            logging.warning("Closing Spark session...")
            self.pyspark_processor.spark.stop()
            logging.warning("Closing consumer...")
            self.consumer_instance.consumer.close()
            logging.warning("Closing producer...")
            self.producer.close()
            logging.warning("Cleanup complete. Exiting.")
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")