from __future__ import annotations

import logging
import signal
import sys
import threading

from src.etl.extract import Extractor
from src.etl.produce import Producer
from src.etl.consume import Consumer

from src.data.pyspark import PySparkProcessor


class LifeCycleManager:
    """
    Manages lifecycle events for data processing pipelines, 
    including graceful shutdown and resource cleanup.
    """
    def __init__(
        self, 
        spark_processor: PySparkProcessor = None, 
        consumer: Consumer = None, 
        producer: Producer = None
    ):
        self.spark_processor = spark_processor
        self.consumer = consumer
        self.producer = producer
        
        self.shutdown_event = threading.Event()
        
        self._setup_signal_handling()

    def _setup_signal_handling(self) -> None:
        """
        Configure signal handlers for graceful shutdown.
        Handles SIGINT (Ctrl+C) and SIGTERM signals.
        """
        def signal_handler(signum, frame):
            logging.warning(f"Shutdown signal {signum} received. Cleaning up resources...")
            self.shutdown_event.set()  # Signal threads to stop
            self._cleanup()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _cleanup(self):
        """
        Systematically clean up resources to ensure 
        graceful shutdown of pipeline components.
        """
        cleanup_steps = [
            (self._stop_spark_session, "Closing Spark session"),
            (self._close_consumer, "Closing consumer"),
            (self._close_producer, "Closing producer")
        ]

        for cleanup_method, log_message in cleanup_steps:
            try:
                logging.warning(log_message)
                cleanup_method()
            except Exception as e:
                logging.error(f"Error during {log_message}: {e}")

    def _stop_spark_session(self):
        """Stop Spark session if initialised"""
        if self.spark_processor and hasattr(self.spark_processor, 'spark'):
            self.spark_processor.spark.stop()

    def _close_consumer(self):
        """Close Kafka consumer if initialised"""
        if self.consumer and hasattr(self.consumer, 'consumer'):
            self.consumer.consumer.close()

    def _close_producer(self):
        """Close Kafka producer if initialised"""
        if self.producer and hasattr(self.producer, 'close'):
            self.producer.close()

    def wait_for_shutdown(self, timeout=None):
        """Block and wait for shutdown event."""
        self.shutdown_event.wait(timeout)