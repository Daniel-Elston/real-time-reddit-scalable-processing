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

import threading


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
        self._setup_signal_handling()

    def extract_real_time_data(self):
        steps = [
            self.consumer_thread.start,
            self.extractor.stream_comments,
            self.consumer_thread.join,
        ]
        self.exe._execute_steps(steps, stage="parent")


    def _setup_signal_handling(self) -> None:
        """Setup graceful shutdown on SIGINT/SIGTERM"""
        def signal_handler():
            logging.warning("Shutting down pipeline...")
            self.producer.close()
            logging.warning("Waiting for consumer to finish processing messages...")
            self.consumer_instance.consumer.close()
            raise SystemExit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
