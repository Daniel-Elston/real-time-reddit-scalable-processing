from __future__ import annotations


from config.pipeline_context import PipelineContext
from config.settings import Params
from src.data.data_module import DataModule
from utils.execution import TaskExecutor

from typing import Optional
import signal
from pathlib import Path

from src.etl.extract import RedditExtractor
from src.etl.produce import Producer
from src.etl.consume import Consumer

import logging
from typing import Dict, Any


# class DataPipeline:
#     """
#     Summary: Loads all raw documents and performs light NLP cleaning.\n
#         Input: Raw documents (local or arxiv) ``data_state key: raw_docs_all``\n
#         Output: Lightly processed documents ``data_state key: proc_docs_all``
#     """

#     def __init__(self, ctx: PipelineContext, exe: TaskExecutor):
#         self.ctx = ctx
#         self.exe = exe
#         self.params: Params = ctx.settings.params

#         # self.dm_raw_docs = DataModule(
#         #     ctx=self.ctx,
#         #     state_key="raw_docs_all",
#         # )

#     def prepare_raw_data(self):
#         print('hello    world')
#         # steps = [
            
#         # ]
#         # self.exe._execute_steps(steps, stage="parent")

class Pipeline:
    def __init__(
        self,
        reddit_creds: Dict[str, Any],
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        subreddit_name: str,
        output_dir: Optional[Path] = None
    ):
        self.extractor = RedditExtractor(reddit_creds)
        self.producer = Producer(kafka_bootstrap_servers, kafka_topic)
        self.consumer = Consumer(
            kafka_bootstrap_servers,
            kafka_topic,
            "reddit_consumer_group",
            output_dir or Path("data/comments")
        )
        self.subreddit_name = subreddit_name
        self.logger = logging.getLogger(self.__class__.__name__)
        self._setup_signal_handling()

    def _setup_signal_handling(self) -> None:
        """Setup graceful shutdown on SIGINT/SIGTERM"""
        def signal_handler(signum, frame):
            self.logger.info("Shutting down pipeline...")
            self.producer.close()
            raise SystemExit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def run(self) -> None:
        """Run the pipeline with proper error handling"""
        try:
            self.extractor.stream_comments(
                self.subreddit_name,
                lambda comment: self.producer.send(comment)
            )
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise