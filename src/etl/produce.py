from __future__ import annotations

import logging

from typing import Any
import json
from config.settings import RedditCommentStructure
from kafka import KafkaProducer
import attr

from config.settings import Config
from config.paths import Paths

from config.pipeline_context import PipelineContext
from config.settings import Config
from config.paths import Paths


class Producer:
    def __init__(
        self, ctx: PipelineContext,
        # bootstrap_servers: str,
        # topic: str
    ):
        self.config: Config = ctx.settings.config
        self.paths: Paths = ctx.paths
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            acks='all',
            retries=3,
            retry_backoff_ms=1000
        )
        self.topic = self.config.kafka_topic

    def send(self, data: RedditCommentStructure) -> None:
        try:
            future = self.producer.send(
                self.topic,
                value=attr.asdict(data)
            )
            future.get(timeout=10)  # Wait for send to complete
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            raise

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()