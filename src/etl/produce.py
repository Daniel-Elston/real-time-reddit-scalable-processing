from __future__ import annotations

import json
import logging

import attr
from kafka import KafkaProducer

from config.pipeline_context import PipelineContext
from config.settings import Config
from config.settings import RedditCommentStructure


class Producer:
    def __init__(
        self, ctx: PipelineContext,
    ):
        self.config: Config = ctx.settings.config

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
