from __future__ import annotations

import logging

from config.pipeline_context import PipelineContext
import json

from config.settings import Config
from config.paths import Paths

from kafka import KafkaConsumer
from datetime import datetime


class Consumer:
    def __init__(
        self, ctx: PipelineContext,
    ):
        self.config: Config = ctx.settings.config
        self.paths: Paths = ctx.paths
        
        self.consumer = KafkaConsumer(
            self.config.kafka_topic,
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            group_id=self.config.group_id,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.output_dir = self.paths.get_path("raw-extracted")

    def process_messages(self, batch_size: int = 100) -> None:
        """Process messages in batches for better performance"""
        messages = []
        try:
            for message in self.consumer:
                messages.append(message.value)
                
                if len(messages) >= self.config.batch_size:
                    self._save_batch(messages)
                    messages = []
                    
        except Exception as e:
            logging.error(f"Error processing messages: {e}")
            if messages:  # Save any remaining messages
                self._save_batch(messages)
            raise
        finally:
            self.consumer.close()

    def _save_batch(self, messages: list) -> None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = self.output_dir / f"reddit_comments_{timestamp}.json"        
        with output_file.open('w') as f:
            json.dump(messages, f, indent=2)