from __future__ import annotations

import logging

from config.pipeline_context import PipelineContext
from config.settings import Config
from config.settings import RedditCommentStructure

import backoff
import praw

from datetime import datetime
from src.etl.produce import Producer


class Extractor:
    def __init__(
        self, ctx: PipelineContext,
        producer: Producer,
    ):
        self.config: Config = ctx.settings.config
        self.reddit = praw.Reddit(**self.config.reddit_creds)
        self.producer: Producer = producer

    @backoff.on_exception(
        backoff.expo,
        (praw.exceptions.PRAWException, Exception),
        max_tries=5
    )

    def stream_comments(self) -> None:
        """Stream comments with automatic retries and proper error handling"""
        try:
            for comment in self.reddit.subreddit(
                self.config.subreddit_name).stream.comments(skip_existing=True):
                comment_data = RedditCommentStructure(
                    comment_id=comment.id,
                    body=comment.body,
                    author=str(comment.author),
                    subreddit=comment.subreddit.display_name,
                    created_utc=datetime.fromtimestamp(comment.created_utc),
                    score=comment.score,
                    parent_id=comment.parent_id
                )
                # callback(comment_data)
                self.producer.send(comment_data)
        except Exception as e:
            logging.error(f"Fatal error in comment stream: {e}")
            raise