from __future__ import annotations

import os
import attr
from typing import Optional
from datetime import datetime


def kafka_bootstrap_servers():
    return os.getenv('KAFKA_BROKER_URL')

def reddit_creds():
    return {
        'client_id': os.getenv('REDDIT_CLIENT_ID'),
        'client_secret': os.getenv('REDDIT_CLIENT_SECRET'),
        'user_agent': os.getenv('REDDIT_USER_AGENT'),
        'username': os.getenv('REDDIT_USERNAME'),
        'password': os.getenv('REDDIT_PASSWORD'),
    }

@attr.s
class Config:
    overwrite: bool = attr.ib(default=True)
    save_fig: bool = attr.ib(default=True)
    
    batch_size:int = 20
    spark_write_mode: str = attr.ib(default="append")
    short_comment_threshold: int = 20
    kafka_topic: str = 'reddit_comments'
    subreddit_name: str = 'all'
    group_id: str = 'reddit_consumer_group'
    app_name:str = attr.ib(default="RedditPipeline")
    
    kafka_bootstrap_servers: str = attr.ib(factory=kafka_bootstrap_servers)
    reddit_creds: dict = attr.ib(factory=reddit_creds)


@attr.s
class RedditCommentStructure:
    """Data structure for Reddit comments"""
    comment_id: str = attr.ib()
    body: str = attr.ib()
    author: str = attr.ib()
    subreddit: str = attr.ib()
    created_utc: datetime = attr.ib()
    score: int = attr.ib()
    parent_id: Optional[str] = attr.ib(default=None)


@attr.s
class Params:
    n_workers:int = attr.ib(default=1)
    threads_per_worker:int = attr.ib(default=1)
    npartitions: int = attr.ib(default=4)
    chunk_size: int = attr.ib(default=100)
    chunk_overlap: int = attr.ib(default=20)
    model_name: str = attr.ib(default="sentence-transformers/all-MiniLM-L6-v2")


@attr.s
class HyperParams:
    None


@attr.s
class Settings:
    """config, params, hyper_params"""
    config: Config = attr.ib(factory=Config)
    params: Params = attr.ib(factory=Params)
    hyper_params: HyperParams = attr.ib(factory=HyperParams)
