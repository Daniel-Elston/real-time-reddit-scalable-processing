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
    write_output: bool = attr.ib(default=True)
    overwrite: bool = attr.ib(default=True)
    save_fig: bool = attr.ib(default=True)
    
    kafka_topic: str = 'reddit_comments'
    subreddit_name: str = 'funny'
    group_id: str = 'reddit_consumer_group'
    
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
    None


@attr.s
class HyperParams:
    None


@attr.s
class Settings:
    """config, params, hyper_params"""
    config: Config = attr.ib(factory=Config)
    params: Params = attr.ib(factory=Params)
    hyper_params: HyperParams = attr.ib(factory=HyperParams)
