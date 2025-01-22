from __future__ import annotations

from typing import Callable

from config.paths import Paths
from config.pipeline_context import PipelineContext
from config.settings import Config
from utils.logging_utils import log_step


class TaskExecutor:
    def __init__(self, ctx: PipelineContext):
        self.config: Config = ctx.settings.config
        self.paths: Paths = ctx.paths

    def run_main_step(self, step: Callable, *args, **kwargs):
        """Pipeline runner for main pipelines scripts (main.py)"""
        logged_step = log_step()(step)
        return logged_step(*args, **kwargs)

    def run_parent_step(self, step: Callable, *args, **kwargs):
        """Pipeline runner for parent pipelines scripts (src/pipelines/*)"""
        logged_step = log_step()(step)
        return logged_step(*args, **kwargs)

    @staticmethod
    def run_child_step(step: Callable, *args, **kwargs):
        """Pipeline runner for child pipelines scripts (lowest level scripts)"""
        return step(*args, **kwargs)

    def _execute_steps(self, steps, stage=None):
        if stage == "main":
            for step in steps:
                self.run_main_step(step)
        elif stage == "parent":
            for step in steps:
                self.run_parent_step(step)
