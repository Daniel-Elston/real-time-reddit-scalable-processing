from __future__ import annotations

import logging
from typing import Callable
from typing import List

from utils.logging_utils import log_step


class PipelineExecutor:
    """Orchestrates the execution of pipeline steps."""

    def run_steps(self, steps: List[Callable], stage: str = None):
        """Loops through each step callable, applying the log_step decorator"""
        for step in steps:
            if stage:
                logging.debug(f"Executing Stage: {stage}")
            self.run_step(step)

    @staticmethod
    def run_step(step: Callable):
        """Wraps the step with a logging decorator, then executes"""
        decorated_step = log_step()(step)
        return decorated_step()
