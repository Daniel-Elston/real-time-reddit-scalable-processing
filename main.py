from __future__ import annotations

import logging

from config.pipeline_context import PipelineContext
from src.pipelines.extract_pipeline import ExtractionPipeline
from utils.execution import TaskExecutor
from utils.project_setup import init_project

import collections.abc
import sys
import queue
sys.modules['collections'].Sequence = collections.abc.Sequence
sys.modules['kafka.vendor.six.moves'] = type(sys)('kafka.vendor.six.moves')
sys.modules['kafka.vendor.six.moves'].queue = queue


class MainPipeline:
    """RAG Pipeline main entry point."""

    def __init__(self, ctx: PipelineContext, exe: TaskExecutor):
        self.ctx = ctx
        self.exe = exe
        ctx.log_context()

    def run(self):
        steps = [
            ExtractionPipeline(ctx=self.ctx, exe=self.exe,).extract_real_time_data,
        ]
        self.exe._execute_steps(steps, stage="main")


if __name__ == "__main__":
    project_dir, project_config, ctx, exe = init_project()
    try:
        logging.info(f"Beginning Top-Level Pipeline from ``main.py``...\n{"=" * 125}")
        MainPipeline(ctx, exe).run()
    except Exception as e:
        logging.error(f"Pipeline terminated due to unexpected error: {e}", exc_info=True)
