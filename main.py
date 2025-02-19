from __future__ import annotations

import logging

from config.pipeline_context import PipelineContext
from src.pipelines.extract_pipeline import ExtractionPipeline
from src.pipelines.dask_pipeline import DaskPipeline
from utils.execution import TaskExecutor
from utils.project_setup import init_project
from src.base.base_pipeline import BasePipeline
from src.data.view_data import ViewData

class MainPipeline:
    """ETL Pipeline main entry point."""
    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def run(self):
        steps = [
            # ExtractionPipeline(self.ctx).extract_real_time_data,
            # DaskPipeline(self.ctx).run,
            ViewData(self.ctx).run
        ]
        BasePipeline(ctx)._execute_steps(steps, stage="main")


if __name__ == "__main__":
    project_dir, project_config, ctx, exe = init_project()
    try:
        logging.info(f"Beginning Top-Level Pipeline from ``main.py``...\n{"=" * 125}")
        MainPipeline(ctx).run()
    except Exception as e:
        logging.error(f"Pipeline terminated due to unexpected error: {e}", exc_info=True)