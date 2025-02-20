from __future__ import annotations

import logging

from config.pipeline_context import PipelineContext
from utils.project_setup import init_project
from src.pipelines.pipeline_factory import PipelineFactory


class MainPipeline:
    """ETL Pipeline main entry point."""
    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def run(self):
        PipelineFactory(self.ctx).dispatch()


if __name__ == "__main__":
    project_dir, project_config, ctx = init_project()
    try:
        logging.info(f"Beginning Top-Level Pipeline from ``main.py``...\n{"=" * 125}")
        MainPipeline(ctx).run()
    except Exception as e:
        logging.error(f"Pipeline terminated due to unexpected error: {e}", exc_info=True)