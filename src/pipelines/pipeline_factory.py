from __future__ import annotations

from typing import Literal
from typing import Type

from config.pipeline_context import PipelineContext
from config.settings import Config
from src.data.dask_data_handler import DaskDataHandler
from src.pipelines.dask_pipeline import DaskPipeline
from src.pipelines.extract_pipeline import ExtractionPipeline
from src.pipelines.life_cycle_manager import LifeCycleManager
from utils.logging_utils import log_pipeline_factory


class PipelineFactory:
    def __init__(self, ctx: PipelineContext):
        """
        Summary
        ----------
        Executes specific pipeline methods based on configurable operations.

        Extended Summary
        ----------
        Adjust the ``config/settings.py``.
        The specific operation is determined by the `Config.operation`

        - `extract`: Executes `ExtractionPipeline.extract_real_time_data`
        - `transform`: Executes `DaskPipeline.distributed_process_and_store`
        - `result`: Executes `DaskDataHandler.read_result`
        """
        self.ctx = ctx
        self.config: Config = ctx.settings.config
        self.operation: Literal["extract", "transform", "result"] = self.config.operation

        self.operation_map: dict[str, tuple[type, str]] = {
            "extract": (ExtractionPipeline, "extract_real_time_data"),
            "transform": (DaskPipeline, "distributed_process_and_store"),
            "result": (DaskDataHandler, "read_result")
        }

    def dispatch(self) -> None:
        """Dispatches the execution of the configured pipeline operation."""
        try:
            # Get pipeline class and method name for the configured operation
            pipeline_class, method_name = self._get_operation()

            # Instantiate the pipeline class and retrieve the method
            pipeline_instance = pipeline_class(self.ctx)
            operation_method = getattr(pipeline_instance, method_name)

            # Log execution details and invoke the method
            log_pipeline_factory(self, pipeline_instance, operation_method)
            operation_method()
        finally:
            if self.operation == "extract":
                LifeCycleManager()._cleanup()

    def _get_operation(self) -> tuple[Type, str]:
        """Validate and return class/method from config"""
        operation_info = self.operation_map.get(self.config.operation)
        if not operation_info:
            raise ValueError(
                f"Unsupported operation '{self.config.operation}'."
                f"Valid: {list(self.operation_map.keys())}"
            )
        return operation_info
