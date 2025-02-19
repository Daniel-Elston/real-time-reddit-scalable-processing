from __future__ import annotations

from abc import ABC

from config.paths import Paths
from config.pipeline_context import PipelineContext
from config.settings import Config
from config.settings import HyperParams
from config.settings import Params
from config.states import DataState
from config.states import ModelState
from config.states import States
from src.base.data_module_handler import DataModuleHandler
from src.base.pipeline_executor import PipelineExecutor


class BasePipeline(ABC):
    def __init__(
        self, ctx: PipelineContext,
    ):
        self.ctx = ctx
        self.paths: Paths = ctx.paths
        self.config: Config = ctx.settings.config
        self.params: Params = ctx.settings.params
        self.hyperparams: HyperParams = ctx.settings.hyper_params
        self.states: States = ctx.states
        self.data_state: DataState = ctx.states.data
        self.model_state: ModelState = ctx.states.model

        self.executor = PipelineExecutor()
        self.data_module_handler = DataModuleHandler(ctx)

    def _execute_steps(self, steps, stage=None):
        """Delegated to PipelineExecutor."""
        self.executor.run_steps(steps, stage=stage)

    def create_data_module(self, path_key):
        """Delegated to DataModuleHandler."""
        return self.data_module_handler.get_or_create_data_module(path_key)

    def load_data_module(self, dm):
        """Delegated to DataModuleHandler."""
        return self.data_module_handler.load_data_module(dm)
