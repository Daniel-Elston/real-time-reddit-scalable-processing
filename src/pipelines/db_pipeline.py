from __future__ import annotations

from config.pipeline_context import StateManager
from src.db.db_factory import DatabaseFactory
from utils.execution import TaskExecutor


class DatabasePipeline:
    """ELTL or ETL pipeline for database operations."""

    def __init__(self, state: StateManager, exe: TaskExecutor):
        self.state = state
        self.exe = exe

        self.db_factory = DatabaseFactory(
            state=self.state,
            db_stage=self.state.db_config.stage,
            db_ops=self.state.db_manager.ops,
            data_handler=self.state.db_manager.handler,
        )
        self.db_factory.create_paths()
        self.steps = self.db_factory.create_steps()

    def __call__(self):
        steps = self.db_factory.create_steps()
        self.exe._execute_steps(steps, stage="parent")
