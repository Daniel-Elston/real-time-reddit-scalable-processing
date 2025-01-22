from __future__ import annotations

import pandas as pd

from config.pipeline_context import StateManager
from src.db.db_components import DatabaseOperations, DataHandler


class DatabaseFactory:
    """Factory for creating and configuring database components based on data type, mode, and stage."""

    def __init__(
        self,
        state: StateManager,
        db_stage: str,
        db_ops: DatabaseOperations,
        data_handler: DataHandler,
    ):
        self.state = state
        self.stage = db_stage

        self.ops = db_ops
        self.handler = data_handler

        self.load_path = None
        self.save_path = None

    def create_paths(self):
        """Create the load and save paths based on the stage."""
        if self.stage == "load1":
            self.load_path = self.state.paths.get_path(f"{self.stage}_transform")
            self.save_path = None
        elif self.stage == "load2":
            self.load_path = None
            self.save_path = self.state.paths.get_path(f"{self.stage}_fetch")
        else:
            raise ValueError(f"Invalid stage: {self.stage}. Expected 'load1' or 'load2'.")

    def create_steps(self):
        """Create the ETL steps based on the stage."""
        if self.load_path is None and self.save_path is None:
            raise RuntimeError("Paths not initialized.")

        if self.stage == "load1":
            # Load1: Create table and insert data
            df = self.state.paths.get_path(self.load_path)

            def create_table_step():
                return self.ops.create_table_if_not_exists(df)

            def insert_batches_step():
                return self.handler.insert_batches_to_db(df)

            return [create_table_step, insert_batches_step]

        elif self.stage == "load2":
            # Load2: Fetch data
            def fetch_data_step():
                return self.handler.fetch_data(self.save_path, None)

            return [fetch_data_step]

        else:
            raise ValueError(f"Invalid stage: {self.stage}. Expected 'load1' or 'load2'.")
