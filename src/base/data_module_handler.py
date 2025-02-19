from __future__ import annotations

from typing import Dict

import pandas as pd

from config.pipeline_context import PipelineContext
from src.data.data_dict import NoDataDict
from src.data.data_module import DataModule
from typing import Any


module_map: Dict[str, dict] = {
    "no-dd": NoDataDict(),
}


class DataModuleHandler:
    def __init__(
        self, ctx: PipelineContext
    ):
        """
        _summary_
        ----------
        Manages creates/retrieval of DataModules. Instantiates DataModules and allows interaction.
        
        _extended_summary_
        ----------
            - Load data from a specified DataModule.
            - Save data to a specified DataModule.
            - Get DataModule instance, or create it if it doesn't exist.
            - Create a DataModule instance.
            - Instantiate a DataModule and load data from it.
        
        Outputs
        ----------
            - Data accessible from DataModules.
            - Data saved to DataModules.
            - Instantiated DataModules.
        
        Parameters
        ----------
        ctx : PipelineContext
            _description_
        """        
        self.ctx = ctx
        self.modules: Dict[str, DataModule] = {}
        self.module_map: Dict[str, dict] = module_map

    def load_data(self, path_key: str):
        """Load data from a specified DataModule."""
        dm = self.get_or_create_data_module(path_key)
        return dm.load()

    def save_data(self, path_key: str, df: pd.DataFrame):
        """Save data to a specified DataModule."""
        dm = self.get_or_create_data_module(path_key)
        return dm.save(df)

    def get_or_create_data_module(self, path_key: str) -> DataModule:
        """Get DataModule instance, or create it if it doesn't exist."""
        if path_key not in self.modules:
            self.modules[path_key] = self._create_data_module(path_key)
        return self.modules[path_key]

    def _create_data_module(self, path_key: str) -> DataModule:
        """Create a DataModule instance."""
        data_dict = self.module_map.get(path_key)
        return DataModule(
            self.ctx,
            data_path=self.ctx.paths.get_path(path_key),
            data_dict=data_dict
        )

    def load_data_module(self, dm: DataModule) -> Any:
        """Instantiate a DataModule and load data from it."""
        if not hasattr(dm, "_loaded_data"):
            dm._loaded_data = dm.load()
            if dm._loaded_data is None:
                raise ValueError(f"Dataset at {dm.data_path} is empty.")
        return dm._loaded_data
