from __future__ import annotations

from pathlib import Path
from typing import Dict
from typing import Optional
from typing import Union

import attr

paths_store = {
    # Raw
    "raw-extracted": Path("data/raw/"),
    # Spark
    "spark-processed": "/home/delst-wsl/wsl-workspace/live-reddit-sentiment/data/processed/",
    # Dask
    "dask-input": "/home/delst-wsl/wsl-workspace/live-reddit-sentiment/data/processed/",
    "dask-output": "/home/delst-wsl/wsl-workspace/live-reddit-sentiment/data/dask/ddf_p.parquet",
    # Result
    "result": Path("reports/result.xlsx")
}


@attr.s
class Paths:
    paths: Dict[str, Path] = attr.ib(factory=dict)

    def __attrs_post_init__(self):
        self.paths = {k: Path(v) for k, v in paths_store.items()}

    def get_path(self, key: Optional[Union[str, Path]]) -> Optional[Path]:
        if key is None:
            return None
        if isinstance(key, Path):
            return key
        return self.paths.get(key)
