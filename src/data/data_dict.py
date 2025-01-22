from __future__ import annotations

import numpy as np
import pandas as pd


class ApplyDataDict:
    def __init__(self):
        """Children to overwrite self.data"""
        self.data = {
            "dtypes": {},
            "use_cols": [],
            "rename_mapping": {},
            "na_values": [],
        }

    def apply_dtypes(self, df):
        for col, dtype in self.data["dtypes"].items():
            if col in df.columns:
                if dtype == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                else:
                    df[col] = df[col].astype(dtype)
        return df

    def apply_use_cols(self, df):
        if not self.data["use_cols"]:  # Check if 'use_cols' is empty or None
            use_cols = df.columns
        else:
            use_cols = [col for col in self.data["use_cols"] if col in df.columns]
        return df[use_cols]

    def apply_rename_mapping(self, df):
        return df.rename(columns=self.data["rename_mapping"])

    def apply_na_values(self, df):
        for v in self.data.get("na_values", []):
            df = df.replace(v, np.nan)
        return df

    def transforms_store(self):
        """Dict of transforms applied in sequence"""
        return {
            "apply_dtypes": self.apply_dtypes,
            "apply_use_cols": self.apply_use_cols,
            "apply_rename_mapping": self.apply_rename_mapping,
            # "apply_na_values": self.apply_na_values,
        }


class NoDataDict(ApplyDataDict):
    def __init__(self):
        super().__init__()
        self.data = {
            "dtypes": {},
            "use_cols": [],
            "rename_mapping": {},
            "na_values": [],
        }


class LogsDataDict(ApplyDataDict):
    def __init__(self):
        super().__init__()
        self.data = {
            "dtypes": {
                "Datetime": "datetime",
                "Exercise": str,
                "logs": str,
            },
            "use_cols": ["Datetime", "Exercise", "logs"],
            "rename_mapping": {
                "Datetime": "datetime",
                "Exercise": "exercise",
            },
        }
