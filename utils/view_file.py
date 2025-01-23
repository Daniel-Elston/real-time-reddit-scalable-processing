from __future__ import annotations

import logging
from pathlib import Path

from utils.file_access import FileAccess


from pyarrow.parquet import ParquetFile

def main():
        
    file_path = "data/processed/.part-00000-204a70f1-b4b8-49d1-a9a2-b1e64818d54d-c000.snappy.parquet.crc"
    pf = ParquetFile(file_path)
    if pf.metadata.num_rows == 0:
        print("Parquet file is empty")


if __name__ == "__main__":
    main()
