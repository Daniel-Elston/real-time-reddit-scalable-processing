from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pprint import pformat

import attr

from src.db.db_components import DatabaseConnection, DatabaseOperations, DataHandler


def db_creds():
    return {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
    }


def db_info():
    return {
        "database": os.getenv("POSTGRES_DB"),
        "schema": os.getenv("POSTGRES_SCHEMA"),
        "table": os.getenv("POSTGRES_TABLE"),
    }


@attr.s
class DatabaseConfig:
    stage: str = attr.ib("load2")
    admin_creds: dict = attr.ib(factory=db_creds)
    db_info: dict = attr.ib(factory=db_info)

    database: str = attr.ib()
    schema: str = attr.ib()
    table: str = attr.ib()

    overwrite: bool = False
    chunk_size: int = 500_000
    batch_size: int = 100_000

    @database.default
    def _database_default(self):
        return self.db_info["database"]

    @schema.default
    def _schema_default(self):
        return self.db_info["schema"]

    @table.default
    def _table_default(self):
        return self.db_info["table"]

    def __attrs_post_init__(self):
        attr_dict = attr.asdict(self)
        logging.warning(f"DatabaseConfig:\n{pformat(attr_dict)}\n")


@dataclass
class DatabaseConnManager:
    config: DatabaseConfig
    conn: DatabaseConnection = field(init=False)
    ops: DatabaseOperations = field(init=False)
    handler: DataHandler = field(init=False)

    def __post_init__(self):
        self.initialize_database()

    def initialize_database(self):
        self.conn = DatabaseConnection(self.config.admin_creds, self.config.db_info)
        self.ops = DatabaseOperations(self.conn, self.config.schema, self.config.table)
        self.handler = DataHandler(
            self.conn, self.config.schema, self.config.table, self.config.batch_size
        )
