from __future__ import annotations

import logging
from pprint import pformat

import attr

from config.paths import Paths
from config.settings import Settings
from config.states import States


@attr.s
class PipelineContext:
    """
    Attributes:
        paths: Paths
        settings: Settings
        states: States
    """

    paths: Paths = attr.ib(factory=Paths)
    settings: Settings = attr.ib(factory=Settings)
    states: States = attr.ib(factory=States)

    def log_context(self):
        """Log all attributes, skipping None or empty ones."""
        attr_dict = attr.asdict(self)
        filtered_attr_dict = {k: v for k, v in attr_dict.items() if v not in (None, {}, [])}

        for key, value in filtered_attr_dict.items():
            logging.debug(f"{key.capitalize()}:\n{pformat(value)}\n")