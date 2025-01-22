from __future__ import annotations

import logging

import attr


@attr.s
class DataState:
    """In-memory state get/set/clear"""
    _state: dict = attr.ib(factory=dict, init=False)

    def set(self, key, value):
        """Store a value in the state"""
        logging.getLogger("file_access").file_track(
            f"SAVING ``{key}`` to {self.__class__.__name__} in memory"
        )
        self._state[key] = value

    def get(self, key):
        """Retrieve a value from the state"""
        logging.getLogger("file_access").file_track(
            f"LOADING ``{key}`` from {self.__class__.__name__} in memory"
        )
        return self._state.get(key)

    def clear(self):
        """Clear all state"""
        self._state = {}


@attr.s
class ModelState:
    """In-memory state get/set/clear"""
    _state: dict = attr.ib(factory=dict, init=False)

    def set(self, key, value):
        """Store a value in the state"""
        logging.getLogger("file_access").file_track(
            f"SAVING ``{key}`` to {self.__class__.__name__} in memory"
        )
        self._state[key] = value

    def get(self, key):
        """Retrieve a value from the state"""
        logging.getLogger("file_access").file_track(
            f"LOADING ``{key}`` from {self.__class__.__name__} in memory"
        )
        return self._state.get(key)

    def clear(self):
        """Clear all state"""
        self._state = {}


@attr.s
class States:
    """In-memory state for data_state, model_state"""
    data: DataState = attr.ib(factory=DataState)
    model: ModelState = attr.ib(factory=ModelState)
