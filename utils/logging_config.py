from __future__ import annotations

import logging.config
import multiprocessing
from pathlib import Path

import colorlog


# Custom log level
FILE_TRACK = 15
logging.addLevelName(FILE_TRACK, "FILE_TRACK")


def file_track(self, message, *args, **kwargs):
    if self.isEnabledFor(FILE_TRACK):
        self._log(FILE_TRACK, message, args, **kwargs)


logging.Logger.file_track = file_track


class HighlightLogger(logging.Logger):
    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        self._highlight_colors = {
            "bright_blue": "\033[1;34m",
            "bright_green": "\033[1;32m",
            "bright_yellow": "\033[1;33m",
            "bright_red": "\033[1;31m",
            "bright_magenta": "\033[1;35m",
            "bright_cyan": "\033[1;36m",
            "reset": "\033[0m",
        }

    def _highlight_log(self, color, msg, *args, **kwargs):
        if isinstance(msg, str):
            highlighted_msg = (
                f"{self._highlight_colors[color]}{msg}{self._highlight_colors['reset']}"
            )
        else:
            highlighted_msg = msg
        return self._log(self.level, highlighted_msg, args, **kwargs)

    def highlight(self, color="bright_blue"):
        return logging.LoggerAdapter(self, extra={"highlight_color": color})


class HighlightLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        color = self.extra.get("highlight_color", "bright_blue")
        if isinstance(msg, str):
            return (
                f"{self.logger._highlight_colors[color]}{msg}{self.logger._highlight_colors['reset']}",
                kwargs,
            )
        return msg, kwargs


def setup_logging(name, project_dir, log_file_name, config):
    """Setup logging configuration with dynamic log file naming, levels, and highlight feature."""

    log_file_path = Path(project_dir, "log", log_file_name)
    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    root_level = config["logging"]["root_level"]
    file_level = config["logging"]["file_level"]
    console_level = config["logging"]["console_level"]

    class HighlightColoredFormatter(colorlog.ColoredFormatter):
        def format(self, record):
            if hasattr(record, "highlight_color"):
                record.log_color = record.highlight_color
            return super().format(record)

    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(levelname)s - %(pathname)s - %(asctime)s - %(filename)s"
                " - %(lineno)d - %(module)s - %(name)s - %(funcName)s - %(message)s",
                "datefmt": "%H:%M:%S",
            },
            "color": {
                "()": HighlightColoredFormatter,
                "format": "%(log_color)s%(asctime)s - %(module)s - %(levelname)s - %(message)s",
                # 'datefmt': '%Y-%m-%d %H:%M:%S',
                "datefmt": "%H:%M:%S",
                "log_colors": {
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "red,bg_white",
                    "FILE_TRACK": "purple",
                },
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": console_level,
                "formatter": "color",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": file_level,
                "formatter": "detailed",
                "filename": str(log_file_path),
                "maxBytes": 1000000,
                "backupCount": 5,
            },
        },
        "loggers": {
            "urllib3": {"level": "WARNING", "handlers": ["console", "file"], "propagate": False},
            "langchain_community.document_loaders": {"level": "WARNING", "handlers": ["console", "file"], "propagate": False},
            "requests": {"level": "WARNING", "handlers": ["console", "file"], "propagate": False},
            "__init__": {"level": "WARNING", "handlers": ["console", "file"], "propagate": False},
            "PIL": {"level": "WARNING", "handlers": ["console", "file"], "propagate": False},
            "pyplot": {"level": "WARNING", "handlers": ["console", "file"], "propagate": False},
            "matplotlib": {"level": "WARNING", "handlers": ["console", "file"], "propagate": False},
            "matplotlib.font_manager": {"level": "WARNING", "handlers": ["console", "file"], "propagate": False},
        },
        "root": {
            "level": root_level,
            "handlers": ["console", "file"],
        },
    }

    if multiprocessing.current_process().name == "MainProcess":
        LOGGING_CONFIG["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": file_level,
            "formatter": "detailed",
            "filename": str(log_file_path),
            "maxBytes": 1000000,
            "backupCount": 5,
        }
        LOGGING_CONFIG["root"]["handlers"].append("file")
        for logger_name in LOGGING_CONFIG["loggers"]:
            LOGGING_CONFIG["loggers"][logger_name]["handlers"].append("file")

    if name == "MAIN":
        LOGGING_CONFIG["root"]["level"] = "DEBUG"
    elif name == "TEST":
        LOGGING_CONFIG["root"]["level"] = "INFO"

    logging.setLoggerClass(HighlightLogger)
    logging.config.dictConfig(LOGGING_CONFIG)
