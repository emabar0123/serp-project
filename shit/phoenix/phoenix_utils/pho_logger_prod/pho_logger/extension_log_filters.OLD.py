import json
import colorlog
import logging


class JSONFormatter(logging.Formatter):
    def format(self, record):
        if isinstance(record.msg, dict):
            return json.dumps(record.msg)
        return record.msg


class MinLogLevelFilter(logging.Filter):
    """
    set the minimum log level possible
    """
    def __init__(self, min_level):
        super().__init__()
        self.min_level = min_level

    def filter(self, record):
        return record.levelno >= self.min_level


class MaxLogLevelFilter(logging.Filter):
    """
    set the maximum log level possible
    """
    def __init__(self, max_level):
        super().__init__()
        self.max_level = max_level

    def filter(self, record):
        return record.levelno <= self.max_level


class ColorizingHandler(logging.Formatter):
    """
    set the handler format and colors
    """

    def __init__(self, add_format=None):
        super().__init__()
        self.add_format = add_format

    def format(self, record):
        log_colors = {
            "INFORMATION": "white",
            "DEBUG": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "cyan"
        }

        log_format = '%(log_color)s%(log_message)s'
        if self.add_format:
            log_format = '%(log_color)s' + self.add_format

        formatter = colorlog.ColoredFormatter(
            log_format,
            log_colors=log_colors,
            reset=True,
            style='%'
        )

        return formatter.format(record)

