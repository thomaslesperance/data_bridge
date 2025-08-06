import functools
import sys
from .logger import logger


class LogAndTerminate:
    def __init__(self, log_message: str = None):
        self.logger = logger
        self.log_message = log_message

    def __call__(self, func) -> callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """
            Executes the stored function inside a try/except block; logs exception.
            """
            try:
                return func(*args, **kwargs)
            except Exception as e:
                default_log_message = f"An error occurred in '{func.__name__}'"
                self.logger.exception(
                    f"{self.log_message or default_log_message}:\n\t\t'{e}'\n"
                )
                sys.exit(1)

        return wrapper
