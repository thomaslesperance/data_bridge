import functools
import sys
from logger import logger


class LogAndTerminate:
    def __init__(self, func, log_message: str = None):
        functools.update_wrapper(self, func)
        self.func = func
        self.log_message = log_message

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            """
            Executes the stored function inside a try/except block; logs exception.
            """
            try:
                return self.func(*args, **kwargs)
            except Exception as e:
                default_log_message = f"An error occurred in '{self.func.__name__}'"
                self.logger.exception(
                    f"{self.log_message or default_log_message}:\n\t\t'{e}'\n"
                )
                sys.exit(1)

        return wrapper
