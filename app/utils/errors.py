import functools
import sys


class RaiseException:
    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = func

    def __call__(self, *args, **kwargs):
        """
        Executes the stored function inside a try/except block; raises exception.
        """
        try:
            return self.func(*args, **kwargs)
        except Exception as e:
            raise Exception(f"An error occurred in '{self.func.__name__}':\n{e}")


class LogAndTerminate:
    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = func

    def __call__(self, *args, **kwargs):
        """
        Executes the stored function inside a try/except block; logs exception.
        """
        try:
            return self.func(*args, **kwargs)
        except Exception as e:
            self.logger.exception(f"An error occurred in '{self.func.__name__}':\n{e}")
            sys.exit(1)
