import logging


base_logger = logging.getLogger("data_bridge_logger")


class StreamNameFilter(logging.Filter):
    """A filter to inject the stream_name into every log record."""

    def __init__(self, stream_name: str) -> None:
        super().__init__()
        self.stream_name = stream_name

    def filter(self, record: logging.LogRecord) -> bool:
        record.stream_name = self.stream_name
        return True


class StreamLogger:
    def __init__(self, stream_name: str) -> None:
        """Initializes a wrapper class for a single logging.Logger instance to be used accross the data stream."""
        self.logger_instance = base_logger
        self.stream_name = stream_name
        self.logger_instance.addFilter(StreamNameFilter(stream_name))

        self.log_format_str = (
            "\n\n{levelname}:\t{asctime}:\t{stream_name}:\n\t{message}\n"
        )
        self.formatter = logging.Formatter(fmt=self.log_format_str, style="{")

        self.default_log_level = 10
        self.logger_instance.setLevel(self.default_log_level)

        self.default_log_file = "errors.log"
        self.default_file_handler = logging.FileHandler(self.default_log_file)
        self.default_file_handler.setFormatter(self.formatter)
        self.logger_instance.addHandler(self.default_file_handler)

    def set_log_level(self, log_level: int | str) -> None:
        self.logger_instance.setLevel(log_level)

    def set_log_file(self, log_file: str) -> None:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(self.formatter)
        self.logger_instance.addHandler(file_handler)
