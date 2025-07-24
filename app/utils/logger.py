import logging


class StreamNameFilter(logging.Filter):
    """A filter to inject the stream_name into every log record."""

    def __init__(self, stream_name):
        super().__init__()
        self.stream_name = stream_name

    def filter(self, record) -> bool:
        record.stream_name = self.stream_name
        return True


logger = logging.getLogger("data_bridge_logger")


def get_configured_logger(stream_name, log_file) -> logging.Logger:
    logger.addFilter(StreamNameFilter(stream_name))
    format_str = "\n\n{levelname}:\t{asctime}:\t{stream_name}:\n\t{message}\n"
    formatter = logging.Formatter(fmt=format_str, style="{")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger
