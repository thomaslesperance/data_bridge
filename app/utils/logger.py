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
