import logging


class JobNameFilter(logging.Filter):
    """A filter to inject the job_name into every log record."""

    def __init__(self, job_name):
        super().__init__()
        self.job_name = job_name

    def filter(self, record):
        record.job_name = self.job_name
        return True


logger = logging.getLogger("data_bridge_logger")
