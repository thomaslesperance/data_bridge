from pydantic import ValidationError
from models import Sources, Destinations, Job, Transformation, Email_Message
from extract import Extractor
from load import Loader


class Data_Stream:

    def __init__(
        self,
        job_name,
        sources,
        destinations,
        job,
        transform_fn=None,
        email_msg=None,
        log_file="run.log",
    ) -> None:

        try:
            self.job_name = job_name
            self.log_file = log_file

            self.sources = {}
            self.destinations = {}
            self.job = {}

            self.sources = Sources(sources)
            self.destinations = Destinations(destinations)
            self.job = Job(job)
            self.transform_fn = Transformation(transform_fn)
            self.email_msg = Email_Message(email_msg)

            # self.logger = logging.configure_logging(log_file, etc)
            self.extractor = Extractor(self.sources, self.job["extract"])
            self.loader = Loader(self.destinations, self.job["load"], self.email_msg)

        except ValidationError as e:
            # self.logger.log(f"CONFIG FILE VALIDATION FAILED: {e}")
            print(e)
        except Exception as e:
            # self.logger.log(f"DATA STREAM FAILED TO INIT: {e}")
            print(e)

    def run(self):
        self.extractor.extract()
        self.transform_fn()
        self.loader.load()
        # self.log_job()
