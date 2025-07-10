from pydantic import ValidationError
from models import (
    Source,
    Destination,
    Job,
    TransformFunc,
    EmailFunc,
    DestinationResponse,
)
from extract import Extractor
from load import Loader


class DataStream:
    """A container class designed to manage the state and orchestrate the flow of data
    between its child components (extractor, loader, and, optionally, transformer).
    """

    def __init__(
        self,
        job_name,
        job,
        avail_sources,
        avail_destinations,
        transform,
        email_fn=None,
        log_file="run.log",
    ) -> None:

        self.job_name = job_name
        self.log_file = log_file
        self.job = job
        self.avail_sources = avail_sources
        self.avail_destinations = avail_destinations
        self.transform = transform
        self.email_fn = email_fn

        config_issue = self._validate_config()
        if config_issue:
            raise ValueError(
                f"Configuration has one or more errors:\n - {config_issue}"
            )

        try:
            self.extractor = Extractor(
                sources=self.sources, extract_config=self.job.extract
            )
            self.loader = Loader(
                destinations=self.destinations, dest_config=self.job.load
            )
            # self.logger = logging.configure_logging(log_file, etc)
        except Exception as e:
            # self.logger.log(f"Data stream failed to assemble sub-components: {e}")
            print(e)

    def _validate_config(self):
        # 1. ---------- Validate job
        try:
            self.job = Job(**self.job)
        except ValidationError as e:
            return f"The 'job' configuration has a structural error: {e}"

        # 2. ---------- Check that source(s)/destination(s) requested by job are available
        if hasattr(self, "job"):
            job_sources = set(self.job.extract.keys())
            if not job_sources.issubset(self.avail_sources.keys()):
                return f"Job references undefined sources: {job_sources - set(self.avail_sources.keys())}"
            else:
                sources_used = {
                    name: self.avail_sources[name] for name in self.job.extract.keys()
                }

            job_dests = set(self.job.load.keys())
            if not job_dests.issubset(self.avail_destinations.keys()):
                return f"Job references undefined destinations: {job_dests - set(self.avail_destinations.keys())}"
            else:
                destinations_used = {
                    name: self.avail_destinations[name] for name in self.job.load.keys()
                }
        else:
            return f"Could not validate if requested sources/destinations are available due to job config failure"

        # 3. ---------- Validate used source(s)/destination(s)
        self.sources = {}
        for name, s in sources_used.items():
            try:
                self.sources[name] = Source(**s)
            except ValidationError as e:
                return f"Selected source '{name} has a structural error: {e}"

        self.destinations = {}
        for name, d in destinations_used.items():
            try:
                self.destinations[name] = Destination(**d)
            except ValidationError as e:
                return f"Selected destination '{name} has a structural error: {e}"

        # 4. ---------- Validate that extract dependencies match source type
        for source_name, dependencies in self.job.extract.items():
            if not self.sources[source_name]:
                return f"Source '{source_name}' extract dependencies could not be validated"
            else:
                source_model = self.sources[source_name]

                # Sources of type="sql" should be SQL query file paths (e.g., "query.sql")
                if source_model.type == "sql":
                    for dep in dependencies:
                        if not dep.endswith(".sql"):
                            return f"Source '{source_name}' requires .sql files, but got '{dep}'"

                # Sources of type="fileshare" or "sftp" should be relative paths
                elif source_model.type in ["fileshare", "sftp"]:
                    for dep in dependencies:
                        if dep.startswith("/") or dep.endswith("/"):
                            return f"Source '{source_name}' requires a relative file path, but got an absolute or directory-like path: '{dep}'"

                # For Google Drive, dependency is likely a file name or ID,
                # temporarily check to ensure it's not an empty string, but go back and update late once I know
                elif source_model.type == "google_drive":
                    for dep in dependencies:
                        if not dep:
                            return (
                                f"Source '{source_name}' requires a non-empty string."
                            )

        # 5. ---------- Validate job-specific logic
        try:
            self.transform = TransformFunc(function=self.transform)
        except ValidationError as e:
            return f"Custom transform function does not meet model specifications: {e}"

        if self.email_fn:
            try:
                self.email_fn = EmailFunc(function=self.email_fn)
            except ValidationError as e:
                return f"Custom email function does not meet model specifications: {e}"

        return None

    def log_job_results(self, dest_responses: list[DestinationResponse]):
        print("Logging the following destination responses:")
        print(dest_responses)

    def run(self):
        extracted_data = self.extractor.extract()
        transformed_data = self.transform(extracted_data)
        dest_responses = self.loader.load(transformed_data, email_fn=self.email_fn)
        self.log_job_results(dest_responses)
