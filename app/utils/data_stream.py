from pydantic import ValidationError
from models import Source, Destination, Job, TransformFunc, EmailFunc
from extract import Extractor
from load import Loader


class DataStream:
    """A container class designed to manage the state and orchestrate the flow of data
    between its child components (extractor, loader, and, optionally, transformer).
    """

    def __init__(
        self,
        job_name,
        avail_sources,
        avail_destinations,
        job,
        transform_fn=None,
        email_fn=None,
        log_file="run.log",
    ) -> None:

        self.job_name = job_name
        self.log_file = log_file
        config_issues = []

        # 1. Validate job
        try:
            self.job = Job(**job)
        except ValidationError as e:
            config_issues.append(f"The 'job' configuration has a structural error: {e}")

        # 2. Check that source(s)/destination(s) requested by job are available
        for source_name, _ in self.job.extract.items():
            if source_name not in avail_sources:
                config_issues.append(ValidationError(
                    f"Job references an undefined source: '{source_name}'"
                ))
        for dest_name, _ in self.job.load.items():
            if dest_name not in avail_destinations:
                config_issues.append(ValidationError(
                    f"Job references an undefined destination: '{dest_name}'"
                )

        # 3. Validate used source(s)/destination(s)
        sources_used = {name: avail_sources[name] for name in job.extract.keys()}
        destinations_used = {
            name: avail_destinations[name] for name in job.load.keys()
        }

        self.sources = {}
        for name, s in sources_used.items():
            try:
                self.sources[name]: Source(**s)
            except ValidationError as e:
                config_issues.append(f"Selected source '{name} has a structural error: {e}")
        
        self.destinations = {}
        for name, d in destinations_used.items():
            try:
                self.destinations[name]: Destination(**d)
            except ValidationError as e:
                config_issues.append(f"Selected destination '{name} has a structural error: {e}")

        # 4. Validate extract/load dependencies
        for source_name, dependencies in self.job.extract.items():
            source_model = self.sources[source_name]

            # Sources of type="sql" should be SQL query file paths (e.g., "query.sql")
            if source_model.type == "sql":
                for dep in dependencies:
                    if not dep.endswith(".sql"):
                        config_issues.append(TypeError(
                            f"Source '{source_name}' requires .sql files, but got '{dep}'"
                        ))
            # Sources of type="fileshare" or "sftp" should be relative paths
            elif source_model.type in ["fileshare", "sftp"]:
                for dep in dependencies:
                    if dep.startswith("/") or dep.endswith("/"):
                        config_issues.append(ValueError(
                            f"Source '{source_name}' requires a relative file path, but got an absolute or directory-like path: '{dep}'"
                        ))
            # For Google Drive, dependency is likely a file name or ID,
            # temporarily check to ensure it's not an empty string, but go back and update late once I know
            elif source_model.type == "google_drive":
                for dep in dependencies:
                    if not dep:
                        config_issues.append(ValueError(
                            f"Source '{source_name}' requires a non-empty string."
                        ))

        # 5. Validate job-specific logic
        if transform_fn:
            try:
                self.transform_fn = TransformFunc(transform_fn)
            except ValidationError as e:
                config_issues.append(f"Custom transform function does not meet model specifications: {e}")
            
        if email_fn:
            try:
                self.email_fn = EmailFunc(email_fn)
            except ValidationError as e:
                config_issues.append(f"Custom email function does not meet model specifications: {e}")

        if config_issues:
            all_errors = "\n - ".join(config_issues)
            raise ValueError(f"Configuration has one or more errors:\n - {all_errors}")
        
        # Attempt to assemble component instances of DataStream instance
        try:
            # self.logger = logging.configure_logging(log_file, etc)
            self.extractor = Extractor(self.sources, self.job.extract)
            self.loader = Loader(self.destinations, self.job.load, self.email_fn)
        except Exception as e:
            # self.logger.log(f"Data stream failed to assemble sub-components: {e}")
            print(e)

    def log_job_results(self, dest_responses):
        print("Logging the following destination responses:")
        print(dest_responses)

    def run(self):
        extracted_data = self.extractor.extract()

        if self.transform_fn:
            transformed_data = self.transform_fn(extracted_data)
            dest_responses = self.loader.load(transformed_data)
        else:
            dest_responses = self.loader.load(extracted_data)

        self.log_job_results(dest_responses)
