import logging
from pydantic import ValidationError
from models import (
    Source,
    Destination,
    Job,
    TransformFunc,
    EmailBuilder,
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
        transform_fn,
        log_file,
        email_builders=None,
    ) -> None:

        self.logger = self._setup_logger(
            job_name, log_file, job.get("log_level", logging.INFO)
        )

        try:
            self._validate_config(
                job_dict=job,
                avail_sources=avail_sources,
                avail_destinations=avail_destinations,
                transform_fn=transform_fn,
                email_builders=email_builders,
            )
        except Exception as e:
            self.logger.exception(
                f"Failed to validate '{job_name}' data stream configuration:\n{e}"
            )
        self.logger.info(f"'{job_name}' job configuration validated with no issues")

        try:
            self.extractor = Extractor(
                sources=self.sources,
                extract_tasks=self.job.extract_tasks,
                logger=self.logger,
            )
        except Exception as e:
            self.logger.exception(f"Failed to instantiate Extractor class:\n{e}")
        self.logger.info(f"Extractor class instantiated successfully")

        try:
            self.loader = Loader(
                destinations=self.destinations,
                load_tasks=self.job.load_tasks,
                email_builders=self.email_builders,
                logger=self.logger,
            )
        except Exception as e:
            self.logger.exception(f"Failed to instantiate Loader class:\n{e}")
        self.logger.info(f"Loader class instantiated successfully")

    def _setup_logger(self, job_name, log_file, log_level) -> logging.Logger:
        logger = logging.getLogger(job_name)
        logger.setLevel(log_level)
        format_str = "{asctime}:{name}:{levelname}:\n{message}\n\n"
        formatter = logging.Formatter(fmt=format_str, style="{")
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def _validate_config(
        self,
        job_dict,
        avail_sources,
        avail_destinations,
        transform_fn,
        email_builders,
    ):
        """
        Orchestrates all validation steps. Raises a single ValueError if any issues are found,
        but tries to collect multiple in a single validation phase if possible.
        """
        issues = []

        # --- Step 1: Validate Job Structure ---
        try:
            self.logger.debug(f"Received job dict to validate: {job_dict}")
            self.job = Job(**job_dict)
        except ValidationError as e:
            raise ValueError(f"Job configuration is invalid:\n{e}")

        # --- Step 2: Check for Existence ---
        used_source_names = {task.source for task in self.job.extract_tasks.values()}
        used_dest_names = {task.destination for task in self.job.load_tasks.values()}

        if not used_source_names.issubset(avail_sources.keys()):
            issues.append(
                f"Job references undefined sources: {used_source_names - avail_sources.keys()}"
            )

        if not used_dest_names.issubset(avail_destinations.keys()):
            issues.append(
                f"Job references undefined destinations: {used_dest_names - avail_destinations.keys()}"
            )

        # --- Step 3: Validate Used Sources & Destinations ---
        sources_to_validate = {name: avail_sources[name] for name in used_source_names}
        dests_to_validate = {name: avail_destinations[name] for name in used_dest_names}

        try:
            self.sources = {
                name: Source(**s) for name, s in sources_to_validate.items()
            }
            self.destinations = {
                name: Destination(**d) for name, d in dests_to_validate.items()
            }
        except ValidationError as e:
            issues.append(f"A source or destination config is invalid:\n{e}")

        # --- Step 4: Validate Dependencies vs. Type (for just extract tasks for now) ---
        if not issues:
            for task_name, task_config in self.job.extract_tasks.items():
                source_model = self.sources[task_config.source]

                # SQL sources require .sql file paths
                if source_model.type == "sql":
                    for dep in task_config.dependencies:
                        if not dep.endswith(".sql"):
                            issues.append(
                                f"Extract task '{task_name}' requires .sql files, but got '{dep}'"
                            )

                # Fileshare and SFTP sources require relative file paths
                elif source_model.type in ["fileshare", "sftp"]:
                    for dep in task_config.dependencies:
                        if dep.startswith("/") or dep.endswith("/"):
                            issues.append(
                                f"Extract task '{task_name}' requires a relative file path, but got '{dep}'"
                            )

                # Google Drive sources require a non-empty string (name or ID)
                elif source_model.type == "google_drive":
                    for dep in task_config.dependencies:
                        if not dep:
                            issues.append(
                                f"Extract task '{task_name}' requires a non-empty file name or ID."
                            )

        # --- Step 5: Validate Job-Specific Functions ---
        try:
            self.transform = TransformFunc(function=transform_fn)
            self.email_builders = {
                name: EmailBuilder(function=func)
                for name, func in (email_builders or {}).items()
            }
        except ValidationError as e:
            issues.append(
                f"A custom function (email builder or transform_fn) has an invalid signature:\n{e}"
            )
        self.logger.info("Transform function validated and instantiated successfully")
        self.logger.info(
            f"Email builder functiouns {self.email_builders.keys()} validated and instantiated successfully"
        )

        # --- Final Report ---
        if issues:
            all_errors = "\n - ".join(issues)
            raise ValueError(f"Configuration has one or more errors:\n - {all_errors}")

    def log_job_results(self, dest_responses: list[DestinationResponse]):
        for dest_response in dest_responses:
            self.logger.info(f"{dest_response}\n\n")

    def run(self):
        try:
            extracted_data = self.extractor.extract()
        except Exception as e:
            self.logger.exception(f"Data extraction phase failed:\n{e}")
        self.logger.info("Data extraction complete...")

        try:
            transformed_data = self.transform(extracted_data)
        except Exception as e:
            self.logger.exception(f"Data transformation phase failed:\n{e}")
        self.logger.info("Data transformation complete...")

        try:
            dest_responses = self.loader.load(transformed_data)
        except Exception as e:
            self.logger.exception(f"Data loading phase failed:\n{e}")
        self.logger.info("Data loading complete...")

        self.log_job_results(dest_responses)
