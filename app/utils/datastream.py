import logging
from logger import logger, StreamNameFilter
from errors import LogAndTerminate
from models import (
    Source,
    Destination,
    Stream,
    TransformFunc,
    EmailBuilder,
    ValidatedConfig,
    DestinationResponse,
)
from extractor import Extractor
from loader import Loader


class DataStream:
    """A container class designed to manage the state and orchestrate the flow of data
    between its child components (extractor, loader, and Stream-specific transformer).
    """

    def __init__(
        self,
        globals,
        sources,
        destinations,
        streams,
        stream_name,
        transform_fn,
        email_builders=None,
    ) -> None:

        self.globals = globals
        self.sources_config = sources
        self.dests_config = destinations
        self.streams_config = streams
        self.stream_name = stream_name
        self.raw_transform_fn = transform_fn
        self.raw_email_builders = email_builders
        self.logger = logger
        self._setup_data_stream()

    @LogAndTerminate()
    def run(self) -> None:
        extracted_data = self.extractor.extract()
        transformed_data = self.transform(extracted_data)
        dest_responses = self.loader.load(transformed_data)
        self._log_dest_responses(dest_responses)

    @LogAndTerminate()
    def _setup_data_stream(self) -> None:
        # Get the specific Stream dictionary
        stream_dict = self.streams_config[self.stream_name]

        # Configure the logger
        log_file = self.globals.get("log_file", f"{self.stream_name}.log")
        log_level = stream_dict.get("log_level", logging.INFO)
        self.logger = self._configure_logger(
            logger=self.logger,
            stream_name=self.stream_name,
            log_file=log_file,
            log_level=log_level,
        )

        # Validate all configs and get the validated Pydantic objects back
        validated_config = self._validate_config(
            stream_dict=stream_dict,
            avail_sources=self.sources_config,
            avail_destinations=self.dests_config,
            transform_fn=self.raw_transform_fn,
            email_builders=self.raw_email_builders,
        )

        # Assign the validated objects to self
        self.stream = validated_config.stream
        self.sources = validated_config.sources
        self.destinations = validated_config.destinations

        log_wrapper = LogAndTerminate()
        self.transform = log_wrapper(validated_config.transform)
        self.email_builders = validated_config.email_builders

        # Instantiate the child components
        self.extractor = Extractor(
            sources=self.sources,
            extract_tasks=self.stream.extract_tasks,
        )
        self.loader = Loader(
            destinations=self.destinations,
            load_tasks=self.stream.load_tasks,
            email_builders=self.email_builders,
        )

    def _configure_logger(
        self, logger, stream_name, log_file, log_level
    ) -> logging.Logger:
        logger.setLevel(log_level)
        logger.addFilter(StreamNameFilter(stream_name))
        format_str = "\n\n{levelname}:\t{asctime}:\t{stream_name}:\n\t{message}\n"
        formatter = logging.Formatter(fmt=format_str, style="{")
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    @LogAndTerminate()
    def _validate_config(
        self,
        stream_dict,
        avail_sources,
        avail_destinations,
        transform_fn,
        email_builders,
    ) -> ValidatedConfig:
        """
        Orchestrates all validation steps and returns a container with
        validated Pydantic objects.
        """
        validated_stream = self._validate_stream(stream_dict)
        avail_sources, avail_destinations = self._check_for_missing_definitions(
            validated_stream, avail_sources, avail_destinations
        )
        used_sources = self._validate_sources(validated_stream, avail_sources)
        validated_sources = self._validate_dependencies(validated_stream, used_sources)
        validated_destinations = self._validate_destinations(
            validated_stream, avail_destinations
        )
        validated_transform = self._validate_transform_function(transform_fn)
        validated_email_builders = self._validate_email_builders(email_builders)

        return ValidatedConfig(
            stream=validated_stream,
            sources=validated_sources,
            destinations=validated_destinations,
            transform=validated_transform,
            email_builders=validated_email_builders,
        )

    def _validate_stream(self, stream_dict: dict) -> Stream:
        return Stream(**stream_dict)

    def _check_for_missing_definitions(
        self, validated_stream: Stream, avail_sources: dict, avail_destinations: dict
    ) -> tuple[dict]:
        issues = []
        used_source_names = {
            task.source for task in validated_stream.extract_tasks.values()
        }
        used_dest_names = {
            task.destination for task in validated_stream.load_tasks.values()
        }

        if not used_source_names.issubset(avail_sources.keys()):
            issues.append(
                f"Stream references undefined sources: {used_source_names - avail_sources.keys()}"
            )
        if not used_dest_names.issubset(avail_destinations.keys()):
            issues.append(
                f"Stream references undefined destinations: {used_dest_names - avail_destinations.keys()}"
            )
        if issues:
            all_errors = "\n - ".join(issues)
            raise ValueError(f"Configuration has one or more errors:\n - {all_errors}")
        else:
            return avail_sources, avail_destinations

    def _validate_sources(
        self, validated_stream: Stream, avail_sources: dict
    ) -> dict[str, Source]:
        used_names = {task.source for task in validated_stream.extract_tasks.values()}
        to_validate = {name: avail_sources[name] for name in used_names}
        return {name: Source(**s) for name, s in to_validate.items()}

    def _validate_destinations(
        self, validated_stream: Stream, avail_destinations: dict
    ) -> dict[str, Destination]:
        used_names = {task.destination for task in validated_stream.load_tasks.values()}
        to_validate = {name: avail_destinations[name] for name in used_names}
        return {name: Destination(**d) for name, d in to_validate.items()}

    def _validate_transform_function(self, transform_fn: callable) -> TransformFunc:
        return TransformFunc(function=transform_fn)

    def _validate_email_builders(
        self, email_builders: dict[str, callable]
    ) -> dict[str, EmailBuilder]:
        return {
            name: EmailBuilder(function=func)
            for name, func in (email_builders or {}).items()
        }

    def _validate_dependencies(
        self, validated_stream: Stream, validated_sources: dict[str, Source]
    ) -> list[str]:
        issues = []
        for task_name, task_config in validated_stream.extract_tasks.items():
            source_model = validated_sources[task_config.source]

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
            if issues:
                all_errors = "\n - ".join(issues)
                raise ValueError(
                    f"Configuration has one or more errors:\n - {all_errors}"
                )
            else:
                return validated_sources

    def _log_dest_responses(self, dest_responses: list[DestinationResponse]) -> None:
        for dest_response in dest_responses:
            self.logger.info(
                f"DataStream '{self.stream_name}' load task completed:\n\t{dest_response}\n\n"
            )
