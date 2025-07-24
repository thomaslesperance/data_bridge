from models import (
    ValidatedConfig,
    Stream,
    Source,
    Destination,
    TransformFunc,
    EmailBuilder,
)
from errors import LogAndTerminate


class ConfigValidator:
    @staticmethod
    def validate(
        stream_name: str,
        raw_config: dict,
        raw_transform_fn: callable,
        raw_email_builders: dict[str, callable],
    ) -> ValidatedConfig:
        """Orchestrates all configuration validation for a data stream."""
        validation_issues = []

        avail_sources = raw_config["sources"]
        avail_dests = raw_config["destinations"]
        stream_dict = raw_config["streams"][stream_name]

        validated_stream = ConfigValidator._val_stream(stream_dict)

        missing_config_issues = ConfigValidator._check_for_missing_config(
            stream=validated_stream,
            avail_sources=avail_sources,
            avail_dests=avail_dests,
        )
        validation_issues.update(missing_config_issues)

        validated_sources = ConfigValidator._val_used_sources(
            validated_stream, avail_sources
        )
        validated_dests = ConfigValidator._val_used_dests(validated_stream, avail_dests)

        source_dep_issues = ConfigValidator._check_source_dependencies(
            validated_stream, validated_sources
        )
        validation_issues.update(source_dep_issues)

        validated_transformer = ConfigValidator._val_transform_function(
            raw_transform_fn
        )
        log_wrapper = LogAndTerminate()
        validated_transformer = log_wrapper(validated_transformer)

        validated_email_builders = ConfigValidator._val_email_builders(
            raw_email_builders
        )

        if validation_issues:
            error_report = "\n - ".join(validation_issues)
            raise ValueError(f"Configuration has errors:\n\t - {error_report}")
        else:
            return ValidatedConfig(
                stream=validated_stream,
                sources=validated_sources,
                destinations=validated_dests,
                transform=validated_transformer,
                email_builders=validated_email_builders,
            )

    @staticmethod
    def _val_stream(stream_dict: dict) -> Stream:
        return Stream(**stream_dict)

    @staticmethod
    def _check_for_missing_config(
        self, validated_stream: Stream, avail_sources: dict, avail_dests: dict
    ) -> list[str]:
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
        if not used_dest_names.issubset(avail_dests.keys()):
            issues.append(
                f"Stream references undefined destinations: {used_dest_names - avail_dests.keys()}"
            )
        return issues

    @staticmethod
    def _val_used_sources(
        validated_stream: Stream, avail_sources: dict
    ) -> dict[str, Source]:
        used_names = {task.source for task in validated_stream.extract_tasks.values()}
        to_validate = {name: avail_sources[name] for name in used_names}
        return {name: Source(**s) for name, s in to_validate.items()}

    @staticmethod
    def _val_used_dests(
        self, validated_stream: Stream, avail_dests: dict
    ) -> dict[str, Destination]:
        used_names = {task.destination for task in validated_stream.load_tasks.values()}
        to_validate = {name: avail_dests[name] for name in used_names}
        return {name: Destination(**d) for name, d in to_validate.items()}

    @staticmethod
    def _check_source_dependencies(
        validated_stream: Stream, validated_sources: dict[str, Source]
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
        return issues

    @staticmethod
    def _val_transform_function(transform_fn: callable) -> TransformFunc:
        return TransformFunc(function=transform_fn)

    @staticmethod
    def _val_email_builders(
        email_builders: dict[str, callable],
    ) -> dict[str, EmailBuilder]:
        return {
            name: EmailBuilder(function=func) for name, func in email_builders.items()
        }
