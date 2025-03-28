import logging
from pathlib import Path
from typing import Callable, Dict, Any, Optional
import sys
from pydantic import ValidationError
from utils.config import load_config, get_job_config
from utils.extract import extract_data
from utils.transform import export_csv_from_data
from utils.load import load_data
from utils.models import InitialPaths, FinalPaths, ValidatedConfigUnion


DEFAULT_TRANSFORM_FN = export_csv_from_data


def _determine_paths(job_name: str) -> InitialPaths:
    """
    Calculates and returns necessary file/directory paths.
    Existence checks (except for script_dir itself) are deferred to Pydantic.
    Validates returned dict using InitialPaths model.

    Args:
        job_name: The name of the job (e.g., "attendance_sync").

    Returns:
        An InitialJobPaths model instance.

    Raises:
        ValueError, ValidationError: On failure.
    """
    try:
        project_root = Path(__file__).resolve().parent.parent
        script_dir = project_root / "data_integration_elements" / f"DIE_{job_name}"
        output_dir = script_dir / "output"
        config_dir = project_root / "config"
        query_file_path = script_dir / "query.sql"
        log_file_path = output_dir / "output.log"
        config_file_path = config_dir / "config.ini"

        initial_paths_data = {
            "project_root": project_root,
            "script_dir": script_dir,
            "output_dir": output_dir,
            "config_dir": config_dir,
            "query_file_path": query_file_path,
            "log_file_path": log_file_path,
            "config_file_path": config_file_path,
        }

        validated_initial_paths = InitialPaths.model_validate(initial_paths_data)
        return validated_initial_paths

    except ValidationError as e:
        raise ValidationError(f"Initial path validation (Pydantic) failed:\n{e}") from e
    except Exception as e:
        raise RuntimeError(f"Error determining initial paths: {e}") from e


def _determine_output_filename(job_config: ValidatedConfigUnion, job_name: str) -> str:
    """
    Determines the file name only of the output file to be loaded.

    Args:
        job_config: A nested dict containing all of the config info associated with this job;
                    validated with pydantic against ValidatedConfigUnion model.
        job_name: The name of the job (e.g., "attendance_sync").

    Raises:
        RuntimeError: If defaulting to job_name for output file name didn't work and still encountered an error.
    """
    try:
        base_filename = job_config.job.base_filename
        return f"{base_filename}.csv"

    except Exception as e:
        raise RuntimeError(f"Error determining output filename: {e}") from e


def _configure_logging(log_file_path: Path) -> None:
    """
    Configures logging for DIE instance.

    Args:
        log_file_path: The absolute path of the log file for this integration.

    Raises:
        RuntimeError: if defaulting to job_name for output file name didn't work and still encountered an error.
    """
    try:
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            filename=str(log_file_path),
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filemode="a",
        )

    except Exception as e:
        raise RuntimeError(f"Failed to configure logging: {e}") from e


class DIE:
    """
    Represents a data integration element (DIE) within the data integration pipeline (DIP) comprised of
    this project with all of its instantiated jobs and config.

    Attributes:
        job_config: A (validated) nested dict containing all of the config info associated with this job based on the config.ini file.
        job_name: The name of the integration job and the suffix of this DIE's directory name (DIE_<job_name>).
        message_builder_fn: The custom email message building function passed to load_data for jobs that send emails.
        paths: A (validated) dict of absolute path strings defining the directory structure of the project.
        transform_fn: The custom transformations unique to this job done to the data extracted
                            from the data source specified in the config.ini file.

    Methods:
        run: Runs the data integration process (Extract, Transform, Load) using the state unique to this DIE.
    """

    def __init__(
        self,
        job_name: str,
        job_config: ValidatedConfigUnion,
        paths: FinalPaths,
        transform_fn: Callable,
        message_builder_fn: Optional[Callable] = None,
    ) -> None:
        """
        Initializes a DIE object.

        Args:
            job_name: The name of the integration job and the suffix of this DIE's directory name (DIE_<job_name>).
            job_config: A (validated) nested dict containing all of the config info associated with this job based on the config.ini file.
            paths: A (validated) Paths model instance containing essential paths.
            transform_fn: The custom transformations unique to this job done to the data extracted
                                from the data source specified in the config.ini file; defaults to exporting a CSV.
            message_builder_fn: The custom email message building function passed to load_data for jobs that send emails.
        """
        self.job_name = job_name
        self.job_config = job_config
        self.paths = paths
        self.transform_fn = transform_fn
        self.message_builder_fn = message_builder_fn
        logging.info(f"DIE instance initialized internally.")

    def run(self) -> None:
        """
        Runs the data integration process (Extract, Transform, Load) using the state unique to this DIE.
        Raises exceptions on failure, but expects caller (main.py) to log them.
        Uses attribute access on self.paths model.

        Raises:
            FileNotFoundError: If transformed data file not found after transform step.
        """
        logging.info(f"Starting data integration")
        # --------------------------------- EXTRACT -----------------------------------
        query_file = self.paths.query_file_path
        header, data = extract_data(self.job_config, query_file)
        source_name = self.job_config.source.source_name
        logging.info(
            f"Data extracted successfully from source '{source_name}'. Records retrieved: {len(data)}"
        )
        # --------------------------------- TRANSFORM ---------------------------------
        intermediate_file_path = self.paths.intermediate_file_path
        transformed_file_path = self.transform_fn(header, data, intermediate_file_path)
        if not transformed_file_path.is_file():
            raise FileNotFoundError(
                f"Transformed data file not found after transform step at: {transformed_file_path or 'None'}"
            )
        logging.info(f"Data transformed and saved to {str(transformed_file_path)}")
        # --------------------------------- LOAD --------------------------------------
        response = load_data(
            self.job_config, transformed_file_path, self.message_builder_fn
        )
        if self.job_config.job.shared_destination:
            logging.info(
                f"Data loaded to {self.job_config.shared_dest_config.destination_name}. Response/Status: {response}"
            )
        else:
            logging.info(
                f"Data loaded to {self.job_config.job.destination}:{self.job_config.host}:{self.job_config.job.path}. Response/Status: {response}"
            )
        # -----------------------------------------------------------------------------
        logging.info(f"Data integration completed successfully!")


def setup_and_get_die(
    job_name: str,
    custom_transform_fn: Optional[Callable] = None,
    message_builder_fn: Optional[Callable] = None,
) -> DIE:
    """
    Performs setup (paths, logging, config) and returns an initialized DIE instance.
    Raises exceptions on failure, but expects caller (main.py) to log them.
    Includes Pydantic validation for paths.

    Args:
        job_name: The name of the integration job and the suffix of this DIE's directory name (DIE_<job_name>).
        custom_transform_fn: The custom transformation defined in main.py unique to this job; defaults to DEFAULT_TRANSFORM_FN at top of die.py.
        message_builder_fn: The custom message builder defined in main.py unique to this job; needed for email-based jobs.

    Returns:
        An initialized DIE object ready to run.

    Raises:
        ValidationError: If final config and paths dicts validation fails.
    """
    try:
        # 1. Determine paths
        initial_paths: InitialPaths = _determine_paths(job_name)

        # 2. Configure logging
        _configure_logging(initial_paths.log_file_path)
        logging.info(
            f"Logging configured for job. Log file: {initial_paths.log_file_path}"
        )
        logging.info(f"Starting setup for job.")

        # 3. Load main configuration
        raw_config = load_config(initial_paths.config_file_path)
        logging.info("Main config file loaded.")

        # 4. Get and validate specific job configuration
        try:
            validated_job_config: ValidatedConfigUnion = get_job_config(
                raw_config, job_name, initial_paths.config_dir
            )
            logging.info("Job-specific configuration loaded and validated.")
        except (
            KeyError,
            ValueError,
            ValidationError,
            TypeError,
            FileNotFoundError,
        ) as e:
            raise ValidationError(
                f"Error assembling and/or validating job config dict: {e}"
            ) from e

        # 5. Determine output filename and assemble final validated paths
        output_filename = _determine_output_filename(validated_job_config, job_name)
        intermediate_file_path: Path = initial_paths.output_dir / output_filename
        logging.info(f"Intermediate file path determined: {intermediate_file_path}")

        # 6. Assemble and validate final paths dict
        try:
            final_paths_data = initial_paths.model_dump()
            final_paths_data["intermediate_file_path"] = intermediate_file_path
            validated_final_paths = FinalPaths.model_validate(final_paths_data)
            logging.info("Final job paths validated successfully.")
        except ValidationError as e:
            raise ValidationError(f"Final path validation failed:\n{e}") from e

        # 7. Determine actual transform function
        transform_fn = custom_transform_fn or DEFAULT_TRANSFORM_FN
        if transform_fn != DEFAULT_TRANSFORM_FN:
            logging.info("Using custom transform function provided.")
        else:
            logging.info("Using default transform function.")

        # 8. Instantiate the DIE class with validated inputs
        die_instance = DIE(
            job_name=job_name,
            job_config=validated_job_config,
            paths=validated_final_paths,
            transform_fn=transform_fn,
            message_builder_fn=message_builder_fn,
        )

        logging.info(f"Setup completed successfully. DIE instance created.")

        return die_instance

    except Exception as e:
        raise RuntimeError(f"Unexpected error during setup process: {e}") from e
