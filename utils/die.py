import logging
from pathlib import Path
from typing import Callable, Dict, Any, Optional
import sys
from pydantic import ValidationError
from utils.config import load_config, get_job_config
from utils.extract import extract_data
from utils.transform import export_csv_from_data
from utils.load import load_data
from utils.models import InitialPaths, FinalPaths


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
        FileNotFoundError, ValueError, ValidationError, RuntimeError: On failure.
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
        sys.stderr.write(
            f"ERROR: Initial path validation (Pydantic) failed for job '{job_name}':\n{e}\n"
        )
        raise ValueError("Initial path validation failed") from e
    except Exception as e:
        sys.stderr.write(f"ERROR determining initial paths for job '{job_name}': {e}\n")
        raise RuntimeError(f"Error determining initial paths: {e}") from e


def _determine_output_filename(job_config: Dict[str, Any], job_name: str) -> str:
    """
    Determines the file name only of the output file to be loaded.

    Args:
        job_config: A nested dict containing all of the config info associated with this job based on the config.ini file.
        job_name: The name of the job (e.g., "attendance_sync").

    Raises:
        RuntimeError: If defaulting to job_name for output file name didn't work and still encountered an error.
    """
    try:
        base_filename = job_config.get("job", {}).get("base_filename", job_name)
        if not base_filename or not isinstance(base_filename, str):
            logging.warning(
                f"Invalid or missing 'base_filename' for job '{job_name}', defaulting to job name."
            )
        base_filename = job_name

        return f"{base_filename}.csv"

    except Exception as e:
        raise RuntimeError(f"Error determining output filename: {e}") from e


def _configure_logging(log_file_path_str: str) -> None:
    """
    Configures logging for DIE instance.

    Args:
        log_file_path_str: The absolute path of the log file for this integration.

    Raises:
        RuntimeError: if defaulting to job_name for output file name didn't work and still encountered an error.
    """
    try:
        log_file_path = Path(log_file_path_str)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Avoid duplicate entries in logs by removing previous handlers before configuring
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        logging.basicConfig(
            filename=str(log_file_path),
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filemode="a",
        )
    except Exception as e:
        sys.stderr.write(
            f"FATAL: Failed to configure logging to {log_file_path_str}: {e}\n"
        )
        raise RuntimeError(f"Failed to configure logging: {e}") from e


class DIE:
    """
    Represents a data integration element (DIE) within the data integration pipeline (DIP) comprised of
    this project with all of its instantiated jobs and config.

    Attributes:
        job_config: A nested dict containing all of the config info associated with this job based on the config.ini file.
        job_name: The name of the integration job and the suffix of this DIE's directory name (DIE_<job_name>).
        message_builder: The custom email message building function passed to load_data for jobs that send emails.
        paths: A dict of absolute path strings defining the directory structure of the project.
        transform_function: The custom transformations unique to this job done to the data extracted
                            from the data source specified in the config.ini file.

    Methods:
        run: Runs the data integration process (Extract, Transform, Load) using the state unique to this DIE.
    """

    def __init__(
        self,
        job_name: str,
        job_config: Dict[str, Any],
        paths: FinalPaths,
        transform_function: Callable,
        message_builder: Optional[Callable] = None,
    ) -> None:
        """
        Initializes a DIE object.

        Args:
            job_name: The name of the integration job and the suffix of this DIE's directory name (DIE_<job_name>).
            job_config: A nested dict containing all of the config info associated with this job based on the config.ini file.
            paths: A validated Paths model instance containing essential paths.
            transform_function: The custom transformations unique to this job done to the data extracted
                                from the data source specified in the config.ini file; defaults to exporting a CSV.
            message_builder: The custom email message building function passed to load_data for jobs that send emails.
        """
        self.job_name = job_name
        self.job_config = job_config
        self.paths = paths
        self.transform_function = transform_function
        self.message_builder = message_builder
        logging.info(f"DIE instance for job '{self.job_name}' initialized internally.")

    def run(self) -> None:
        """
        Runs the data integration process (Extract, Transform, Load) using the state unique to this DIE.
        Raises exceptions on failure, but expects caller (main.py) to log them.
        Uses attribute access on self.paths model.

        Raises:
            FileNotFoundError: If transformed data file not found after transform step.
        """
        logging.info(f"Starting data integration for job: {self.job_name}")

        # --- EXTRACT --- Data source must be specified in project config.ini
        query_file = self.paths.query_file_path
        header, data = extract_data(self.job_config, query_file)
        source_name = self.job_config.get("source", {}).get(
            "source_name", "Unknown Source"
        )
        logging.info(
            f"Data extracted successfully from source '{source_name}'. Records retrieved: {len(data)}"
        )

        # --- TRANSFORM --- 'custom_transform' defaults to 'export_csv_from_data' if not defined in main.py
        intermediate_file_path = self.paths.intermediate_file_path
        transformed_file_path = self.transform_function(
            header, data, intermediate_file_path
        )
        if not transformed_file_path or not Path(transformed_file_path).is_file():
            raise FileNotFoundError(
                f"Transformed data file not found after transform step at: {transformed_file_path or 'None'}"
            )
        logging.info(f"Data transformed and saved to {transformed_file_path}")

        # --- LOAD --- Data destination must be specified in project config.ini
        response = load_data(
            self.job_config, transformed_file_path, self.message_builder
        )
        dest_type = self.job_config.get("job", {}).get("destination_type", "N/A")
        logging.info(
            f"Data load initiated. Destination type: '{dest_type}'. Response/Status: {response}"
        )

        logging.info(
            f"Data integration run for job: {self.job_name} completed successfully."
        )


def setup_and_get_die(
    job_name: str,
    custom_transform: Optional[Callable] = None,
    message_builder: Optional[Callable] = None,
) -> DIE:
    """
    Performs setup (paths, logging, config) and returns an initialized DIE instance.
    Raises exceptions on failure, but expects caller (main.py) to log them.
    Includes Pydantic validation for paths.

    Args:
        job_name: The name of the integration job and the suffix of this DIE's directory name (DIE_<job_name>).
        custom_transform: The custom transformation defined in main.py unique to this job; defaults to exporting a CSV.
        message_builder: The custom message builder defined in main.py unique to this job; needed for email-based jobs.

    Returns:
        An initialized DIE object ready to run.

    Raises:
        FileNotFoundError, ValueError, RuntimeError, ValidationError: If setup fails.
    """

    # 1. Determine paths
    initial_paths: InitialPaths = _determine_paths(job_name)

    # 2. Configure logging
    _configure_logging(str(initial_paths.log_file_path))
    logging.info(
        f"Logging configured for job: {job_name}. Log file: {initial_paths.log_file_path}"
    )
    logging.info(f"Starting setup for job: {job_name}")

    # 3. Load main configuration
    config = load_config(initial_paths.config_file_path)
    logging.info("Main config file loaded.")

    # 4. Get and validate specific job configuration
    job_config = get_job_config(config, job_name, initial_paths.config_dir)
    logging.info("Job-specific configuration loaded and validated.")

    # 5. Determine output filename and assemble final validated paths
    output_filename = _determine_output_filename(job_config, job_name)
    intermediate_file_path: Path = initial_paths.output_dir / output_filename
    logging.info(f"Intermediate file path determined: {intermediate_file_path}")

    try:
        final_paths_data = initial_paths.model_dump()
        final_paths_data["intermediate_file_path"] = intermediate_file_path

        validated_final_paths = FinalPaths.model_validate(final_paths_data)
        logging.info("Final job paths validated successfully.")
    except ValidationError as e:
        logging.error(f"Final path validation failed for job '{job_name}':\n{e}")
        raise ValueError(f"Final path validation failed") from e

    # 6. Determine actual transform function
    actual_transform_function = custom_transform or export_csv_from_data
    if actual_transform_function == custom_transform and custom_transform is not None:
        logging.info("Using custom transform function provided.")
    else:
        logging.info("Using default CSV export transform function.")

    # 7. Instantiate the DIE class
    die_instance = DIE(
        job_name=job_name,
        job_config=job_config,
        paths=validated_final_paths,
        transform_function=actual_transform_function,
        message_builder=message_builder,
    )

    logging.info(
        f"Setup completed successfully for job: {job_name}. DIE instance created."
    )

    return die_instance
