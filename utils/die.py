import logging
from pathlib import Path
from typing import Callable, Dict, Any, Optional
import sys
from utils.config import load_config, get_job_config
from utils.extract import extract_data
from utils.transform import export_csv_from_data
from utils.load import load_data


def _determine_paths(job_name: str) -> Dict[str, str]:
    """
    Calculates and returns necessary file/directory paths.

    Args:
        job_name: The name of the job (e.g., "attendance_sync").

    Returns:
        A dictionary of paths.
    """
    try:
        project_root = Path(__file__).resolve().parent.parent
        script_dir = project_root / "data_integration_elements" / f"DIE_{job_name}"
        output_dir = script_dir / "output"
        config_dir = project_root / "config"

        if not script_dir.is_dir():
            raise FileNotFoundError(
                f"DIE script directory not found for job '{job_name}': {script_dir}"
            )

        query_file_path = script_dir / "query.sql"
        log_file_path = output_dir / "output.log"
        config_file_path = config_dir / "config.ini"

        if not config_file_path.is_file():
            raise FileNotFoundError(f"Main config file not found: {config_file_path}")
        if not query_file_path.is_file():
            raise FileNotFoundError(
                f"Query file not found for job '{job_name}': {query_file_path}"
            )

        paths = {
            "project_root": str(project_root),
            "script_dir": str(script_dir),
            "output_dir": str(output_dir),
            "config_dir": str(config_dir),
            "query_file_path": str(query_file_path),
            "log_file_path": str(log_file_path),
            "config_file_path": str(config_file_path),
        }

        if not all(p for p in paths.values() if isinstance(p, str)):
            raise ValueError(
                f"Could not determine all essential paths for DIE: {job_name}"
            )

        return paths

    except Exception as e:
        raise e


def _determine_output_filename(job_config: Dict[str, Any], job_name: str) -> str:
    """
    Determines the file name only of the output file to be loaded.

    Args:
        job_config: A nested dict containing all of the config info associated with this job based on the config.ini file.
        job_name: The name of the job (e.g., "attendance_sync").

    Raises:
        RuntimeError: if defaulting to job_name for output file name didn't work and still encountered an error.
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
        paths: Dict[str, str],
        transform_function: Callable,
        message_builder: Optional[Callable] = None,
    ) -> None:
        """
        Initializes a DIE object.

        Args:
            job_name: The name of the integration job and the suffix of this DIE's directory name (DIE_<job_name>).
            job_config: A nested dict containing all of the config info associated with this job based on the config.ini file.
            paths: A dict of absolute path strings defining the directory structure of the project.
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

        Raises:
            FileNotFoundError: If transformed data file not found after transform step.
        """
        logging.info(f"Starting data integration for job: {self.job_name}")

        # --- EXTRACT --- Data source must be specified in project config.ini
        header, data = extract_data(self.job_config, self.paths["query_file_path"])
        logging.info(
            f"Data extracted from {self.job_config['source']['type']} database:  {self.job_config['source_name']['name']}"
        )

        # --- TRANSFORM --- 'custom_transform' defaults to 'export_csv_from_data' if not defined in main.py
        transformed_data_file_path = self.transform_function(
            header, data, self.paths["intermediate_file_path"]
        )
        if (
            not transformed_data_file_path
            or not Path(transformed_data_file_path).is_file()
        ):
            raise FileNotFoundError(
                f"Transformed data file not found after transform step at: {transformed_data_file_path or 'None'}"
            )
        logging.info(f"Data transformed and saved to {transformed_data_file_path}")

        # --- LOAD --- Data destination must be specified in project config.ini
        response = load_data(
            self.job_config, transformed_data_file_path, self.message_builder
        )
        logging.info(f"Data transfer completed. Response: {response}")

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

    Args:
        job_name: The name of the integration job and the suffix of this DIE's directory name (DIE_<job_name>).
        custom_transform: The custom transformation defined in main.py unique to this job; defaults to exporting a CSV.
        message_builder: The custom message builder defined in main.py unique to this job; needed for email-based jobs.

    Returns: An initialized DIE instance.
    """
    # TO-DO: after implementing data validation with pydantic, validate "job_config" and "paths" dicts
    # especially before passing as arguments to constructor

    # 1. Determine paths
    paths = _determine_paths(job_name)

    # 2. Configure logging
    _configure_logging(paths["log_file_path"])
    logging.info(
        f"Logging configured for job: {job_name}. Log file: {paths['log_file_path']}"
    )
    logging.info(f"Starting setup for job: {job_name}")

    # 3. Load main configuration
    config = load_config(paths["config_file_path"])
    logging.info("Main config file loaded.")

    # 4. Get and validate specific job configuration
    job_config = get_job_config(config, job_name, paths["config_dir"])
    logging.info("Job-specific configuration loaded and validated.")

    # 5. Determine output filename and full path
    output_filename = _determine_output_filename(job_config, job_name)
    intermediate_file_path = str(Path(paths["output_dir"]) / output_filename)
    paths["intermediate_file_path"] = intermediate_file_path
    logging.info(f"Intermediate file path determined: {intermediate_file_path}")

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
        paths=paths,
        transform_function=actual_transform_function,
        message_builder=message_builder,
    )

    logging.info(
        f"Setup completed successfully for job: {job_name}. DIE instance created."
    )

    return die_instance
