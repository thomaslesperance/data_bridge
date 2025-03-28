from pathlib import Path
import logging
import configparser
from typing import Optional, Callable
from pydantic import ValidationError
from utils.models import InitialPaths, FinalPaths, ValidatedConfigUnion
from utils.die import DIE
from utils.transform import export_csv_from_data

# Must update this if new function defined for default behavior of transform step
# Currently, it's just a simple export to CSV...
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


def load_config(config_file_path: Path) -> configparser.ConfigParser:
    """
    Loads the configuration from the given file.

    Args:
        config_file_path: The path to the config.ini file (string or Path object).

    Returns:
        A configparser.ConfigParser object.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        configparser.Error: If there's an error parsing the config file.
    """
    config = configparser.ConfigParser()
    try:
        config.read(config_file_path)
        if not config.sections():
            raise FileNotFoundError(
                f"Config file not found or empty: {config_file_path}"
            )
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Error loading config file: {e}")
    except configparser.Error as e:
        raise configparser.Error(f"Error loading config file: {e}")

    return config


def get_job_config(
    config: configparser.ConfigParser, job_name: str, config_dir: Path
) -> ValidatedConfigUnion:
    """
    Retrieves configuration for a specific job, creating a nested dictionary.

    Args:
        config: The ConfigParser object.
        job_name: The name of the job section (e.g., "attendance_sync").
        config_dir: The path to the project config directory.

    Returns:
        A (validated) dict with nested dictionaries for 'source', 'shared_dest',
        and 'job' configurations. If the job loads to its own unique destination,
        the 'shared_dest' property will be None.

    Raises:
        ValidationError: If validation of collated config dict does not match ValidatedConfigUnion model.
    """
    try:
        job = dict(config[job_name])
        job["job_name"] = job_name

        source = dict(config[job["source"]])
        source["source_name"] = job["source"]
        driver_file_path = config_dir / source["driver_file"]
        source["driver_file"] = driver_file_path

        shared_dest = None

        uses_shared_dest = job["is_shared_destination"]
        if uses_shared_dest:
            shared_dest = dict(config[job["shared_destination"]])
            shared_dest["shared_dest_name"] = job["shared_destination"]

        initial_job_config = {source, shared_dest, job}
        validated_job_config = ValidatedConfigUnion.model_validate(initial_job_config)

        return validated_job_config

    except ValidationError as e:
        raise ValidationError(f"Configuration data does not match data model: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to get job configuration: {e}") from e


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
        custom_transform_fn: The custom transformation defined in main.py unique to this job;
                            defaults to DEFAULT_TRANSFORM_FN at top of die.py.
        message_builder_fn: The custom message builder defined in main.py unique to this job; needed for email-based jobs.

    Returns:
        An initialized DIE object ready to run.

    Raises:
        ValidationError: If final config and paths dicts validation fails.
    """
    try:
        # 1. --------- Determine paths --------------------------------------------------
        initial_paths: InitialPaths = _determine_paths(job_name)

        # 2. ---------Configure logging -------------------------------------------------
        _configure_logging(initial_paths.log_file_path)
        logging.info(
            f"Logging configured for job. Log file: {initial_paths.log_file_path}"
        )
        logging.info(f"Starting setup for job.")

        # 3. ---------Load main configuration -------------------------------------------
        raw_config = load_config(initial_paths.config_file_path)
        logging.info("Main config file loaded.")

        # 4. ---------Get and validate specific job configuration -----------------------
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

        # 5. ---------Determine output filename and assemble final validated paths ------
        output_filename = _determine_output_filename(validated_job_config, job_name)
        intermediate_file_path: Path = initial_paths.output_dir / output_filename
        logging.info(f"Intermediate file path determined: {intermediate_file_path}")

        # 6. ---------Assemble and validate final paths dict ----------------------------
        try:
            final_paths_data = initial_paths.model_dump()
            final_paths_data["intermediate_file_path"] = intermediate_file_path
            validated_final_paths = FinalPaths.model_validate(final_paths_data)
            logging.info("Final job paths validated successfully.")
        except ValidationError as e:
            raise ValidationError(f"Final path validation failed:\n{e}") from e

        # 7. ---------Determine actual transform function -------------------------------
        transform_fn = custom_transform_fn or DEFAULT_TRANSFORM_FN
        if transform_fn != DEFAULT_TRANSFORM_FN:
            logging.info("Using custom transform function provided.")
        else:
            logging.info("Using default transform function.")

        # 8. ---------Instantiate the DIE class with validated inputs -------------------
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
