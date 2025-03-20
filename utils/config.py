import configparser
from pathlib import Path
import logging


def load_config(config_file_path: str | Path) -> configparser.ConfigParser:
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
        config.read(str(config_file_path))
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
    config: configparser.ConfigParser, job_name: str, config_dir: str | Path
) -> dict:
    """
    Retrieves configuration for a specific job, creating a nested dictionary.

    Args:
        config: The ConfigParser object.
        job_name: The name of the job section (e.g., "attendance_sync").

    Returns:
        A dictionary with nested dictionaries for 'source', 'service',
        and 'job' configurations. If the job uses its own, unshared service and
        destination, the 'service' property will be None.

    Raises:
        KeyError: If the job section, source, or destination is not found.
        ValueError: If required keys are missing.
    """
    # | --- Assemble job_congig dict ---
    try:
        job_config = dict(config[job_name])
        job_config["job_name"] = job_name
    except KeyError:
        raise KeyError(f"Job '{job_name}' not found in config.")

    # | --- Assemble source_config dict ---
    source_name = job_config.get("source")
    if not source_name:
        raise ValueError(f"Job '{job_name}' must specify 'source'.")

    try:
        source_config = dict(config[source_name])
        source_config["source_name"] = source_name
        driver_file_path = Path(config_dir) / source_config["driver_file"]
        source_config["driver_file"] = str(driver_file_path)
    except KeyError:
        raise KeyError(f"Source config '{source_name}' not found for job '{job_name}'.")

    # | --- Assemble service_config dict ---
    destination_type = job_config.get("destination_type")
    if not destination_type:
        raise ValueError(f"Job '{job_name}' must specify 'destination_type'.")

    service_config = None
    if destination_type == "shared_service":
        service_name = job_config.get("service")
        if not service_name:
            raise ValueError(
                f"Job '{job_name}' with destination_type 'shared_service' must specify 'service' key."
            )
        try:
            service_config = dict(config[service_name])
            service_config["service_name"] = service_name
        except KeyError:
            raise KeyError(
                f"Shared service '{service_name}' config not found for job '{job_name}'."
            )
    else:
        # It's has job-specific service and destination
        pass

    # | --- Assemble final_config dict ---
    final_config = {
        "source": source_config,
        "service": service_config,
        "job": job_config,
    }

    return final_config


def determine_output_filename(job_config: dict, job_name: str) -> str:
    """
    Determines the output filename. Defaults to job_name.csv.

    Args:
        job_config: The combined, nested job configuration dictionary.
        job_name: The name of the job.

    Returns:
        The determined output filename (e.g., "attendance.csv").
    """
    try:
        base_filename = job_config["job"].get(
            "base_filename", job_name
        )  # Access base_filename from nested 'job' dict
        return f"{base_filename}.csv"
    except Exception as e:
        raise Exception(f"Error determining output filename: {e}")


def locate(job_name: str) -> dict:
    """
    Calculates and returns necessary file/directory paths.

    Args:
        job_name: The name of the job (e.g., "attendance_sync").

    Returns:
        A dictionary of paths.
    """
    try:
        # Correctly calculate project_root relative to *this* file (config.py)
        project_root = Path(__file__).resolve().parent.parent
        script_dir = project_root / "data_integration_elements" / f"DIE_{job_name}"
        output_dir = script_dir / "output"
        config_dir = project_root / "config"

        query_file_path = script_dir / "query.sql"
        log_file_path = output_dir / "output.log"
        config_file_path = config_dir / "config.ini"

        return {
            "project_root": str(project_root),
            "script_dir": str(script_dir),
            "output_dir": str(output_dir),
            "config_dir": str(config_dir),
            "query_file_path": str(query_file_path),
            "log_file_path": str(log_file_path),
            "config_file_path": str(config_file_path),
        }
    except Exception as e:
        raise Exception(f"Error determining paths: {e}")
        # logging.exception(f"Error determining paths: {e}")
        # return {
        #     "project_root": None,
        #     "script_dir": None,
        #     "output_dir": None,
        #     "config_dir": None,
        #     "query_file_path": None,
        #     "log_file_path": None,
        #     "config_file_path": None,
        # }
