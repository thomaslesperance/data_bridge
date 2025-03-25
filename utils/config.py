import configparser
from pathlib import Path
from typing import Any
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


def _check_key(
    config_dict: dict, key: str, config_name: str, condition_info: str = ""
) -> any:
    """
    Checks for key presence and non-empty/non-None value within a config dict.

    Also strips leading/trailing whitespace from string values before validation.

    Args:
        config_dict: The dictionary representing a configuration section
                     (e.g., source_config, job_config).
        key: The string name of the key to validate within config_dict.
        config_name: A string representing the name of the configuration section
                     (e.g., "'job'", "'service'") used for error messages.
        condition_info: An optional string providing context about why this
                        key is required (e.g., "(required when destination_type='sftp')").
                        Defaults to "".

    Returns:
        The validated (and potentially whitespace-stripped) value associated
        with the key. The type depends on the original value.

    Raises:
        TypeError: If config_dict is not actually a dictionary.
        ValueError: If the key is missing in config_dict, or if the value
                    is None or an empty string/list/dict/tuple after stripping.
    """

    # Check if  "config_dict" is a dict
    if not isinstance(config_dict, dict):
        raise TypeError(
            f"Configuration section '{config_name}' must be a dictionary, but got {type(config_dict).__name__}."
        )

    # Check if required key is present
    if key not in config_dict:
        raise ValueError(
            f"Missing required key '{key}' in {config_name}{condition_info}."
        )

    value = config_dict[key]

    # Handle strings specifically to strip whitespace
    if isinstance(value, str):
        value = value.strip()

    # Check if required key is falsy (None, empty string/list/dict)
    if value is None or (isinstance(value, (str, list, dict, tuple)) and not value):
        raise ValueError(
            f"Required key '{key}' in {config_name} cannot be empty or None{condition_info}."
        )

    return value


def _validate_job_config(job_data: dict):
    """
    Validates the structure and required keys of a resolved job configuration dictionary.

    Assumes input `job_data` has keys 'source', 'service', 'job', where the
    values are dictionaries representing the resolved configurations, and 'service'
    might be None or empty if not applicable.

    Args:
        job_data: A dictionary containing 'source', 'service', and 'job' config dicts.

    Raises:
        ValueError: If validation fails due to missing keys, empty values,
                    incorrect types, or unmet conditional requirements.
        TypeError: If the main config or essential sub-configs are not dictionaries.
    """
    if not isinstance(job_data, dict):
        raise TypeError("job_data must be a dictionary.")

    # --- 1. Define required keys (based on config.ini) ---

    # Keys required in the "source" dictionary
    SOURCE_REQUIRED_KEYS = [
        "type",
        "user",
        "password",
        "conn_string",
        "driver_name",
        "driver_file",
    ]

    # Keys always required in the "job" dictionary
    JOB_BASE_REQUIRED = ["source", "destination_type"]

    # Keys required in the "service" dictionary, based on service["type"]
    SERVICE_BASE_REQUIRED = ["type"]
    SERVICE_CONDITIONAL_REQS = {
        "smtp": ["host", "port"],
        "fileshare": ["mount_path"],
    }

    # Keys required within the "job" dictionary, depending on job["destination_type"]
    JOB_CONDITIONAL_REQS = {
        # Case 1: "destination_type" is "shared_service"
        "shared_service": {
            "base": ["service"],
            # Keys required in "job" based on the service["type"] of the shared service being used
            "by_service_type": {
                "smtp": ["recipients", "sender_email", "base_filename"],
                "fileshare": [
                    "path"
                ],  # "path" is relative to the service's "mount_path"
            },
        },
        # Case 2: "destination_type" defines its own service (e.g., sftp)
        "sftp": ["host", "user", "password", "port", "base_filename", "remote_path"],
    }

    # --- 2. Validate top-level structure and get config parts ---
    logging.info(f"Validating job_data: {job_data}")
    source_config = job_data.get("source")
    service_config = job_data.get("service")  # Can be None or {}
    job_config = job_data.get("job")

    if not isinstance(source_config, dict):
        raise TypeError("'source' section in job_data must be a dictionary.")
    if not isinstance(job_config, dict):
        raise TypeError("'job' section in job_data must be a dictionary.")
    # "service" is allowed to be None initially

    # --- 3. Validate "source" config ---
    logging.info("Validating 'source' config...")
    for key in SOURCE_REQUIRED_KEYS:
        _check_key(source_config, key, "'source'")

    # --- 4. Validate 'job' config (base) ---
    logging.info("Validating 'job' config (base)...")
    for key in JOB_BASE_REQUIRED:
        _check_key(job_config, key, "'job'")

    # Get the destination_type
    dest_type = job_config.get("destination_type", "").strip()
    if not dest_type:
        raise ValueError("Key 'destination_type' in 'job' config cannot be empty.")
    logging.info(f"Destination type: {dest_type}")

    # --- 5. Conditional validation based on job["destination_type"] ---

    # Scenario A: Destination uses a shared service
    if dest_type == "shared_service":
        logging.debug(
            "Destination type is 'shared_service'. Validating service and job specifics..."
        )
        # 5.A.1: Validate "service" config presence and base keys
        if not service_config or not isinstance(service_config, dict):
            raise ValueError(
                "Config section 'service' must be a non-empty dictionary when destination_type is 'shared_service'."
            )

        service_type = None
        for key in SERVICE_BASE_REQUIRED:
            condition = f" (required in 'service' when destination_type='{dest_type}')"
            value = _check_key(service_config, key, "'service'", condition)
            if key == "type":
                service_type = value

        if not service_type:
            raise ValueError("Could not determine 'type' from 'service' config.")

        logging.debug(f"Shared service type: {service_type}")

        if service_type not in SERVICE_CONDITIONAL_REQS:
            raise ValueError(
                f"Invalid 'type' ('{service_type}') found in 'service' config. Allowed types: {list(SERVICE_CONDITIONAL_REQS.keys())}"
            )

        # 5.A.2: Validate "service" config conditional keys
        required_service_keys = SERVICE_CONDITIONAL_REQS[service_type]
        for key in required_service_keys:
            condition = f" (required for service type='{service_type}')"
            _check_key(service_config, key, "'service'", condition)

        # 5.A.3: Validate "job" config base keys for shared service
        shared_service_base_job_keys = JOB_CONDITIONAL_REQS["shared_service"]["base"]
        for key in shared_service_base_job_keys:
            condition = f" (required when destination_type='{dest_type}')"
            _check_key(job_config, key, "'job'", condition)

        # 5.A.4: Validate "job" config keys specific to the type of shared service
        if service_type in JOB_CONDITIONAL_REQS["shared_service"]["by_service_type"]:
            required_job_keys_for_service = JOB_CONDITIONAL_REQS["shared_service"][
                "by_service_type"
            ][service_type]
            for key in required_job_keys_for_service:
                condition = f" (required in job for service type='{service_type}')"
                _check_key(job_config, key, "'job'", condition)
        else:
            # This means the service type is defined in SERVICE_CONDITIONAL_REQS but not in JOB_CONDITIONAL_REQS["shared_service"]["by_service_type"]
            raise ValueError(
                f"No supporting 'job' keys defined for shared service type '{service_type}'."
            )

    # Scenario B: Destination is defined directly in "job" config dict (e.g., SFTP)
    elif dest_type in JOB_CONDITIONAL_REQS:
        logging.info(
            f"Destination type '{dest_type}' defines its own parameters. Validating 'job' specifics..."
        )
        # 5.B.1: Ensure "service" config is not present or is empty
        if service_config and isinstance(service_config, dict) and service_config:
            raise ValueError(
                f"'service' config is present but destination_type is '{dest_type}'. Please update destination_type to a supported value (e.g., sftp)."
            )

        # 5.B.2: Validate "job" config keys specific to this direct destination type
        required_job_keys = JOB_CONDITIONAL_REQS[dest_type]
        for key in required_job_keys:
            condition = f" (required for destination_type='{dest_type}')"
            _check_key(job_config, key, "'job'", condition)

    # Scenario C: Unknown "destination_type"
    else:
        allowed_types = list(JOB_CONDITIONAL_REQS.keys())
        raise ValueError(
            f"Invalid 'destination_type': '{dest_type}' in 'job' config. Allowed types: {allowed_types}"
        )

    logging.info(
        "Job configuration as loaded from project config.ini validated successfully."
    )


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
        # It's has job-specific service and destination; service_config dict remains None
        pass

    # | --- Assemble collated final_config dict ---
    final_config = {
        "source": source_config,
        "service": service_config,
        "job": job_config,
    }

    # | --- Validate the shape and content of assembled job config ---
    #       ---Will raise exceptions if there are issues---
    _validate_job_config(final_config)

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
        base_filename = job_config["job"].get("base_filename", job_name)
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

        paths = {
            "project_root": str(project_root),
            "script_dir": str(script_dir),
            "output_dir": str(output_dir),
            "config_dir": str(config_dir),
            "query_file_path": str(query_file_path),
            "log_file_path": str(log_file_path),
            "config_file_path": str(config_file_path),
        }

        if not all(paths.values()):
            raise ValueError(f"Could not locate all paths for DIE: {job_name}")

        return paths

    except Exception as e:
        raise Exception(f"Error determining paths: {e}")
