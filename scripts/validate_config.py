import configparser
import argparse
import sys
from pathlib import Path
from pydantic import ValidationError
import logging

# Defaults to stderr
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

try:
    from utils.models import (
        SourceSkyward,
        SharedDestSmtp,
        SharedDestFileshare,
        JobUniqueSftp,
        JobSharedSmtp,
        JobSharedFileshare,
    )
except ImportError as e:
    log.critical(f"Error: Could not import models from models.py: {e}")
    log.critical("Ensure models.py exists and is accessible in your PYTHONPATH.")
    sys.exit(1)

# Map known data source and shared destination sections to models
# Should be low maintenance since there won't be that many
KNOWN_SOURCE_MODELS = {
    "skyward": SourceSkyward,
    # Add other known source sections and their models here
}

KNOWN_SHARED_DEST_MODELS = {
    "internal_smtp": SharedDestSmtp,
    "skyward_exports": SharedDestFileshare,
    # Add other known shared dest sections and their models here
}

KNOWN_SHARED_DEST_JOB_MODELS = {
    "internal_smtp": JobSharedSmtp,
    "skyward_exports": JobSharedFileshare,
}

KNOWN_UNIQUE_DEST_JOB_MODELS = {"sftp": JobUniqueSftp}


def validate_config(config_file_path: Path):
    """
    Reads a config.ini file and validates its sections against appropriate
    Pydantic models based on section name or content.

    Args:
        config_file_path: Absolute path object pointing to the config.ini file.

    Returns:
        True if validation succeeds for all sections, False otherwise.
    """
    if not config_file_path.is_file():
        log.error(f"Configuration file not found: {config_file_path}")
        return False

    config = configparser.ConfigParser(interpolation=None)

    try:
        config.read(config_file_path)
    except configparser.Error as e:
        log.error(f"Error parsing configuration file {config_file_path}: {e}")
        return False

    all_valid = True
    validation_errors = []
    validated_sources = {}
    validated_shared_dests = {}
    validated_jobs = {}
    context_data = {
        "skip_path_existence_check": True,
        "config_dir": config_file_path.parent,  # Pass config dir context
    }

    log.info(f"Starting validation for {config_file_path}...")

    # Validate each section of config.ini file before quitting
    for section_name in config.sections():
        log.info(f"  Validating section: [{section_name}]")
        section_data = dict(config.items(section_name))
        validation_model = None
        validated_object = None
        category = "Unknown"

        try:
            # 1. Check if section is a known Data Source
            if section_name in KNOWN_SOURCE_MODELS:
                category = "Source"
                validation_model = KNOWN_SOURCE_MODELS[section_name]
                # Add the field expected by the model
                section_data["source_name"] = section_name
                validated_object = validation_model.model_validate(
                    section_data, context=context_data
                )
                validated_sources[section_name] = validated_object

            # 2. Check if section is a known Shared Destination
            elif section_name in KNOWN_SHARED_DEST_MODELS:
                category = "Shared Destination"
                validation_model = KNOWN_SHARED_DEST_MODELS[section_name]
                # Add the field expected by the model
                section_data["shared_dest_name"] = section_name
                validated_object = validation_model.model_validate(
                    section_data, context=context_data
                )
                validated_shared_dests[section_name] = validated_object

            # 3. Assume section is a Job section
            else:
                category = "Job"
                # Add the field expected by the model
                section_data["job_name"] = section_name

                # Check for discriminating keys to select the right Job model
                is_shared = config.getboolean(
                    section_name, "is_shared_destination", fallback=None
                )
                if is_shared is None:
                    raise ValueError(
                        "'is_shared_destination' key is missing or invalid boolean."
                    )

                if is_shared:
                    shared_dest_name = section_data.get("shared_destination")
                    if not shared_dest_name:
                        raise ValueError(
                            "'shared_destination' key is missing for shared destination job."
                        )
                    if shared_dest_name in KNOWN_SHARED_DEST_JOB_MODELS:
                        validation_model = KNOWN_SHARED_DEST_JOB_MODELS[
                            shared_dest_name
                        ]
                    else:
                        raise ValueError(
                            f"Unknown shared_destination type: '{shared_dest_name}'"
                        )

                else:
                    protocol = section_data.get("protocol")
                    if not protocol:
                        raise ValueError(
                            "'protocol' key is missing for unique destination job."
                        )

                    if protocol in KNOWN_UNIQUE_DEST_JOB_MODELS:
                        validation_model = KNOWN_UNIQUE_DEST_JOB_MODELS[protocol]
                    else:
                        raise ValueError(
                            f"Unknown unique destination protocol: '{protocol}'"
                        )

                validated_object = validation_model.model_validate(
                    section_data, context=context_data
                )
                validated_jobs[section_name] = validated_object

            log.info(f"    Section [{section_name}] ({category}) is valid.")

        except ValidationError as e:
            all_valid = False
            model_name = (
                validation_model.__name__ if validation_model else "Unknown Model"
            )
            error_details = [
                f"      - Field '{'.'.join(map(str, err['loc']))}': {err['msg']}"
                for err in e.errors()
            ]
            section_error_message = (
                f"  Validation with Pydantic ran and failed for section [{section_name}] "
                f"(Attempted Model: {model_name}):\n" + "\n".join(error_details)
            )
            validation_errors.append(section_error_message)
            log.warning(
                f"    Validation FAILED for section [{section_name}]. See details below."
            )

        except (
            KeyError,
            ValueError,
            configparser.NoOptionError,
            configparser.NoSectionError,
        ) as e:
            # Catch errors determining type (missing keys) or converting types (e.g., getboolean)
            all_valid = False
            section_error_message = f"  Error processing section [{section_name}]: Invalid structure or missing key - {e}"
            validation_errors.append(section_error_message)
            log.warning(f"    Processing ERROR for section [{section_name}].")

        except Exception as e:
            # Catch unexpected errors during validation
            all_valid = False
            model_name = (
                validation_model.__name__ if validation_model else "Unknown Model"
            )
            section_error_message = f"  Unexpected error validating section [{section_name}] (Model: {model_name}): {e}"
            validation_errors.append(section_error_message)
            log.error(
                f"    UNEXPECTED ERROR during validation for [{section_name}].",
                exc_info=True,
            )

    # Cross-validation: make sure any sources and shared destinations exists
    log.info("Performing cross-validation checks...")
    for job_name, job_obj in validated_jobs.items():
        # Check if the source listed in the job exists
        if job_obj.source not in validated_sources:
            all_valid = False
            error = f"  Cross-validation failed for job [{job_name}]: Source '{job_obj.source}' is not defined in the [Data Sources] sections."
            validation_errors.append(error)
            log.warning(error)

        # Check if the shared destination listed in the job exists (if applicable)
        if hasattr(job_obj, "shared_destination") and job_obj.shared_destination:
            if job_obj.shared_destination not in validated_shared_dests:
                all_valid = False
                error = f"  Cross-validation failed for job [{job_name}]: Shared destination '{job_obj.shared_destination}' is not defined in the [Shared Destinations] sections."
                validation_errors.append(error)
                log.warning(error)

    log.info("Cross-validation finished.")

    # --- Final Summary ---
    if all_valid:
        log.info(
            "\nValidation successful: All sections conform to the expected format and all cross-references are valid."
        )
        return True
    else:
        log.error("\nValidation failed. Errors found:")
        for error in validation_errors:
            print(error)
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate a config.ini file using Pydantic models defined in models.py."
    )
    parser.add_argument(
        "-c",
        "--config-file",
        dest="config_file",
        type=str,
        default=None,
        help=(
            "Absolute path to the config.ini file to validate.\n"
            "If omitted, defaults to '<project_root>/config/config.ini'"
        ),
    )
    args = parser.parse_args()

    # If script argument provided, check if it's an abs path and run validation on it,
    # Otherwise, default to location for config file defined by project directory structure
    validation_success = False
    if args.config_file is not None:
        config_path_input = Path(args.config_file)
        if config_path_input.is_absolute():
            validation_success = validate_config(config_path_input)
        else:
            log.error("Specified paths to config.ini file must be absolute paths.")
            sys.exit(1)
    else:
        default_config_path = project_root / "config" / "config.ini"
        validation_success = validate_config(default_config_path)

    if validation_success:
        sys.exit(0)
    else:
        sys.exit(1)
