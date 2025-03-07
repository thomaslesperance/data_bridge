from pathlib import Path
import sys
import logging

from utils.extract import extract_data
from utils.transform import transform_data
from utils.load import load_data
from utils.config import (
    load_config,
    get_job_config,
    determine_output_filename,
    configure_logging,
    locate,
)


def main():
    # | --- Initialize ---
    job_name = Path(__file__).resolve().parent.name.replace("DIE_", "")
    paths = locate(job_name)
    if not all(paths.values()):
        print("Error")
        sys.exit(1)

    config = load_config(paths["config_file_path"])
    job_config = get_job_config(config, job_name)

    Path(paths["output_dir"]).mkdir(parents=True, exist_ok=True)
    configure_logging(paths["log_file_path"])
    logging.info(f"Starting data integration for job: {job_name}")

    intermediate_filename = determine_output_filename(job_config, job_name)
    intermediate_file_path = Path(paths["output_dir"]) / intermediate_filename

    try:
        # | --- Extract ---
        header, data = extract_data(job_config, paths["query_file_path"])
        logging.info(
            f"SUCCESS: Data extracted from {job_config['source']['source_name']} database."
        )

        # | --- Transform ---
        transformed_data_file_path = transform_data(
            job_config, (header, data), str(intermediate_file_path)
        )
        logging.info(
            f"SUCCESS: Data transformed and saved to {transformed_data_file_path}"
        )

        # | --- Load ---
        response = load_data(job_config, str(transformed_data_file_path))
        logging.info(f"SUCCESS: Data loaded.\n\nDestination response: {response}.")

    except Exception as e:
        logging.exception(f"An error occurred in job {job_name}: {e}")


if __name__ == "__main__":
    main()
