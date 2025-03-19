import logging
from pathlib import Path
from typing import Callable
from utils.config import load_config, get_job_config, determine_output_filename, locate
from utils.extract import extract_data
from utils.transform import export_csv_from_data
from utils.load import load_data


class DIE:
    def __init__(
        self,
        job_name: str,
        custom_transform: Callable = export_csv_from_data,
        message_builder: Callable = None,
    ):
        # Extract and load phase details are required in config.ini, so their central functions are used
        # Transform phase can be unique with custom function(s), but defaults to exporting unchanged CSV file
        self.job_name = job_name
        self.custom_transform = custom_transform
        self.message_builder = message_builder

        # Get paths and assemble job configuration
        self.paths = locate(self.job_name)
        if not all(self.paths.values()):
            raise ValueError(f"Could not locate all paths for DIE: {self.job_name}")

        self.config = load_config(self.paths["config_file_path"])
        self.job_config = get_job_config(
            self.config, self.job_name, self.paths["config_dir"]
        )

        # Determine output file name
        self.output_dir = Path(self.paths["output_dir"])
        self.intermediate_file_path = self.output_dir / determine_output_filename(
            self.job_config, self.job_name
        )

        # Create output directory and configure logging
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = Path(self.paths["log_file_path"])
        self._configure_logging()

    def _configure_logging(self):
        """Configures logging for this DIE instance."""
        logging.basicConfig(
            filename=str(self.log_file),
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    def run(self):
        """Runs the data integration process (Extract, Transform, Load)."""
        logging.info(f"Starting data integration for job: {self.job_name}")
        try:
            # --- EXTRACT ---
            header, data = extract_data(self.job_config, self.paths["query_file_path"])
            logging.info(
                f"Data extracted from {self.job_config['source']['type']} database:  {self.job_config['source']['name']}"
            )

            # --- TRANSFORM ---
            transformed_data_file_path = self.custom_transform(
                header, data, str(self.intermediate_file_path)
            )
            logging.info(f"Data transformed and saved to {transformed_data_file_path}")

            # --- LOAD ---
            response = load_data(
                self.job_config, transformed_data_file_path, self.message_builder
            )
            logging.info(f"Data transfer completed. Response: {response}")

        except Exception as e:
            logging.exception(f"An error occurred in job {self.job_name}: {e}")
            raise
