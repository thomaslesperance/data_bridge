import logging
from pathlib import Path
from typing import Callable
from utils.config import load_config, get_job_config, determine_output_filename, locate
from utils.extract import extract_data
from utils.transform import export_csv_from_data
from utils.load import load_data


class DIE:
    """
    Represents a data integration element (DIE) within the data integration pipeline (DIP) comprised of
    this project with all of its instantiated jobs and config.

    Attributes:
        job_name: The name of the integration job and the suffix of this DIE's directory name (DIE_<job_name>).
        custom_transform: The custom transformations unique to this job done to the data extracted
                            from the data source specified in the config.ini file.
        message_builder: The custom email message building function passed to load_data for jobs that send emails.
        paths: A dict of absolute path strings defining the directory structure of the project.
        job_config: A nested dict containing all of the config info associated with this job based on the config.ini file.

    Methods:
        run: Runs the data integration process (Extract, Transform, Load) using the state unique to this DIE.
    """

    def __init__(
        self,
        job_name: str,
        custom_transform: Callable = export_csv_from_data,
        message_builder: Callable = None,
    ) -> None:
        """
        Initializes a DIE object.

        Args:
            job_name: The name of the integration job and the suffix of this DIE's directory name (DIE_<job_name>).
            custom_transform: The custom transformations unique to this job done to the data extracted
                                from the data source specified in the config.ini file.
            message_builder: The custom email message building function passed to load_data for jobs that send emails.
        """
        self.job_name = job_name
        self.custom_transform = custom_transform
        self.message_builder = message_builder

        # Get paths and assemble job configuration
        self.paths = locate(self.job_name)
        if not all(self.paths.values()):
            raise ValueError(f"Could not locate all paths for DIE: {self.job_name}")

        self._config = load_config(self.paths["config_file_path"])
        self.job_config = get_job_config(
            self._config, self.job_name, self.paths["config_dir"]
        )

        # Determine output file name
        self._output_dir = Path(self.paths["output_dir"])
        self._intermediate_file_path = self._output_dir / determine_output_filename(
            self.job_config, self.job_name
        )

        # Create output directory and configure logging
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._log_file = Path(self.paths["log_file_path"])
        self._configure_logging()

    def _configure_logging(self) -> None:
        """Configures logging for this DIE instance using logging from Python STL."""
        logging.basicConfig(
            filename=str(self._log_file),
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    def run(self) -> None:
        """Runs the data integration process (Extract, Transform, Load) using the state unique to this DIE."""
        logging.info(f"Starting data integration for job: {self.job_name}")
        try:
            # --- EXTRACT --- Data source must be specified in project config.ini
            header, data = extract_data(self.job_config, self.paths["query_file_path"])
            logging.info(
                f"Data extracted from {self.job_config['source']['type']} database:  {self.job_config['source_name']['name']}"
            )

            # --- TRANSFORM --- 'custom_transform' defaults to 'export_csv_from_data' if not defined in main.py
            transformed_data_file_path = self.custom_transform(
                header, data, str(self._intermediate_file_path)
            )
            logging.info(f"Data transformed and saved to {transformed_data_file_path}")

            # --- LOAD --- Data destination must be specified in project config.ini
            response = load_data(
                self.job_config, transformed_data_file_path, self.message_builder
            )
            logging.info(f"Data transfer completed. Response: {response}")

        except Exception as e:
            logging.exception(
                f"An error occurred in data integration for job: {self.job_name}\n\n{e}"
            )
