import logging
from typing import Callable, Any, Optional
from utils.extract import extract_data
from utils.load import load_data
from utils.models import FinalPaths, ValidatedConfigUnion


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
        Runs the data integration process (Extract, Transform, Load) using the state
        unique to this DIE. Raises exceptions on failure, but expects caller (main.py)
        to log them. Uses attribute access on self.paths model.

        Raises:
            FileNotFoundError: If transformed data file not found after transform step.
        """
        logging.info(f"Starting data integration")
        # --------------------------------- EXTRACT -----------------------------------
        query_file = self.paths.query_file_path
        header, data = extract_data(self.job_config, query_file)
        source_name = self.job_config.source.source_name
        logging.info(
            f"""Data extracted successfully from source '{source_name}'. 
                Records retrieved: {len(data)}"""
        )
        # --------------------------------- TRANSFORM ---------------------------------
        intermediate_file_path = self.paths.intermediate_file_path
        transformed_file_path = self.transform_fn(header, data, intermediate_file_path)
        logging.info(f"Data transformed and saved to {str(transformed_file_path)}")
        # --------------------------------- LOAD --------------------------------------
        response = load_data(
            self.job_config, transformed_file_path, self.message_builder_fn
        )
        if self.job_config.job.shared_destination:
            logging.info(
                f"""Data loaded to 
                    {self.job_config.shared_dest.destination_name}. 
                        Response/Status: {response}"""
            )
        else:
            logging.info(
                f"""Data loaded to {self.job_config.job.destination}:
                    {self.job_config.host}:{self.job_config.job.path}. 
                        Response/Status: {response}"""
            )
        # -----------------------------------------------------------------------------
        logging.info(f"Data integration completed successfully!")
