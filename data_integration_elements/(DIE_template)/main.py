import os
import sys
import configparser
import logging
from pathlib import Path

# SET THESE: variables to select data source and destination details from congfig.ini
database_name = ""
driver_file_name = ""
server_name = ""
email_settings_section = ""

# data_integration_pipeline/
#     ├── config/
#     |   └── config.ini
#     |   └── driver file (e.g., openedge.jar)
#     ├── utils/
#     |   ├── __init__.py
#     |   ├── extract.py
#     |   ├── transform.py
#     |   └── load.py
#     └── data_integration_elements/
#         └── DIE_test/
#             ├── main.py   <-- You are here
#             ├── query.sql
#             └── output/
#                 └── output.log

# Add the parent directory to sys.path to enable import from locally developed utils
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from utils.extract import extract_data
from utils.transform import transform_data
from utils.load import send_email_with_smtp

# Variables that define directory structure of DIP
script_dir = Path(__file__).resolve().parent
output_dir = script_dir / "output"
config_dir = project_root / "config"

query_file_path = script_dir / "query.sql"
intermediate_file_path = output_dir / f"{server_name}.csv"
log_file_path = output_dir / "output.log"
config_file_path = config_dir / "config.ini"
jar_file_path = config_dir / driver_file_name

# Load configuration file (config.ini)
config = configparser.ConfigParser()
config.read(str(config_file_path))

# Configure logging
logging.basicConfig(
    filename=str(log_file_path),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    try:
        # EXTRACT (from selected data source into memory)
        data = extract_data(
            config, database_name, str(query_file_path), str(jar_file_path)
        )
        logging.info(f"Data extracted from {database_name} database")

        # TRANSFORM (data according to selected server specs and persist as file at intermediate_file_path)
        transformed_data_file_path = transform_data(
            data, server_name, str(intermediate_file_path)
        )
        logging.info(f"Data processed according to {server_name} specifications")

        # LOAD (file to selected destination)
        response = send_email_with_smtp(
            config=config,
            smtp_section=server_name,
            email_section=email_settings_section,
            subject="",
            body="",
            attachments=[str(transformed_data_file_path)],
        )

        logging.info(
            f"Secure data transfer to {server_name} successful\n\{server_name} response: {response}"
        )

    ## Catch-all error handling; each phase above has more detailed logging and error handling
    except Exception as e:
        logging.exception(f"'Catch-all'...An error occurred: {e}")


if __name__ == "__main__":
    main()
