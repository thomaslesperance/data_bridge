import os
import sys
import configparser
import logging

# Add the parent directory to sys.path to enable import from locally developed utils
sys.path.append(os.path.join(os.path.abspath(__file__), "..", ".."))
from utils.extract import extract_data
from utils.transform import transform_data
from utils.load import transfer_file_to_file_share

# **SET THESE** variables to select data source and destination details from congfig.ini
# Make these script args
database_name = ""
driver_file_name = ""
server_name = ""

# Variables that define directory structure of DIP
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, "output")
config_dir = os.path.join(script_dir, "..", "..", "config")

query_file_path = os.path.join(script_dir, "query.sql")
intermediate_file_path = os.path.join(output_dir, f"{server_name}.csv")
log_file_path = os.path.join(output_dir, "output.log")
config_file_path = os.path.join(config_dir, "config.ini")
jar_file_path = os.path.join(config_dir, driver_file_name)

# Load configuration file (config.ini)
config = configparser.ConfigParser()
config.read(config_file_path)

# Configure logging
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    try:
        # EXTRACT (from selected data source into memory)
        data = extract_data(config, database_name, query_file_path, jar_file_path)
        logging.info(f"Data extracted from {database_name} database")

        # TRANSFORM (data according to selected server specs and persist as file at intermediate_file_path)
        tranformed_data_file_path = transform_data(
            data, server_name, intermediate_file_path
        )
        logging.info(f"Data processed according to {server_name} specifications")

        # LOAD (file to selected destination)
        response = transfer_file_to_file_share(
            config, server_name, tranformed_data_file_path
        )
        logging.info(
            f"Secure data transfer to {server_name} successful\n\{server_name} response: {response}"
        )

    ## Catch-all error handling; each phase above has more detailed logging and error handling
    except Exception as e:
        logging.exception(f"'Catch-all'...An error occurred: {e}")


if __name__ == "__main__":
    main()
