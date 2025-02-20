import logging


def transfer_file_with_sftp(config, server, tranformed_data_file_path):
    """
    Transfers data to the specified server via SFTP.

    Args:
        config: The configuration object.
        server: The name of the server section in the config.
        tranformed_data_file_path: The path to the CSV file.
    """
    try:
        server_host_name = config[server]["host"]
        server_user = config[server]["user"]
        server_password = config[server]["password"]
        server_port = config[server]["port"]

        #... (Implementation to transfer the file via SFTP)

    except Exception as e:
        logging.exception(f"Failed to transfer data via SFTP: {e}")
        raise


def transfer_file_to_file_share(config, server, tranformed_data_file_path):
    print("some stuff")