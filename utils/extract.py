import jaydebeapi
import logging


def connect_to_db(user, password, conn_string, driver, jar_file_path):
    """
    Connects to the database_name using the provided credentials and driver.

    Args:
        user: The database username.
        password: The database password.
        conn_string: The database connection string.
        driver: The database driver name.
        jar_path: The path to the JAR file containing the driver.

    Returns:
        The database_name connection object.
    """
    try:
        return jaydebeapi.connect(driver, conn_string, [user, password], jar_file_path)
    except Exception as e:
        logging.exception(f"Failed to connect to database: {e}")
        raise


def load_query(query_file_path):
    """
    Loads the SQL query from the specified file.

    Args:
        query_file_path: The path to the SQL query file.

    Returns:
        The SQL query as a string.
    """
    try:
        with open(query_file_path, "r") as f:
            query = f.read()
        return query
    except Exception as e:
        logging.exception(f"Failed to load query from file: {e}")
        raise


def query_db(db_connection, query):
    """
    Executes the SQL query on the database connection.

    Args:
        db_connection: The database connection object.
        query: The SQL query to execute.

    Returns:
        The result of the query (an array of tuples).
    """
    try:
        with db_connection.cursor() as cursor:
            cursor.execute(query)
            headers = [col[0] for col in cursor.description]
            cursor_data = cursor.fetchall()
        return headers, cursor_data
    except Exception as e:
        logging.exception(f"Failed to execute database query: {e}")
        raise


def extract_data(config, database_name, query_file_path, jar_file_path):
    """
    Extracts data from the specified database_name.

    Args:
        config: The configuration object storing database credentials and connection details.
        database: The name of the database section in the config.
        query_file_path: The path to the SQL query file.
        jar_file_path: The path to the JDBC driver JAR file.

    Returns:
        A tuple containing the header (list of strings) and the data as a list of tuples.

    Raises:
        Exception: If there is an error during data extraction.
    """
    try:
        db_user = config.get(database_name, "user")
        db_password = config.get(database_name, "password")
        db_conn_string = config.get(database_name, "conn_string")
        db_driver = config.get(database_name, "driver")

        if not all([db_user, db_password, db_conn_string, db_driver]):
            missing_settings = []
            if not db_user:
                missing_settings.append("user")
            if not db_password:
                missing_settings.append("password")
            if not db_conn_string:
                missing_settings.append("conn_string")
            if not db_driver:
                missing_settings.append("driver")
            raise ValueError(
                f"Missing required database settings in config.ini: {', '.join(missing_settings)}"
            )

        with connect_to_db(
            db_user, db_password, db_conn_string, db_driver, jar_file_path
        ) as db_connection:
            logging.info(f"Connected to {database_name} database_name")
            query = load_query(query_file_path)
            header, data = query_db(db_connection, query)
            logging.info(f"Data retrieved from {database_name} database_name")
        return header, data

    except Exception as e:
        logging.exception(f"An error occurred during data extraction: {e}")
        raise
