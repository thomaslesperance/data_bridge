import jaydebeapi
import logging


def connect_to_db(user, password, conn_string, driver, jar_file_path):
    """
    Connects to the database using the provided credentials and driver.

    Args:
        user: The database username.
        password: The database password.
        conn_string: The database connection string.
        driver: The database driver.
        jar_path: The path to the JAR file (optional).

    Returns:
        The database connection object.
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
            cursor_data = cursor.fetchall()
        return cursor_data
    except Exception as e:
        logging.exception(f"Failed to execute database query: {e}")
        raise


def extract_data(config, database, query_file_path, jar_file_path):
    """
    Extracts data from the specified database.

    Args:
        config: The configuration object storing database credentials and connection details.
        database: The name of the database section in the config.
        query_file_path: The path to the SQL query file.

    Returns:
        The extracted data (in memory, not file).
    """
    try:
        db_user = config[database]["user"]
        db_password = config[database]["password"]
        db_conn_string = config[database]["conn_string"]
        db_driver = config[database]["driver"]

        with connect_to_db(
            db_user, db_password, db_conn_string, db_driver, jar_file_path
        ) as db_connection:
            logging.info(f"Connected to {database} database")
            query = load_query(query_file_path)
            data = query_db(db_connection, query)
            logging.info(f"Data retrieved from {database} database")
        return data
    except Exception as e:
        logging.exception(f"An error occurred during data extraction: {e}")
        raise
