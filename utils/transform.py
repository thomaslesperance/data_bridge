import os
import random
import string
from datetime import datetime
import logging

import pytz


def generate_random_filename(length: int = 12, file_extension: str = "txt", prefix = "WFISD") -> str:
    """Generates a random filename with the specified length,
    prepended by the current date.

    Args:
      length: The desired length of the random part of the filename.
      file_extension: The file extension (without the dot).

    Returns:
      The generated random filename.
    """
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.choice(characters) for _ in range(length))

    # Get current date in Central Time
    tz = pytz.timezone("US/Central")
    current_date = datetime.now(tz).strftime("%Y%m%d")

    return f"{prefix}_{current_date}_{random_string}.{file_extension}"


def export_csv_from_cursor(
    cursor,
    file_name: str = generate_random_filename(file_extension="csv"),
    use_quotes: bool = True,
    no_header_quotes: bool = False,
    include_header: bool = True,
    remove_commas: bool = False,
    separator: str = ",",
    output_dir: str = None,
) -> tuple:
    """Exports data from a cursor object to a CSV file.

    Args:
      cursor: The database cursor object.
      file_name: The absolute path to the output CSV file.
      use_quotes: Whether to enclose data fields in quotes.
      no_header_quotes: Whether to exclude quotes from header fields.
      include_header: Whether to include a header row with field names.
      remove_commas: Whether to remove commas from data fields.
      separator: The field separator to use in the CSV file.
      output_dir: The directory where the CSV file should be saved.
                  Defaults to the current working directory.
    Returns:
      A tuple containing the filename and the CSV data as a string.

    @author: Jeffrey Gordon
    @modified by: Thomas L'Esperance
    """

    csv_data = ""

    if include_header:
        field_names = [description[0] for description in cursor.description]
        for field_name in field_names:
            if use_quotes and not no_header_quotes:
                csv_data += f'"{field_name}"'
            else:
                csv_data += str(field_name)
            csv_data += separator
        csv_data = csv_data[:-1]  # Remove trailing separator
        csv_data += "\r\n"

    row = cursor.fetchone()
    while row:
        for col in row:
            formatted_data = str(col).replace("\n", "").strip() if col else ""
            if remove_commas:
                formatted_data = formatted_data.replace(separator, "")
            if use_quotes:
                csv_data += f'"{formatted_data}"'
            else:
                csv_data += formatted_data
            csv_data += separator

        csv_data = csv_data[:-1]  # Remove trailing separator
        csv_data += "\r\n"
        row = cursor.fetchone()

    csv_data = csv_data.strip()

    if output_dir:
        file_name = os.path.join(output_dir, file_name)

    with open(file_name, "w") as file:
        print(csv_data, file=file)

    return file_name, csv_data


def apply_versatrans_transformations(data, intermediate_file_path):
    return export_csv_from_cursor(data=data, file_name=intermediate_file_path)


def transform_data(data, server, intermediate_file_path):
    return server_transformations[server](data, intermediate_file_path)

server_transformations = {"versatrans": apply_versatrans_transformations}