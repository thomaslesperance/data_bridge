import os
import random
import string
from datetime import datetime
import pytz


def generate_random_filename(
    length: int = 12, file_extension: str = "txt", prefix="WFISD"
) -> str:
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


def export_csv_from_tuple_array(
    data,
    returned_file_name,
    header,
    use_quotes: bool = True,
    no_header_quotes: bool = False,
    separator: str = ",",
):
    """Exports data from a list of tuples to a CSV file.

    Args:
        data: The list of tuples representing the data.
        returned_file_name: The absolute path to the CSV file.
        header: An optional list of strings for the header row.
        use_quotes: Whether to enclose data fields in quotes.
        no_header_quotes: Whether to exclude quotes from header fields.
        separator: The field separator.

    Returns:
        The filename of the created CSV file.
    """

    csv_data = ""

    if header:
        for field_name in header:
            if use_quotes and not no_header_quotes:
                csv_data += f'"{field_name}"{separator}'
            else:
                csv_data += f"{field_name}{separator}"
        csv_data = csv_data[:-1] + "\r\n"  # Remove trailing separator, add newline

    for row in data:
        for col in row:
            formatted_data = str(col).replace("\n", "").strip() if col else ""
            if use_quotes:
                csv_data += f'"{formatted_data}"{separator}'
            else:
                csv_data += f"{formatted_data}{separator}"
        csv_data = csv_data[:-1] + "\r\n"  # Remove trailing separator, add newline

    csv_data = csv_data.strip()

    dir_name = os.path.dirname(returned_file_name)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name)

    with open(returned_file_name, "w") as file:
        file.write(csv_data)  # Use file.write for efficiency

    return returned_file_name


def apply_versatrans_transformations(header, data_rows, intermediate_file_path):
    transformed_data_file_path = export_csv_from_tuple_array(
        data=data_rows,
        returned_file_name=intermediate_file_path,
        header=header,
    )
    return transformed_data_file_path


def apply_internal_smtp_transformations(header, data_rows, intermediate_file_path):
    transformed_data_file_path = export_csv_from_tuple_array(
        data=data_rows,
        returned_file_name=intermediate_file_path,
        header=header,
    )
    return transformed_data_file_path


def transform_data(data, server_name, intermediate_file_path):
    header, data_rows = data
    return server_transformations[server_name](
        header, data_rows, intermediate_file_path
    )


server_transformations = {
    "versatrans": apply_versatrans_transformations,
    "internal_smtp_server": apply_internal_smtp_transformations,
}
