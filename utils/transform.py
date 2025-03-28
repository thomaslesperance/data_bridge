import os
import csv
import logging
from typing import List, Tuple, Any
from pathlib import Path


def export_csv_from_data(
    header: List[str],
    data: List[Tuple[Any, ...]],
    returned_file_name: Path,
    use_quotes: bool = True,
) -> str:
    """
    Exports data to a CSV file.

    Args:
        data: A list of tuples, where each tuple represents a row.
        returned_file_name: The path to the output CSV file.
        header: A list of strings representing the column headers.
        use_quotes: Whether to enclose fields in double quotes (default: True).

    Returns:
        The path to the created CSV file (str).
    """
    try:
        if not header:
            raise ValueError("Header list not found or empty")

        with open(returned_file_name, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(
                csvfile, quoting=csv.QUOTE_ALL if use_quotes else csv.QUOTE_NONE
            )
            if header:
                writer.writerow(header)
            writer.writerows(data)

        logging.info(
            f"CSV file created: {str(returned_file_name)}\nData rows: {len(data)}"
        )
        return returned_file_name

    except Exception as e:
        raise Exception(f"Error exporting data to CSV: {e}")
