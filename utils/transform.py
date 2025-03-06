import os
import csv
import logging
from typing import List, Tuple, Dict, Callable, Any

# Type alias for data (header + rows)
Data = Tuple[List[str], List[Tuple[Any, ...]]]

# Type alias for transformation functions
TransformFunction = Callable[[List[str], List[Tuple[Any, ...]], str], str]


def export_csv_from_data(
    data: List[Tuple[Any, ...]],
    returned_file_name: str,
    header: List[str],
    use_quotes: bool = True,
) -> str:
    """
    Exports data to a CSV file.

    Args:
        data: A list of tuples, where each tuple represents a row.
        returned_file_name: The path to the output CSV file (str).
        header: A list of strings representing the column headers.
        use_quotes: Whether to enclose fields in double quotes (default: True).

    Returns:
        The path to the created CSV file (str).

    Raises:
        Exception: If any error occurs during CSV creation.
    """
    try:
        # Ensure the directory exists
        dir_name = os.path.dirname(returned_file_name)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name)

        with open(returned_file_name, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(
                csvfile, quoting=csv.QUOTE_ALL if use_quotes else csv.QUOTE_NONE
            )
            if header:
                writer.writerow(header)
            writer.writerows(data)

        logging.info(f"CSV file created: {returned_file_name}")
        return returned_file_name

    except Exception as e:
        logging.exception(f"Error exporting data to CSV: {e}")
        raise


def example_job_transformations(
    header: List[str], data_rows: List[Tuple[Any, ...]], intermediate_file_path: str
) -> str:
    """Example custom transform: Simply exports to CSV (no actual transformation).

    Args:
        header: Column headers.
        data_rows: The data rows.
        intermediate_file_path: Path to save the transformed data.

    Returns:
        Path to the transformed data file.
    """
    # In a real-world scenario, you would modify data_rows here
    # before exporting to CSV.  This is just a placeholder.
    logging.info("Applying example_job transformations (placeholder)")
    transformed_data_file_path = export_csv_from_data(
        data=data_rows,
        returned_file_name=intermediate_file_path,
        header=header,
    )
    return transformed_data_file_path


# --- Dictionary for transformations beyond simple CSV export function ---
special_transformations: Dict[str, TransformFunction] = {
    "example_job": example_job_transformations,
    # Add more job_name: function mappings here as needed
}


def transform_data(
    job_config: Dict[str, Any], data: Data, intermediate_file_path: str
) -> str:
    """
    Transforms data based on the job configuration.

    If the job_name is a key in `special_transformations`, the corresponding
    function is called.  Otherwise, the data is exported to CSV with no changes.

    Args:
        job_config: The nested job configuration dictionary.
        data: A tuple containing the header (list of strings) and data rows
              (list of tuples).
        intermediate_file_path: The path to save the transformed data.

    Returns:
        The path to the transformed data file.

    Raises:
        Exception: If any error occurs during transformation.
    """
    header, data_rows = data
    job_name = job_config["job"].get("job_name")

    try:
        if job_name in special_transformations:
            logging.info(f"Applying custom transformation for job: {job_name}")
            transform_function = special_transformations[job_name]
            return transform_function(header, data_rows, intermediate_file_path)
        else:
            logging.info(
                f"No specific transformation found for job: {job_name}. Performing default CSV export."
            )
            return export_csv_from_data(
                data=data_rows,
                returned_file_name=intermediate_file_path,
                header=header,
            )
    except Exception as e:
        logging.exception(f"Error during data transformation: {e}")
        raise
