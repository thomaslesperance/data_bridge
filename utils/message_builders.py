from typing import Dict, Any, Tuple, Callable
import logging
import csv


def build_attendance_email(
    job_config: Dict[str, Any], file_path: str
) -> Tuple[str, str]:
    """Builds email subject and body for attendance_sync job.

    Args:
        job_config: The nested job configuration.
        file_path: Path to the transformed data file.

    Returns:
        A tuple: (subject, body).
    """
    try:
        # Example logic (replace with your actual logic)
        with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            num_students = sum(1 for row in reader) - 1  # Count rows, excluding header
        school_year = "2025"  # You could get from config or file content

        subject = "Attendance Report"
        body = f"""These students have a Discipline Action 05, 06, 25 or 26 and have 3 or more days Total Time

Number of students in file: {num_students}

School Year: {school_year}
[attachment]
"""
        return subject, body

    except Exception as e:
        logging.exception(f"Error building attendance email: {e}")
        raise


def build_gradebook_email(
    job_config: Dict[str, Any], file_path: str
) -> Tuple[str, str]:
    """Builds email subject and body for the gradebook_sync job."""
    try:
        # Example logic
        subject = "Gradebook Data"
        body = f"Attached is your Versa Trans file for {job_config['job'].get('date', 'today')}\n[attachment]"  # Example
        return subject, body
    except Exception as e:
        logging.exception(f"Error building attendance email: {e}")
        raise


def build_test_job_1_email(
    job_config: Dict[str, Any], file_path: str
) -> Tuple[str, str]:
    """Build email subject and body for test_job_1"""
    try:
        subject = "Test Job 1 Email"
        body = "Attachment included\n[attachment]"
        return subject, body
    except Exception as e:
        logging.exception(f"Error building attendance email: {e}")
        raise


def build_test_email_email(
    job_config: Dict[str, Any], file_path: str
) -> Tuple[str, str]:
    """Build email subject and body for test_job_1"""
    try:
        subject = "TEST TEST TEST"
        body = f"""This email confirms email functionality of refactored data integration pipeline codebase.
                Details: {job_config["source"]["source_name"]},{job_config["service"]["service_name"]},{job_config["job"]["job_name"]}, {file_path}
                """
        return subject, body
    except Exception as e:
        logging.exception(f"Error building attendance email: {e}")
        raise


# Dictionary mapping job names to their email building functions
message_builders: Dict[str, Callable[[Dict[str, Any], str], Tuple[str, str]]] = {
    "attendance_sync": build_attendance_email,
    "gradebook_sync": build_gradebook_email,
    "test_job_1": build_test_job_1_email,
    "test_email": build_test_email_email,
    # Add more job-specific functions here
}


def build_message(job_config: Dict[str, Any], file_path: str) -> Tuple[str, str]:
    """Builds the email subject and body.

    Args:
        job_config:  The job configuration.
        file_path: The path to the data file.

    Returns:
        A tuple: (subject, body).

    Raises:
        ValueError: If no message builder is found for the job.
    """
    job_name = job_config["job"].get("job_name")
    if job_name in message_builders:
        builder_function = message_builders[job_name]
        return builder_function(job_config, file_path)
    else:
        # You *could* have a default message here, or raise an error.
        raise ValueError(f"No message builder found for job: {job_name}")
