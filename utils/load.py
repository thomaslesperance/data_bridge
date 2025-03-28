import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import logging
import os
from typing import Dict, Callable, Any, Tuple, Union
from pathlib import Path
from utils.models import ValidatedConfigUnion

# Type alias for message builder functions
MessageBuilderFunction = Callable[[ValidatedConfigUnion, Path], Tuple[str, str]]

# Type alias for load functions
LoadFunction = Callable[
    [ValidatedConfigUnion, Path, Union[MessageBuilderFunction, None]], str
]


def prepare_email(
    job_config: ValidatedConfigUnion,
    file_path: Path,
    message_builder: MessageBuilderFunction,
) -> MIMEMultipart:
    """Prepares the email message, including building the subject/body.

    Args:
        job_config: The (validated) nested job configuration.
        file_path: Path to the intermediate file to attach.
        message_builder: The custom message builder function defined in DIE main.py.

    Returns:
        A MIMEMultipart object representing the complete email message.
    """
    email_config = job_config.job

    # --- Build the message (subject and body) using the provided builder from main.py---
    try:
        subject, body = message_builder(job_config, file_path)
    except Exception as e:
        raise Exception(f"Error in message builder function: {e}")

    msg = MIMEMultipart()
    msg["From"] = email_config.sender_email
    msg["To"] = COMMASPACE.join(email_config.recipients)
    msg["Date"] = formatdate(localtime=True)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with open(file_path, "rb") as fil:
        part = MIMEApplication(fil.read(), Name=os.path.basename(file_path))
    part["Content-Disposition"] = (
        f'attachment; filename="{os.path.basename(file_path)}"'
    )
    msg.attach(part)

    return msg


def send_email_with_smtp(
    job_config: ValidatedConfigUnion,
    file_path: Path,
    message_builder: MessageBuilderFunction = None,
) -> str:
    """Sends data via email using SMTP.  Prepares and sends the email.

    Args:
        job_config: The (validated) nested job configuration dictionary.
        file_path: Path to the file to attach.
        message_builder: Custom function to build the email message.

    Returns:
        A string indicating the result of the email sending operation.

    Raises:
        ValueError: If message builder function not provided.
        smtplib.SMTPException: If an SMTP error occurs.
        FileNotFoundError: If the attachment file doesn't exist.
    """
    try:
        # --- Determine is shared or unique smtp destination ---
        if job_config.job.is_shared_destination:
            smtp_config = job_config.shared_dest
        else:  # All smtp config in job sub-dict
            smtp_config = job_config.job

        # --- Prepare the email message ---
        if message_builder is None:
            raise ValueError(
                "A 'message_builder' function must be provided for SMTP jobs."
            )
        msg = prepare_email(job_config, file_path, message_builder)

        # --- Connect and Send ---
        with smtplib.SMTP(host=smtp_config.host, port=smtp_config.port) as smtp:
            logging.info(
                f"Connecting to SMTP server (no encryption): {smtp_config.host}:{smtp_config.port}"
            )

            if "user" in smtp_config and "password" in smtp_config:
                smtp.login(smtp_config.user, smtp_config.password)

            logging.info(f"Sending email from: {msg['From']}")
            logging.info(f"Sending email to  : {msg['To']}")

            sendmail_response = smtp.sendmail(
                msg["From"],
                msg["To"].split(","),
                msg.as_string(),
            )

            if sendmail_response:
                response_message = f"Email sending issues: {sendmail_response}"
                logging.warning(response_message)
                return response_message
            else:
                response_message = "Email sent successfully."
                logging.info(response_message)
                return response_message

    except smtplib.SMTPException as e:
        raise smtplib.SMTPException(f"SMTP Error: {e}")
    except FileNotFoundError:
        raise FileNotFoundError("Attachment file not found.")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}")


# --- Dict for choosing correct load functions corresponding to config.ini details---
load_functions: Dict[str, LoadFunction] = {
    "smtp": send_email_with_smtp,
    # "sftp": transfer_file_with_sftp,
    # "fileshare": transfer_file_to_share,
    # Add more load functions here for other data transfer methods as needed
}


def load_data(
    job_config: ValidatedConfigUnion,
    file_path: Path,
    message_builder: MessageBuilderFunction = None,
) -> str:
    """Performs the load operation, looking up the correct function in load_functions.

    Args:
        job_config: The (validated) nested job configuration dictionary.
        file_path: The path to the file to be loaded.
        message_builder: Optional custom message builder function for email defined in DIE main.py.

    Returns:
        A string indicating the result of the load operation.

    Raises:
        ValueError: If no load function is found for the destination type.
    """
    try:
        # Ensure transformed data present for loading
        if not file_path.is_file():
            raise FileNotFoundError(
                f"""Transformed data file not found for loading: {file_path or 'None'}"""
            )

        # Determine load protocol for job
        load_protocol = None
        is_shared_destination = job_config.job.is_shared_destination
        if is_shared_destination:
            load_protocol = job_config.shared_dest.protocol
        else:
            load_protocol = job_config.job.protocol

        # Select corresponding load function
        if load_protocol in load_functions:
            load_function: LoadFunction = load_functions[load_protocol]
            result = load_function(job_config, file_path, message_builder)
            return result

        else:
            raise ValueError(f"No function found for load protocol: {load_protocol}")

    except Exception as e:
        raise Exception(f"An error occurred during the load operation: {e}")
