import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import logging
import os
from typing import Dict, Callable, Any, Tuple

# Type alias for message builder functions.
MessageBuilderFunction = Callable[[Dict[str, Any], str], Tuple[str, str]]

# Type alias for load functions.
LoadFunction = Callable[[Dict[str, Any], str, MessageBuilderFunction], str]


def prepare_email(
    job_config: Dict[str, Any], file_path: str, message_builder: MessageBuilderFunction
) -> MIMEMultipart:
    """Prepares the email message, including building the subject/body.

    Args:
        job_config: The nested job configuration.
        file_path: Path to the intermediate file to attach.
        message_builder: The custom message builder function defined in DIE main.py.

    Returns:
        A MIMEMultipart object representing the complete email message.

    Raises:
        ValueError: If required config keys are missing.
        FileNotFoundError: If the attachment file doesn't exist.
        Exception: If the message builder function fails.
    """
    if job_config["job"].get("destination_type") == "shared_service":
        email_config = job_config["job"]
    else:  # job-specific smtp destination
        email_config = job_config["job"]

    # Check if needed keys are present and create and validate recipient list
    required_email_keys = ["recipients", "sender_email"]
    if not all(email_config.get(key) for key in required_email_keys):
        missing_keys = [key for key in required_email_keys if not email_config.get(key)]
        raise ValueError(f"Missing required email keys: {missing_keys}")

    recipient_emails = [
        email.strip() for email in email_config.get("recipients").split(",")
    ]
    if not all(isinstance(email, str) for email in recipient_emails):
        raise TypeError("recipient_emails must be a list of strings.")

    # --- Build the message (subject and body) using the provided builder from main.py---
    try:
        subject, body = message_builder(job_config, file_path)
    except Exception as e:
        logging.exception(f"Error in message builder function: {e}")
        raise

    msg = MIMEMultipart()
    msg["From"] = email_config.get("sender_email")
    msg["To"] = COMMASPACE.join(recipient_emails)
    msg["Date"] = formatdate(localtime=True)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Attachment not found: {file_path}")
    with open(file_path, "rb") as fil:
        part = MIMEApplication(fil.read(), Name=os.path.basename(file_path))
    part["Content-Disposition"] = (
        f'attachment; filename="{os.path.basename(file_path)}"'
    )
    msg.attach(part)

    return msg


def send_email_with_smtp(
    job_config: Dict[str, Any],
    file_path: str,
    *,
    message_builder: MessageBuilderFunction = None,
) -> str:
    """Sends data via email using SMTP.  Prepares and sends the email.

    Args:
        job_config: The nested job configuration dictionary.
        file_path: Path to the file to attach.
        message_builder:  Custom function to build the email message.

    Returns:
        A string indicating the result of the email sending operation.

    Raises:
        ValueError: If required config keys are missing, or no message builder.
        smtplib.SMTPException: If an SMTP error occurs.
        FileNotFoundError: If the attachment file doesn't exist.
    """
    try:
        if job_config["job"].get("destination_type") == "shared_service":
            smtp_config = job_config["service"]
            email_config = job_config["job"]
        else:  # job-specific smtp destination
            smtp_config = job_config["job"]  # All SMTP settings are here
            email_config = job_config["job"]

        # Validate existence of required keys
        required_smtp_keys = [
            "host",
            "port",
        ]  # user and password might not always be required
        required_email_keys = ["recipients", "sender_email"]

        if not all(smtp_config.get(key) for key in required_smtp_keys):
            missing_keys = [
                key for key in required_smtp_keys if not smtp_config.get(key)
            ]
            raise ValueError(f"Missing required SMTP keys: {missing_keys}")

        if not all(email_config.get(key) for key in required_email_keys):
            missing_keys = [
                key for key in required_email_keys if not email_config.get(key)
            ]
            raise ValueError(f"Missing required email keys: {missing_keys}")

        # --- Prepare the email message ---
        if message_builder is None:  # Raise error if message_builder is None
            raise ValueError(
                "A 'message_builder' function must be provided for SMTP jobs."
            )
        msg = prepare_email(job_config, file_path, message_builder)

        # --- Connect and Send ---
        with smtplib.SMTP(
            host=smtp_config.get("host"), port=int(smtp_config.get("port"))
        ) as smtp:
            logging.info(
                f"Connecting to SMTP server (no encryption): {smtp_config.get('host')}:{smtp_config.get('port')}"
            )

            if "user" in smtp_config and "password" in smtp_config:
                smtp.login(smtp_config.get("user"), smtp_config.get("password"))

            logging.info(f"Sending email from: {msg['From']}")
            logging.info(f"Sending email to  : {msg['To']}")

            sendmail_response = smtp.sendmail(
                msg["From"],
                msg["To"].split(","),  # Correct: split into a list
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
        logging.exception(f"SMTP Error: {e}")
        raise
    except FileNotFoundError:
        logging.exception("Attachment file not found.")
        raise
    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
        raise


# --- Module-Level Dictionary for Load Functions ---
load_functions: Dict[str, LoadFunction] = {
    "smtp": send_email_with_smtp,
    # "sftp": transfer_file_with_sftp,
    # Add more load functions here for other data transfer methods
}


def load_data(
    job_config: Dict[str, Any],
    file_path: str,
    message_builder: MessageBuilderFunction = None,
) -> str:
    """Performs the load operation, looking up the correct function in load_functions.

    Args:
        job_config: The nested job configuration dictionary.
        file_path: The path to the file to be loaded.
        message_builder: Optional custom message builder function for email defined in DIE main.py.

    Returns:
        A string indicating the result of the load operation.

    Raises:
        ValueError: If no load function is found for the destination type.
        Exception:  If any error occurs during the load operation.
    """
    try:
        destination_type = job_config["job"].get("destination_type")
        if destination_type == "shared_service":
            service_type = job_config["service"]["type"]
        else:
            service_type = destination_type

        if service_type in load_functions:
            load_function = load_functions[service_type]
            # Call the load function, passing the message_builder if needed.
            if service_type == "smtp":
                result = load_function(
                    job_config, file_path, message_builder=message_builder
                )
            else:
                result = load_function(
                    job_config, file_path, message_builder=message_builder
                )  # Still need to pass for consistency of load_functions
            return result
        else:
            raise ValueError(
                f"No load function found for destination type: {service_type}"
            )

    except Exception as e:
        logging.exception(f"An error occurred during the load operation: {e}")
        raise
