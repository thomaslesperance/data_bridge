import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import logging
import os
from typing import Dict, Callable, Any
from utils.message_builders import build_message

# Type alias for load functions
LoadFunction = Callable[[Dict[str, Any], str], str]


def prepare_email(job_config: Dict[str, Any], file_path: str) -> MIMEMultipart:
    """Prepares the email message (subject, body, attachments).

    Args:
        job_config: The nested job configuration.
        file_path:  Path to the file to attach.

    Returns:
        A MIMEMultipart object representing the complete email message.

    Raises:
        ValueError: If required config keys are missing.
        FileNotFoundError: If the attachment file doesn't exist.
    """
    email_config = job_config["job"]

    required_email_keys = ["recipients", "sender_email"]

    if not all(email_config.get(key) for key in required_email_keys):
        missing_keys = [key for key in required_email_keys if not email_config.get(key)]
        raise ValueError(f"Missing required email keys: {missing_keys}")

    recipient_emails = [
        email.strip() for email in email_config.get("recipients").split(",")
    ]
    if not all(isinstance(email, str) for email in recipient_emails):
        raise TypeError("recipient_emails must be a list of strings.")

    # Create job-specific email message text
    subject, body = build_message(job_config, file_path)

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


def send_email_with_smtp(job_config: Dict[str, Any], msg: MIMEMultipart) -> str:
    """Sends a pre-constructed email via SMTP.

    Args:
        job_config: The nested job configuration dictionary.
        msg: The MIMEMultipart email message to send.

    Returns:
        A string indicating the result of the email sending.

    Raises:
        ValueError: If required config keys are missing.
        smtplib.SMTPException: If an SMTP error occurs.
    """
    try:
        if job_config["job"].get("destination_type") == "shared_service":
            smtp_config = job_config["service"]
        else:  # job-specific smtp destination
            smtp_config = job_config["job"]  # All SMTP settings are here

        required_smtp_keys = [
            "host",
            "port",
        ]  # user and password might not always be required

        if not all(smtp_config.get(key) for key in required_smtp_keys):
            missing_keys = [
                key for key in required_smtp_keys if not smtp_config.get(key)
            ]
            raise ValueError(f"Missing required SMTP keys: {missing_keys}")

        with smtplib.SMTP(
            host=smtp_config.get("host"), port=int(smtp_config.get("port"))
        ) as smtp:
            logging.info(
                f"Connecting to SMTP server: {smtp_config.get('host')}:{smtp_config.get('port')}"
            )

            if "security" in smtp_config:
                if smtp_config.get("security") == "starttls":
                    starttls_response = smtp.starttls()
                    if starttls_response[0] != 250:
                        raise Exception(f"STARTTLS failed: {starttls_response}")
                # Consider other security options here with an if/else

            if "user" in smtp_config and "password" in smtp_config:
                smtp.login(smtp_config.get("user"), smtp_config.get("password"))

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
        logging.exception(f"SMTP Error: {e}")
        raise
    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
        raise


# --- Module-Level Dictionary for Load Functions ---
load_functions: Dict[str, LoadFunction] = {
    "smtp": send_email_with_smtp,
    # "sftp": transfer_file_with_sftp,
    # Add more load functions here
}


def load_data(job_config: Dict[str, Any], file_path: str) -> str:
    """
    Performs the load operation based on the job configuration.

    Args:
        job_config: The nested job configuration dictionary.
        file_path: The path to the file to be loaded.

    Returns:
        A string indicating the result message of the load operation.

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
            if service_type == "smtp":
                msg = prepare_email(job_config, file_path)
                result = load_function(job_config, msg)
                return result
            else:
                result = load_function(job_config, file_path)
                return result
        else:
            raise ValueError(
                f"No load function found for destination type: {service_type}"
            )
    except Exception as e:
        logging.exception(f"An error occurred during the load operation: {e}")
        raise
