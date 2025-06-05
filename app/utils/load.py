import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import logging
import paramiko
import os
from typing import Dict, Callable, Any, Tuple, Union
from pathlib import Path, PurePosixPath
from utils.models import ValidatedConfigUnion

# ------------------------------------------------------------------------------------------
# -------------------------------- TYPE ALIASES --------------------------------------------
# ------------------------------------------------------------------------------------------

# Type alias for message builder functions
MessageBuilderFunction = Callable[[ValidatedConfigUnion, Path], Tuple[str, str]]

# Type alias for load functions
LoadFunction = Callable[
    [ValidatedConfigUnion, Path, Union[MessageBuilderFunction, None]], str
]

# ------------------------------------------------------------------------------------------
# -------------------------------- SMTP ----------------------------------------------------
# ------------------------------------------------------------------------------------------


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


# ------------------------------------------------------------------------------------------
# -------------------------------- SFTP ----------------------------------------------------
# ------------------------------------------------------------------------------------------


def sftp_upload_minimal(
    hostname: str,
    port: int,
    username: str,
    password: str,
    local_filepath: Path,
    remote_directory: str,
    disable_host_key_check: bool = False,
) -> str:
    """
    Connects to an SFTP server using password authentication and uploads a single file.
    Handles basic connection and transfer operations.

    Args:
        hostname: Server hostname or IP address.
        port: Server port (usually 22 for SFTP).
        username: SFTP username.
        password: SFTP password (plain string).
        local_filepath: Path object for the local file to upload.
        remote_directory: The remote directory path where the file should be placed.
                          The base filename from local_filepath will be appended.
        disable_host_key_check: If True, skips host key verification.
                                **SECURITY RISK**: Only use for trusted networks or testing.

    Returns:
        A string confirming successful upload and the remote path.

    Raises:
        FileNotFoundError: If the local file does not exist.
        paramiko.AuthenticationException: If authentication fails (bad user/pass).
        paramiko.SSHException: For other SSH/SFTP connection errors (e.g., host down,
                                host key mismatch if checks enabled).
        IOError: For file transfer errors (e.g., permission denied on server).
    """
    # Construct remote path reliably complying with posixpath (forward slashes)
    remote_filename = local_filepath.name
    remote_full_path = PurePosixPath(remote_directory) / remote_filename

    logging.info(
        f"Attempting SFTP upload (Password Auth): {local_filepath} -> "
        f"sftp://{username}@{hostname}:{port}{remote_full_path if remote_full_path.startswith('/') else '/' + remote_full_path}"
    )

    client = None
    sftp = None
    try:
        client = paramiko.SSHClient()

        # --- Host key check
        if disable_host_key_check:
            # Warning:
            # AutoAddPolicy bypasses host key verification entirely. This means you
            # have NO protection against Man-In-The-Middle (MITM) attacks if the
            # server's key is unknown. An attacker could intercept the connection.
            # ONLY use this for initial testing on completely trusted networks
            # where you understand and accept the risk.
            logging.warning(
                "!!! SECURITY ALERT !!! Disabling SFTP host key verification using AutoAddPolicy. Vulnerable to MITM attacks."
            )
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        else:
            # Default, more secure behavior: Load system known host keys.
            # This will RAISE an SSHException if the host is unknown or the key
            # has changed (indicating potential MITM or server change).
            # Ensure your ~/.ssh/known_hosts file (or equivalent) is correct.
            logging.info("Loading system host keys for SFTP verification.")
            client.load_system_host_keys()

        logging.info(f"Connecting via SSH to {hostname}:{port} as user '{username}'...")
        client.connect(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            timeout=20,
            auth_timeout=20,
            # Disable looking for keys explicitly since we only want password auth here
            key_filename=None,
            look_for_keys=False,
        )
        logging.info("SSH connection established successfully.")

        sftp = client.open_sftp()
        logging.info("SFTP session opened over SSH connection.")
        logging.info(
            f"Uploading {local_filepath.name} via SFTP to remote path: {remote_full_path}"
        )
        sftp.put(str(local_filepath), remote_full_path)
        logging.info("SFTP upload completed successfully.")

        success_message = (
            f"Successfully uploaded {local_filepath.name} to "
            f"sftp://{hostname}:{port}{remote_full_path if remote_full_path.startswith('/') else '/' + remote_full_path}"
        )
        return success_message

    except paramiko.AuthenticationException:
        raise paramiko.AuthenticationException(
            f"SFTP Authentication failed for user '{username}' on {hostname}:{port}."
        )
    except paramiko.SSHException as ssh_err:
        # Covers various connection errors, including host key mismatches
        raise paramiko.SSHException(
            f"SSH connection or protocol error connecting to {hostname}:{port}. Error: {ssh_err}"
        )
    except IOError as io_err:
        # Covers errors during the actual file transfer (permissions, disk space etc.)
        raise IOError(
            f"SFTP file transfer IO error for remote path {remote_full_path}. Error: {io_err}"
        )
        raise
    except Exception as e:
        raise Exception(
            f"An unexpected error occurred during SFTP operation to {hostname}. Error: {e}",
            exc_info=True,
        )
    finally:
        if sftp is not None:
            try:
                logging.info("Closing SFTP session.")
                sftp.close()
            except Exception as e_close:
                logging.warning(
                    f"Error closing SFTP session: {e_close}", exc_info=False
                )
        if client is not None:
            try:
                logging.info("Closing SSH client connection.")
                client.close()
            except Exception as e_close:
                logging.warning(
                    f"Error closing SSH client connection: {e_close}", exc_info=False
                )


def transfer_file_with_sftp(
    config: ValidatedConfigUnion,
    local_file_path: Path,
) -> str:
    """
    Pipeline load function for SFTP uploads using Password Authentication.

    Conforms to the LoadFunction type alias. Extracts SFTP details from the
    validated config object and calls the core SFTP worker function.
    Designed to be extended later to read config flags from the config object for
    key-auth, host key checks etc.

    Args:
        config: The validated configuration object for the job. Expected to
                contain a 'job' attribute of type JobUniqueSftp.
        local_file_path: Path object for the local file to upload.

    Returns:
        A success message string from the SFTP upload worker function.

    Raises:
        TypeError: If the config object does not contain the expected structure
                   (e.g., ValidatedConfigUnique with JobUniqueSftp).
        ValueError: If essential SFTP configuration is missing within the job config
                    (though Pydantic validation should prevent this).
        (and any exceptions raised by sftp_upload_minimal like FileNotFoundError,
         AuthenticationException, SSHException, IOError)
    """
    logging.info(
        f"Initiating SFTP load via pipeline function for local file: {local_file_path}"
    )

    # Extract parameters
    job_config = config.job
    hostname = job_config.host
    port = job_config.port
    username = job_config.user
    password = job_config.password.get_secret_value()
    remote_directory = job_config.remote_path

    # --- Future security enhancements
    # Here, you would check for additional flags in job_config if they existed, e.g.:
    # use_key_auth = getattr(job_config, 'use_key_auth', False)
    # pkey_path = getattr(job_config, 'private_key_path', None)
    # strict_host_keys = getattr(job_config, 'strict_host_key_check', True) # Default to strict
    #
    # if use_key_auth and pkey_path:
    #     # Call a different worker function or pass key info to an enhanced sftp_upload
    #     log.info("Attempting SFTP transfer using key-based authentication...")
    #     # result_message = sftp_upload_with_key(...)
    # else:
    #     # Continue with password auth worker call
    #     log.info("Using password-based authentication for SFTP.")
    #     result_message = sftp_upload_minimal(...)
    #
    # The 'disable_host_key_check' flag for the worker would be set based on 'strict_host_keys'
    # disable_host_key_check_param = not strict_host_keys # Inverse relationship
    # then disable_host_key_check_param would be passed to the worker call for disable_host_key_check

    # --- Call the core worker function
    logging.info("Calling minimal SFTP upload worker with password authentication.")
    try:
        result_message = sftp_upload_minimal(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            local_filepath=local_file_path,
            remote_directory=remote_directory,
            disable_host_key_check=True,  # Can change this once config file template updated to use added security
        )

        return result_message

    except Exception as e:
        raise Exception(
            f"SFTP load function failed during transfer for job '{job_config.job_name}'. Error Type: {type(e).__name__}"
        )


# ------------------------------------------------------------------------------------------
# -------------------------------- MAP & MAIN LOADER ---------------------------------------
# ------------------------------------------------------------------------------------------

# --- Dict for choosing correct load functions corresponding to config.ini details---
load_functions: Dict[str, LoadFunction] = {
    "smtp": send_email_with_smtp,
    "sftp": transfer_file_with_sftp,
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
                f"Transformed data file not found for loading: {file_path or 'None'}"
            )

        # Determine load protocol for job
        load_protocol = None
        is_shared_destination = job_config.job.is_shared_destination
        if is_shared_destination:
            load_protocol = job_config.shared_dest.protocol
        else:
            load_protocol = job_config.job.protocol

        # Select and run corresponding load function
        if load_protocol in load_functions:
            load_function: LoadFunction = load_functions[load_protocol]
            result = load_function(job_config, file_path, message_builder)
            return result

        else:
            raise ValueError(f"No function found for load protocol: {load_protocol}")

    except Exception as e:
        raise Exception(f"An error occurred during the load operation: {e}")
