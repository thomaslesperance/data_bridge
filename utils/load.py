import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import logging
import os


# TO DO: Double check security of this function in Python docs before using in prod
def send_email_with_smtp(
    config,
    smtp_section,
    email_section,
    subject,
    body,
    attachments,
):
    """Sends a plain text email via SMTP, enforcing STARTTLS encryption.

    Args:
        config: The configuration object containing details data from parsed config.ini file.
        smtp_section: The name of the section in config.ini with SMTP server details.
        email_section: The name of the section in config.ini with email-specific settings (recipients, sender).
        subject: The email subject.
        body: The email body (plain text).
        attachments: A list of file paths (strings) representing attachments.

    Returns:
        A string containing the response message from the SMTP server after sending.

    Raises:
        smtplib.SMTPException: If any SMTP error occurs.
        FileNotFoundError: If an attachment file doesn't exist.
        ValueError: If the config section is missing required keys.
        KeyError: If a specified section is not found in the config.
        Exception: For other errors.
    """

    try:
        smtp_config = dict(config[smtp_section])
        email_config = dict(config[email_section])
    except KeyError as e:
        logging.exception(f"Section not found in config.ini: {e}")
        raise

    required_smtp_keys = ["host", "username", "password", "port"]
    required_email_keys = ["recipients", "sender_email"]

    if not all(config.has_option(smtp_section, key) for key in required_smtp_keys):
        missing_keys = [
            key
            for key in required_smtp_keys
            if not config.has_option(smtp_section, key)
        ]
        raise ValueError(
            f"Missing required keys in section '{smtp_section}': {missing_keys}"
        )

    if not all(config.has_option(email_section, key) for key in required_email_keys):
        missing_keys = [
            key
            for key in required_email_keys
            if not config.has_option(email_section, key)
        ]
        raise ValueError(
            f"Missing required keys in section '{email_section}': {missing_keys}"
        )
    try:
        recipient_emails = [
            email.strip()
            for email in config.get(email_section, "recipients").split(",")
        ]
        if not all(isinstance(email, str) for email in recipient_emails):
            raise TypeError("recipient_emails must be a list of strings.")

        msg = MIMEMultipart()
        msg["From"] = email_config["sender_email"]
        msg["To"] = COMMASPACE.join(recipient_emails)
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        for file_path in attachments:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Attachment not found: {file_path}")
            with open(file_path, "rb") as fil:
                part = MIMEApplication(fil.read(), Name=os.path.basename(file_path))
            part["Content-Disposition"] = (
                f'attachment; filename="{os.path.basename(file_path)}"'
            )
            msg.attach(part)

        with smtplib.SMTP(
            host=smtp_config["host"], port=int(smtp_config["port"])
        ) as smtp:
            logging.info(
                f"Connecting to SMTP server: {smtp_config['host']}:{smtp_config['port']}"
            )

            if "security" in smtp_config:
                starttls_response = smtp.starttls()
                if starttls_response[0] != 250:
                    raise Exception(f"STARTTLS failed: {starttls_response}")
                smtp.login(smtp_config["username"], smtp_config["password"])

            sendmail_response = smtp.sendmail(
                email_config["sender_email"], recipient_emails, msg.as_string()
            )

            if sendmail_response:
                response_message = f"Email sending issues: {sendmail_response}"
                logging.warning(response_message)
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


def transfer_file_with_sftp(config, server_name, transformed_data_file_path):
    """
    Transfers data to the specified server via SFTP.

    Args:
        config: The configuration object.
        server_name: The name of the server section in the config.
        tranformed_data_file_path: The path to the transformed intermediate CSV file.
    """
    try:
        server_host_name = config[server_name]["host"]
        server_user = config[server_name]["user"]
        server_password = config[server_name]["password"]
        server_port = config[server_name]["port"]

        # ... (Implementation to transfer the file via SFTP)

    except Exception as e:
        logging.exception(f"Failed to transfer data via SFTP: {e}")
        raise


def transfer_file_with_ftps(config, server_name, transformed_data_file_path):
    print("Some stuff")


def transfer_file_to_file_share(config, server_name, transformed_data_file_path):
    print("some stuff")


def transfer_file_to_google_drive(config, server_name, transformed_data_file_path):
    print("Some stuff")


# Setting Up a New SFTP Transfer Process (End-to-End)

# This is the practical, real-world process, combining technical steps with communication:

# Initial Contact:

# You (or someone on your team) contacts the administrator/IT contact at the destination server. This is usually done by phone or email.
# Explain that you need to set up an SFTP transfer process to their server.
# Gather initial information:
# Server Address: The hostname (e.g., sftp.example.com) or IP address of the SFTP server.
# Port Number: Usually 22, but confirm it. Some organizations use non-standard ports for security.
# Authentication Method: Ask if they prefer password authentication or key-based authentication. Strongly advocate for key-based authentication – it's much more secure.
# Username: The username you'll use to connect.
# Destination Directory: Ask which directory on the remote server you should upload files to.
# Firewall Considerations: Ask if there are any firewall rules you need to be aware of (on their side or yours). They might need to whitelist your server's IP address.
# Key-Based Authentication (Recommended):

# Generate Key Pair (on your server):
# Bash

# ssh-keygen -t rsa -b 4096 -f /path/to/your/private_key
# -t rsa: Specifies the key type (RSA is a good choice).
# -b 4096: Specifies the key size (4096 bits is recommended for strong security).
# -f /path/to/your/private_key: Specifies the output file path. This will create two files:
# /path/to/your/private_key (the private key – keep this secret)
# /path/to/your/private_key.pub (the public key)
# Passphrase: You'll be prompted for a passphrase. Use a strong passphrase. This encrypts the private key itself, adding an extra layer of security. If someone steals your private key file, they also need the passphrase to use it.
# Send Public Key to Destination Admin: Send the contents of the .pub file (the public key) to the destination server administrator. They need to add this public key to the authorized_keys file in the home directory of the user account you'll be using on their server (usually in ~/.ssh/authorized_keys).
# Password Authentication (Less Secure):

# If you must use password authentication, the destination server admin will provide you with a username and password. Treat this password with extreme care.
# Host Key Verification (Crucial):

# Obtain the Host Key Fingerprint: This is the most critical step for security. You must obtain the host key fingerprint from the destination server administrator through a trusted channel (e.g., phone call, verified email, in-person).  Do not simply accept the fingerprint presented by the server during the first connection – that's how man-in-the-middle attacks happen.

# Methods to get the fingerprint (from the admin):

# They provide it directly: They might just give you the fingerprint string (e.g., SHA256:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx).
# They run a command: They might run ssh-keyscan -t rsa -p <port> <server_address> on their server and send you the output.
# If using ssh-keyscan, the output should then be manually added to the known_hosts file
# Create/Update known_hosts File (on your server):  Create a file (e.g., /path/to/your/known_hosts) on your server. Add a line to this file in the format:

# <server_address>,<ip_address> ssh-rsa <host_key>
# or using the hash (recommended)

# |1|<hashed_hostname>|<salt> <key_type> <key_data>
# <server_address>: The hostname of the SFTP server.
# <ip_address>: The IP address of the server.
# <host_key>: The actual host key data (which the ssh-keyscan command outputs).
# <hashed_hostname>, <salt>, <key_type>, <key_data>: these values are found in the output of ssh-keyscan.
# Test the Connection:

# Use a command-line SFTP client (like sftp on Linux/macOS) to test the connection:
# Bash

# sftp -i /path/to/your/private_key -P <port> <username>@<server_address>
# -i: Specifies the private key file (if using key-based auth).
# -P: Specifies the port number.
# If everything is configured correctly, you should connect successfully.
# If you get a warning about the host key not being recognized, double-check the fingerprint you received from the admin. If it doesn't match, do not proceed – you could be the victim of an attack.
# Integrate into Your Python Code:

# Use the sftp_put and check_host_key functions, passing in the correct parameters:
# server_address, port_num, auth_user
# Either auth_pass or (private_key_path, private_key_pass)
# user_known_hosts_file (to check_host_key, then pass the result to sftp_put's cnopts parameter)
# Ongoing Monitoring and Maintenance:

# Regularly check the log file for errors.
# Periodically review the security configuration (e.g., are key passphrases still strong, are host keys still valid).
# Consider rotating SSH keys periodically for even better security.

# Draft of functions to use in SFTP SOP
# import pysftp
# import os
# import paramiko
# import logging

# def sftp_put(
#     remote_file: str,
#     local_file: str,
#     server_address: str,
#     auth_user: str,
#     auth_pass: str = None,  # Make password optional
#     private_key_path: str = None,  # Add key-based auth
#     private_key_pass: str = None, # Add key passphrase
#     port_num: int = 22,
#     log_file: str = "./output/sftp_put.log",
#     cnopts = None
# ):
#     """Transfers a local file to a remote server via SFTP.

#     Args:
#         remote_file: Path to the remote file (where to store the file).
#         local_file: Path to the local file to be transferred.
#         server_address: Hostname or IP address of the SFTP server.
#         auth_user: Username for authentication.
#         auth_pass: Password for authentication (optional if using key-based auth).
#         private_key_path: Path to the private key file (optional).
#         private_key_pass: Passphrase for the private key (optional).
#         port_num: Port number for the SFTP connection (default: 22).
#         log_file: File to use for logging (optional).
#         cnopts: pysftp.CnOpts() object to use

#     Returns:
#         True if the transfer was successful, False otherwise.

#     Raises:
#         FileNotFoundError: If the local file does not exist.
#         pysftp.exceptions.SFTPError: For SFTP-specific errors.
#         paramiko.ssh_exception.SSHException: For general SSH errors.
#         OSError: For other operating system errors.
#         Exception: Catch-all for unexpected exceptions.
#     """

#     # Input validation
#     if not os.path.exists(local_file):
#         raise FileNotFoundError(f"Local file not found: {local_file}")
#     if not auth_pass and not private_key_path:
#         raise ValueError("Must provide either a password or a private key path.")

#     # Create directory for log file
#     log_dir = os.path.dirname(log_file)
#     if log_dir and not os.path.exists(log_dir):
#         os.makedirs(log_dir)

#     # Logging setup (use the logging module!)
#     logger = logging.getLogger("sftp_put")
#     logger.setLevel(logging.DEBUG)  # Set the desired logging level
#     if not logger.handlers: # Add File Handler if one does not exist already
#         fh = logging.FileHandler(log_file)
#         formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#         fh.setFormatter(formatter)
#         logger.addHandler(fh)

#     if cnopts is None:
#         cnopts = pysftp.CnOpts()
#         cnopts.log = log_file # For pysftp logs
#     if cnopts.hostkeys is None:
#         # Load known hosts if a known_hosts file has been provided, otherwise do not verify.
#         # This is necessary for the host key check, which is mandatory if known_hosts is not None.
#         cnopts.hostkeys = None

#     try:
#         with pysftp.Connection(
#             host=server_address,
#             username=auth_user,
#             password=auth_pass,
#             private_key=private_key_path,
#             private_key_pass=private_key_pass,
#             port=port_num,
#             cnopts=cnopts
#         ) as s:
#             s.put(local_file, remote_file)
#             logger.info(f"Successfully transferred {local_file} to {server_address}:{remote_file}")
#             return True

#     except pysftp.exceptions.SFTPError as e:
#         logger.error(f"SFTP Error: {e}")
#         raise  # Re-raise the exception for the caller to handle
#     except paramiko.ssh_exception.SSHException as e:
#         logger.error(f"SSH Error: {e}")
#         raise
#     except OSError as e:
#         logger.error(f"OS Error: {e}")
#         raise
#     except Exception as e:
#         logger.exception(f"An unexpected error occurred: {e}")  # Log traceback
#         raise

# def check_host_key(server_address: str, user_known_hosts_file:str, port_num: int = 22):
#     """
#     Checks the host key of an SFTP server against a user-provided known_hosts file.

#     Args:
#         server_address: The hostname or IP address of the SFTP server.
#         user_known_hosts_file: The file path to a file in the style of a .ssh/known_hosts file
#         port_num: The port number for the SFTP connection (default: 22).

#     Returns:
#         A `pysftp.CnOpts` object with hostkeys set, for use in the `sftp_put`
#         function.

#     Raises:
#         FileNotFoundError:  If user_known_hosts_file does not exist.
#         paramiko.ssh_exception.SSHException: If there's an issue loading keys.
#         Exception: Catch-all for other unexpected exceptions.
#     """

#     if not os.path.exists(user_known_hosts_file):
#         raise FileNotFoundError(f"Known hosts file not found: {user_known_hosts_file}")

#     cnopts = pysftp.CnOpts()

#     try:
#         cnopts.hostkeys.load(user_known_hosts_file)
#     except FileNotFoundError:
#         raise FileNotFoundError(f"Known hosts file not found: {user_known_hosts_file}")
#     except paramiko.ssh_exception.SSHException as e:
#         raise paramiko.ssh_exception.SSHException(f"Error loading known hosts file: {e}")
#     except Exception as e:
#         raise Exception(f"An unexpected error has occurred: {e}")

#     return cnopts

# # Example 1: Password Authentication
# try:
#     success = sftp_put(
#         remote_file="/remote/path/data.txt",
#         local_file="/local/path/data.txt",
#         server_address="your_sftp_server.com",
#         auth_user="your_username",
#         auth_pass="your_password",
#         log_file="./sftp_transfer.log"
#     )
#     if success:
#         print("Transfer successful!")
# except Exception as e:
#     print(f"Transfer failed: {e}")

# # Example 2: Key-Based Authentication
# try:
#     success = sftp_put(
#         remote_file="/remote/path/data2.txt",
#         local_file="/local/path/data2.txt",
#         server_address="your_sftp_server.com",
#         auth_user="your_username",
#         private_key_path="/path/to/your/private_key",
#         private_key_pass="your_key_passphrase",  # Omit if key has no passphrase
#         log_file="./sftp_transfer.log"
#     )
#     if success:
#         print("Transfer successful (key-based)!")
# except Exception as e:
#     print(f"Transfer failed (key-based): {e}")

# # Example 3: Host Key Verification
# try:
#     # Create a CnOpts object to check the host keys
#     cnopts = check_host_key("your_sftp_server.com","/path/to/known_hosts_file")

#     success = sftp_put(
#         remote_file="/remote/path/data2.txt",
#         local_file="/local/path/data2.txt",
#         server_address="your_sftp_server.com",
#         auth_user="your_username",
#         private_key_path="/path/to/your/private_key",
#         private_key_pass="your_key_passphrase",  # Omit if key has no passphrase
#         log_file="./sftp_transfer.log",
#         cnopts = cnopts
#     )
#     if success:
#         print("Transfer successful (key-based)!")
# except Exception as e:
#     print(f"Transfer failed (key-based): {e}")

# # Example 4: No host key verification
# try:

#     success = sftp_put(
#         remote_file="/remote/path/data2.txt",
#         local_file="/local/path/data2.txt",
#         server_address="your_sftp_server.com",
#         auth_user="your_username",
#         private_key_path="/path/to/your/private_key",
#         private_key_pass="your_key_passphrase",  # Omit if key has no passphrase
#         log_file="./sftp_transfer.log",
#         cnopts=pysftp.CnOpts() # Do not verify host keys

#     )
#     if success:
#         print("Transfer successful (key-based)!")
# except Exception as e:
#     print(f"Transfer failed (key-based): {e}")


# Security of Keys and known_hosts Files

# "You're absolutely correct in your understanding of the security implications.

# known_hosts per Script (Possible, but not recommended): Yes, you could technically have a separate known_hosts file for each script. However, this is generally not recommended.

# Management Overhead: It becomes a nightmare to manage. If a host key changes (which can happen for legitimate reasons), you have to update every known_hosts file.
# Increased Risk of Error: It's much easier to make a mistake and accidentally use the wrong known_hosts file, potentially opening you up to a MITM attack.
# Duplication: It's highly likely that many of your scripts will connect to the same SFTP servers, so you'll be duplicating the same host key information in multiple files.
# Recommended Approach: Centralized known_hosts: The best practice is to have a single, centralized known_hosts file (or a small number, organized logically) that's shared by all your scripts.

# User-Specific: Often, this is the user's ~/.ssh/known_hosts file (the standard location).
# System-Wide (Less Common): You could have a system-wide known_hosts file (e.g., /etc/ssh/ssh_known_hosts), but this is less common and requires careful management of permissions.
# Dedicated File (Good Option): You could also create a dedicated known_hosts file specifically for your application (e.g., /opt/my_app/config/known_hosts). This keeps it separate from the user's personal known_hosts file. Your check_host_key function would then use this path.
# Security is Multi-Layered: You're spot on. SFTP and host key verification secure the data in transit. Protecting the system where your private keys and known_hosts files are stored is a separate, but equally important, concern. This involves:

# File Permissions: The private key file must have very restrictive permissions (e.g., chmod 600 /path/to/private_key). Only the user running the script should be able to read it.
# Operating System Security: This includes:
# User Accounts: Use dedicated user accounts for running your scripts, with limited privileges (principle of least privilege).
# Regular Updates: Keep the operating system and all software up-to-date with security patches.
# Strong Passwords: Use strong, unique passwords for all user accounts.
# Firewall: Configure the firewall to allow only necessary inbound and outbound connections.
# Intrusion Detection/Prevention Systems (IDS/IPS): Consider using IDS/IPS to monitor for suspicious activity.
# Network Security: This includes:
# Firewall: A network firewall should protect your server from unauthorized access.
# Network Segmentation: Separate your SFTP server from other critical systems on your network.
# Access Control Lists (ACLs): Use ACLs to restrict network access to the SFTP server.
# Physical security: Ensure the physical security of the server, to avoid tampering.
# In summary: Use named loggers for better organization and control. Centralize your known_hosts file for easier management and reduced risk.
# And, most importantly, remember that securing your data is a multi-layered process involving secure protocols (SFTP),
# secure configuration (host key verification), and secure system administration (file permissions, OS security, network security)."
