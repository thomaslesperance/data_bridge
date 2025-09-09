from io import BytesIO
from email.message import EmailMessage
from pandas import DataFrame


def df_to_csv_buffer(
    df: DataFrame, keep_df_index=False, encoding: str = "utf-8"
) -> BytesIO:
    bytes_buffer = BytesIO()
    df.to_csv(bytes_buffer, index=keep_df_index, encoding=encoding)
    bytes_buffer.seek(0)
    return bytes_buffer


def build_email_msg(
    subject: str,
    sender: str,
    recipients: str | list[str],
    body: str,
    attachments: dict[str, BytesIO] = None,
) -> EmailMessage:
    """Build an email.EmailMessage object from arguments."""
    msg = EmailMessage()

    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipients

    msg.set_content(body)
    for filename, file_buffer in attachments.items:
        msg.add_attachment(file_buffer, filename=filename)

    return msg
