from datetime import datetime   
from email.message import Message
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication 
from .models import StreamData


def build_email_msg(subject: str, body: str, date: str = datetime.now(), attachments: list[StreamData] = None) -> Message:
    """Build an MIMEMultipart email message object from arguments. Will attempt to convert any attachments to bytes buffers
    and use the associated filename in the StreamData object."""
    file_buffers = {}
    for attachment in attachments:
        if data_item.data_format == "dataframe":
            file_buffers[] = df_to_csv_buffer(data_item.content)
        if data_item.data_format == "file_path":
            with open(data_item.content) as file:
                file_buffer = io.BytesIO(file)
                file_buffer.seek(0)
                data_item = file_buffer
    msg = MIMEMultipart()
    # msg["Date"] = formatdate(localtime=True)
    # msg["Subject"] = subject
    # msg.attach(MIMEText(text))
    # attachments list

    for buffer in load_data:
        attachment = MIMEApplication(buffer)
        attachment["Content-Disposition"] = 'attachment; filename="file_name.csv"'
        msg.attach(attachment)