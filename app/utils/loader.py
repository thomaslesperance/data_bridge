import shutil
import pysftp
import smtplib
from app.utils.models import LoadStep, StreamData, DestinationResponse
from app.utils.macros import macro_registry


class Loader:

    @classmethod
    def load(
        cls, load_step_config: LoadStep, step_outputs: dict[str, StreamData]
    ) -> DestinationResponse:
        protocol = load_step_config.protocol
        load_method = cls.protocol_to_method[protocol]
        dest_response = load_method(load_step_config, step_outputs)
        return dest_response

    @classmethod
    def _smtp_load(cls, load_step_config, step_outputs) -> DestinationResponse:
        """Expects StreamData.data_format='email_message'"""
        smtp_config = load_step_config.dest_config
        from_addr = smtp_config.default_sender_email
        recipients = load_step_config.recipients
        to_addrs = cls._resolve_email_recipients(recipients, step_outputs)
        email_message = step_outputs[load_step_config.input.content]
        data_format = load_step_config.input.data_format

        if data_format not in ("email_message"):
            raise ValueError(
                f"Load destinations with protocol='smtp' require StreamData.data_format='email_message'; got {data_format}; check previous transform steps"
            )

        try:
            with smtplib.SMTP(smtp_config.host, int(smtp_config.port)) as smtp:
                smtp.starttls()
                smtp.login(smtp_config.user, smtp_config.password)
                send_errors = smtp.sendmail(
                    from_addr=from_addr, to_addrs=to_addrs, msg=email_message.as_bytes()
                )
            if send_errors:
                raise Exception(send_errors)
        except Exception as e:
            return DestinationResponse(
                destination_name=smtp_config.name,
                status="failure",
                message=f"LOAD FAILED: Email message '{load_step_config.step_name}' failed to send:\n\t{e}",
                records_processed=None,
            )
        return DestinationResponse(
            destination_name=smtp_config.name,
            status="success",
            message=f"LOAD SUCCESSFUL: Email message '{load_step_config.step_name}' sent",
            records_processed=1,
        )

    @classmethod
    def _resolve_email_recipients(recipients, step_outputs) -> list[str]:
        resolved_recipients = None
        if isinstance(recipients, str):
            resolved_recipients = [recipients]
        for recipient in recipients:
            if recipient.startswith("step:"):
                output_name = recipient.replace("step:", "")
                email_list = step_outputs[output_name]
                resolved_recipients.extend(email_list)
            else:
                resolved_recipients.append(recipient)

    @classmethod
    def _share_load(cls, load_step_config, step_outputs) -> DestinationResponse:
        """Expects StreamData.data_format='file_buffer' or 'file_path'"""
        load_data = step_outputs[load_step_config.input]
        mount_path = load_step_config.dest_config.mount_path
        remote_dir = f"{mount_path}/{load_step_config.remote_dir}"
        remote_file = f"{remote_dir}/{load_data.file_name}"
        dest_name = load_step_config.dest_config.name
        data_format = load_data.data_format

        try:
            if data_format not in ("file_buffer", "file_path"):
                raise ValueError(
                    f"Load destinations with protocol='smb' require StreamData.data_format='file_path' or 'file_buffer'; got {data_format}; check previous transform steps"
                )

            if data_format == "file_buffer":
                load_data.content.seek(0)
                shutil.copyfileobj(load_data.content, remote_file)

            if data_format == "file_path":
                shutil.copy(load_data.content, remote_file)
        except Exception as e:
            return DestinationResponse(
                destination_name=dest_name,
                status="failure",
                message=f"LOAD FAILED: File {remote_file} not loaded to '{dest_name}':\n\t{e}",
                records_processed=None,
            )
        return DestinationResponse(
            destination_name=dest_name,
            status="success",
            message=f"LOAD SUCCESSFUL: File {remote_file} loaded to '{dest_name}'",
            records_processed=1,
        )

    @classmethod
    def _sftp_load(cls, load_step_config, step_outputs) -> DestinationResponse:
        """Expects StreamData.data_format='file_buffer' or 'file_path'"""
        load_data = step_outputs[load_step_config.input]
        mount_path = load_step_config.dest_config.mount_path
        remote_dir = f"{mount_path}/{load_step_config.remote_dir}"
        remote_file = f"{remote_dir}/{load_data.file_name}"
        dest_name = load_step_config.dest_config.name
        data_format = load_data.data_format

        sftp_config = {
            key: value
            for key, value in load_step_config.dest_config.items()
            if key in ("user", "password", "host", "port")
        }

        try:
            if data_format not in ("file_buffer", "file_path"):
                raise ValueError(
                    f"Load destinations with protocol='smb' require StreamData.data_format='file_path' or 'file_buffer'; got {data_format}; check previous transform steps"
                )

            with pysftp.Connection(**sftp_config) as sftp:
                with sftp.cd(mount_path):
                    if load_data.data_format == "file_buffer":
                        load_data.content.seek(0)
                        sftp.putfo(load_data.content, remote_file)

                    if load_data.data_format == "file_path":
                        sftp.put(load_data.content, remote_file)
        except Exception as e:
            return DestinationResponse(
                destination_name=dest_name,
                status="failure",
                message=f"LOAD FAILED: File {remote_file} not loaded to '{dest_name}':\n\t{e}",
                records_processed=None,
            )
        return DestinationResponse(
            destination_name=dest_name,
            status="success",
            message=f"LOAD SUCCESSFUL: File {remote_file} loaded to '{dest_name}'",
            records_processed=1,
        )

    @classmethod
    def _drive_load(cls, load_step_config, step_outputs) -> DestinationResponse:
        pass


Loader.protocol_to_method = {
    "smtp": Loader._smtp_load,
    "fileshare": Loader._share_load,
    "sftp": Loader._sftp_load,
    "google_drive": Loader._drive_load,
}
