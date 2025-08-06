import shutil
import pysftp
import smtplib
import io
from logger import logger
from models import LoadStep, StreamData, DestinationResponse
from transformutils import df_to_csv_buffer


from email.utils import COMMASPACE, formatdate


class Loader:

    @classmethod
    def load(
        cls,
        load_step_config: LoadStep,
        step_outputs: dict[str, StreamData],
        macro_registry: dict[str, callable],
    ) -> DestinationResponse:
        protocol = load_step_config.get("protocol")
        load_method = cls.protocol_to_method.get(protocol)
        dest_response = load_method(
            load_step_config=load_step_config,
            step_outputs=step_outputs,
            macro_registry=macro_registry,
        )
        return dest_response

    @classmethod
    def _smtp_load(
        cls, load_step_config, step_outputs, macro_registry
    ) -> DestinationResponse:
        """message: email.message.Message"""
        raw_email_params = load_step_config.get("email_params")
        resolved_email_params = cls._resolve_email_params(
            raw_query_params=raw_email_params,
            step_outputs=step_outputs,
            macro_registry=macro_registry,
        )
        dest_config = load_step_config.dest_config
        load_data = [step_outputs[item] for item in load_step_config.input]
        email_message_obj = load_step_config.email_builder(
            dest_config, load_data, resolved_email_params
        )

        with smtplib.SMTP(dest_config.host, int(dest_config.port)) as smtp:
            smtp.starttls()
            smtp.login(dest_config.user, dest_config.password)
            smtp.sendmail(
                from_addr=dest_config.default_sender_email,
                to_addrs=resolved_email_params.recipients,
                msg=email_message_obj.as_bytes(),
            )

    @classmethod
    def _share_load(
        cls, load_step_config, step_outputs, macro_registry
    ) -> DestinationResponse:
        load_data = step_outputs[load_step_config.input]
        mount_path = load_step_config.dest_config.mount_path
        remote_file_path = load_step_config.remote_file_path
        remote_file = f"{mount_path}/{remote_file_path}"
        dest_name = load_step_config.dest_config.name

        try:
            if load_data.data_format == "dataframe":
                file_buffer = df_to_csv_buffer(load_data.content)
                shutil.copyfileobj(file_buffer, remote_file)

            if load_data.data_format == "file_buffer":
                load_data.content.seek(0)
                shutil.copyfileobj(load_data.content, remote_file)

            if load_data.data_format == "file_path":
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
    def _sftp_load(
        cls, load_step_config, step_outputs, macro_registry
    ) -> DestinationResponse:
        load_data = step_outputs[load_step_config.input]
        mount_path = load_step_config.dest_config.mount_path
        remote_file_path = load_step_config.remote_file_path
        remote_file = f"{mount_path}/{remote_file_path}"
        dest_name = load_step_config.dest_config.name
        sftp_config = {
            key: value
            for key, value in load_step_config.dest_config.items()
            if key in ("user", "password", "host", "port")
        }

        try:
            with pysftp.Connection(**sftp_config) as sftp:
                with sftp.cd(mount_path):
                    if load_data.data_format == "dataframe":
                        file_buffer = df_to_csv_buffer(load_data.content)
                        sftp.putfo(file_buffer, remote_file_path)

                    if load_data.data_format == "file_buffer":
                        load_data.content.seek(0)
                        sftp.putfo(load_data.content, remote_file_path)

                    if load_data.data_format == "file_path":
                        sftp.put(load_data.content, remote_file_path)
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
    def _drive_load(
        cls, load_step_config, step_outputs, macro_registry
    ) -> DestinationResponse:
        pass


Loader.protocol_to_method = {
    "smtp": Loader._smtp_load,
    "fileshare": Loader._share_load,
    "sftp": Loader._sftp_load,
    "google_drive": Loader._drive_load,
}
