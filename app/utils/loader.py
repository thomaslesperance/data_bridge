import shutil
import pysftp
from logger import logger
from models import LoadStep, StreamData, DestinationResponse
from transformutils import df_to_csv_buffer


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

    def _smtp_load(self, dest_config, message) -> DestinationResponse:
        """message: email.message.Message"""
        pass

    def _share_load(
        self, dest_config, load_data: dict[str, StreamData]
    ) -> DestinationResponse:
        for file_name, stream_data in load_data.items():
            if stream_data.data_format == "dataframe":
                file_data = df_to_csv_buffer(stream_data.content)
            remote_file = dest_config.mount_path / file_name
            shutil.copyfileobj(file, remote_file)

    def _sftp_load(self, dest_config, load_data) -> DestinationResponse:
        pass
        # with pysftp.Connection(**THIRD_FUTURE_SFTP) as sftp:
        #     with sftp.cd(DISCIPLINE_REMOTE_PATH):
        #         sftp.putfo(
        #             disc_csv_buffer, f"{DISCIPLINE_REMOTE_PATH}/{DISCIPLINE_FILE_NAME}"
        #         )
        #     with sftp.cd(ATTENDANCE_REMOTE_PATH):
        #         sftp.putfo(
        #             att_csv_buffer, f"{ATTENDANCE_REMOTE_PATH}/{ATTENDANCE_FILE_NAME}"
        #         )

    def _drive_load(self, dest_config, load_data) -> DestinationResponse:
        pass


Loader.protocol_to_method = {
    "smtp": Loader._smtp_load,
    "fileshare": Loader._share_load,
    "sftp": Loader._sftp_load,
    "google_drive": Loader._drive_load,
}
