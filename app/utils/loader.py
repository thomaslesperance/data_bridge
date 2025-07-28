import shutil
import pysftp
from logger import logger
from models import StreamData, DestinationResponse
from transformutils import df_to_csv_buffer


class Loader:

    def __init__(self, destinations, load_tasks, email_builders) -> None:
        self.destinatios = destinations
        self.load_tasks = load_tasks
        self.email_builders = email_builders
        self.dest_protocol_to_method = {
            "smtp": self._smtp_load,
            "fileshare": self._share_load,
            "sftp": self._sftp_load,
            "google_drive": self._drive_load,
        }
        self.processed_load_tasks = []
        self._setup_loader()

    def _setup_loader(self) -> None:
        for _, task_config in self.load_tasks.items():
            dest_name = task_config.destination
            dest_config = self.destinations[dest_name]
            dest_protocol = dest_config.protocol
            load_method = self.dest_protocol_to_method[dest_protocol]
            load_dependencies = task_config.dependencies
            email_builder = task_config.email_builder
            load_task = {
                "dest_name": dest_name,
                "dest_config": dest_config,
                "method": load_method,
                "dependencies": load_dependencies,
                "email_builder": email_builder,
            }
            self.processed_load_tasks.append(load_task)
            logger.debug(f"Extract task added: {load_task}")

    def __call__(
        self, all_load_data: dict[str, StreamData]
    ) -> list[DestinationResponse]:
        all_dest_responses = []
        for load_task in self.processed_load_tasks:
            load_method = load_task["method"]
            dest_config = load_task["dest_config"]
            task_data = {}
            load_dependencies = load_task.get("dependencies", [])

            for dependency in load_dependencies:
                if dependency in all_load_data.keys():
                    task_data[dependency] = all_load_data[dependency]
                else:
                    logger.warning(
                        f"Loader could not find {dependency} in extraxted/transformed data to load to {load_task["dest_name"]}"
                    )
                    continue

            email_builder = load_task["email_builder"]
            if email_builder:
                email_builder_fn = self.email_builders[email_builder]
                message = email_builder_fn(task_data)
                response = load_method(dest_config, message)
                all_dest_responses.append(response)
                logger.debug(f"Response for email task received: {response}")
            else:
                response = load_method(dest_config, task_data)
                all_dest_responses.append(response)
                logger.debug(f"Response for load task received: {response}")
        return all_dest_responses

    def _smtp_load(self, dest_config, message) -> DestinationResponse:
        """message: email.message.Message"""

    def _share_load(
        self, dest_config, load_data: dict[str, StreamData]
    ) -> DestinationResponse:
        for file_name, stream_data in load_data.items():
            if stream_data.data_format == "dataframe":
                file_data = df_to_csv_buffer(stream_data.content)
            remote_file = dest_config.mount_path / file_name
            shutil.copyfileobj(file, remote_file)

    def _sftp_load(self, dest_config, load_data) -> DestinationResponse:
        print("Some stuff")
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
        print("Some stuff")
