from models import PipelineData, DestinationResponse


class Loader:

    def __init__(self, destinations, load_tasks, email_builders, logger):
        self.dest_protocol_to_method = {
            "smtp": self._smtp_load,
            "fileshare": self._share_load,
            "sftp": self._sftp_load,
            "google_drive": self._drive_load,
        }

        self.load_tasks = []
        self.email_builders = email_builders
        self.logger = logger

        try:
            for _, task_config in load_tasks.items():
                dest_name = task_config.destination
                dest_config = destinations[dest_name]
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
                self.logger.debug(f"Extract task added: {load_task}")
                self.load_tasks.append(load_task)
        except Exception as e:
            raise Exception(
                f"Failed to compile load task list in Loader constructor:\n{e}"
            )

    # @message: email.message.Message
    def _smtp_load(self, dest_config, message) -> DestinationResponse:
        try:
            print("Some stuff")
        except Exception as e:
            raise Exception(f"Load method '_smtp_load' failed:\n{e}")

    def _share_load(
        self, dest_config, load_data: dict[str, PipelineData]
    ) -> DestinationResponse:
        try:
            print("Some stuff")
        except Exception as e:
            raise Exception(f"Load method '_share_load' failed:\n{e}")

    def _sftp_load(
        self, dest_config, load_data: dict[str, PipelineData]
    ) -> DestinationResponse:
        try:
            print("Some stuff")
        except Exception as e:
            raise Exception(f"Load method '_sftp_load' failed:\n{e}")

    def _drive_load(
        self, dest_config, load_data: dict[str, PipelineData]
    ) -> DestinationResponse:
        try:
            print("Some stuff")
        except Exception as e:
            raise Exception(f"Load method '_drive_load' failed:\n{e}")

    def load(self, all_load_data: dict[str, PipelineData]) -> list[DestinationResponse]:
        all_dest_responses = []

        for load_task in self.load_tasks:
            try:
                load_method = load_task["method"]
                dest_config = load_task["dest_config"]
                task_data = {}
                load_dependencies = load_task.get("dependencies", [])
                for dependency in load_dependencies:
                    if dependency in all_load_data.keys():
                        task_data[dependency] = all_load_data[dependency]
                    else:
                        self.logger.warning(
                            f"Loader could not find {dependency} in extraxted/transformed data to load to {load_task["dest_name"]}"
                        )
                        continue

                email_builder = load_task["email_builder"]
                if email_builder:
                    email_builder_fn = self.email_builders[email_builder]
                    message = email_builder_fn(task_data)
                    response = load_method(dest_config, message)
                    all_dest_responses.append(response)
                    self.logger.debug(f"Response for email task received: {response}")
                else:
                    response = load_method(dest_config, task_data)
                    all_dest_responses.append(response)
                    self.logger.debug(f"Response for load task received: {response}")
            except Exception as e:
                raise Exception(
                    f"Loading {task_data} to '{load_task["dest_name"]}' failed:\n{e}"
                )
        return all_dest_responses
