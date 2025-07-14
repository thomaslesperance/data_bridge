from models import PipelineData, DestinationResponse


class Loader:

    def __init__(self, destinations, load_tasks, email_builders):
        self.dest_protocol_to_method = {
            "smtp": self._smtp_load,
            "fileshare": self._share_load,
            "sftp": self._sftp_load,
            "google_drive": self._drive_load,
        }

        self.email_builders = email_builders
        self.load_tasks = []

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
            self.load_tasks.append(load_task)

    # @message: email.message.Message
    def _smtp_load(self, dest_config, message) -> DestinationResponse:
        print("Some stuff")

    def _share_load(
        self, dest_config, load_data: dict[str, PipelineData]
    ) -> DestinationResponse:
        print("Some stuff")

    def _sftp_load(
        self, dest_config, load_data: dict[str, PipelineData]
    ) -> DestinationResponse:
        print("Some stuff")

    def _drive_load(
        self, dest_config, load_data: dict[str, PipelineData]
    ) -> DestinationResponse:
        print("Some stuff")

    # For jobs["example_complex_job"]:
    # load_tasks = [
    # {
    #   "dest_name": sftp_server,
    #   "dest_config": {
    #     "protocol": "sftp",
    #     "host": "123.456.789.1011",
    #     "user": "user",
    #     "password": "password",
    #     "port": "22",
    # },
    #   "method": self._sftp_load,
    #   "dependencies": ["formatted_grades.csv", "active_teachers.csv"]
    #   "email_builder": None,
    # },
    # {
    #   "dest_name": google_drive_account,
    #   "dest_config": {
    #     "protocol": "google_drive",
    #     "access_token": CREDS_DIR / "token.pickle",
    # },
    #   "method": self._drive_load,
    #   "dependencies": "remote/rel/path/summary.csv"
    #   "email_builder": None,
    # },
    # {
    #   "dest_name": smtp_server,
    #   "dest_config": {
    #     "protocol": "smtp",
    #     "host": "smtp.domain.net",
    #     "port": "25",
    #     "default_sender_email": "jobs@example.com",
    # },
    #   "method": self._smtp_load,
    #   "dependencies": "email_1_data.csv"
    #   "email_builder": "build_teacher_email",
    # },
    #   "dest_name": smtp_server,
    #   "dest_config": {
    #     "protocol": "smtp",
    #     "host": "smtp.domain.net",
    #     "port": "25",
    #     "default_sender_email": "jobs@example.com",
    # },
    #   "method": self._smtp_load,
    #   "dependencies": ["email_2_data_A.csv", "email_2_data_B.csv"]
    #   "email_builder": "build_admin_email",
    # },
    #   "dest_name": smtp_server,
    #   "dest_config": {
    #     "protocol": "smtp",
    #     "host": "smtp.domain.net",
    #     "port": "25",
    #     "default_sender_email": "jobs@example.com",
    # },
    #   "method": self._smtp_load,
    #   "dependencies": None
    #   "email_builder": "build_status_email",
    # },
    # ]
    #
    # all_load_data = {
    #   "formatted_grades.csv": <PipelineData object>,
    #   "active_teachers.csv": <PipelineData object>,
    #   "remote/rel/path/summary.csv": <PipelineData object>,
    #   "email_1_data.csv": <PipelineData object>,
    #   "email_2_data_A.csv": <PipelineData object>,
    #   "email_2_data_B.csv": <PipelineData object>,
    # }

    def load(self, all_load_data: dict[str, PipelineData]) -> list[DestinationResponse]:
        all_dest_responses = []

        for load_task in self.load_tasks:
            load_method = load_task["method"]
            dest_config = load_task["dest_config"]
            task_data = {}
            for dependency in load_task["dependencies"]:
                if dependency in all_load_data.keys():
                    task_data[dependency] = all_load_data[dependency]
                else:
                    # logger.log("Loader Could not find {dependency_name} to load to {load_task["dest_name"]} as requested")
                    continue

            email_builder = load_task["email_builder"]
            if email_builder:
                email_builder_fn = self.email_builders[email_builder]
                message = email_builder_fn(task_data)
                response = load_method(dest_config, message)
                all_dest_responses.append(response)
            else:
                response = load_method(dest_config, task_data)
                all_dest_responses.append(response)

        return all_dest_responses
