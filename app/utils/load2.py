from models import PipelineData, DestinationResponse


class Loader:

    def __init__(self, destinations, load_config, email_fn=None):
        self.dest_protocol_to_method = {
            "smtp": self._smtp_load,
            "fileshare": self._share_load,
            "sftp": self._sftp_load,
            "google_drive": self._drive_load,
        }

        self.email_fn = email_fn
        self.load_tasks = []

        for dest_name, load_dependencies in load_config.items():
            dest_config = destinations[dest_name]
            dest_protocol = dest_config.protocol
            for dependency in load_dependencies:
                load_task = {
                    "dest_name": dest_name,
                    "dest_config": dest_config,
                    "method": self.dest_protocol_to_method[dest_protocol],
                    "dependency": dependency,
                }
                self.load_tasks.append(load_task)

    def _smtp_load(self, dest_config, load_data) -> DestinationResponse:
        print("Some stuff")

    def _share_load(self, dest_config, load_data) -> DestinationResponse:
        print("Some stuff")

    def _sftp_load(self, dest_config, load_data) -> DestinationResponse:
        print("Some stuff")

    def _drive_load(self, dest_config, load_data) -> DestinationResponse:
        print("Some stuff")

    # For jobs["example_complex_job"]:
    # load_tasks = [
    # {
    #   "dest_name": dest_name,
    #   "dest_config": dest_config,
    #   "method": load_method_ref,
    #   "dependency": load_dependency_path
    # }, ...
    # ]
    # all_load_data = {
    #   "formatted_grades.csv": <PipelineData object>,
    #   "active_teachers.csv": <PipelineData object>,
    #   "remote/rel/path/summary.csv": <PipelineData object>,
    #   "email_data.csv": <PipelineData object>,
    # }

    def load(self, all_load_data: dict[str, PipelineData]) -> list[DestinationResponse]:
        all_dest_responses = []

        for load_task in self.load_tasks:
            dependency_name = load_task["dependency"]
            if dependency_name not in all_load_data:
                # logger.log("Loader Could not find {dependency_name} to load to {load_task["dest_name"]} as requested")
                continue

            data_item = all_load_data[dependency_name]
            method = load_task["method"]
            dest_config = load_task["dest_config"]
            dest_response = method(dest_config, data_item)

            all_dest_responses.append(dest_response)

        return all_dest_responses
