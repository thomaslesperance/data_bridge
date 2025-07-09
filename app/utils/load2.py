from models import Destination, PipelineData, EmailFunc, DestinationResponse


class Loader:

    def __init__(self, destinations, dest_config, email_fn=None):
        self.dest_method_map = {
            "smtp_server": self._smtp_load,
            "fileshare": self._share_load,
            "sftp_server": self._sftp_load,
            "google_drive_account": self._drive_load,
        }

        self.email_fn = email_fn
        self.load_tasks = []

        # load_tasks = [{"dest_name": dest_name, "dest_config": dest_config, "method": load_method_ref, "dependency": load_dependency_path}, ...]
        for dest_name, load_dependencies in dest_config.items():
            if isinstance(load_dependencies, list):
                for dependency in load_dependencies:
                    load_task = {}
                    load_task["dest_name"] = dest_name
                    load_task["dest_config"] = destinations[dest_name]
                    load_task["method"] = self.dest_method_map[dest_name]
                    load_task["dependency"] = dependency
                    self.load_tasks.append(load_task)
            elif isinstance(load_dependencies, str):
                load_task = {}
                load_task["dest_name"] = dest_name
                load_task["dest_config"] = destinations[dest_name]
                load_task["method"] = self.dest_method_map[dest_name]
                load_task["dependency"] = load_dependencies
                self.load_tasks.append(load_task)

    def _smtp_load(
        self,
        dest_config: Destination,
        data_to_load: PipelineData,
        email_msg: EmailFunc = None,
    ) -> DestinationResponse:
        print("Some stuff")

    def _share_load(
        self, dest_config: Destination, data_to_load: PipelineData
    ) -> DestinationResponse:
        print("Some stuff")

    def _sftp_load(
        self, dest_config: Destination, data_to_load: PipelineData
    ) -> DestinationResponse:
        """
        Attempts to upload data to an SFTP server and returns a standardized response.
        """
        # Assumes data_to_load.content is a Path object to a local file
        local_file_path = data_to_load.content
        remote_path = f"/uploads/{local_file_path.name}"

        try:
            # Attempt the connection and file transfer
            with pysftp.Connection(
                host=dest_config.host, username=dest_config.user
            ) as sftp:
                sftp.put(local_file_path, remote_path)

            # If successful, create a success response object
            response = DestinationResponse(
                destination_name=dest_config.name,
                status="success",
                message=f"File successfully uploaded to {remote_path}.",
            )

        except Exception as e:
            # If any error occurs, create a failure response object
            response = DestinationResponse(
                destination_name=dest_config.name,
                status="failure",
                message=f"SFTP upload failed: {e}",
            )

        return response

    def _drive_load(
        self, dest_config: Destination, data_to_load: PipelineData
    ) -> DestinationResponse:
        print("Some stuff")

    def load(
        self, data: PipelineData, email_msg: EmailFunc = None
    ) -> list[DestinationResponse]:
        """
        Orchestrates all load tasks and collects their standardized responses.
        """
        # TODO: This needs to accept a flat dict of PipelineData objects where:
        # key=load dependency report.csv and value=PipelineData object
        all_responses = []
        for task in self.load_tasks:
            method_to_call = task["method"]
            destination_config = task["dest_config"]
            single_response = method_to_call(destination_config, data, email_msg)
            all_responses.append(single_response)

        return all_responses
