class Loader:

    def __init__(self, validated_destinations, validated_load_config, email_msg = None):
        self.dest_method_map = {
            "smtp_server":,
            "fileshare":,
            "sftp_server":,
            "google_drive_account":,
        }

        self.email_msg = email_msg
        self.load_tasks = []

        # extract_tasks = [{"dest_name": dest_name, "load_config": load_dict, "method": load_method_ref, "dependency": load_dependency_path}, ...]
        for dest_name, load_dependencies in validated_load_config.items():
            if isinstance(load_dependencies, list):
                for dependency in load_dependencies:
                    extract_task = {}
                    extract_task["dest_name"] = dest_name
                    extract_task["load_config"] = validated_destinations[dest_name]
                    extract_task["method"] = self.dest_method_map[dest_name]
                    extract_task["dependency"] = dependency
                    self.extract_tasks.append(extract_task)
            elif isinstance(load_dependencies, str):
                extract_task = {}
                extract_task["dest_name"] = dest_name
                extract_task["load_config"] = validated_destinations[dest_name]
                extract_task["method"] = self.dest_method_map[dest_name]
                extract_task["dependency"] = load_dependencies
                self.extract_tasks.append(extract_task)

    def _smtp_load(self, dest_dict, email_msg, data_file = None):
        print("Some stuff")
    
    def _share_load(self, dest_dict, data_file):
        print("Some stuff")
    
    def _sftp_load(self, dest_dict, data_file):
        print("Some stuff")
    
    def _drive_load(self, dest_dict, data_file):
        print("Some stuff")
    
    def load(self):
        # server_response = load_method(params)
        # self.server_response = server_response
        print("Some stuff")
            