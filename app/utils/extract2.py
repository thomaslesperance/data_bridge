from models import ValidatedConfigUnion


class Extractor():

    def __init__(self, sources, extract_config):
        self.source_method_map = {
            "db1": self._sql_extract, 
            "db2": self._sql_extract, 
            "fileshare": self._fileshare_extract, 
            "google_drive_account": self._drive_extract,
            "sftp_server": self._sftp_extract,
        }
            
        validated_sources = ValidatedConfigUnion.model_validate(sources)
        validated_extract_config = ValidatedConfigUnion.model_validate(extract_config)

        self.extract_tasks = []

        # extract_tasks = [{"source_name": source_name, "source_config": source_dict, "method": extract_method_ref, "dependency": extract_dependency_path}, ...]
        for source_name, extract_dependencies in validated_extract_config.items():
            if isinstance(extract_dependencies, list):
                for dependency in extract_dependencies:
                    extract_task = {}
                    extract_task["source_name"] = source_name
                    extract_task["source_config"] = validated_sources[source_name]
                    extract_task["method"] = self.source_method_map[source_name]
                    extract_task["dependency"] = dependency
                    self.extract_tasks.append(extract_task)
            elif isinstance(extract_dependencies, str):
                extract_task = {}
                extract_task["source_name"] = source_name
                extract_task["source_config"] = validated_sources[source_name]
                extract_task["method"] = self.source_method_map[source_name]
                extract_task["dependency"] = extract_dependencies
                self.extract_tasks.append(extract_task)                

    def _get_column_headers(self, curs):
        return [desc[0] for desc in curs.description]
    
    def _sql_extract(self, source_dict, query_file_path):
        print("Some stuff")
    
    def _fileshare_extract(self, source_dict, file_path):
        print("Some stuff")
    
    def _drive_extract(self, source_dict, file_path):
        print("Some stuff")

    def _sftp_extract(self, source_dict, file_path):
        print("Some stuff")
    
    def extract(self):
        data = {}
        for extract_task in self.extract_tasks:
            method = extract_task["method"]
            data_frame = method(extract_task["source_config"], extract_task["dependency"])
            data[f"{extract_task["source_name"]}__{extract_task["dependency"]}"] = data_frame
        return data
