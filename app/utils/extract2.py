from models import PipelineData


class Extractor:

    def __init__(self, sources, extract_config):
        self.source_type_to_method = {
            "sql": self._sql_extract,
            "fileshare": self._fileshare_extract,
            "google_drive": self._drive_extract,
            "sftp": self._sftp_extract,
        }

        self.extract_tasks = []

        for source_name, extract_dependencies in extract_config.items():
            source_config = sources[source_name]
            source_type = source_config.type
            for dependency in extract_dependencies:
                extract_task = {
                    "source_name": source_name,
                    "source_config": source_config,
                    "method": self.source_type_to_method[source_type],
                    "dependency": dependency,
                }
                self.extract_tasks.append(extract_task)

    def _sql_extract(self, source_config, query_file_path) -> PipelineData:
        """Returns pd.DataFrame for PipelineData.content type"""
        print("Some stuff")

    def _fileshare_extract(self, source_config, rel_file_path) -> PipelineData:
        """Returns file io.BytesIO for PipelineData.content type"""
        print("Some stuff")

    def _sftp_extract(self, source_config, rel_file_path) -> PipelineData:
        """Returns file io.BytesIO for PipelineData.content type"""
        print("Some stuff")

    def _drive_extract(self, source_config, dependency) -> PipelineData:
        """Returns file io.BytesIO for PipelineData.content type"""
        print("Some stuff")

    # For jobs["example_complex_job"]:
    # extract_tasks = [
    # {
    #   "source_name": source_name,
    #   "source_config": source_dict,
    #   "method": extract_method_ref,
    #   "dependency": extract_dependency_path
    # }, ...
    # ]
    # extracted_data = {
    #   "grades.sql": <PipelineData object>,
    #   "students.sql": <PipelineData object>,
    #   "teachers.sql": <PipelineData object>,
    #   "remote/rel/path/export_file.csv": <PipelineData object>,
    #   "remote/rel/path/file.xlsx": <PipelineData object>,
    # }

    def extract(self) -> dict[str, PipelineData]:
        extracted_data = {}

        for extract_task in self.extract_tasks:
            method = extract_task["method"]
            data = method(extract_task["source_config"], extract_task["dependency"])
            extracted_data[extract_task["dependency"]] = data
        return extracted_data
