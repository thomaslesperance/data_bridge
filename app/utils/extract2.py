from models import PipelineData


class Extractor:

    def __init__(self, sources, extract_tasks):
        self.source_type_to_method = {
            "sql": self._sql_extract,
            "fileshare": self._fileshare_extract,
            "google_drive": self._drive_extract,
            "sftp": self._sftp_extract,
        }

        self.extract_tasks = []

        for _, task_config in extract_tasks.items():
            source_name = task_config.source
            source_config = sources[source_name]
            source_type = source_config.type
            for dependency in task_config.dependencies:
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
    #   "source_name": "db1",
    #   "source_config": {
    #       "type": "sql",
    #       "user": "user",
    #       "password": "password",
    #       "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
    #       "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
    #       # You do not need to specify JAR file abs path if CLASSPATH set in ~/.zprofile
    #   },
    #   "method": self._sql_extract,
    #   "dependency": "grades.sql",
    # },
    # {
    #   "source_name": "db1",
    #   "source_config": {
    #       "type": "sql",
    #       "user": "user",
    #       "password": "password",
    #       "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
    #       "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
    #       # You do not need to specify JAR file abs path if CLASSPATH set in ~/.zprofile
    #   },
    #   "method": self._sql_extract,
    #   "dependency": "students.sql",
    # },
    # {
    #   "source_name": "fileshare",
    #   "source_config": {
    #       "type": "fileshare",
    #       "mount_path": "/abs/path/to/share/root",
    # },
    #   "method": self._fileshare_extract,
    #   "dependency": "remote/rel/path/export_file.csv",
    # },
    # ]
    #
    # extracted_data = {
    #   "grades.sql": <PipelineData object>,
    #   "students.sql": <PipelineData object>,
    #   "remote/rel/path/export_file.csv": <PipelineData object>,
    # }

    def extract(self) -> dict[str, PipelineData]:
        extracted_data = {}

        for extract_task in self.extract_tasks:
            method = extract_task["method"]
            data = method(extract_task["source_config"], extract_task["dependency"])
            extracted_data[extract_task["dependency"]] = data
        return extracted_data
