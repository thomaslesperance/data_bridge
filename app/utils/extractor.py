from logger import logger
from errors import LogAndTerminate
from models import StreamData


class Extractor:

    def __init__(self, sources, init_extract_tasks) -> None:
        self.sources = sources
        self.init_extract_tasks = init_extract_tasks
        self.logger = logger
        self.source_type_to_method = {
            "sql": self._sql_extract,
            "fileshare": self._fileshare_extract,
            "google_drive": self._drive_extract,
            "sftp": self._sftp_extract,
        }
        self.extract_tasks = []
        self._setup_extractor()

    @LogAndTerminate()
    def _setup_extractor(self) -> None:
        for _, task_config in self.init_extract_tasks.items():
            source_name = task_config.source
            source_config = self.sources[source_name]
            source_type = source_config.type
            for dependency in task_config.dependencies:
                extract_task = {
                    "source_name": source_name,
                    "source_config": source_config,
                    "method": self.source_type_to_method[source_type],
                    "dependency": dependency,
                }
                self.extract_tasks.append(extract_task)
                self.logger.debug(f"Extract task added: {extract_task}")

    def _sql_extract(self, source_config, query_file_path) -> StreamData:
        """Returns pd.DataFrame for StreamData.content type"""
        print("Some stuff")

    def _fileshare_extract(self, source_config, rel_file_path) -> StreamData:
        """Returns file io.BytesIO for StreamData.content type"""
        print("Some stuff")

    def _sftp_extract(self, source_config, rel_file_path) -> StreamData:
        """Returns file io.BytesIO for StreamData.content type"""
        print("Some stuff")

    def _drive_extract(self, source_config, dependency) -> StreamData:
        """Returns file io.BytesIO for StreamData.content type"""
        print("Some stuff")

    @LogAndTerminate()
    def extract(self) -> dict[str, StreamData]:
        extracted_data = {}
        for extract_task in self.extract_tasks:
            method = extract_task["method"]
            data = method(extract_task["source_config"], extract_task["dependency"])
            extracted_data[extract_task["dependency"]] = data
            self.logger.debug(
                f"Extracted data: {extracted_data[extract_task["dependency"]]}"
            )
        return extracted_data
