from models import PipelineData


class Extractor:

    def __init__(self, sources, extract_tasks, logger):
        self.source_type_to_method = {
            "sql": self._sql_extract,
            "fileshare": self._fileshare_extract,
            "google_drive": self._drive_extract,
            "sftp": self._sftp_extract,
        }

        self.extract_tasks = []
        self.logger = logger

        try:
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
                    self.logger.debug(f"Extract task added: {extract_task}")
        except Exception as e:
            raise Exception(
                f"Failed to compile extract task list in Extractor constructor:\n{e}"
            )

    def _sql_extract(self, source_config, query_file_path) -> PipelineData:
        """Returns pd.DataFrame for PipelineData.content type"""
        try:
            print("Some stuff")
        except Exception as e:
            raise Exception(f"Extract method '_sql_extract' failed:\n{e}")

    def _fileshare_extract(self, source_config, rel_file_path) -> PipelineData:
        """Returns file io.BytesIO for PipelineData.content type"""
        try:
            print("Some stuff")
        except Exception as e:
            raise Exception(f"Extract method '_fileshare_extract' failed:\n{e}")

    def _sftp_extract(self, source_config, rel_file_path) -> PipelineData:
        """Returns file io.BytesIO for PipelineData.content type"""
        try:
            print("Some stuff")
        except Exception as e:
            raise Exception(f"Extract method '_sftp_extract' failed:\n{e}")

    def _drive_extract(self, source_config, dependency) -> PipelineData:
        """Returns file io.BytesIO for PipelineData.content type"""
        try:
            print("Some stuff")
        except Exception as e:
            raise Exception(f"Extract method '_drive_extract' failed:\n{e}")

    def extract(self) -> dict[str, PipelineData]:
        extracted_data = {}

        for extract_task in self.extract_tasks:
            try:
                method = extract_task["method"]
                data = method(extract_task["source_config"], extract_task["dependency"])
                extracted_data[extract_task["dependency"]] = data
                self.logger.debug(
                    f"Extracted data: {extracted_data[extract_task["dependency"]]}"
                )
            except Exception as e:
                raise Exception(
                    f"Data extraction from '{extract_task["source_name"]}' using '{extract_task["dependency"]}' failed:\n{e}"
                )
        return extracted_data
