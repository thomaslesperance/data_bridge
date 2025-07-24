from logger import logger
from models import StreamData
import jaydebeapi
import pandas as pd
import re


class Extractor:

    def __init__(self, sources, extract_tasks) -> None:
        self.sources = sources
        self.extract_tasks = extract_tasks
        self.source_type_to_method = {
            "sql": self._sql_extract,
            "fileshare": self._fileshare_extract,
            "google_drive": self._drive_extract,
            "sftp": self._sftp_extract,
        }
        self.openedge_to_pandas_dtype = {
            # Character Types
            "character": "category",
            "varchar": "string",
            "lvarchar": "string",
            # Numeric Types (Exact)
            "bit": "boolean",
            "tinyint": "Int8",
            "smallint": "Int16",
            "integer": "Int32",
            "bigint": "Int64",
            "numeric": "object",
            # Numeric Types (Approximate)
            "real": "float32",
            "float": "float64",
            # Datetime Types
            "date": "datetime64[ns]",
            "time": "timedelta64[ns]",
            "timestamp": "datetime64[ns]",
            "timestamp_timezone": "'datetime64[ns, America/Chicago]'",
            # Binary Types
            "varbinary": "object",
            "varbina": "object",
            "lvarbinary": "object",
        }
        self.processed_extract_tasks = []
        self._setup_extractor()

    def _setup_extractor(self) -> None:
        for _, task_config in self.extract_tasks.items():
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
                self.processed_extract_tasks.append(extract_task)
                logger.debug(f"Extract task added: {extract_task}")

    def __call__(self) -> dict[str, StreamData]:
        extracted_data = {}
        for extract_task in self.processed_extract_tasks:
            method = extract_task["method"]
            data = method(extract_task["source_config"], extract_task["dependency"])
            extracted_data[extract_task["dependency"]] = data
            logger.debug(
                f"Extracted data: {extracted_data[extract_task["dependency"]]}"
            )
        return extracted_data

    def _sql_extract(self, source_config, query_file_path) -> StreamData:
        """Returns pd.DataFrame for StreamData.content type"""
        with jaydebeapi.connect(
            jclassname=source_config.driver_name,
            url=source_config.conn_string,
            driver_args=[source_config.user, source_config.password],
        ) as conn:
            dtypes = self._get_dtypes_from_query_map(query_file_path, conn)
            with conn.cursor() as curs:
                curs.execute(query_file_path)
                data = curs.fetchall()

                if isinstance(data, tuple):
                    data = [data]

                columns = [item[0] for item in curs.description]
                df = pd.DataFrame(data=data, columns=columns)
                df = df.astype(dtype=dtypes)
                return df

    def _fileshare_extract(self, source_config, rel_file_path) -> StreamData:
        """Returns file io.BytesIO for StreamData.content type"""
        print("Some stuff")

    def _sftp_extract(self, source_config, rel_file_path) -> StreamData:
        """Returns file io.BytesIO for StreamData.content type"""
        print("Some stuff")

    def _drive_extract(self, source_config, dependency) -> StreamData:
        """Returns file io.BytesIO for StreamData.content type"""
        print("Some stuff")

    def _get_dtypes_from_query_map(self, query_string: str, connection) -> dict:
        """
        Parses a special, well-formatted comment block in a SQL query to
        determine data types. Relies on a strict line format convention.

            --[ TABLE-COLUMN-ALIAS MAP ]
            --TABLE,COLUMN[,ALIAS]
            --...
            --[ END MAP ]
        """
        map_pattern = re.compile(
            r"--\[\s+TABLE-COLUMN-ALIAS\s+MAP\s+\]\s*--TABLE,COLUMN\[,ALIAS\]\s*(.*?)--\[\s*END\s+MAP\s*\]",
            re.DOTALL | re.IGNORECASE,
        )
        match = map_pattern.search(query_string)
        map_content = match.group(1).strip()

        table_column_pairs = set()
        lookup_map = {}

        for line in map_content.split("\n"):
            clean_line = line.strip()
            parts = clean_line[2:].strip().split(",")
            source_table = parts[0].strip()
            source_column = parts[1].strip()
            final_alias = parts[2].strip() if len(parts) == 3 else source_column

            table_column_pairs.add((source_table, source_column))
            lookup_map[final_alias] = (source_table, source_column)

        where_clauses = ['("TBL" = ? AND "COL" = ?)'] * len(table_column_pairs)
        dtype_query = f'SELECT "TBL", "COL", "COLTYPE" FROM SYSPROGRESS."SYSCOLUMNS" WHERE {" OR ".join(where_clauses)}'
        query_params = tuple([item for pair in table_column_pairs for item in pair])

        db_type_results = {}
        try:
            with connection.cursor() as curs:
                curs.execute(dtype_query, query_params)
                for tbl, col, db_type in curs.fetchall():
                    db_type_results[(tbl, col)] = db_type
        except Exception as e:
            print(f"An error occurred while fetching data types: {e}")
            return {}

        final_dtype_map = {}
        for final_alias, source_pair in lookup_map.items():
            db_type = db_type_results.get(source_pair, "varchar")
            pandas_type = self.openedge_to_pandas_dtype.get(db_type, "object")
            final_dtype_map[final_alias] = pandas_type

        return final_dtype_map
