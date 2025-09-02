import re
import io
import shutil
import jaydebeapi
import pandas as pd
import pysftp
from app.utils.models import ExtractStep, StreamData
from app.utils.macros import macro_registry


class Extractor:

    @classmethod
    def extract(
        cls, extract_step_config: ExtractStep, step_outputs: dict[str, StreamData]
    ) -> StreamData:
        protocol = extract_step_config.protocol
        extract_method = cls.protocol_to_method[protocol]
        extracted_data = extract_method(extract_step_config, step_outputs)
        return extracted_data

    @classmethod
    def _fileshare_extract(cls, extract_step_config, step_outputs=None) -> StreamData:
        """Returns file io.BytesIO for StreamData.content type"""
        remote_file = extract_step_config.remote_file_path
        file_buffer = io.BytesIO()
        shutil.copyfileobj(remote_file, file_buffer)
        file_buffer.seek(0)
        return StreamData(data_format="file_buffer", content=file_buffer)

    @classmethod
    def _sftp_extract(cls, extract_step_config, step_outputs=None) -> StreamData:
        """Returns file io.BytesIO for StreamData.content type"""
        sftp_config = {
            key: value
            for key, value in extract_step_config.source_config.items()
            if key in ("user", "password", "host", "port")
        }
        mount_path = extract_step_config.source_config.mount_path
        remote_file_path = extract_step_config.remote_file
        file_buffer = io.BytesIO()
        with pysftp.Connection(**sftp_config) as sftp:
            with sftp.cd(mount_path):
                sftp.getfo(remote_file_path, file_buffer)
        file_buffer.seek(0)
        return StreamData(data_format="file_buffer", content=file_buffer)

    @classmethod
    def _drive_extract(cls, extract_step_config, step_outputs=None) -> StreamData:
        """Returns file io.BytesIO for StreamData.content type"""
        pass

    @classmethod
    def _sql_extract(cls, extract_step_config, step_outputs=None) -> StreamData:
        """Returns pd.DataFrame for StreamData.content type"""
        raw_query_params = extract_step_config.query_params
        resolved_query_params = cls._resolve_query_params(
            raw_query_params, step_outputs
        )

        raw_query_string = extract_step_config.query_file.read_text()
        resolved_query_string = cls._hydrate_query_with_params(
            raw_query_string, resolved_query_params
        )

        source_config = extract_step_config.source_config
        data_frame = cls._query_database(source_config, resolved_query_string)

        return StreamData(data_format="dataframe", content=data_frame)

    @classmethod
    def _query_database(cls, source_config: dict, query_string: str) -> pd.DataFrame:
        with jaydebeapi.connect(
            jclassname=source_config.driver_name,
            url=source_config.conn_string,
            driver_args=[source_config.user, source_config.password],
        ) as conn:
            dtypes = cls._get_dtypes_from_query_map(query_string, conn)
            with conn.cursor() as curs:
                curs.execute(query_string)
                data = curs.fetchall()

                if isinstance(data, tuple):
                    data = [data]

                columns = [item[0] for item in curs.description]
                df = pd.DataFrame(data=data, columns=columns)
                df = df.astype(dtype=dtypes)
        return df

    @classmethod
    def _resolve_query_params(
        cls, raw_query_params, step_outputs
    ) -> dict[str, str | StreamData]:
        resolved_query_params = {}
        for key, value in raw_query_params.items():
            if value.startswith("step:"):
                output_name = value.replace("step:", "")
                resolved_query_params.update({key: step_outputs[output_name].content})
            if value.startswith("macro:"):
                macro_name = value.replace("macro:", "")
                resolved_query_params.update({key: macro_registry[macro_name]()})
            else:
                resolved_query_params.update({key: value})
        return resolved_query_params

    @classmethod
    def _hydrate_query_with_params(
        cls, raw_query_string: str, resolved_query_params: dict[str, str | StreamData]
    ) -> str:
        resolved_query_string = raw_query_string
        for key, value in resolved_query_params.items():
            if isinstance(value, StreamData):
                if value.data_format == "python_string":
                    resolved_query_string.replace(f"::{key}::", f"'{value}'")
                if value.data_format == "python_int":
                    resolved_query_string.replace(f"::{key}::", f"{value}")
                if value.data_format == "python_list":
                    combined_string = cls._get_combined_param_string(value)
                    resolved_query_string.replace(f"::{key}::", combined_string)
            else:
                resolved_query_string.replace(f"::{key}::", value)
        return resolved_query_string

    @classmethod
    def _get_combined_param_string(param_list: list):
        single_quotes = [f"'{value}'" for value in param_list]
        return ", ".join(single_quotes)

    @classmethod
    def _get_dtypes_from_query_map(cls, query_string: str, connection) -> dict:
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
            pandas_type = cls.openedge_to_pandas_dtype.get(db_type, "object")
            final_dtype_map[final_alias] = pandas_type

        return final_dtype_map


Extractor.protocol_to_method = {
    "sql": Extractor._sql_extract,
    "fileshare": Extractor._fileshare_extract,
    "google_drive": Extractor._drive_extract,
}

Extractor.openedge_to_pandas_dtype = {
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
