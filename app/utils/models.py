import io
from pathlib import Path
from datetime import datetime
from email.message import Message
from typing import Annotated, Union, Literal, Callable
from pydantic import (
    BaseModel,
    Secret,
    EmailStr,
    Field,
    model_validator,
    field_validator,
)
import pandas as pd


# ------------------------------------------------------------------------
# ------------------- CONFIGURATION STATE MODELS -------------------------
# ------------------------------------------------------------------------


# ------------------- DATA SOURCE MODELS ---------------------------------
class SourceSql(BaseModel):
    protocol: Literal["sql"]
    user: Secret
    password: Secret
    conn_string: Secret
    driver_name: str


class SourceSmb(BaseModel):
    protocol: Literal["smb"]
    mount_path: str

    @model_validator(mode="after")
    def path_exists(self):
        path_to_check = Path(self.mount_path)
        if not path_to_check.is_dir():
            raise ValueError(
                f"mount_path for source fileshare does not point to a directory: {self.mount_path}"
            )
        return self


class SourceGoogleDrive(BaseModel):
    protocol: Literal["google_drive"]
    access_token: Path

    @model_validator(mode="after")
    def token_exists(self):
        if not self.access_token.is_file():
            raise ValueError(
                f"access_token for Google Drive source does not point to a file: {self.access_token}"
            )
        return self


class SourceSftp(BaseModel):
    protocol: Literal["sftp"]
    user: Secret
    password: Secret
    host: str
    port: str = "22"


Source = Annotated[
    Union[SourceSql, SourceSmb, SourceGoogleDrive, SourceSftp],
    Field(discriminator="protocol"),
]


# ------------------- DATA DESTINATION MODELS ----------------------------
class DestSmtp(BaseModel):
    protocol: Literal["smtp"]
    host: str
    port: str = 25
    default_sender_email: EmailStr


class DestSmb(BaseModel):
    protocol: Literal["smb"]
    mount_path: str

    @model_validator(mode="after")
    def path_exists(self):
        path_to_check = Path(self.mount_path)
        if not path_to_check.is_dir():
            raise ValueError(
                f"mount_path for destination fileshare does not point to a directory: {self.mount_path}"
            )
        return self


class DestSftp(BaseModel):
    protocol: Literal["sftp"]
    host: str
    user: Secret
    password: Secret
    port: str = "22"


class DestGoogleDrive(BaseModel):
    protocol: Literal["google_drive"]
    access_token: Path

    @field_validator("access_token", mode="after")
    @classmethod
    def token_must_be_file(cls, v: Path):
        if not v.is_file():
            raise ValueError(
                f"access_token for Google Drive destination does not point to a file: {v}"
            )
        return v


Destination = Annotated[
    Union[DestSmtp, DestSmb, DestSftp, DestGoogleDrive],
    Field(discriminator="protocol"),
]


# ------------------- DATA STREAM MODELS ---------------------------------
class Stream(BaseModel):
    log_level: int
    steps: list["Step"]


Step = Annotated[
    Union["ExtractStep", "TransformStep", "LoadStep"],
    Field(discriminator="step_type"),
]

ExtractStep = Annotated(
    ["SqlExtractStep", "SftpExtractStep", "SmbExtractStep", "GoogleDriveExtractStep"],
    Field(discriminator="protocol"),
)


LoadStep = Annotated(
    ["SqlExtractStep", "SftpLoadStep", "SmbLoadStep", "GoogleDriveLoadStep"],
    Field(discriminator="protocol"),
)


class BaseStep(BaseModel):
    step_name: str
    step_type: str


class TransformStep(BaseStep):
    step_type: Literal["transform"]
    function: "TransformFunc"
    input: str | list[str]
    output: str | list[str]


## ------------------- EXTRACT STEP MODELS -------------------------------
class BaseExtractStep(BaseStep):
    step_type: Literal["extract"]
    protocol: str
    source_config: Source
    output: str | list[str]


class SqlExtractStep(BaseExtractStep):
    protocol: Literal["sql"]
    query_file_path: Path
    path_params: dict[str, str]
    query_params: dict[str, str]


class SftpExtractStep(BaseExtractStep):
    protocol: Literal["sftp"]
    remote_file_path: str
    path_params: dict[str, str]


class SmbExtractStep(BaseExtractStep):
    protocol: Literal["smb"]
    remote_file_path: str
    path_params: dict[str, str]


class GoogleDriveExtractStep(BaseExtractStep):
    protocol: Literal["google_drive"]
    remote_file_path: str
    path_params: dict[str, str]


## ------------------- LOAD STEP MODELS ----------------------------------
class BaseLoadStep(BaseStep):
    step_type: Literal["load"]
    protocol: str
    dest_config: Destination
    input: str | list[str]


class SmtpLoadStep(BaseLoadStep):
    protocol: Literal["smtp"]
    email_builder: "EmailBuilder"
    email_params: dict[str, str]


class SftpLoadStep(BaseLoadStep):
    protocol: Literal["sftp"]
    remote_file_path: str
    path_params: dict[str, str]


class SmbLoadStep(BaseLoadStep):
    protocol: Literal["smb"]
    remote_file_path: str
    path_params: dict[str, str]


class GoogleDriveLoadStep(BaseLoadStep):
    protocol: Literal["google_drive"]
    remote_file_path: str
    path_params: dict[str, str]


# ------------------------------------------------------------------------
# ------------------- STREAM-SPECIFIC LOGIC MODELS -----------------------
# ------------------------------------------------------------------------
class TransformFunc(BaseModel):
    function: Callable[[dict[str, "StreamData"]], dict[str, "StreamData"]]

    def __call__(
        self, extracted_data: dict[str, "StreamData"]
    ) -> dict[str, "StreamData"]:
        return self.function(extracted_data)


class EmailBuilder(BaseModel):
    function: Callable[[dict[str, "StreamData"]], Message]

    def __call__(self, email_data: dict[str, "StreamData"]) -> Message:
        return self.function(email_data)


# # ------------------------------------------------------------------------
# # ------------------- COMBINED CONFIG MODELS -----------------------------
# # ------------------------------------------------------------------------
# class ValidatedConfig(BaseModel):
#     stream: Stream
#     sources: dict[str, Source]
#     destinations: dict[str, Destination]
#     transformer: TransformFunc
#     email_builders: dict[str, EmailBuilder]

#     @classmethod
#     def from_raw_config(
#         cls,
#         stream_name: str,
#         raw_config: dict,
#         raw_transform_fn: Callable,
#         raw_email_builders: dict[str, Callable],
#     ) -> "ValidatedConfig":
#         """
#         A factory method to build and validate the complete configuration
#         from raw inputs.
#         """
#         validation_issues = []

#         avail_sources = raw_config.get("sources", {})
#         avail_dests = raw_config.get("destinations", {})
#         stream_dict = raw_config.get("streams", {}).get(stream_name)

#         if not stream_dict:
#             raise ValueError(f"Stream '{stream_name}' not found in configuration.")

#         validated_stream = Stream(**stream_dict)

#         used_source_names = {t.source for t in validated_stream.extract_tasks.values()}
#         used_dest_names = {t.destination for t in validated_stream.load_tasks.values()}

#         if not used_source_names.issubset(avail_sources.keys()):
#             validation_issues.append(
#                 f"Stream references undefined sources: {used_source_names - avail_sources.keys()}"
#             )
#         if not used_dest_names.issubset(avail_dests.keys()):
#             validation_issues.append(
#                 f"Stream references undefined destinations: {used_dest_names - avail_dests.keys()}"
#             )

#         validated_sources = {
#             name: Source(**avail_sources[name]) for name in used_source_names
#         }
#         validated_dests = {
#             name: Destination(**avail_dests[name]) for name in used_dest_names
#         }

#         for (
#             extract_task_name,
#             extract_task_config,
#         ) in validated_stream.extract_tasks.items():
#             source_model = validated_sources[extract_task_config.source]

#             # SQL sources require .sql file paths
#             if source_model.type == "sql":
#                 for dep in extract_task_config.dependencies:
#                     if not dep.endswith(".sql"):
#                         validation_issues.append(
#                             f"Extract task '{extract_task_name}' requires .sql files, but got '{dep}'"
#                         )

#             # Smb and SFTP sources require relative file paths
#             elif source_model.type in ["smb", "sftp"]:
#                 for dep in extract_task_config.dependencies:
#                     if dep.startswith("/") or dep.endswith("/"):
#                         validation_issues.append(
#                             f"Extract task '{extract_task_name}' requires a relative file path, but got '{dep}'"
#                         )

#             # Google Drive sources require a non-empty string (name or ID)
#             elif source_model.type == "google_drive":
#                 for dep in extract_task_config.dependencies:
#                     if not dep:
#                         validation_issues.append(
#                             f"Extract task '{extract_task_name}' requires a non-empty file name or ID."
#                         )

#         validated_transformer = TransformFunc(function=raw_transform_fn)
#         validated_email_builders = {
#             name: EmailBuilder(function=builder) for name, builder in raw_email_builders
#         }

#         if validation_issues:
#             error_report = "\n - ".join(validation_issues)
#             raise ValueError(f"Configuration has errors:\n\t - {error_report}")

#         return cls(
#             stream=validated_stream,
#             sources=validated_sources,
#             destinations=validated_dests,
#             transformer=validated_transformer,
#             email_builders=validated_email_builders,
#         )


# ------------------------------------------------------------------------
# ------------------- OPERATIONAL STATE MODELS ---------------------------
# ------------------------------------------------------------------------
class StreamData(BaseModel):
    """
    A standardized container for data flowing through the data stream.

    Example uses of each data_format (and implied data type of content):
        dataframe: SQL extractions will be converted into a pandas DataFrames by default;
        in_memory_stream: Small files read from a Smb or SFTP server will be passed as byte buffers until processed;
        file: Large files moved from one place to another need not be loaded into memory and can be streamed piecemeal;
    """

    data_format: Literal["dataframe", "file", "in_memory_stream"]
    content: Union[pd.DataFrame, Path, io.BytesIO]
    metadata: dict = {}

    @model_validator(mode="after")
    def check_content_type_matches_format(self) -> "StreamData":
        """Ensures the type of 'content' matches the 'data_format' string."""

        # Check for in-memory stream
        if self.data_format == "in_memory_stream" and not isinstance(
            self.content, io.BytesIO
        ):
            raise ValueError("For 'in_memory_stream', content must be io.BytesIO")

        # Check for dataframe
        if self.data_format == "dataframe" and not isinstance(
            self.content, pd.DataFrame
        ):
            raise ValueError("For 'dataframe', content must be a pandas DataFrame")

        # Check for file
        if self.data_format == "file" and not isinstance(self.content, Path):
            raise ValueError("For 'file', content must be a Path object")

        return self


class DestinationResponse(BaseModel):
    """A standardized model for reporting the outcome of a load operation."""

    destination_name: str
    status: Literal["success", "failure"]
    message: str
    records_processed: int | None = None
    timestamp: datetime = Field(default_factory=datetime.now)


class DataStore(BaseModel):
    step_outputs: dict[str, StreamData]
    dest_responses: list[DestinationResponse]
    meta_data: dict = {}
