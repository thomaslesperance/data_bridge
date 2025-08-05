import io
import uuid
import logging
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
    AfterValidator,
    BeforeValidator,
)
from pydantic.config import ConfigDict
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
    log_level: int = logging.INFO
    steps: list["Step"]


def check_is_sql_file(v: any) -> any:
    """Ensures the input string ends with .sql."""
    if not isinstance(v, str) or not v.endswith(".sql"):
        raise ValueError(f"Path must be a string ending in .sql; got '{v}'")
    return v


def validate_is_relative_path(v: str) -> str:
    """Raises a ValueError if the path starts or ends with a slash."""
    if v.startswith("/") or v.endswith("/"):
        raise ValueError(
            f"Path must be relative and not start or end with '/', but got '{v}'"
        )
    return v


RemoteRelPathStr = Annotated[str, AfterValidator(validate_is_relative_path)]
RelativeSqlPath = Annotated[
    Path, BeforeValidator(check_is_sql_file), BeforeValidator(validate_is_relative_path)
]

Step = Annotated[
    Union["ExtractStep", "TransformStep", "LoadStep"],
    Field(discriminator="step_type"),
]

ExtractStep = Annotated(
    Union[
        "SqlExtractStep", "SftpExtractStep", "SmbExtractStep", "GoogleDriveExtractStep"
    ],
    Field(discriminator="protocol"),
)


LoadStep = Annotated(
    Union["SqlExtractStep", "SftpLoadStep", "SmbLoadStep", "GoogleDriveLoadStep"],
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
    query_file_path: RelativeSqlPath
    path_params: dict[str, str] | None = None
    query_params: str | dict[str, str] | None = None


class SftpExtractStep(BaseExtractStep):
    protocol: Literal["sftp"]
    remote_file_path: RemoteRelPathStr
    path_params: dict[str, str] | None = None


class SmbExtractStep(BaseExtractStep):
    protocol: Literal["smb"]
    remote_file_path: RemoteRelPathStr
    path_params: dict[str, str] | None = None


class GoogleDriveExtractStep(BaseExtractStep):
    protocol: Literal["google_drive"]
    remote_file_path: RemoteRelPathStr
    path_params: dict[str, str] | None = None


## ------------------- LOAD STEP MODELS ----------------------------------
class BaseLoadStep(BaseStep):
    step_type: Literal["load"]
    protocol: str
    dest_config: Destination
    input: str | list[str]


class EmailParams(BaseModel):
    model_config = ConfigDict(extra="allow")
    recipients: str | list[str]


class SmtpLoadStep(BaseLoadStep):
    protocol: Literal["smtp"]
    email_builder: "EmailBuilder"
    email_params: EmailParams


class SftpLoadStep(BaseLoadStep):
    protocol: Literal["sftp"]
    remote_file_path: RemoteRelPathStr
    path_params: dict[str, str] | None = None


class SmbLoadStep(BaseLoadStep):
    protocol: Literal["smb"]
    remote_file_path: RemoteRelPathStr
    path_params: dict[str, str] | None = None


class GoogleDriveLoadStep(BaseLoadStep):
    protocol: Literal["google_drive"]
    remote_file_path: RemoteRelPathStr
    path_params: dict[str, str] | None = None


# ------------------------------------------------------------------------
# ------------------- STREAM-SPECIFIC LOGIC MODELS -----------------------
# ------------------------------------------------------------------------
class TransformFunc(BaseModel):
    function: Callable[[dict[str, "StreamData"]], dict[str, "StreamData"]]

    def __call__(self, data: dict[str, "StreamData"]) -> dict[str, "StreamData"]:
        return self.function(data)


class EmailBuilder(BaseModel):
    function: Callable[[dict[str, "StreamData"]], Message]

    def __call__(self, email_data: dict[str, "StreamData"]) -> Message:
        return self.function(email_data)


# ------------------------------------------------------------------------
# ------------------- OPERATIONAL STATE MODELS ---------------------------
# ------------------------------------------------------------------------
class StreamData(BaseModel):
    """
    A standardized container for data flowing through the data stream.

    Example uses of each data_format (and implied data type of content):
        dataframe: SQL extractions will be converted into a pandas DataFrames by default;
        file_buffer: Small files read from a Smb or SFTP server will be passed as byte buffers until processed;
        file: Large files moved from one place to another need not be loaded into memory and can be streamed piecemeal;
    """

    data_format: Literal[
        "dataframe",
        "file_path",
        "file_buffer",
        "python_string",
        "python_int",
        "python_list",
        "python_dict",
    ]
    content: Union[pd.DataFrame, Path, io.BytesIO, str, int, list, dict]
    metadata: dict = {}

    @model_validator(mode="after")
    def check_content_type_matches_format(self) -> "StreamData":
        """Ensures the type of 'content' matches the 'data_format' string."""

        # Check for in-memory stream
        if self.data_format == "file_buffer" and not isinstance(
            self.content, io.BytesIO
        ):
            raise ValueError("For 'file_buffer', content must be of type 'io.BytesIO'")

        # Check for dataframe
        if self.data_format == "dataframe" and not isinstance(
            self.content, pd.DataFrame
        ):
            raise ValueError("For 'dataframe', content must be of type 'pd.DataFrame'")

        # Check for file
        if self.data_format == "file_path" and not isinstance(self.content, Path):
            raise ValueError("For 'file_path', content must be of type 'pathlib.Path'")

        # Check for Python str
        if self.data_format == "python_string" and not isinstance(self.content, str):
            raise ValueError("For 'python_string', content must be of type 'str'")

        # Check for Python int
        if self.data_format == "python_int" and not isinstance(self.content, int):
            raise ValueError("For 'python_int', content must be of type 'int'")

        # Check for Python list
        if self.data_format == "python_list" and not isinstance(self.content, list):
            raise ValueError("For 'python_list', content must be of type 'list'")

        # Check for Python dict
        if self.data_format == "python_dict" and not isinstance(self.content, dict):
            raise ValueError("For 'python_dict', content must be of type 'dict'")

        return self


class DestinationResponse(BaseModel):
    """A standardized model for reporting the outcome of a load operation."""

    destination_name: str
    status: Literal["success", "failure"]
    message: str
    records_processed: int | None = None
    timestamp: datetime = Field(default_factory=datetime.now)


class DataStore(BaseModel):
    # Validate assignments to instance during run
    model_config = ConfigDict(validate_assignment=True)

    # --- Run Metadata ---
    run_id: str = Field(default_factory=lambda: f"run_{uuid.uuid4().hex}")
    stream_name: str
    status: Literal["running", "success", "failed"] = "running"
    start_time: datetime = Field(default_factory=datetime.now)
    end_time: datetime | None = None

    # --- Operational State ---
    step_outputs: dict[str, StreamData] = {}
    dest_responses: list[DestinationResponse] = []
