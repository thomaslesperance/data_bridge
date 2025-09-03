from io import BytesIO
from uuid import uuid4
from datetime import datetime
from email.message import Message
from typing import Annotated, Union, Literal, Callable
from pydantic import (
    BaseModel,
    Secret,
    EmailStr,
    Field,
    model_validator,
    AfterValidator,
    BeforeValidator,
)
from pydantic.config import ConfigDict
from pandas import DataFrame


# ------------------------------------------------------------------------
# ------------------- CONFIGURATION STATE MODELS -------------------------
# ------------------------------------------------------------------------


# ------------------- DATA SOURCE MODELS ---------------------------------
class SourceSql(BaseModel):
    name: str
    protocol: Literal["sql"]
    user: Secret
    password: Secret
    conn_string: Secret
    driver_name: str


class SourceSmb(BaseModel):
    name: str
    protocol: Literal["smb"]
    mount_path: str


class SourceGoogleDrive(BaseModel):
    name: str
    protocol: Literal["google_drive"]
    access_token: str


class SourceSftp(BaseModel):
    name: str
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
    name: str
    protocol: Literal["smtp"]
    host: str
    user: Secret | None = None
    password: Secret | None = None
    port: str = "25"
    default_sender_email: EmailStr


class DestSmb(BaseModel):
    name: str
    protocol: Literal["smb"]
    mount_path: str


class DestSftp(BaseModel):
    name: str
    protocol: Literal["sftp"]
    host: str
    user: Secret
    password: Secret
    port: str = "22"


class DestGoogleDrive(BaseModel):
    name: str
    protocol: Literal["google_drive"]
    access_token: str


Destination = Annotated[
    Union[DestSmtp, DestSmb, DestSftp, DestGoogleDrive],
    Field(discriminator="protocol"),
]


# ------------------- DATA STREAM MODELS ---------------------------------
class Stream(BaseModel):
    log_file: str
    log_level: int = 20
    steps: list["Step"]


class BaseStep(BaseModel):
    step_name: str
    step_type: str


class TransformStep(BaseStep):
    step_type: Literal["transform"]
    function: "TransformFunc" | "EmailBuilder"
    input: str | list[str] | None = None
    output: str | list[str] | None = None


Step = Annotated[
    Union["ExtractStep", "TransformStep", "LoadStep"], Field(discriminator="step_type")
]


ExtractStep = Annotated[
    Union[
        "SqlExtractStep", "SftpExtractStep", "SmbExtractStep", "GoogleDriveExtractStep"
    ],
    Field(discriminator="protocol"),
]


LoadStep = Annotated[
    Union["SqlExtractStep", "SftpLoadStep", "SmbLoadStep", "GoogleDriveLoadStep"],
    Field(discriminator="protocol"),
]


## ------------------- EXTRACT STEP MODELS -------------------------------
class BaseExtractStep(BaseStep):
    step_type: Literal["extract"]
    protocol: str
    source_config: Source
    output: str


class SqlExtractStep(BaseExtractStep):
    protocol: Literal["sql"]
    query_file: "RelSqlFilePath"
    query_params: str | dict[str, str] | None = None


class SftpExtractStep(BaseExtractStep):
    protocol: Literal["sftp"]
    remote_file: "RelFilePath"


class SmbExtractStep(BaseExtractStep):
    protocol: Literal["smb"]
    remote_file: "RelFilePath"


class GoogleDriveExtractStep(BaseExtractStep):
    protocol: Literal["google_drive"]
    remote_file: "RelFilePath"


## ------------------- LOAD STEP MODELS ----------------------------------
class BaseLoadStep(BaseStep):
    step_type: Literal["load"]
    protocol: str
    dest_config: Destination
    input: str


class SmtpLoadStep(BaseLoadStep):
    protocol: Literal["smtp"]
    recipients: str | list[str]


class SftpLoadStep(BaseLoadStep):
    protocol: Literal["sftp"]
    remote_dir: "RelDirPath"


class SmbLoadStep(BaseLoadStep):
    protocol: Literal["smb"]
    remote_dir: "RelDirPath"


class GoogleDriveLoadStep(BaseLoadStep):
    protocol: Literal["google_drive"]
    remote_dir: "RelDirPath"


## ------------------- STEP UTILS ----------------------------------------
def no_starting_slash(v: str) -> str:
    """Raises a ValueError if input string starts with a forward slash."""
    if v.startswith("/"):
        raise ValueError(f"Path must not start with '/'; got '{v}'")
    return v


def no_ending_slash(v: str) -> str:
    """Raises a ValueError if input string ends with a forward slash."""
    if v.endswith("/"):
        raise ValueError(f"Path must not end with '/'; got '{v}'")
    return v


def has_sql_extension(v: any) -> any:
    """Raises ValueError if input string doesn't end with .sql."""
    if not v.endswith(".sql"):
        raise ValueError(f"Path must end with '.sql'; got '{v}'")
    return v


RelDirPath = Annotated[str, AfterValidator(no_starting_slash)]
RelFilePath = Annotated[RelDirPath, AfterValidator(no_ending_slash)]
RelSqlFilePath = Annotated[RelFilePath, BeforeValidator(has_sql_extension)]


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
        file_path: Large files moved from one place to another need not be loaded into memory and can be streamed piecemeal;
        python_<type>: For data transmitted between transform steps to eliminate redundant conversions;
    """

    data_format: Literal[
        "dataframe",
        "file_buffer",
        "file_path",
        "email_message",
        "python_string",
        "python_int",
        "python_list",
        "python_dict",
    ]
    content: Union[pd.DataFrame, BytesIO, str, Message, str, int, list, dict]
    file_name: str = "no_file_name"
    metadata: dict = {}

    @model_validator(mode="after")
    def check_content_type_matches_format(self) -> "StreamData":
        """Ensures the type of 'content' matches the 'data_format' string."""

        # Check for dataframe
        if self.data_format == "dataframe" and not isinstance(self.content, DataFrame):
            raise ValueError("For 'dataframe', content must be of type 'pd.DataFrame'")

        # Check for in-memory stream
        if self.data_format == "file_buffer" and not isinstance(self.content, BytesIO):
            raise ValueError("For 'file_buffer', content must be of type 'io.BytesIO'")

        # Check for email message
        if self.data_format == "emai_message" and not isinstance(self.content, Message):
            raise ValueError(
                "For 'emai_message', content must be of type 'email.message.Message'"
            )

        # Check for file
        if self.data_format == "file_path" and not isinstance(self.content, str):
            raise ValueError("For 'file_path', content must be a path of type 'str'")

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
    run_id: str = Field(default_factory=lambda: f"run_{uuid4().hex}")
    stream_name: str
    status: Literal["running", "success", "failed"] = "running"
    start_time: datetime = Field(default_factory=datetime.now)
    end_time: datetime | None = None

    # --- Operational State ---
    step_outputs: dict[str, StreamData] = {}
    dest_responses: list[DestinationResponse] = []
