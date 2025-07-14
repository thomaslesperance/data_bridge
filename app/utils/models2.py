import io
import pandas as pd
from pathlib import Path
from typing import Annotated, Union, Literal, Any, Callable
from pydantic import (
    BaseModel,
    Secret,
    EmailStr,
    Field,
    model_validator,
    field_validator,
)
from datetime import datetime
from email.message import Message


# ------------------------------------------------------------------------
# ------------------- CONFIGURATION STATE MODELS -------------------------
# ------------------------------------------------------------------------


# ------------------- DATA SOURCE MODELS ---------------------------------
class SourceSql(BaseModel):
    type: Literal["sql"]
    user: Secret
    password: Secret
    conn_string: Secret
    driver_name: str


class SourceFileshare(BaseModel):
    type: Literal["fileshare"]
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
    type: Literal["google_drive"]
    access_token: Path

    @model_validator(mode="after")
    def token_exists(self):
        if not self.access_token.is_file():
            raise ValueError(
                f"access_token for Google Drive source does not point to a file: {self.access_token}"
            )
        return self


class SourceSftp(BaseModel):
    type: Literal["sftp"]
    user: Secret
    password: Secret
    host: str
    port: str = "22"


Source = Annotated[
    Union[SourceSql, SourceFileshare, SourceGoogleDrive, SourceSftp],
    Field(discriminator="type"),
]


# ------------------- DATA DESTINATION MODELS ----------------------------
class DestSmtp(BaseModel):
    protocol: Literal["smtp"]
    host: str
    port: str = 25
    default_sender_email: EmailStr


class DestFileshare(BaseModel):
    protocol: Literal["fileshare"]
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
    Union[DestSmtp, DestFileshare, DestSftp, DestGoogleDrive],
    Field(discriminator="protocol"),
]


# ------------------- DATA INTEGRATION JOB MODEL -------------------------
class ExtractTaskConfig(BaseModel):
    source: str
    dependencies: str


class LoadTaskConfig(BaseModel):
    destination: str
    dependencies: str | list[str] | None = None
    email_builder: str | None = None

    @field_validator("dependencies", mode="before")
    @classmethod
    def normalize_dependencies_to_list(cls, v: Any) -> list[str]:
        """
        Allows a single string dependency (e.g., "report.csv") to be passed
        by automatically converting it to a list (e.g., ["report.csv"]).
        """
        if isinstance(v, str):
            return [v]
        return v


class Job(BaseModel):
    extract_tasks: dict[str, ExtractTaskConfig]
    load_tasks: dict[str, LoadTaskConfig]


# ------------------------------------------------------------------------
# ------------------- OPERATIONAL STATE MODELS ---------------------------
# ------------------------------------------------------------------------
class PipelineData(BaseModel):
    """
    A standardized container for data flowing through the pipeline.
    Example uses of each data_format (and implied data type of content):
    dataframe: SQL extractions will be converted into a pandas DataFrames by default;
    in_memory_stream: Small files read from a fileshare or SFTP server will be passed as byte buffers until processed;
    file: Large files moved from one place to another need not be loaded into memory and can be streamed piecemeal;
    """

    data_format: Literal["dataframe", "file", "in_memory_stream"]
    content: Union[pd.DataFrame, Path, io.BytesIO]
    metadata: dict = {}

    @model_validator(mode="after")
    def check_content_type_matches_format(self) -> "PipelineData":
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


# ------------------------------------------------------------------------
# ------------------- JOB-SPECIFIC LOGIC MODELS --------------------------
# ------------------------------------------------------------------------
class TransformFunc(BaseModel):
    function: Callable[[dict[str, PipelineData]], dict[str, PipelineData]]

    def __call__(
        self, extracted_data: dict[str, PipelineData]
    ) -> dict[str, PipelineData]:
        return self.function(extracted_data)


class EmailBuilder(BaseModel):
    function: Callable[[dict[str, PipelineData]], Message]

    def __call__(self, email_data: dict[str, PipelineData]) -> Message:
        return self.function(email_data)
