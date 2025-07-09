import io
import pandas as pd
from pathlib import Path
from typing import Annotated, Union, Literal, Dict, List, Callable
from pydantic import BaseModel, Secret, EmailStr, Field, model_validator
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

    @model_validator(mode="after")
    def token_exists(self):
        if not self.access_token.is_file():
            raise ValueError(
                f"access_token for Google Drive destination does not point to a file: {self.access_token}"
            )
        return self


Destination = Annotated[
    Union[DestSmtp, DestFileshare, DestSftp, DestGoogleDrive],
    Field(discriminator="protocol"),
]


# ------------------- DATA INTEGRATION JOB MODEL -------------------------
class Job(BaseModel):
    extract: Dict[str, str | List[str]]
    load: Dict[str, str | List[str]]


# ------------------------------------------------------------------------
# ------------------- OPERATIONAL STATE MODELS ---------------------------
# ------------------------------------------------------------------------
class PipelineData(BaseModel):
    """A standardized container for data flowing through the pipeline."""

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
    """
    The function it wraps must accept a dictionary where keys are extract dependency
    names and values are PipelineData objects ("student_query.sql": PipelineData(...),}).
    It must return a dictionary of the same type ({"final_report.csv": PipelineData(...),}).
    """

    function: Callable[[Dict[str, PipelineData]], Dict[str, PipelineData]]

    def __call__(self, data: Dict[str, PipelineData]) -> Dict[str, PipelineData]:
        """Allows an instance of this class to be called directly like a function."""
        return self.function(data)


class EmailFunc(BaseModel):
    function: Callable[..., Message]

    def __call__(self, *args, **kwargs) -> Message:
        return self.function(*args, **kwargs)
