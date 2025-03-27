from typing import List, Union, Literal
from pydantic import (
    BaseModel,
    FilePath,
    DirectoryPath,
    SecretStr,
    EmailStr,
    field_validator,
)
from pathlib import Path


# ------------------------------------------------------------------------------------------
# -------------------------------- HELPER FUNCTIONS ----------------------------------------
# ------------------------------------------------------------------------------------------
# For email recipient lists
def parse_comma_separated_str(value: str) -> List[str]:
    """
    Parses a non-empty comma-separated string into a list of non-empty, stripped strings.

    Args:
        value: Comma-separated string.

    Returns:
        A non-empty list of non-empty, stripped strings.

    Raises:
        ValueError: If input isn't a string or results in an empty list.
    """
    if not isinstance(value, str):
        raise ValueError("Input must be a string.")
    parsed = [item.strip() for item in value.split(",") if item.strip()]
    if not parsed:
        raise ValueError("Input must contain at least one non-empty value.")
    return parsed


# ------------------------------------------------------------------------------------------
# -------------------------------- CONFIG MODELS -------------------------------------------
# ------------------------------------------------------------------------------------------


# ------------------- DATA SOURCE MODELS -------------------
class BaseSource(BaseModel):
    """Base model for data source configurations."""

    source_name: str  # Added dynamically from config.ini section name
    type: str
    user: str
    password: SecretStr
    conn_string: SecretStr
    driver_name: str
    driver_file: FilePath


class SourceSkyward(BaseSource):
    """Configuration specific to Skyward data sources."""

    type: Literal["skyward"]


# ------------------- SHARED DESTINATION MODELS -------------------
class BaseSharedDest(BaseModel):
    """Base model for shared destination configurations."""

    destination_name: str  # Added dynamically from config.ini section name
    type: str


class SharedDestSmtp(BaseSharedDest):
    """Configuration specific to shared SMTP destinations."""

    type: Literal["smtp"]
    host: str
    port: int = 25


class SharedDestFileshare(BaseSharedDest):
    """Configuration specific to shared Fileshare destinations."""

    type: Literal["fileshare"]
    mount_path: DirectoryPath


# ------------------- JOB SECTION MODELS --------------------------
class BaseJob(BaseModel):
    """Common fields parsed directly from a job's <job_name> INI section."""

    job_name: str  # Added dynamically from config.ini section name
    source: str
    shared_destination: bool
    destination: str
    base_filename: str


# --- Jobs loading to unique destinations ---
class JobUniqueSftp(BaseJob):
    """<job_name> INI section details for unique SFTP destinations."""

    shared_destination: Literal[False]
    destination: Literal["sftp"]
    host: str
    user: str
    password: SecretStr
    port: int = 22
    remote_path: str


# --- Jobs loading to shared destinations ---
class JobSharedSmtp(BaseJob):
    """<job_name> INI section details for jobs using shared SMTP destinations."""

    shared_destination: Literal[True]
    recipients: List[EmailStr]
    sender_email: EmailStr

    @field_validator("recipients", mode="before")
    @classmethod
    def parse_recipients(cls, v):
        return parse_comma_separated_str(v)


class JobSharedFileshare(BaseJob):
    """<job_name> INI section details for jobs using shared Fileshare destinations."""

    shared_destination: Literal[True]
    path: str


# ------------------- FINAL VALIDATED CONFIG MODELS ---------------
class ValidatedConfigUnique(BaseModel):
    """Fully validated configuration for a job with a unique destination."""

    source: "SourceUnion"
    job: "JobUniqueUnion"


class ValidatedConfigSmtp(BaseModel):
    """Fully validated configuration for a job using a shared SMTP destination."""

    source: "SourceUnion"
    shared_dest_config: SharedDestSmtp
    job: JobSharedSmtp


class ValidatedConfigFileshare(BaseModel):
    """Fully validated configuration for a job using a shared Fileshare destination."""

    source: "SourceUnion"
    shared_dest_config: SharedDestFileshare
    job: JobSharedFileshare


# ------------------- UNION TYPE ALIASES --------------------------

# Source Types
SourceUnion = Union[SourceSkyward]

# Shared Destination Types
SharedDestUnion = Union[SharedDestSmtp, SharedDestFileshare]

# Job Config (Unique Destination) Types
JobUniqueUnion = Union[JobUniqueSftp]

# Job Config (Shared Destination) Types
JobSharedUnion = Union[JobSharedSmtp, JobSharedFileshare]

# Combined, Unvalidated Type
InitialConfigUnion = Union[JobUniqueUnion, JobSharedUnion]

# Combined, Validatec Type (represents collated config object used througout pipeline)
ValidatedConfigUnion = Union[
    ValidatedConfigUnique, ValidatedConfigSmtp, ValidatedConfigFileshare
]

# ------------------------------------------------------------------------------------------
# -------------------------------- PATHS MODELS --------------------------------------------
# ------------------------------------------------------------------------------------------


class InitialPaths(BaseModel):
    """Model for paths dict used by pipeline to locate all needed files and directories."""

    project_root: DirectoryPath
    script_dir: DirectoryPath
    output_dir: DirectoryPath
    config_dir: DirectoryPath
    query_file_path: FilePath
    log_file_path: FilePath
    config_file_path: FilePath

    # Allows using pathlib.Path type hint directly
    model_config = {"arbitrary_types_allowed": True}


class FinalPaths(InitialPaths):
    """Adds the job-config-dependent intermediate path."""

    intermediate_file_path: Path
