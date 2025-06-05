from typing import List, Union, Literal, Any
from pydantic import (
    BaseModel,
    FilePath,
    DirectoryPath,
    SecretStr,
    EmailStr,
    field_validator,
    model_validator,
    ValidationInfo,
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
class SourceSkyward(BaseModel):
    """Base model for data source configurations."""

    source_name: str  # Added dynamically from config.ini section name
    user: str
    password: SecretStr
    conn_string: SecretStr
    driver_name: str
    driver_file: str

    # Allows passing context to disable this check on driver_file field; for use in config file
    # validation outside pipeline context
    @model_validator(mode="after")
    def check_driver_file_exists(self, info: ValidationInfo) -> "SourceSkyward":
        context = info.context or {}
        skip_check = context.get("skip_path_existence_check", False)

        if not skip_check:
            config_dir = context.get("config_dir", Path("."))
            resolved_path = (config_dir / self.driver_file).resolve()

            if not resolved_path.is_file():
                raise ValueError(
                    f"driver_file path does not point to a file: '{resolved_path}' (checking enabled)"
                )

        return self


# ------------------- SHARED DESTINATION MODELS -------------------
class BaseSharedDest(BaseModel):
    """Base model for shared destination configurations."""

    shared_dest_name: str  # Added dynamically from config.ini section name
    protocol: str  # Strings must match keys in load_functions map in load.py


class SharedDestSmtp(BaseSharedDest):
    """Configuration specific to shared SMTP destinations."""

    shared_dest_name: Literal["internal_smtp"]
    protocol: Literal[
        "smtp"
    ]  # Strings must match keys in load_functions map in load.py
    host: str
    port: int = 25


class SharedDestFileshare(BaseSharedDest):
    """Configuration specific to shared fileshare destinations."""

    shared_dest_name: Literal["skyward_exports"]
    protocol: Literal[
        "fileshare"
    ]  # Strings must match keys in load_functions map in load.py
    mount_path: str

    @model_validator(mode="after")
    def check_mount_path_exists(self, info: ValidationInfo) -> "SharedDestFileshare":
        context = info.context or {}
        skip_check = context.get("skip_path_existence_check", False)

        if not skip_check:
            path_to_check = Path(self.mount_path)
            if not path_to_check.is_dir():
                raise ValueError(
                    f"mount_path does not point to a directory: '{self.mount_path}' (checking enabled)"
                )
        return self


# ------------------- JOB SECTION MODELS --------------------------
class BaseJob(BaseModel):
    """Common fields parsed directly from a job's <job_name> INI section."""

    job_name: str  # Added dynamically from config.ini section name
    source: str
    is_shared_destination: bool
    base_filename: str

    # Since subclasses of this class specify a Literal choice of this field, manual coercion
    # needs to happen first before the raw string is matched to the subclass or errors occur
    @field_validator("is_shared_destination", mode="before")
    @classmethod
    def coerce_string_to_bool(cls, v: Any) -> bool:
        """Coerce 'true'/'false' strings to boolean before standard validation."""
        if isinstance(v, str):
            v_lower = v.strip().lower()
            if v_lower in ("true", "yes", "1", "on"):
                return True
            elif v_lower in ("false", "no", "0", "off"):
                return False
            else:
                raise ValueError(f"Invalid boolean string: '{v}'")
        return v


# --- Jobs loading to unique destinations ---
class BaseJobUniqueDest(BaseJob):
    """Common INI section details for unique SFTP destinations."""

    is_shared_destination: Literal[False]
    protocol: str  # Strings must match keys in load_functions map in load.py


class JobUniqueSftp(BaseJobUniqueDest):
    """<job_name> INI section details for unique SFTP destinations."""

    protocol: Literal[
        "sftp"
    ]  # Strings must match keys in load_functions map in load.py
    host: str
    user: str
    password: SecretStr
    port: int = 22
    remote_path: str


# --- Jobs loading to shared destinations ---
class BaseJobSharedDest(BaseJob):
    """Comon fields for jobs loading to shared destinations."""

    is_shared_destination: Literal[True]
    shared_destination: str


class JobSharedSmtp(BaseJobSharedDest):
    """<job_name> INI section details for jobs using shared SMTP destinations."""

    shared_destination: Literal["internal_smtp"]
    recipients: List[EmailStr]
    sender_email: EmailStr

    @field_validator("recipients", mode="before")
    @classmethod
    def parse_recipients(cls, v):
        return parse_comma_separated_str(v)


class JobSharedFileshare(BaseJobSharedDest):
    """<job_name> INI section details for jobs using shared Fileshare destinations."""

    shared_destination: Literal["skyward_exports"]
    path: str


# ------------------- FINAL VALIDATED CONFIG MODELS ---------------
class ValidatedConfigUnique(BaseModel):
    """Fully validated configuration for a job with a unique destination."""

    source: "SourceUnion"
    shared_dest: None
    job: "JobUniqueUnion"


class ValidatedConfigSmtp(BaseModel):
    """Fully validated configuration for a job using a shared SMTP destination."""

    source: "SourceUnion"
    shared_dest: SharedDestSmtp
    job: JobSharedSmtp


class ValidatedConfigFileshare(BaseModel):
    """Fully validated configuration for a job using a shared Fileshare destination."""

    source: "SourceUnion"
    shared_dest: SharedDestFileshare
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


# job_config: nested dict validated with pydantic models above; has the following shape:
# job_config = {
#     "source": {
#         "source_name",
#         "user",
#         "password",
#         "conn_string",
#         "driver_name",
#         "driver_file"
#     },
#     "shared_dest": None || {
#         "shared_dest_name",
#         "protocol",
#         "host",
#         "port",
#         "mount_path"
#     },
#     "job": {
#         # Common to all jobs
#         "job_name",
#         "source",
#         "is_shared_destination",
#         "base_filename",
#         # Common to all shared dest jobs
#         "shared_destination",
#         ## shared: internal_smtp jobs
#         "recipients",
#         "sender_email",
#         ## shared: skyward_exports jobs
#         "path",
#         # Unique destination SFTP jobs
#         "protocol",
#         "host",
#         "user",
#         "password",
#         "port",
#         "remote_path",
#     }
# }
