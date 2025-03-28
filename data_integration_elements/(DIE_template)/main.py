from pathlib import Path
import sys
import logging

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from utils.die import DIE, setup_and_get_die  # Class imported only for type hints
from utils.transform import (
    export_csv_from_data,
)  # Or any others needed to build custom transform functions as needed
from utils.models import ValidatedConfigUnion
from utils.transform import CustomTransformFunction
from utils.load import MessageBuilderFunction
from typing import (
    List,
    Tuple,
    Any,
    Dict,
    Optional,
    Callable,
)  # Kept unused type imports for cases where custom transform functions defined

# ----------------------------------------------------------------------------------------------- #
# ---------------------------CONFIGURE JOB-SPECIFIC LOGIC HERE----------------------------------- #
# ----------------------------------------------------------------------------------------------- #
# --- Define custom transform function(s) (if any) for THIS JOB ---
#     Uncomment and complete custom function(s) below
#     Otherwise, job defaults to basic CSV export function in utils
#     Extract and load functions are chosen based on required job settings in config.ini
# ----------------------------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------------------------- #
# --- For jobs that send an email (with a CSV): ------------------------------------------------- #
# ----------------------------------------------------------------------------------------------- #
# def message_builder(
#   job_config: ValidatedConfigUnion,
#   file_path: Path
# ) -> Tuple[str, str]:
#     """
#     Assembles the custom email associated with job.

#     Args:
#         job_config: The combined, nested job configuration dictionary.
#         file_path: The path to the attachment of the email, in case dynamic text
#                     based on it is needed.

#     Returns:
#         A tuple containing the customized email subect and body text.
#     """
#     subject = "Example Email for <job_name> Integration"
#     body = "This is a test email <job_name> job, which sends an email."
#     return subject, body
# ----------------------------------------------------------------------------------------------- #
# ----------------------------------------------------------------------------------------------- #

message_builder_function: Optional[MessageBuilderFunction] = None

# ----------------------------------------------------------------------------------------------- #
# --- For jobs that modify the CSV file or have more advanced procedures: ----------------------- #
# ----------------------------------------------------------------------------------------------- #
# def custom_transform(
#   header: List[str],
#   data:  List[Tuple[Any,...]],
#   intermediate_file_path: Path
# ) -> Path:
#     """
#     Performs the customized data transformation(s) unique to this DIE.

#     Args:
#         header: The first row of fields containing the untransformed data headers.
#         data: The rows of untransformed data.
#         intermediate_file_path: The path where the transformed data should be stored before loading.

#     Returns:
#         The file path to the completed and transformed data, ready to be loaded.
#     """
#     # ... execute any needed preparatory logic
#     intermediate_file = export_csv_from_tuple_array(header, data, intermediate_file_path)
#     # ... apply transformations to intermediate_file and store in transformed_file
#     return transformed_file
# ----------------------------------------------------------------------------------------------- #
# ----------------------------------------------------------------------------------------------- #

custom_transform_function: Optional[CustomTransformFunction] = None


def main() -> None:
    """
    Top-level execution function. Handles setup and run phases,
    and performs top-level exception logging.
    """
    try:
        # --- Phase 1: Setup ---
        try:
            job_name = Path(__file__).resolve().parent.name.replace("DIE_", "")
            if not job_name:
                raise ValueError(
                    "Could not determine job name from directory structure."
                )

            die_instance: DIE = setup_and_get_die(
                job_name=job_name,
                custom_transform=custom_transform_function,
                message_builder=message_builder_function,
            )

        except Exception as setup_error:
            # Check if logging was configured before error occured
            if logging.getLogger().hasHandlers():
                logging.exception(
                    f"Error during setup phase (logging was not configured):\n{setup_error}; "
                )
            else:
                sys.stderr.write(
                    f"Error during setup phase (logging was configured):\n{setup_error}; "
                )
                import traceback

                traceback.print_exc(file=sys.stderr)
            raise setup_error

        # --- Phase 2: Run ---
        if die_instance:
            try:
                die_instance.run()

            except Exception as run_error:
                logging.exception(f"Error during run phase:\n{run_error}'")
                raise run_error

    except Exception:
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
