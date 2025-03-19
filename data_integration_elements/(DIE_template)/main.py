from pathlib import Path
import sys

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from utils.die import DIE

# --- Define custom transform function(s) (if any) for THIS JOB ---
#     Uncomment and complete custom function(s) below
#     Otherwise, job defaults to basic CSV export function in utils
#     Extract and load functions are chosen based on required job settings in config.ini

# --- For jobs that send a CSV file in an email:
# def message_builder(job_config: Dict[str, Any], file_path: str) -> Tuple[str, str]:
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

# --- For jobs that modify the CSV file or have more advanced procedures:
# def custom_transform(header: List[str], data:  List[Tuple[Any,...]], intermediate_file_path) -> str:
#     """
#     Performs the customized data transformation(s) unique to this DIE.

#     Args:
#         header: The first row of fields containing the untransformed data headers.
#         data: The rows of untransformed data.
#         intermediate_file_path: The path where the transformed data should be stored before loading.

#     Returns:
#         The file path to the completed and transformed data, ready to be loaded.
#     """
#     intermediate_file = export_csv_from_tuple_array(data, intermediate_file_path, header)
#     # ... apply transformations to intermediate_file and store in transformed_file
#     return transformed_file

# Comment out following line(s) if custom function(s) defined above:
message_builder = None
custom_transform = None


def main() -> None:
    """
    Identifies the name of integration job from parent DIE dir name, instantiates DIE class,
    passes custom funcitons to the class and runs the DIE object.
    """
    job_name = Path(__file__).resolve().parent.name.replace("DIE_", "")
    die = DIE(
        job_name=job_name,
        custom_transform=custom_transform,
        message_builder=message_builder,
    )
    die.run()


if __name__ == "__main__":
    main()
