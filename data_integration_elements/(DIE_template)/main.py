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
#     """Builds email subject and body for <job_name>."""
#     subject = "Example Email for <job_name> Integration"
#     body = "This is a test email <job_name> job, which sends an email."
#     return subject, body

# --- For jobs that modify the CSV file or have more advanced procedures:
# def custom_transform(header: List[str], data:  List[Tuple[Any,...]], intermediate_file_path):
#     intermediate_file = export_csv_from_tuple_array(data, intermediate_file_path, header)
#     # ... apply transformations to intermediate_file and store in transformed_file
#     return transformed_file

# Comment out following line(s) if custom function(s) defined above:
message_builder = None
custom_transform = None


def main():
    job_name = Path(__file__).resolve().parent.name.replace("DIE_", "")
    die = DIE(
        job_name=job_name,
        custom_transform=custom_transform,
        message_builder=message_builder,
    )
    die.run()


if __name__ == "__main__":
    main()
