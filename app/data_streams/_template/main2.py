import pandas as pd
import io
from pathlib import Path
from email.message import Message
from utils.models import PipelineData
from utils.data_stream import DataStream
from config import sources, destinations, jobs, PROJECT_LOG_FILE


# DIAGRAMATIC REPRESENTATION OF CUSTOM LOGIC INTERACTION WITH PIPELINE CODE:
#
# Extractor.extract()
#     │
#     └─> returns flat dict: {"students.sql": <PipelineData>, ...}
#                                              │
#                                              ▼
#                          transform_fn(extracted_data)
#                                │
#                                ▼
#        returns flat dict: {"report.csv": <PipelineData>, "email_data.csv": <PipelineData>, ...}
#                 │
#                 ▼
# Loader.load(all_load_data)
#     │
#     ├─> (For an SFTP Task) ───> _sftp_load(PipelineData for "report.csv") ───> [SFTP Server]
#     │
#     └─> (For an Email Task) ───> email_builder_fn({"email_data.csv": <PipelineData>})
#                                      │
#                                      ▼
#                                  Message Object
#                                      │
#                                      ▼
#                        _smtp_load(Message) ───────────────> [SMTP Server]
#
#
# ---------- JOB-SPECIFIC LOGIC -----------------
## EXAMPLE: -------------------------------------
## Here are functions that correctly simulate the transform step for the "example_complex_job"
## job in the "jobs" dict of the sample.config.py file:

# The transform function will receive this from the DataStream instance extractor...
# extracted_data = {
#   "grades.sql": <PipelineData object>,
#   "students.sql": <PipelineData object>,
#   "remote/rel/path/export_file.csv": <PipelineData object>,
# }

# And, after performing some example logic, it should hand this to the DataStream instance loader...
# all_load_data = {
#   "formatted_grades.csv": <PipelineData object>,
#   "active_teachers.csv": <PipelineData object>,
#   "remote/rel/path/summary.csv": <PipelineData object>,
#   "email_1_data.csv": <PipelineData object>,
#   "email_2_data_A.csv": <PipelineData object>,
#   "email_2_data_B.csv": <PipelineData object>,
# }

# The result is a job-specific mapping of data source-dependency to data destination-dependency mapping,
# in other words, the transform step is solely responsible for ensuring the load dependencies for the job
# are composed of the correct components of extracted data.


def transform_fn(extracted_data: dict[str, PipelineData]) -> dict[str, PipelineData]:
    """
    An example transform function that merges hypothetical student and grade data taken from
    SQL extractions and then prepares a CSV report in memory for loading.
    Meant to showcase the transformation of data from extract dependencies to load dependencies.
    """
    extracted_grades_data = extracted_data["grades.sql"].content
    extracted_student_data = extracted_data["students.sql"].content
    extracted_legacy_data = extracted_data["remote/rel/path/export_file.csv"].content
    df = pd.merge(extracted_student_data, extracted_grades_data, on="student_id")

    grades_load_data = df.loc[df["grades"].isna()].copy()
    teachers_load_data = df.loc[df["teachers"] == "active", "employee_id"].copy()

    email_1_data = df.loc[df["teacher"].isna()].copy()
    output_buffer_1 = io.BytesIO()
    email_1_data.to_csv(output_buffer_1, index=False, encoding="utf-8")
    output_buffer_1.seek(0)

    email_2_A_data = df.loc[df["admin"].isna()].copy()
    output_buffer_2 = io.BytesIO()
    email_2_A_data.to_csv(output_buffer_2, index=False, encoding="utf-8")
    output_buffer_2.seek(0)

    email_2_B_data = df.loc[df["admin_status"] == "active", "employee_id"].copy()
    output_buffer_3 = io.BytesIO()
    email_2_B_data.to_csv(output_buffer_3, index=False, encoding="utf-8")
    output_buffer_3.seek(0)

    all_load_data = {
        "formatted_grades.csv": PipelineData(
            data_format="dataframe",
            content=grades_load_data,
        ),
        "active_teachers.csv": PipelineData(
            data_format="dataframe",
            content=teachers_load_data,
        ),
        "remote/rel/path/summary.csv": PipelineData(
            data_format="in_memory_stream",
            content=extracted_legacy_data,
        ),
        "email_1_data.csv": PipelineData(
            data_format="in_memory_stream",
            content=output_buffer_1,
        ),
        "email_2_data_A.csv": PipelineData(
            data_format="in_memory_stream",
            content=output_buffer_2,
        ),
        "email_2_data_B.csv": PipelineData(
            data_format="in_memory_stream",
            content=output_buffer_3,
        ),
    }

    return all_load_data


# Example email functions for the example job "example_complex_job":
# The passed email_data dict keys being dependency names for the email-based load task
# and the values being associated PipelineData


# email_data = {"email_1_data.csv": <PipelineData object>}
def build_teacher_email(email_data: dict[str, PipelineData]) -> Message:
    # ... logic to build and return the teacher email Message object ...
    pass


# email_data = {
#   "email_2_data_A.csv": <PipelineData object>,
#   "email_2_data_B.csv": <PipelineData object>
# }
def build_admin_email(email_data: dict[str, PipelineData]) -> Message:
    # ... logic to build and return the admin email Message object ...
    pass


# email_data = {}
def build_status_email(email_data: dict[str, PipelineData]) -> Message:
    # ... logic to build and return a simple status email ...
    pass


# Map the builder names from the config to the actual functions
email_builders = {
    "build_teacher_email": build_teacher_email,
    "build_admin_email": build_admin_email,
    "build_status_email": build_status_email,
}
## END EXAMPLE: ---------------------------------
# ---------- END JOB-SPECIFIC LOGIC -------------


def main():
    job_name = Path(__file__).resolve().parent.name
    data_stream = DataStream(
        job_name=job_name,
        job=jobs[job_name],
        avail_sources=sources,
        avail_destinations=destinations,
        transform_fn=transform_fn,
        email_builders=email_builders,
        log_file=PROJECT_LOG_FILE,
    )
    data_stream.run()


if __name__ == "__main__":
    main()


# TODO:
# 1. Pydantic models + data stream skeleton
# 2. Logging system (find popular lightweight library; intelligent format)
# 3. Migrate jobs one-by-one
#     --Extract/load methods as needed
