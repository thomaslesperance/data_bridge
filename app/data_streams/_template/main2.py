import pandas as pd
import io
from pathlib import Path
from email.message import Message
from utils.models import PipelineData
from utils.data_stream import DataStream
from config import config


# ---------- JOB-SPECIFIC LOGIC -----------------
## EXAMPLE (see README for details): -------------------------------------
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

    # TODO: Refactor this operation to a new utils.utils module
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


def build_teacher_email(email_data: dict[str, PipelineData]) -> Message:
    # ... logic to build and return the teacher email Message object ...
    pass


def build_admin_email(email_data: dict[str, PipelineData]) -> Message:
    # ... logic to build and return the admin email Message object ...
    pass


def build_status_email(email_data: dict[str, PipelineData]) -> Message:
    # ... logic to build and return a simple status email ...
    pass


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
        **config,
        job_name=job_name,
        transform_fn=transform_fn,
        email_builders=email_builders,
    )
    data_stream.run()


if __name__ == "__main__":
    main()


# TODO:
# X 1. Pydantic models + data stream skeleton
# 2. Logging system
#   find popular lightweight library if not in STL
#   intelligent format
#   log/exception decorators
# 2a. spread kwargs into constructors
# 3. Write unit tests for core pipeline components
# 4. Migrate jobs one-by-one
#     --Extract/load methods as needed
#     --Start with IA as good example that uses pandas
