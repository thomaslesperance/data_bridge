import pandas as pd
from pathlib import Path
from utils.models import StreamData
from app.utils.transformutils import df_to_csv_buffer
from email.message import Message
from app.utils.datastream import DataStream
from config import data_bridge_config


# ---------- JOB-SPECIFIC LOGIC -----------------
## EXAMPLE (see README for details): -------------------------------------
def transform_fn(extracted_data: dict[str, StreamData]) -> dict[str, StreamData]:
    """
    An example transform function that merges hypothetical student and grade data taken from
    SQL extractions and then prepares a CSV report in memory for loading.
    Meant to showcase the transformation of data from extract dependencies to load dependencies.
    """
    extracted_grades_df = extracted_data["grades.sql"].content
    extracted_student_df = extracted_data["students.sql"].content
    extracted_legacy_buffer = extracted_data["remote/rel/path/export_file.csv"].content
    df = pd.merge(extracted_student_df, extracted_grades_df, on="student_id")

    grades_load_df = extracted_grades_df.loc[df["grades"].isna()].copy()
    teacher_mask = df["teachers"].isna()
    teachers_load_df = extracted_student_df.loc[~teacher_mask, "employee_id"].copy()

    email_1_df = grades_load_df.loc[grades_load_df["teacher"].isna()].copy()
    email_1_buffer = df_to_csv_buffer(email_1_df)

    email_2a_df = teachers_load_df.loc[teachers_load_df["admin"].isna()].copy()
    email_2a_buffer = df_to_csv_buffer(email_2a_df)

    admin_mask = teachers_load_df["admin_status"].isna()
    email_2b_df = teachers_load_df.loc[~admin_mask, "employee_id"].copy()
    email_2b_buffer = df_to_csv_buffer(email_2b_df)

    all_load_data = {
        "formatted_grades.csv": StreamData(
            data_format="dataframe",
            content=grades_load_df,
        ),
        "active_teachers.csv": StreamData(
            data_format="dataframe",
            content=teachers_load_df,
        ),
        "remote/rel/path/summary.csv": StreamData(
            data_format="in_memory_stream",
            content=extracted_legacy_buffer,
        ),
        "email_1_data.csv": StreamData(
            data_format="in_memory_stream",
            content=email_1_buffer,
        ),
        "email_2_data_A.csv": StreamData(
            data_format="in_memory_stream",
            content=email_2a_buffer,
        ),
        "email_2_data_B.csv": StreamData(
            data_format="in_memory_stream",
            content=email_2b_buffer,
        ),
    }

    return all_load_data


def build_teacher_email(email_data: dict[str, StreamData]) -> Message:
    # ... logic to build and return the teacher email Message object ...
    pass


def build_admin_email(email_data: dict[str, StreamData]) -> Message:
    # ... logic to build and return the admin email Message object ...
    pass


def build_status_email(email_data: dict[str, StreamData]) -> Message:
    # ... logic to build and return a simple status email ...
    pass


email_builders = {
    "build_teacher_email": build_teacher_email,
    "build_admin_email": build_admin_email,
    "build_status_email": build_status_email,
}
## END EXAMPLE: ---------------------------------
# ---------- END JOB-SPECIFIC LOGIC -------------


# ---------- TEMPLATE LOGIC ---------------------
def main():
    try:
        stream_name = Path(__file__).resolve().parent.name
        data_stream = DataStream(
            **data_bridge_config,
            stream_name=stream_name,
            transform_fn=transform_fn,
            email_builders=email_builders,
        )
        data_stream.run()
    except Exception as e:
        print(e)
        message = f"Uncaught exception in DataStream '{stream_name}'; exception could not be logged normally:\n\t\t'{e}'\n"
        with open(f"error.log", "a") as file:
            print(message, file=file)


if __name__ == "__main__":
    main()
# ---------- END TEMPLATE LOGIC -----------------
