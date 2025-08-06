import io
from datetime import datetime
from email.message import Message
import pandas as pd
from app.utils.models import (
    StreamData,
    TransformFunc,
    EmailBuilder,
    Destination,
    EmailParams,
)
from app.utils.transformutils import df_to_csv_buffer


def transform_fn_1(data: dict[str, StreamData]) -> dict[str, StreamData]:
    """
    An example transform function that merges hypothetical student and grade data taken from
    SQL extractions and then prepares a CSV report in memory for loading.
    Meant to showcase the transformation of data from extract dependencies to load dependencies.
    """
    extracted_grades_df = data["grades.sql"].content
    extracted_student_df = data["students.sql"].content
    extracted_legacy_buffer = data["remote/rel/path/export_file.csv"].content
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


def transform_fn_2(data: dict[str, StreamData]) -> dict[str, StreamData]:
    pass


def transform_fn_3(data: dict[str, StreamData]) -> dict[str, StreamData]:
    pass


def transform_fn_4(data: dict[str, StreamData]) -> dict[str, StreamData]:
    pass


def email_builder_1(
    dest_config: Destination,
    load_data: dict[str, StreamData],
    email_params: EmailParams,
) -> Message:

    date = "08-05-2025"
    subject = "Example email subject line".title()
    body = "Example email body text."

    # ... logic to extract details from data and add to email body text dynamically

    return msg


def email_builder_2(
    dest_config: Destination,
    load_data: dict[str, StreamData],
    email_params: EmailParams,
) -> Message:
    # ... logic to build and return the admin email Message object ...
    pass


def email_builder_3(
    dest_config: Destination,
    load_data: dict[str, StreamData],
    email_params: EmailParams,
) -> Message:
    # ... logic to build and return a simple status email ...
    pass


transform_fns: dict[str, TransformFunc] = {
    "transform_fn_1": transform_fn_1,
    "transform_fn_2": transform_fn_2,
    "transform_fn_3": transform_fn_3,
    "transform_fn_4": transform_fn_4,
}

email_builders: dict[str, EmailBuilder] = (
    {
        "email_builder_1": email_builder_1,
        "email_builder_2": email_builder_2,
        "email_builder_3": email_builder_3,
    },
)

# ------------------- TEMPLATE CODE --------------------------------------
function_registry = {}
function_registry["transform_fns"] = transform_fns
function_registry["email_builders"] = email_builders
