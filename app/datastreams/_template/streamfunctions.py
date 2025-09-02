from datetime import datetime
from email.message import Message
import pandas as pd
from app.utils.models import StreamData, TransformFunc, EmailBuilder
from app.utils.transformutils import df_to_csv_buffer, build_email_msg
from app.utils.macros import macro_registry


def transform_fn_1(data: dict[str, StreamData]) -> dict[str, StreamData]:
    pass


def transform_fn_2(data: dict[str, StreamData]) -> dict[str, StreamData]:
    pass


def transform_fn_3(data: dict[str, StreamData]) -> dict[str, StreamData]:
    pass


def email_builder_1(email_data: dict[str, "StreamData"]) -> Message:
    pass


stream_functions: dict[str, TransformFunc | EmailBuilder] = {
    "transform_fn_1": transform_fn_1,
    "transform_fn_2": transform_fn_2,
    "transform_fn_3": transform_fn_3,
    "email_builder_1": email_builder_1,
}
