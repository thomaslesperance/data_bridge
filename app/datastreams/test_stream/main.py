import pandas as pd
from pathlib import Path
from utils.models import StreamData
from app.utils.transformutils import df_to_csv_buffer
from email.message import Message
from app.utils.datastream import DataStream
from config import data_bridge_config


# ---------- STREAM-SPECIFIC LOGIC --------------
## EXAMPLE (see README for details): ------------
def transform_fn(extracted_data: dict[str, StreamData]) -> dict[str, StreamData]:
    df = extracted_data["query.sql"].content
    data = df_to_csv_buffer(df=df)

    all_load_data = {
        "report.csv": StreamData(
            data_format="in_memory_stream",
            content=data,
        )
    }

    return all_load_data


# def build_teacher_email(email_data: dict[str, StreamData]) -> Message:
#     # ... logic to build and return the teacher email Message object ...
#     pass


# def build_admin_email(email_data: dict[str, StreamData]) -> Message:
#     # ... logic to build and return the admin email Message object ...
#     pass


# def build_status_email(email_data: dict[str, StreamData]) -> Message:
#     # ... logic to build and return a simple status email ...
#     pass


email_builders = {}
## END EXAMPLE: ---------------------------------
# ---------- END STREAM-SPECIFIC LOGIC ----------


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
