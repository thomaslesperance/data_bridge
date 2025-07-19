import io
import pandas as pd


def df_to_csv_buffer(
    df: pd.DataFrame, keep_df_index=False, encoding: str = "utf-8"
) -> io.BytesIO:
    bytes_buffer = io.BytesIO()
    df.to_csv(bytes_buffer, index=keep_df_index, encoding=encoding)
    return bytes_buffer.seek(0)
