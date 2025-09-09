from pathlib import Path

from app.utils.config import get_stream_config
from app.utils.logger import StreamLogger
from app.utils.datastream import DataStream
from .streamfunctions import stream_functions


def main():
    try:
        stream_name = Path(__file__).resolve().parent.stem
        stream_logger = StreamLogger(stream_name)

        config_file = (
            Path(__file__).resolve().parent.parent.parent.parent / "config.yaml"
        )

        stream_config = get_stream_config(
            stream_name=stream_name,
            config_file=config_file,
            stream_functions=stream_functions,
        )

        stream_logger.set_log_level(stream_config.log_level)
        stream_logger.set_log_file(stream_config.log_file)

        data_stream = DataStream(
            stream_name=stream_name,
            stream_config=stream_config,
            stream_logger=stream_logger.logger_instance,
        )

        data_stream.run()

    except Exception as e:
        print(e)
        message = f"Uncaught exception in DataStream '{stream_name}'; exception could not be logged normally:\n\t\t'{e}'\n"
        with open(f"error.log", "a") as file:
            print(message, file=file)


if __name__ == "__main__":
    main()
