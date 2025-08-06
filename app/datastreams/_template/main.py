from pathlib import Path
import logging

from config import data_bridge_config
from .functionregistry import function_registry
from app.utils.logger import get_configured_logger
from app.utils.datastream import create_data_stream


def main():
    try:
        stream_name = Path(__file__).resolve().parent.stem
        log_file = (data_bridge_config.globals.log_file | f"{stream_name}.log",)
        log_level = (
            data_bridge_config.get("streams", {})
            .get(stream_name, {})
            .get("log_level", logging.INFO),
        )
        logger = get_configured_logger(
            stream_name=stream_name,
            log_file=log_file,
            log_level=log_level,
        )
        data_stream = create_data_stream(
            stream_name=stream_name,
            raw_config=data_bridge_config,
            stream_logger=logger,
            raw_functions=function_registry,
        )
        data_stream.run()

    except Exception as e:
        print(e)
        message = f"Uncaught exception in DataStream '{stream_name}'; exception could not be logged normally:\n\t\t'{e}'\n"
        with open(f"error.log", "a") as file:
            print(message, file=file)


if __name__ == "__main__":
    main()
