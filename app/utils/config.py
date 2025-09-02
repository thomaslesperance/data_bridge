import os
import re
from pathlib import Path
from yaml import safe_load
from dotenv import load_dotenv
from app.utils.errors import LogAndTerminate
from app.utils.models import Stream, TransformFunc, EmailBuilder


@LogAndTerminate()
def get_stream_config(
    stream_name: str,
    config_file: Path,
    stream_functions: dict[str, TransformFunc | EmailBuilder] = {},
) -> Stream:
    """
    Parses .env file and config.py file, hydrates needed section with environment variables and
    user-defined stream functions, passes hydrated config dict to pydantic model.
    """

    load_dotenv()

    with open(config_file, "r") as f:
        config_str = f.read()

    config_str = re.sub(r"\$\{(\w+)\}", _env_var_substituter, config_str)
    raw_combined_config = safe_load(config_str)
    raw_stream_config = raw_combined_config["streams"][stream_name]

    for step_config in raw_stream_config["steps"]:
        if "function" in step_config.keys():
            function_name = step_config["function"]
            step_config["function"] = stream_functions[function_name]

    return Stream(**raw_stream_config)


def _env_var_substituter(match):
    var_name = match.group(1)
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Environment variable '{var_name}' not set.")
    return value
