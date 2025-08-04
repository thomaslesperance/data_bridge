import logging
from datetime import datetime
from errors import LogAndTerminate
from models import Stream, DataStore
from extractor import Extractor
from loader import Loader


class DataStream:
    """A orchestrator class designed to manage the state and flow of data
    throughout data stream steps.
    """

    def __init__(
        self, stream_name: str, stream_config: Stream, stream_logger: logging.Logger
    ) -> None:

        self.name = stream_name
        self.config = stream_config
        self.logger = stream_logger
        self.data_store = DataStore(stream_name=stream_name)
        self.logger.info(
            f"Run ID: {self.data_store.run_id} initiated:\n\t{self.data_store}"
        )

    @LogAndTerminate()
    def run(self) -> None:
        for step in self.config.steps:
            step_outputs = self.data_store.step_outputs
            if "extract" in step.type:
                extracted_data = Extractor.extract(step, step_outputs)
                self.data_store[step.output] = extracted_data

            if "transform" in step.type:
                input_data = {item: step_outputs.get(item) for item in step.input}
                output_data = step.function(input_data)
                step_outputs.update(output_data)

            if "load" in step.type:
                dest_response = Loader.load(step, step_outputs)
                self.data_store.dest_responses.append(dest_response)

        self.data_store.end_time = datetime.now()
        self.data_store.status = "success"
        self.logger.info(
            f"Run ID: {self.data_store.run_id} complete:\n\t{self.data_store}"
        )


@LogAndTerminate()
def create_data_stream(
    stream_name: str,
    raw_config: dict,
    stream_logger: logging.Logger,
    raw_functions: dict[str, callable] = {},
) -> DataStream:
    """Hydrates the data stream dict with associated source and destination config and functions from
    provided registry. Passes hydrated raw stream config to pydantic model. Passes validated config
    and logger to DataStream constructor. Returns DataStream instance."""

    stream = raw_config["streams"][stream_name]
    stream_steps = stream["steps"]
    defined_sources = raw_config["sources"]
    defined_dests = raw_config["destinations"]

    hydrated_stream_steps = []
    for step in stream_steps:
        hydrated_step = _hydrate_step(
            step=step,
            sources=defined_sources,
            dests=defined_dests,
            macro_reg=macro_registry,
            function_reg=raw_functions,
        )
        hydrated_stream_steps.append(hydrated_step)

    stream["steps"] = hydrated_stream_steps
    validated_stream_config = Stream(**stream)

    return DataStream(
        stream_name=stream_name,
        stream_config=validated_stream_config,
        stream_logger=stream_logger,
    )


def _hydrate_step(
    step: dict, sources: dict, dests: dict, macro_reg: dict, function_reg: dict
) -> dict:
    """
    Hydrates a step dictionary by replacing config names with config objects
    and resolving path placeholders with macro values.
    """
    hydrated_step = step.copy()

    # --- Configuration Hydration ---
    if "source_config" in hydrated_step:
        hydrated_step["source_config"] = sources.get(hydrated_step["source_config"])

    if "dest_config" in hydrated_step:
        hydrated_step["dest_config"] = dests.get(hydrated_step["dest_config"])

    if "function" in hydrated_step:
        hydrated_step["function"] = function_reg.get(hydrated_step["function"])

    if "email_builder" in hydrated_step:
        hydrated_step["email_builder"] = function_reg.get(
            hydrated_step["email_builder"]
        )

    # --- Path Parameter Replacement ---
    for key in ("query_file_path", "remote_file_path"):
        if key in hydrated_step:
            path_string = hydrated_step[key]
            params = hydrated_step.get("path_params", {})
            resolved_path_string = _resolve_path_from_params(
                path=path_string, params=params, macro_reg=macro_reg
            )
            hydrated_step[key] = resolved_path_string

    return hydrated_step


def _resolve_path_from_params(path: str, params: dict, macro_reg: dict) -> str:
    resolved_path = path

    for param_name, param_value_source in params.items():
        placeholder = f"::{param_name}::"

        replacement_value = ""
        if param_value_source.startswith("macro:"):
            macro_name = param_value_source.split(":", 1)[1]
            macro_function = macro_reg.get(macro_name)
            if macro_function:
                replacement_value = str(macro_function())
        else:
            replacement_value = param_value_source

        # Replace the placeholder in the path string
        if replacement_value:
            resolved_path = resolved_path.replace(placeholder, replacement_value)

    return resolved_path


def _macro_school_year():
    cur_date = datetime.now()
    cur_month = cur_date.month
    if cur_month <= 7:
        return cur_date.year
    else:
        return cur_date.year + 1


def _macro_yyyymmdd():
    cur_date = datetime.now()
    string = cur_date.strftime("%Y%M%D")
    return string


macro_registry = {"SCHOOL_YEAR": _macro_school_year, "YYYYMMDD": _macro_yyyymmdd}
