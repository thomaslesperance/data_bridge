from logging import Logger
from datetime import datetime
from app.utils.models import Stream, DataStore
from app.utils.errors import LogAndTerminate
from app.utils.extractor import Extractor
from app.utils.loader import Loader


class DataStream:
    """A orchestrator class designed to manage the state and flow of data
    throughout data stream steps.
    """

    def __init__(
        self, stream_name: str, stream_config: Stream, stream_logger: Logger
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
                self.data_store.step_outputs[step.output] = extracted_data

            if "transform" in step.type:
                input_data = {item: step_outputs[item] for item in step.input}
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
