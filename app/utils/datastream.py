from errors import LogAndTerminate
from extractor import Extractor
from loader import Loader


class DataStream:
    """A container class designed to manage the state and orchestrate the flow of data
    between its child components (extractor, loader, and Stream-specific transformer).
    """

    def __init__(
        self,
        extractor,
        transformer,
        loader,
    ) -> None:

        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    @LogAndTerminate()
    def run(self) -> None:
        extracted_data = self.extractor()
        transformed_data = self.transformer(extracted_data)
        dest_responses = self.loader(transformed_data)
        return dest_responses


@LogAndTerminate()
def create_data_stream(
    stream_name: str,
    raw_config: dict,
    raw_transform_fn: callable,
    raw_email_builders: dict[str, callable] = {},
) -> DataStream:
    validated_config = validated_config.from_raw_config(
        stream_name=stream_name,
        raw_config=raw_config,
        raw_transform_fn=raw_transform_fn,
        raw_email_builders=raw_email_builders,
    )
    extractor = Extractor(
        validated_config.sources,
        validated_config.stream.extract_tasks,
    )
    loader = Loader(
        destinations=validated_config.destinations,
        load_tasks=validated_config.stream.load_tasks,
        email_builders=validated_config.email_builders,
    )

    return DataStream(
        extractor=extractor, transformer=validated_config.transformer, loader=loader
    )
