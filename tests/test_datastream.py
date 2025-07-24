# Contract I am testing for in datastream.py:
#
# DataStream.__init__()
#   Assign attributes and call _setup_data_stream() (orchestrator)
#   Return None (function only produces side effects)
#
# DataStream._setup_data_stream() (orchestrator)
#   Access variables
#   Call _configure_logger() (non-pure helper)
#   Call _validate_config() (orchestrator)
#   Assign attributes
#   Wrap transform_fn
#   Call Extractor()
#   Call Loader()
#   Return None (function only produces side effects)
#
# Datastream._configure_logger() (non-pure helper)
#
# DataStream._validate_config() (orchestrator)
#   Call _validate_stream() (helper)
#   Call _check_for_missing_definitions() (pure helper)
#   Call _validate_sources() (helper)
#   Call _check_source_dependencies() (pure helper)
#   Call _validate_destinations() (helper)
#   Call _validate_transform_function() (helper)
#   Call _validate_email_builders() (helper)
