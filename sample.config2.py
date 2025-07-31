from pathlib import Path
import logging

# abs paths and remote rel paths are strings, local rel paths are Path objects
DATA_BRIDGE_ROOT = Path(__file__).parent.resolve()
CREDS_DIR = DATA_BRIDGE_ROOT / "creds"
LOG_FILE = DATA_BRIDGE_ROOT / "app" / "data_streams" / "data_bridge.log"


data_bridge_config = {
    # Global variables
    "globals": {
        "log_file": LOG_FILE,
    },
    # Data Sources
    "sources": {
        "db1": {
            "protocol": "sql",
            "user": "user",
            "password": "password",
            "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
            "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
            # You do not need to specify JAR file abs path if CLASSPATH set in ~/.zprofile
        },
        "db2": {
            "protocol": "sql",
            "user": "user",
            "password": "password",
            "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
            "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
        },
        "fileshare": {
            "protocol": "smb",
            "mount_path": "/abs/path/to/share/root",
        },
        "google_drive_account": {
            "protocol": "google_drive",
            "access_token": CREDS_DIR / "token.pickle",
        },
        "sftp_server": {
            "protocol": "sftp",
            "user": "user",
            "password": "password",
            "host": "123.456.789.1011",
            "port": "22",
        },
    },
    # Data Destinations
    "destinations": {
        "smtp_server": {
            "protocol": "smtp",
            "host": "smtp.domain.net",
            "port": "25",
            "default_sender_email": "jobs@example.com",
        },
        "fileshare": {
            "protocol": "smb",
            "mount_path": "/abs/path/to/share/root",
        },
        "sftp_server": {
            "protocol": "sftp",
            "host": "123.456.789.1011",
            "user": "user",
            "password": "password",
            "port": "22",
        },
        "google_drive_account": {
            "protocol": "google_drive",
            "access_token": CREDS_DIR / "token.pickle",
        },
    },
    # Job Configurations
    "streams": {
        "example_stream": {
            "log_level": logging.INFO,
            "steps": [
                {
                    "step_name": "get_no_email_list",
                    "step_type": "extract",
                    "protocol": "smb",
                    "source_config": "fileshare",
                    "output": "no_email_list",
                    "remote_file_path": "remote/rel/path/to/::school_year::_report.csv",
                    "path_params": {"school_year": "macro:SCHOOL_YEAR"},
                },
                {
                    "step_name": "grades_initial_extract",
                    "step_type": "extract",
                    "protocol": "sql",
                    "source_config": "db1",
                    "output": "raw_grades_data",
                    "query_file_path": "grades.sql",
                    "query_params": {"campus_code": "123"},
                    "path_params": None,
                },
                {
                    "step_name": "students_initial_extract",
                    "step_type": "extract",
                    "protocol": "sql",
                    "source_config": "db1",
                    "output": "raw_students_data",
                    "query_file_path": "students_::today::.sql",
                    "query_params": {"campus_code": "123"},
                    "path_params": {"today": "macro:YYYYMMDD"},
                },
                {
                    "step_name": "determine_high_achievers",
                    "step_type": "transform",
                    "function": "determine_high_achievers",
                    "input": [
                        "raw_grades_data",
                        "raw_students_data",
                    ],
                    "output": ["high_achiever_IDs"],
                },
                {
                    "step_name": "determine_at_risk",
                    "step_type": "transform",
                    "function": "determine_at_risk",
                    "input": [
                        "raw_grades_data",
                        "raw_students_data",
                    ],
                    "output": ["at_risk_IDs"],
                },
                {
                    "step_name": "high_ach_parent_ids",
                    "step_type": "extract",
                    "protocol": "sql",
                    "source_config": "db2",
                    "output": "high_ach_parent_ids",
                    "query_file_path": "high_ach_parents.sql",
                    "query_params": {"ids": "step:high_achiever_IDs"},
                    "path_params": None,
                },
                {
                    "step_name": "at_risk_parent_ids",
                    "step_type": "extract",
                    "protocol": "sql",
                    "source_config": "db2",
                    "output": "at_risk_parent_ids",
                    "query_file_path": "at_risk_parents.sql",
                    "query_params": {"ids": "step:at_risk_IDs"},
                    "path_params": None,
                },
                {
                    "step_name": "get_raw_mailing_list",
                    "step_type": "extract",
                    "protocol": "sql",
                    "source_config": "db2",
                    "output": "mailing_list",
                    "query_file_path": "mailing_list.sql",
                    "query_params": {
                        "at_risk_ids": "step:at_risk_parent_ids",
                        "high_ach_ids": "step:high_ach_parent_ids",
                    },
                    "path_params": None,
                },
                {
                    "step_name": "format_mailing_list",
                    "step_type": "transform",
                    "function": "format_mailing_list",
                    "input": ["mailing_list", "no_email_list"],
                    "output": [
                        "formatted_mailing_list",
                        "admin_report",
                        "admin_emails",
                    ],
                },
                {
                    "step_name": "send_campus_mailing_list",
                    "step_type": "load",
                    "protocol": "smtp",
                    "dest_config": "smtp_server",
                    "input": ["formatted_mailing_list", "admin_report"],
                    "email_builder": "build_mailing_list_email",
                    "email_params": {
                        "recipients": "step:admin_emails",
                        "subject_line": "ATTN: Campus Admin",
                        "daily_quote": "E=MC^2",
                    },
                },
                {
                    "step_name": "archive_admin_report",
                    "step_type": "load",
                    "protocol": "smb",
                    "dest_config": "sftp_server",
                    "input": ["admin_report"],
                    "remote_file_path": "archive/reports/admin_reports/::date::/admin_report_::timestamp::.csv",
                    "path_params": {
                        "timestamp": "macro::TIMESTAMP",
                        "date": "macro:YYYYMMDD",
                    },
                },
            ],
        }
    },
}


# Of course. Here is a concise checklist of the key development tasks to complete your new `data_bridge` system, assuming the `config.py` file is your starting point.

# ***
# ### ## 1. Update `models.py` for Validation

# Your first step is to build the Pydantic models that will validate your new configuration structure.

# * **Create Step-Specific Models:** Define separate Pydantic models for each step `type` (`ExtractStep`, `TransformStep`, `LoadStep`). Use a `Literal` field in each as the discriminator.
# * **Create a Discriminated Union:** Combine the step-specific models into a single `Step` type using `typing.Union` and `Field(discriminator="type")`.
# * **Create a Root `StreamConfig` Model:** Build a main model that contains the `steps: list[Step]` and references to the `sources` and `destinations` dictionaries.
# * **Implement the Root Validator:** Add a `@model_validator` to the `StreamConfig` model to perform the cross-validation checks (e.g., ensuring an SMTP load step has an `email_builder`).

# ***
# ### ## 2. Build the Orchestrator in `datastream.py`

# This file will contain the engine that runs the pipeline.

# * **Create the `DataStream` Class:** This class will be initialized with the validated configuration.
# * **Define the Macro Registry:** Create the dictionary (`MACRO_REGISTRY`) that maps template strings like `{{TIMESTAMP}}` to their corresponding Python functions.
# * **Implement the Parameter Resolver:** Write the `_resolve_params` helper method inside the `DataStream` class. This function will handle static values, resolve macros from the registry, and look up dependencies from the `data_store`.
# * **Build the `run()` Method:** Create the main `run()` method that initializes the `data_store`, loops through the configured steps, calls the parameter resolver for each step, and executes the appropriate component (`Extractor`, `Transformer`, or `Loader`).

# ***
# ### ## 3. Enhance `Extractor` and `Loader` Classes

# Update your core components to handle the logic defined in the new configuration.

# * **Modify the `Extractor`:**
#     * Update it to accept the resolved `params` dictionary.
#     * Implement the safer custom solution for named SQL parameters, where you replace `:name` with `?` placeholders and pass the values securely to `cursor.execute()`.
#     * Add logic to handle parameter substitution for fileshare paths.
# * **Modify the `Loader`:**
#     * Update it to handle the new configuration keys, such as `destination_path` for SFTP and fileshare loads.

# ***
# ### ## 4. Update `main.py` Entry Point

# Keep this file simple. Its only job is to kick off the process.

# * **Instantiate and Run:** Modify the `main()` function to call your Pydantic `StreamConfig` model to validate the configuration, instantiate the `DataStream` class with the validated config, and then call the `.run()` method.


## New config design rules:

# param-able keys:
# ** any param that is a path
#
# Associated param keys:
# path_params, query_params, email_params
# each are dicts

# Any param value in these param dicts is a single string.
# If it references a step's output, it's prefixed with step:.
#     The value following the prefix matching the reference of a single output item from a previous step
# If it references a runtime-generated value, it's prefixed with macro:.
#     The value following the prefix being a key in the macro map to a pre-defined function for determining the runtime value

#
#
# local paths are Path type
# remote paths are str type


# Config file rules:
# 1. All values are static except:

# 2. Paths can have placeholders 'rel/path/to/report_::<placeholder_name>::.csv`

# 3. "..._params" keys are dicts of strings whose values are either static or dynamic:

# macro: run a pre-defined fuction with given name to use it's return value here
# step: use the data in the data_store object under this name

# "email_params": {
#     "recipients": "step:ref_to_previous_step_output"  <-- required
#     "subject": "static value!",
# }
