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
            "name": "db1",
            "protocol": "sql",
            "user": "user",
            "password": "password",
            "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
            "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
            # You do not need to specify JAR file abs path if CLASSPATH set in ~/.zprofile
        },
        "db2": {
            "name": "db2",
            "protocol": "sql",
            "user": "user",
            "password": "password",
            "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
            "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
        },
        "fileshare": {
            "name": "fileshare",
            "protocol": "smb",
            "mount_path": "/abs/path/to/share/root",
        },
        "google_drive_account": {
            "name": "google_drive_account",
            "protocol": "google_drive",
            "access_token": CREDS_DIR / "token.pickle",
        },
        "sftp_server": {
            "name": "sftp_server",
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
            "name": "smtp_server",
            "protocol": "smtp",
            "host": "smtp.domain.net",
            "user": "user",
            "password": "password",
            "port": "25",
            "default_sender_email": "jobs@example.com",
        },
        "fileshare": {
            "name": "fileshare",
            "protocol": "smb",
            "mount_path": "/abs/path/to/share/root",
        },
        "sftp_server": {
            "name": "sftp_server",
            "protocol": "sftp",
            "host": "123.456.789.1011",
            "user": "user",
            "password": "password",
            "port": "22",
        },
        "google_drive_account": {
            "name": "google_drive_account",
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
                    "output": "high_achiever_IDs",
                },
                {
                    "step_name": "determine_at_risk",
                    "step_type": "transform",
                    "function": "determine_at_risk",
                    "input": [
                        "raw_grades_data",
                        "raw_students_data",
                    ],
                    "output": "at_risk_IDs",
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
                    "input": "admin_report",
                    "remote_file_path": "archive/reports/admin_reports/::date::/admin_report_::school_year::.csv",
                    "path_params": {
                        "school_year": "macro:SCHOOL_YEAR",
                        "date": "macro:YYYYMMDD",
                    },
                },
            ],
        }
    },
}


# --- Config file rules ---
# -- local paths are coerces to Path objects
# -- remote paths are kept as str
# Steps:
# 1. All values are static except:
# 2. Paths can have placeholders 'rel/path/to/report_::<placeholder_name>::.csv`
# 3. "..._params" keys are dicts of strings whose values are either static or dynamic:
# Dynamic options:
#       macro: run a pre-defined fuction with given name to use it's return value here
#       step: use the data in the data_store object under this name
#           "email_params": {
#               "recipients": "step:ref_to_previous_step_output"  <-- required
#               "subject": "static value!",
#           }
#   Currently available param keys:
#       path_params: str, or macro
#       query_params: str, macro, or step
#       email_params: str, macro, or step
#       (each are dicts)


# TODO:

# file name & data_format architectural update
#   extract step config can optionally set data_format and filename (might need crosschecking if both specified)
#   transform step config sets neither as user makes this explicit in functions
#   load step config can optionally specify final file name(s); data formats are coerced based on load method
#       Need to figure out issue of multiple inputs (only needed with email steps, but might be helpful in general
#       Will file type be an issue and if so will the suffix on filename be sufficient?
#       Will email steps mess things up since they are also user-defined
# I THINK THE KEY HERE (to avoid reworking the app over and over) IS TO UNDERSTAND THE CONTRACT BETWEEN:
# THE CONFIG FILE, THE APP METHOD BEHAVIOR, AND THE DATASTREAM OBJECT
# THEN, DOES THAT CONTRACT SATISFY ALL POSSIBLE (REASONABLE) NEEDS FOR THIS APP
# WHAT TOOLS AND METHODS DO PROFESSIONALS USE TO DO THIS SORT OF APP ARCHITECTURE/LOGIC/API DESIGN?

# email builder helper

# change gets to [] except where defaults possible

# figure out LogAndTerminate and if log and re-raise gives clean (or repeated and hard to read) error blocks in log file

# Factor out param hydration for query and email params keys

# Use jaydebeapi query param tool (need new functions) and possibly sql util file

# in datastream.py implement a dependency graph and ensure that every input required by a step corresponds to an output from a previous step

# _share_load and _sftp_load methods contain almost identical logic for handling the different data_format types

# variable names in signatures when viewed together with all functions in call stack; clean up

# Look into `from __future__ import annotations` solution to type hint issue

# type hint all methods with short but helpful doc strings
