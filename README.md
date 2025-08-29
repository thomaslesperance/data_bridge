# Data Bridge

## Overview
This project is a data integration pipeline ("data bridge") composed of one or more data integration processes ("streams"). The data streams consist of Python scripts that 1), define all stream-specific logic to be performed on extracted data, 2) instantiate a DataStream class using a central configuration file and call its `run` method. Using only the central configuration file, a user may define individual streams that are themselves composed of one or more data extraction tasks and one or more data loading tasks. These tasks will rely on other sections of the configuration file that define data source and destination configuration in addition to methods in the Extractor and Loader classes that carry out data transfer of the requested protocol.

Each DataStream instance is composed of instantiated Extractor and Loader classes. The configuration passed to the DataStream constructor is fed down into the constructors for these components.

For each data stream, the user must define the new stream in the configuration file and implement the stream-specific logic in the main.py script of the data stream. Additonal extract or load methods can be added to the Extractor and Loader classes as the need arises.

## Flow of Data in Data Stream:
```
data_stream.extractor.extract()
                        │
                        ▼
          returns flat dict: {"students.sql": <StreamData>, ...}
                                            │
                                            ▼
                data_stream.transform(extracted_data)
                                │
                                ▼
                  returns flat dict: {"report.csv": <StreamData>, "email_data.csv": <StreamData>, ...}
                                                  │
                                                  ▼
                                Loader.load(all_load_data)
                                        │
                                        ├─> (Normal load task) ─-> _sftp_load(<StreamData> for "report.csv") ──> [SFTP Server]
                                        │
                                        └─> (Email load task)  ──> email_builder_fn({"email_data.csv": <StreamData>})
                                                                            │
                                                                            ▼
                                                        _smtp_load(email.message.Message) ──> [SMTP Server]
```
## Features
* Pydantic - models are defined for configuration state and to standardize data the flowing within the data stream
* Pandas - the extract and load methods take advantage of pandas where possible
* In-memory data processing - unless chunking of larger files is needed, all data flows into and out of the data stream without diskIO
* Centralized log system - all data streams set up in the data bridge instance will log to a single log file with a readable format
* Elegant error-handling - errors are handled using class-based decorator factories to improve code readability
* Modularity - functions are pure where possible, classes or methods can be extended for this project or dropped easily into other projects

## "example_complex_stream"
Configuration for an example data stream is given in the sample.config.py file. The following illustrates the transformation of data within the data stream using this example:

### Extractor instance -> user-defined `transform_fn`:
```
extracted_data = {
  "grades.sql": <StreamData object>,
  "students.sql": <StreamData object>,
  "remote/rel/path/export_file.csv": <StreamData object>,
}
```
The transform step ensures the load dependencies for the stream are composed of the correct components of extracted data.

### user-defined `transform_fn` -> Loader instance:
```
all_load_data = {
  "formatted_grades.csv": <StreamData object>,
  "active_teachers.csv": <StreamData object>,
  "remote/rel/path/summary.csv": <StreamData object>,
  "email_1_data.csv": <StreamData object>,
  "email_2_data_A.csv": <StreamData object>,
  "email_2_data_B.csv": <StreamData object>,
}
```
Data for build_teacher_email:
```
    email_data = {"email_1_data.csv": <StreamData object>}
```
Data for build_admin_email:
```
    email_data = {
        "email_2_data_A.csv": <StreamData object>,
        "email_2_data_B.csv": <StreamData object>
    }
```
Data for build_status_email:
```
    email_data = {}
```

## Install in Production Environment
```
  cd /path/to/project/parent
  git clone <repo URL>
  cd <repo name>
  python3 -m venv venv
    MacOS: source venv/bin/activate
    Windows: .\venv\Scripts\activate
  pip3 install -r requirements.txt
  pip3 install . --no-deps  
```
## Project TODO:
### 1. Write unit tests for core pipeline components
  Potential units for unit tests:
  * DataStream()
  * DataStream.instance._validate_config()
  * DataStream.instance.run()
  * Extractor()
  * Extractor.instance.extract()
  * Loader()
  * Loader.instance.load()

### 2. Replace legacy integrations one-by-one
  * Extract/load methods as needed
  * Start with IA as good example that uses pandas
  * Ensure max encryption possible on all inbound/outbound data
  * Ensure all optimization principles followed (SQL, disk IO, pandas)
  * Test that each works and record performance metrics

### 3. Establish secret management
  * Need separate server for environment variable injection?*
  * Should data_bridge instance be in a docker container?

### 4. Establish CI/CD
  * Do some research...


## Note 7-24-25
Two potential issues I've noticed after pushing this project farther along:
1. The current set up does not allow for query parameters. It would not be difficult to allow a space for static query parameters to 
be defined in the stream config. But many existing jobs seems to use dynamic parameters for their queries.
2. Building on the first, several existing jobs use the results from one extraction to generate a list of parameters for a successive query.
The current setup does not allow for that without totally breaking the system and importing extractor logic into the main.py file.

Hopefully these will be a non-issue with my efficient query methodology, namely, running several small index-based queries and then 
using pandas to filter and combine data between multiple dataframes.


## Dynamic Configuration Rules

The configuration file supports several methods for creating dynamic values at runtime.

### Parameter Resolution (`macro:` and `step:`)

The following parameter keys support dynamic value resolution:

  * `query_params`
  * `path_params`
  * `email_params`

Values within these keys can be resolved in two ways:

1.  **`macro:MACRO_NAME`**: Executes a predefined function. The return value is used.
    ```yaml
    path_params:
      school_year: "macro:SCHOOL_YEAR"
    ```
2.  **`step:STEP_OUTPUT_NAME`**: Uses the data output from a previous step in the stream.
    ```yaml
    query_params:
      ids: "step:high_achiever_IDs"
    ```

### String Interpolation (`::placeholder::`)

The following keys are treated as templates where placeholders can be substituted:

  * `remote_file_path`
  * `query_file_path`

These keys can contain placeholders like `::placeholder_name::`. The placeholder name must match a key from a corresponding `..._params` dictionary for that step.

```yaml
# The value for "school_year" from path_params will be
# substituted into the ::school_year:: placeholder.

remote_file_path: "archive/reports/admin_report_::school_year::.csv"
path_params:
  school_year: "macro:SCHOOL_YEAR"
```

-----


# --- Config file rules ---
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
#       path_params: macro, step (str's could just be typed into the filepath)
#       query_params: str, macro, step
#       email_params str, macro, step
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

# change to YAML config file
# use environment variables
# create prepare_config module that:
#   reads yaml
#   selects requested sources & dests
#   enriches with environment variables
#   runs pydantic validation
#   returns validated stream_config object

# email builder helper
# change gets to [] except where defaults possible
# figure out LogAndTerminate and if log and re-raise gives clean (or repeated and hard to read) error blocks in log file
# Factor out and centralize param resolution
# Use jaydebeapi query param tool (need new functions) and possibly sql util file
# in datastream.py implement a dependency graph and ensure that every input required by a step corresponds to an output from a previous step
# _share_load and _sftp_load methods contain almost identical logic for handling the different data_format types
# variable names in signatures when viewed together with all functions in call stack; clean up
# Look into `from __future__ import annotations` solution to type hint issue
# type hint all methods with short but helpful doc strings

# Put jobs in
# Docker
# Docker Hub
# Docker Compose (lists each job as individual services)
# Makefile?
# Azure Key Vault, Azure Arc, Azure SDK (in Python?)
# Github Actions
# Windows Task Scheduler (schedule different services from Docker compose or)
