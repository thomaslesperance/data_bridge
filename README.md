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
# Dynamic options:
#       macro: run a pre-defined fuction with given name to use it's return value here
#       step: use the data in the data_store object under this name

# Remote file paths for extracting:
# -Must be strings
# -Must be relative (with respect to source config mount path)
# -Must point to a file, not a directory (since you're grabbing a file from the destination)

# Remote file paths for loading:
# -Must be strings
# -Must be relative (with respect to dest config mount path)
# -Must point to a directory, not a file (since filename determined elsewhere)

# Local file paths for query files:
# -Must be strings
# -Must be relative (with respect to main.py file of running data_stream)
# -Must point to a file and not a directory
# -Must end in '.sql'

# Extract steps:
#   Give output in a set data_format based on protocol
#   Output a single StreamData object

# Transform steps:
#   Change data_format to what receiving load step(s) expect
#   Input and output multiple StreamData objects
#   Manually set file names

# Load steps:
#   Expect input in a set data_format based on protocol
#   Input a single StreamData object
#   Options for 'recipients' key where protocol='smtp':
#       Hard-coded email address
#       Hard-coded list of email addresses
#       A "step:" value that references a previous step output: StreamData.data_format="python_list"

# Query params:
#   A hard-coded value in the config file
#   A "macro:" value
#   A "step:" value that references a previous step output: StreamData.data_format="python_<any>"

# Log levels
# https://docs.python.org/3/library/logging.html#logging-levels


# --- TODO ---:
# Clean up where possible (extractor and loader)
    # Factor out data_format checks from each load and extract method by adding a second item to each key
    # in method map that specifies which data_format it accepts
# type hint all methods, include short but helpful doc strings
# clean up variable names in signatures across call stacks
# implement dependency graph in config.py to ensure that every input required by a step corresponds to an output from a previous step
# Put test jobs in for each extract/load type
# Write unit tests
# Complete README

# --- FUTUE TODO ---:
# Docker
# Docker Hub
# Kubernetes / Docker Swarm
# Github Actions
# Azure Key Vault, Azure Arc, Azure SDK (in Python?)
# Scheduling system (schedule different services from Docker compose or)
# ? Docker Compose (development) (lists each job as individual services?)
# ? Makefile
