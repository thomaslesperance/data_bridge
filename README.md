# Data Bridge

## Overview (OUTDATED)
This project is a data integration pipeline ("data bridge") composed of one or more data integration processes ("streams"). The data streams consist of Python scripts that 1), define all stream-specific logic to be performed on extracted data, 2) instantiate a DataStream class using a central configuration file and call its `run` method. Using only the central configuration file, a user may define individual streams that are themselves composed of one or more data extraction tasks and one or more data loading tasks. These tasks will rely on other sections of the configuration file that define data source and destination configuration in addition to methods in the Extractor and Loader classes that carry out data transfer of the requested protocol.

Each DataStream instance is composed of instantiated Extractor and Loader classes. The configuration passed to the DataStream constructor is fed down into the constructors for these components.

For each data stream, the user must define the new stream in the configuration file and implement the stream-specific logic in the main.py script of the data stream. Additonal extract or load methods can be added to the Extractor and Loader classes as the need arises.

## Flow of Data in Data Stream (OUTDATED)
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

## "example_stream"
Configuration for an example data stream is given in the sample.config.yaml file. The following illustrates the transformation of data within the data stream using this example:
     
    ...

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
