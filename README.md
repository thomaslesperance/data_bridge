# Data Bridge

## Overview
This project is a data integration pipeline ("bridge") composed of one or more data integration elements ("Streams"). The DIEs consist primarily of a single Python script that calls on utility functions to extract, transform, and load data. A config file should be set by the user to define available data source and destination details, including credentials, ports, connection strings, and so on. In this file, job-specific configuration is also defined specifying which source(s) and destination(s) each job will integratate and dependencies that will be used to facilitate the integration (e.g., query files, remote file paths to source files, etc.). 

## Install in Production Environment
In clean, working development environment:
    pip3 freeze > requirements.txt
In production environment:
    cd /path/to/project/parent
    git clone <repo URL>
    cd <repo name>
    python3 -m venv venv
    MacOS: source venv/bin/activate
    Windows: .\venv\Scripts\activate
    pip3 install -r requirements.txt
    pip3 install . --no-deps

# DIAGRAMATIC REPRESENTATION OF CUSTOM LOGIC INTERACTION WITH PIPELINE CODE:

 Extractor.extract()
     │
     └─> returns flat dict: {"students.sql": <PipelineData>, ...}
                                              │
                                              ▼
                          transform_fn(extracted_data)
                                │
                                ▼
        returns flat dict: {"report.csv": <PipelineData>, "email_data.csv": <PipelineData>, ...}
                 │
                 ▼
 Loader.load(all_load_data)
     │
     ├─> (For an SFTP Task) ───> _sftp_load(PipelineData for "report.csv") ───> [SFTP Server]
     │
     └─> (For an Email Task) ───> email_builder_fn({"email_data.csv": <PipelineData>})
                                      │
                                      ▼
                                  Message Object
                                      │
                                      ▼
                        _smtp_load(Message) ───────────────> [SMTP Server]


## "example_complex_job"
A simple and a comlex job config example are given in the sample.config.py file. The following illustrates the transformation of data using this example:

The file /app/data_streams/_template/main.py contains code that correctly implements an example
transformation for "example_complex_job" config in sample.config.py.
The transform function will receive this flat dict from the DataStream instance extractor...
extracted_data = {
  "grades.sql": <PipelineData object>,
  "students.sql": <PipelineData object>,
  "remote/rel/path/export_file.csv": <PipelineData object>,
}
And, after performing some example logic, it should hand this flat dict to the DataStream instance loader...
all_load_data = {
  "formatted_grades.csv": <PipelineData object>,
  "active_teachers.csv": <PipelineData object>,
  "remote/rel/path/summary.csv": <PipelineData object>,
  "email_1_data.csv": <PipelineData object>,
  "email_2_data_A.csv": <PipelineData object>,
  "email_2_data_B.csv": <PipelineData object>,
}
The result is a job-specific mapping of data source-dependency to data destination-dependency mapping,
in other words, the transform step is solely responsible for ensuring the load dependencies for the job
are composed of the correct components of extracted data.

Example email functions for the example job "example_complex_job":
The passed email_data dict keys being dependency names for the email-based load task
and the values being associated PipelineData
For build_teacher_email
    email_data = {"email_1_data.csv": <PipelineData object>}
For build_admin_email
    email_data = {
        "email_2_data_A.csv": <PipelineData object>,
        "email_2_data_B.csv": <PipelineData object>
    }
For build_status_email
    email_data = {}

For jobs["example_complex_job"]:
    extract_tasks = [
    {
      "source_name": "db1",
      "source_config": {
          "type": "sql",
          "user": "user",
          "password": "password",
          "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
          "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
          # You do not need to specify JAR file abs path if CLASSPATH set in ~/.zprofile
      },
      "method": self._sql_extract,
      "dependency": "grades.sql",
    },
    {
      "source_name": "db1",
      "source_config": {
          "type": "sql",
          "user": "user",
          "password": "password",
          "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
          "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
          # You do not need to specify JAR file abs path if CLASSPATH set in ~/.zprofile
      },
      "method": self._sql_extract,
      "dependency": "students.sql",
    },
    {
      "source_name": "fileshare",
      "source_config": {
          "type": "fileshare",
          "mount_path": "/abs/path/to/share/root",
    },
      "method": self._fileshare_extract,
      "dependency": "remote/rel/path/export_file.csv",
    },
    ]
    
    extracted_data = {
      "grades.sql": <PipelineData object>,
      "students.sql": <PipelineData object>,
      "remote/rel/path/export_file.csv": <PipelineData object>,
    }

For jobs["example_complex_job"]:
    load_tasks = [
    {
      "dest_name": sftp_server,
      "dest_config": {
        "protocol": "sftp",
        "host": "123.456.789.1011",
        "user": "user",
        "password": "password",
        "port": "22",
    },
      "method": self._sftp_load,
      "dependencies": ["formatted_grades.csv", "active_teachers.csv"]
      "email_builder": None,
    },
    {
      "dest_name": google_drive_account,
      "dest_config": {
        "protocol": "google_drive",
        "access_token": CREDS_DIR / "token.pickle",
    },
      "method": self._drive_load,
      "dependencies": "remote/rel/path/summary.csv"
      "email_builder": None,
    },
    {
      "dest_name": smtp_server,
      "dest_config": {
        "protocol": "smtp",
        "host": "smtp.domain.net",
        "port": "25",
        "default_sender_email": "jobs@example.com",
    },
      "method": self._smtp_load,
      "dependencies": "email_1_data.csv"
      "email_builder": "build_teacher_email",
    },
      "dest_name": smtp_server,
      "dest_config": {
        "protocol": "smtp",
        "host": "smtp.domain.net",
        "port": "25",
        "default_sender_email": "jobs@example.com",
    },
      "method": self._smtp_load,
      "dependencies": ["email_2_data_A.csv", "email_2_data_B.csv"]
      "email_builder": "build_admin_email",
    },
      "dest_name": smtp_server,
      "dest_config": {
        "protocol": "smtp",
        "host": "smtp.domain.net",
        "port": "25",
        "default_sender_email": "jobs@example.com",
    },
      "method": self._smtp_load,
      "dependencies": None
      "email_builder": "build_status_email",
    },
    ]
    
    all_load_data = {
      "formatted_grades.csv": <PipelineData object>,
      "active_teachers.csv": <PipelineData object>,
      "remote/rel/path/summary.csv": <PipelineData object>,
      "email_1_data.csv": <PipelineData object>,
      "email_2_data_A.csv": <PipelineData object>,
      "email_2_data_B.csv": <PipelineData object>,
    }