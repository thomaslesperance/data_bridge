# Project Configuration
general_config = {
    "project_root_dir": "/abs/path/to/project/root"
}

# Data Sources
sources = {
    "db1": {
        "user": "user",
        "password": "password",
        "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
        "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
        "driver_file": "openedge.jar",
    },
    "db2": {
        "user": "user",
        "password": "password",
        "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
        "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
        "driver_file": "openedge.jar",
    },
    "fileshare": {
        "mount_path" "/mnt/remote/share",
    },
    "google_drive_account": {
        "path": "/path/in/drive",
        "access_token": "/path/to/token.pickle",
    }
}
    
# Data Destinations
destinations = {
    "smtp_server": {
        "protocol": "smtp",
        "host": "smtp.domain.net",
        "port": "25",
        "default_sender_email": "jobs@example.com",
    },
    "fileshare": {
        "protocol" "fileshare",
        "mount_path" "/mnt/remote/share",
    },
    "vendor_sftp": {
        "protocol" "sftp",
        "host" "123.456.789.1011",
        "user" "user",
        "password" "password",
        "port" "22",
    },
    "google_drive_account": {
        "protocol": "google_drive",
        "path": "/path/in/drive",
        "access_token": "/path/to/token.pickle",
    },
}

# Job Configurations
jobs = {
    "example_sftp_job_simple": {
        "extract": {
            "sources": ["db1"],
            "query_files": ["example.sql"],
        },
        "load": {
            "destinations": ["vendor_sftp"],
            "remote_abs_file_paths": ["/dir/in/share/filename.xlsx"],
            "email_recipients": [],
            "sender_email": "",
        },
    },
    "example_smtp_job_simple": {},
    "example_share_job_simple": {},
    "example_job_complex": {},
}
