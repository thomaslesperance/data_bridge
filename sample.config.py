from pathlib import Path

# abs paths and remote rel paths are strings, local rel paths are Path objects
PROJECT_ROOT = Path(__file__).parent.resolve()
CREDS_DIR = PROJECT_ROOT / "creds"
PROJECT_LOG_FILE = PROJECT_ROOT / "app" / "data_streams" / "data_bridge.log"

# Data Sources
sources = {
    "db1": {
        "type": "sql",
        "user": "user",
        "password": "password",
        "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
        "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
        # You do not need to specify JAR file abs path if CLASSPATH set in ~/.zprofile
    },
    "db2": {
        "type": "sql",
        "user": "user",
        "password": "password",
        "conn_string": "jdbc:datadirect:openedge://domain.net:12345;databaseName=databaseName",
        "driver_name": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
    },
    "fileshare": {
        "type": "fileshare",
        "mount_path": "/abs/path/to/share/root",
    },
    "google_drive_account": {
        "type": "google_drive",
        "access_token": CREDS_DIR / "token.pickle",
    },
    "sftp_server": {
        "type": "sftp",
        "user": "user",
        "password": "password",
        "host": "123.456.789.1011",
        "port": "22",
    },
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
        "protocol": "fileshare",
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
}

# Job Configurations
jobs = {
    "example_simple_job": {
        "extract": {"db1": "students.sql"},
        "load": {"emails": ("smtp_server", "report.csv")},
    },
    "example_complex_job": {
        "extract": {
            "db1": ["grades.sql", "students.sql"],
            "db2": "teachers.sql",
            "fileshare": "remote/rel/path/export_file.csv",
            "sftp_server": "remote/rel/path/file.xlsx",
        },
        "load": {
            "sftp_server": ["formatted_grades.csv", "active_teachers.csv"],
            "google_drive_account": "remote/rel/path/summary.csv",
            "emails": [
                ("teacher_email", "smtp_server", "email_1_data.csv"),
                (
                    "admin_email",
                    "smtp_server",
                    "email_2_data_A.csv",
                    "email_2_data_B.csv",
                ),
            ],
        },
    },
}
