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
        "extract": {
            "get_data": {
                "source": "db1",
                "dependencies": "query.sql",
            }
        },
        "load": {
            "send_data": {
                "destination": "sftp_server",
                "dependencies": "report.csv",
            }
        },
    },
    "example_complex_job": {
        "extract": {
            "get_course_data": {
                "source": "db1",
                "dependencies": ["grades.sql", "students.sql"],
            },
            "get_legacy_export": {
                "source": "fileshare",
                "dependencies": "remote/rel/path/export_file.csv",
            },
        },
        "load": {
            "sftp_upload_grades": {
                "destination": "sftp_server",
                "dependencies": ["formatted_grades.csv", "active_teachers.csv"],
            },
            "drive_summary_upload": {
                "destination": "google_drive_account",
                "dependencies": "remote/rel/path/summary.csv",
            },
            "teacher_notification": {
                "destination": "smtp_server",
                "dependencies": "email_1_data.csv",
                "email_builder": "build_teacher_email",
            },
            "admin_summary_email": {
                "destination": "smtp_server",
                "dependencies": ["email_2_data_A.csv", "email_2_data_B.csv"],
                "email_builder": "build_admin_email",
            },
            "job_complete_notification": {
                "destination": "smtp_server",
                "dependencies": None,
                "email_builder": "build_status_email",
            },
        },
    },
}
