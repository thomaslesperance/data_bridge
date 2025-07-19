from pathlib import Path

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
    },
    # Job Configurations
    "streams": {
        "example_simple_stream": {
            "log_level": 20,
            "extract_tasks": {
                "get_data": {
                    "source": "db1",
                    "dependencies": "query.sql",
                }
            },
            "load_tasks": {
                "send_data": {
                    "destination": "sftp_server",
                    "dependencies": "report.csv",
                }
            },
        },
        "example_complex_stream": {
            "log_level": 10,
            "extract_tasks": {
                "get_student_data": {
                    "source": "db1",
                    "dependencies": ["grades.sql", "students.sql"],
                },
                "get_legacy_export": {
                    "source": "fileshare",
                    "dependencies": "remote/rel/path/export_file.csv",
                },
            },
            "load_tasks": {
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
    },
}
