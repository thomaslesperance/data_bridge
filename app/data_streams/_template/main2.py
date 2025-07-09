from pathlib import Path
from utils.models import TransformFunc, EmailFunc
from utils.data_stream import DataStream
from config import sources, destinations, jobs, PROJECT_LOG_FILE

# Define job-specific transformation logic or email message builder
transform_fn: TransformFunc | None = None
email_fn: EmailFunc | None = None


def main():
    job_name = Path(__file__).resolve().parent.name
    data_stream = DataStream(
        job_name=job_name,
        avail_sources=sources,
        avail_destinations=destinations,
        job=jobs[job_name],
        transform_fn=transform_fn,
        email_fn=email_fn,
        log_file=PROJECT_LOG_FILE,
    )
    data_stream.run()


if __name__ == "__main__":
    main()


# TODO:
# 1. Pydantic models
# 2. Logging system (find popular lightweight library; intelligent format)
# 3. Migrate jobs one-by-one
#     --Extract/load methods as needed
