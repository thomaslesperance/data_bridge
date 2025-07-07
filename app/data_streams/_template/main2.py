from pathlib import Path
from utils.data_stream import Data_Stream
from config import sources, destinations, jobs, PROJECT_LOG_FILE

transform_fn = (
    "Define job-specific transform function of standardized signature (use pandas!)"
)
email_msg = "Define email builder function of standardized signature"


def main():

    job_name = Path(__file__).resolve().parent.name.replace("DIE_", "")

    die = Data_Stream(
        job_name=job_name,
        sources=sources,
        destinations=destinations,
        job=jobs[job_name],
        transform_fn=transform_fn,
        email_msg=email_msg,
        log_file=PROJECT_LOG_FILE,
    )
    die.run()


if __name__ == "__main__":
    main()
