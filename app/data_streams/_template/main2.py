from pathlib import Path
from utils.die import DIE
from config import sources, destinations, jobs

transform_fn = "Define job-specific transform function of standardized signature (use pandas!)"
email_msg = "Define email builder function of standardized signature"

def main():

    job_name = Path(__file__).resolve().parent.name.replace("DIE_", "")

    die = DIE(
        job_name=job_name,
        sources=sources, 
        destinations=destinations, 
        jobs=jobs, 
        transform_fn=transform_fn, 
        email_msg=email_msg
    )
    die.run()


if __name__ == "__main__":
    main()