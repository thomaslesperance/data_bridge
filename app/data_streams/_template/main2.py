import pandas as pd
import io
from pathlib import Path
from email.message import Message
from utils.models import PipelineData
from utils.data_stream import DataStream
from config import sources, destinations, jobs, PROJECT_LOG_FILE

# ---------- JOB-SPECIFIC LOGIC -----------------
## EXAMPLE:
## Here are functions that correctly simulate the transform step for the "example_complex_job"
## job in the "jobs" dict of the sample.config.py file:

# The transform function will receive this from the DataStream instance extractor...
# extracted_data = {
#   "grades.sql": <PipelineData object>,
#   "students.sql": <PipelineData object>,
#   "teachers.sql": <PipelineData object>,
#   "remote/rel/path/export_file.csv": <PipelineData object>,
#   "remote/rel/path/file.xlsx": <PipelineData object>,
# }

# And, after performing some example logic, it should hand this to the DataStream instance loader...
# all_load_data = {
#   "formatted_grades.csv": <PipelineData object>,
#   "active_teachers.csv": <PipelineData object>,
#   "remote/rel/path/summary.csv": <PipelineData object>,
#   "email_data.csv": <PipelineData object>,
# }

# The result is a job-specific mapping of data source-dependency to data destination-dependency mapping,
# in other words, the transform step is solely responsible for ensuring the load dependencies for the job
# are composed of the correct components of extracted data.


def transform(extracted_data: dict[str, PipelineData]) -> dict[str, PipelineData]:
    """
    An example transform function that merges hypothetical student and grade data taken from
    SQL extractions and then prepares a CSV report in memory for loading.
    """
    grades_df = extracted_data["grades.sql"].content
    students_df = extracted_data["students.sql"].content
    final_report_df = pd.merge(students_df, grades_df, on="student_id")

    output_buffer = io.StringIO()
    final_report_df.to_csv(output_buffer, index=False)
    output_buffer.seek(0)

    # The key, "report.csv", must match a dependency in the job's 'load' config.
    return {
        "report.csv": PipelineData(
            data_format="in_memory_stream",
            content=io.BytesIO(output_buffer.getvalue().encode("utf-8")),
        ),
    }


# The email function, in the case of the "example_complex_job", would expect "email_data.csv"
# as the argument passed as email_data parameter.
def email_fn(email_data: dict[str, PipelineData]) -> Message:
    return None


# ---------- TEMPLATE CODE-----------------------
def main():
    job_name = Path(__file__).resolve().parent.name
    data_stream = DataStream(
        job_name=job_name,
        avail_sources=sources,
        avail_destinations=destinations,
        job=jobs[job_name],
        transform=transform,
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
