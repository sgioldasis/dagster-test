from typing import Union

import dagster as dg


# @dg.job
# def ingestion_job():
# Define a job that selects all assets in the "ingestion" group
ingestion_job = dg.define_asset_job(
    name="ingestion_job",
    selection="group:ingestion"
)

# @dg.schedule(cron_schedule="@daily", target="*")
# def daily_jaffle(context: dg.ScheduleEvaluationContext) -> Union[dg.RunRequest, dg.SkipReason]:
#     # return dg.SkipReason("Skipping. Change this to return a RunRequest to launch a run.")
#     return dg.RunRequest(run_key=None, run_config={})


# @dg.schedule(cron_schedule="*/1 * * * *", target="ingestion")
# def every_minute(context: dg.ScheduleEvaluationContext) -> Union[dg.RunRequest, dg.SkipReason]:
#     # return dg.SkipReason("Skipping. Change this to return a RunRequest to launch a run.")
#     return dg.RunRequest(run_key=None, run_config={})

@dg.schedule(cron_schedule="*/1 * * * *", job=ingestion_job)
def every_minute(context: dg.ScheduleEvaluationContext) -> Union[dg.RunRequest, dg.SkipReason]:
    # return dg.SkipReason("Skipping. Change this to return a RunRequest to launch a run.")
    return dg.RunRequest(run_key=None, run_config={})
