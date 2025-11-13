from typing import Union

import dagster as dg


# @dg.schedule(cron_schedule="@daily", target="*")
# def daily_jaffle(context: dg.ScheduleEvaluationContext) -> Union[dg.RunRequest, dg.SkipReason]:
#     # return dg.SkipReason("Skipping. Change this to return a RunRequest to launch a run.")
#     return dg.RunRequest(run_key=None, run_config={})


@dg.schedule(cron_schedule="*/1 * * * *", target="*")
def every_minute(context: dg.ScheduleEvaluationContext) -> Union[dg.RunRequest, dg.SkipReason]:
    # return dg.SkipReason("Skipping. Change this to return a RunRequest to launch a run.")
    return dg.RunRequest(run_key=None, run_config={})
