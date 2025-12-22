
import dagster as dg

# Create a job to materialize raw_customers and its dependent assets
raw_customers_job = dg.define_asset_job(
    name="raw_customers_job",
    selection=dg.AssetSelection.keys(
        dg.AssetKey(["target", "main", "raw_customers"]),
        dg.AssetKey(["target", "main", "stg_customers"]),
        dg.AssetKey(["target", "main", "customers"])
    )
)

# Schedule the job to run every minute
raw_customers_schedule = dg.ScheduleDefinition(
    name="raw_customers_schedule",
    job=raw_customers_job,
    cron_schedule="* * * * *",
)
