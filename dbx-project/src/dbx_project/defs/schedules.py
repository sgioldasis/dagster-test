
import dagster as dg

# Create a job specifically for the check
freshness_check_job = dg.define_asset_job(
    name="customers_freshness_check_job",
    selection=dg.AssetSelection.checks_for_assets(dg.AssetKey(["target", "main", "customers"]))
)

# Schedule the job
customers_freshness_schedule = dg.ScheduleDefinition(
    name="customers_freshness_schedule",
    job=freshness_check_job,
    cron_schedule="* * * * *",
)
