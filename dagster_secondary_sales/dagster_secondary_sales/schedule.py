import dagster as dg
from .jobs import secondary_sales_data

# Single schedule that runs the complete pipeline (raw extraction + dbt models)
secondary_sales_schedule = dg.ScheduleDefinition(
    job=secondary_sales_data,
    cron_schedule="30 4 * * *",
    name="secondary_sales_schedule",
)

# Export as a list
secondary_sales_schedules = [
    secondary_sales_schedule,
]