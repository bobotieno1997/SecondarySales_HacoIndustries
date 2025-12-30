from dagster import define_asset_job, AssetSelection, in_process_executor
from .assets import secondary_sales_dbt_assets


# Combine the raw asset key with the list of dbt keys
secondary_sales_data = define_asset_job(
    name="secondary_sales_refresh",
    selection=AssetSelection.keys("secondary_sales_raw") | AssetSelection.keys(*secondary_sales_dbt_assets.keys),
    executor_def=in_process_executor
)
