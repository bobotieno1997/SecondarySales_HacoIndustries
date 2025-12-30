from pathlib import Path
import os
import sys
from dagster import AssetKey

import pandas as pd
from dotenv import load_dotenv

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
)

from dagster_dbt import (
    dbt_assets,
    DbtCliResource,
)

from .project import secondary_sales_project

# ------------------------------------------------------------------
# Project root & environment
# ------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from .db.db_config import engine_pg  # noqa: E402

load_dotenv()

# ------------------------------------------------------------------
# RAW ASSET
# ------------------------------------------------------------------
@asset(
    name="secondary_sales_raw",
    description="Loads all sheets from Secondary Sales Excel into raw schema tables",
    compute_kind="pandas",
    op_tags={"dagster/priority": "10"}
)

def secondary_sales_raw(context: AssetExecutionContext) -> Output:

    file_name = "Secondary Sales"
    folder_path = os.getenv("folder_path")

    if not folder_path:
        raise ValueError("Environment variable 'folder_path' is not set")

    file_path = os.path.join(folder_path, f"{file_name}.xlsx")

    xl = pd.ExcelFile(file_path)
    sheets = xl.sheet_names

    loaded_tables: dict[str, int] = {}

    for sheet in sheets:
        df = pd.read_excel(file_path, sheet_name=sheet)

        table_name = (
            sheet.strip()
            .lower()
            .replace(" ", "_")
        )

        df.to_sql(
            name=table_name,
            con=engine_pg,
            schema="raw",
            if_exists="replace",
            index=False,
        )

        row_count = len(df)
        loaded_tables[f"raw.{table_name}"] = row_count

        context.log.info(
            f"Loaded {row_count} rows into raw.{table_name}"
        )

    return Output(
        value=None,
        metadata={
            "file_path": file_path,
            "tables_loaded": MetadataValue.json(loaded_tables),
        },
    )

# ------------------------------------------------------------------
# DBT ASSETS
# ------------------------------------------------------------------
@dbt_assets(
    manifest=secondary_sales_project.manifest_path,
)
def secondary_sales_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    yield from dbt.cli(
        ["build"],
        context=context,
    ).stream()
