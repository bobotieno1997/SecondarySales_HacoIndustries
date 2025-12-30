from pathlib import Path
from dagster_dbt import DbtProject

secondary_sales_project = DbtProject(
    project_dir=Path(__file__)
        .joinpath("..", "..", "..", "secondary_sales")
        .resolve(),
    packaged_project_dir=Path(__file__)
        .joinpath("..", "..", "dbt-project")
        .resolve(),
    profiles_dir=Path.home().joinpath(".dbt").resolve(), 
)

secondary_sales_project.prepare_if_dev()
