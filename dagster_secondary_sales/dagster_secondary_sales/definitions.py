# from dagster import Definitions, load_assets_from_modules
# from dagster_dbt import DbtCliResource

# from . import assets
# from .jobs import secondary_sales_data
# from .schedule import secondary_sales_schedules
# from .project import secondary_sales_project

# all_assets = load_assets_from_modules([assets])

# defs = Definitions(
#     assets=all_assets,
#     jobs=[secondary_sales_data],
#     schedules=secondary_sales_schedules,
#     resources={
#         "dbt": DbtCliResource(
#             project_dir=secondary_sales_project.project_dir,
#             profiles_dir=secondary_sales_project.profiles_dir,
#         )
#     },
# )


from dagster import Definitions, load_assets_from_modules, define_asset_job, AssetSelection
from dagster_dbt import DbtCliResource

from . import assets
from .jobs import secondary_sales_data
from .schedule import secondary_sales_schedules
from .project import secondary_sales_project

all_assets = load_assets_from_modules([assets])
all_jobs= [secondary_sales_data]
all_schedules=[secondary_sales_schedules]


defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=secondary_sales_schedules,
    resources={
        "dbt": DbtCliResource(
            project_dir=secondary_sales_project.project_dir,
            profiles_dir=secondary_sales_project.profiles_dir,
        )
    },
)