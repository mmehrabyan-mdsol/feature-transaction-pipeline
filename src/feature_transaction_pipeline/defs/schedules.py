from dagster import build_schedule_from_partitioned_job, define_asset_job
from .assets import daily_features


daily_job = define_asset_job(
    name="daily_features_job",
    selection=[daily_features]
)

daily_schedule = build_schedule_from_partitioned_job(
    daily_job,
    hour_of_day=11,
    # execution_timezone="Asia/Yerevan"
)