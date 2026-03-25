from dagster import build_schedule_from_partitioned_job, define_asset_job
from .assets import transaction_features

job = define_asset_job("daily_features_job", selection=[transaction_features])

schedule = build_schedule_from_partitioned_job(job)