from .config import settings
from datetime import datetime
from .resources import FeatureEngine
from dagster import (
    asset,
    AssetExecutionContext,
    ResourceParam,
    MaterializeResult,
    DailyPartitionsDefinition,
    BackfillPolicy,
    RetryPolicy,
    Backoff,
)

daily_partitions = DailyPartitionsDefinition(
    start_date=settings.DAILY_PARTITIONS_START_DATE
)


@asset(
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.single_run(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=300,
        backoff=Backoff.EXPONENTIAL
    ),
)
def transaction_features(
    context: AssetExecutionContext,
    engine: ResourceParam[FeatureEngine],
) -> MaterializeResult:

    try:
        pk_range = context.partition_key_range
        start_date = datetime.strptime(pk_range.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(pk_range.end, "%Y-%m-%d").date()
    except Exception:
        start_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
        end_date = start_date

    context.log.info(f"Processing from {start_date} to {end_date}")
    engine.run_pipeline(start_date, end_date, context.log)
    return MaterializeResult(
        metadata={
            "status": "success",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "chunks": engine.num_chunks,
            "output_path": engine.output_dir,
        }
    )