from .config import settings
from datetime import datetime
from feature_transaction_pipeline.defs.resources import FeatureEngine
from dagster import (asset, AssetExecutionContext,ResourceParam, MaterializeResult,DailyPartitionsDefinition,
                     BackfillPolicy, RetryPolicy,Backoff)


daily_partitions = DailyPartitionsDefinition(
    start_date=settings.DAILY_PARTITIONS_START_DATE
)

@asset(
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.single_run(),
    retry_policy=RetryPolicy(max_retries=3, delay=300, backoff=Backoff.EXPONENTIAL),
)
def transaction_features(
        context: AssetExecutionContext,
        engine: ResourceParam[FeatureEngine],

) -> MaterializeResult:
    # 1. Extract Date Range from Context
    try:
        pk_range = context.partition_key_range
        start_date = datetime.strptime(pk_range.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(pk_range.end, "%Y-%m-%d").date()
    except Exception as e:
        start_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
        end_date = start_date
        context.log.info(f"Fallback Exception {e}")

    # 2. Delegate to the Resource
    engine.run_backfill(start_date, end_date, context.log)

    # 3. Return Metadata (Production Insight)
    return MaterializeResult(
        metadata={
            "status": "Success",
            "chunks": engine.num_chunks,
            "output_path": engine.output_dir
        }
    )