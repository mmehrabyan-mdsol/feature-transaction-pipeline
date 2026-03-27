# assets/.py

from dagster import asset, DailyPartitionsDefinition, BackfillPolicy,AssetExecutionContext
from datetime import datetime
from .utils import load_parquet, compute_features
from .config import settings

import polars as pl
daily_partitions = DailyPartitionsDefinition(
    start_date=settings.DAILY_PARTITIONS_START_DATE
)


@asset(
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.single_run(),

)
def transaction_features(context: AssetExecutionContext):
    # --- LOGIC TO JOIN DAILY AND BACKFILL MODES ---

    try:
        partition_range = context.partition_key_range
        start_date = datetime.strptime(partition_range.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(partition_range.end, "%Y-%m-%d").date()
    except Exception as e:
        # Fallback for unexpected execution contexts
        start_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
        end_date = start_date
        context.log.info(f"Fallback Exception {e}")

    context.log.info(f"EXECUTION MODE: {'Daily' if start_date == end_date else 'Backfill'}")
    context.log.info(f"Target Range: {start_date} -> {end_date}")

    # --- MEMORY EFFICIENT CHUNKING ---

    base_lf = load_parquet()
    # used a 10-bucket split for even tighter memory control during big backfills
    num_chunks = settings.NUM_CHUNKS
    chunk_results = []

    for i in range(num_chunks):
        # 1. Filter by User Hash (Processes 10% of users at a time)
        # This ensures ALL history for a user is in this chunk for the rolling mean
        user_group_lf = base_lf.filter(pl.col("client_id").hash() % num_chunks == i)
        context.log.info(f"Executing chunk {i}")
        # 2. Apply the rolling logic
        # Polars handles the 'window' internally within this user group
        processed_lf = compute_features(user_group_lf, start_date, end_date)

        chunk_results.append(processed_lf)

    return pl.concat(chunk_results).collect()