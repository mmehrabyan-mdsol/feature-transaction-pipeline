import datetime as dt
import polars as pl
from dagster import asset, DailyPartitionsDefinition, BackfillPolicy
from .utils import _load_trx_data,compute_daily_features,compute_backfill_features,row_count
from .config import settings
import os

daily_partitions = DailyPartitionsDefinition(start_date=settings.DAILY_PARTITIONS_START_DATE)


@asset
def load_trx_data(context) -> pl.LazyFrame:
    df = _load_trx_data(context)
    if row_count(df) == 0:
        raise ValueError("Loaded dataset is empty!")
    return df


@asset(partitions_def=daily_partitions, backfill_policy=BackfillPolicy.single_run())
def daily_features(context, load_trx_data: pl.LazyFrame) -> pl.DataFrame:
    """
    Computes daily features across the partition range.
    Returns a concatenated DataFrame of all daily results.
    """
    # Dagster range is inclusive start, exclusive end
    start = dt.date.fromisoformat(context.partition_key_range.start)
    end = dt.date.fromisoformat(context.partition_key_range.end)

    # Generate list of days to process
    partition_dates = [start + dt.timedelta(days=i) for i in range((end - start).days)]

    # Prepare output directory once
    out_dir = os.path.join(settings.BASE_DATA_PATH, settings.DAILY_OUTPUT_PATH)
    os.makedirs(out_dir, exist_ok=True)

    results = []

    for p_date in partition_dates:
        df_result = compute_daily_features(context, load_trx_data, p_date)

        if df_result.height == 0:
            context.log.warning(f"No data for {p_date}, skipping.")
            continue

        output_path = f"{out_dir}/daily_features_{p_date}.parquet"
        df_result.write_parquet(output_path, compression="snappy")
        context.log.info(f"Daily Features successfully saved: {output_path}")

        results.append(df_result)

    if results:
        return pl.concat(results)
    else:
        context.log.warning("No daily features computed for any partition.")
        return pl.DataFrame()


@asset
def backfill_features(context, load_trx_data: pl.LazyFrame) -> pl.DataFrame:

    df_result = compute_backfill_features(context, load_trx_data)
    #for saving locally
    out_dir = os.path.join(settings.BASE_DATA_PATH, settings.BACKFILL_OUTPUT_PATH)
    os.makedirs(out_dir, exist_ok=True)
    output_path = f"{out_dir}/backfill_features.parquet"
    df_result.write_parquet(output_path, compression="snappy")
    context.log.info(f"Backfill successfully saved: {output_path}")
    return df_result