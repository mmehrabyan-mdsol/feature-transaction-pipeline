from huggingface_hub import hf_hub_download
import polars as pl
import datetime as dt
import tarfile
from .config import settings

# Download the tar.gz into project directory
def hf_hub_download_gz(context, path=settings.BASE_DATA_PATH):
    try:
        archive_path = hf_hub_download(
            repo_id="ai-lab/MBD-mini",
            filename="detail.tar.gz",
            repo_type="dataset",
            cache_dir="./data"
        )

        # Data will be extract into ./data
        with tarfile.open(archive_path, "r:gz") as tar:
            tar.extractall(path)
    except Exception as e:
        context.log.error(f"Failed to download dataset: {e}")

def row_count(df: pl.LazyFrame) -> int:
    return  df.select(pl.len()).collect().item()


def validate_columns(df: pl.LazyFrame | pl.DataFrame, required: list[str]) -> None:

    if isinstance(df, pl.LazyFrame):
        column_names = df.collect_schema().names()
    else:
        column_names = df.columns

    missing = [col for col in required if col not in column_names]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _load_trx_data(context) -> pl.LazyFrame:
    path = "data/detail/trx/fold=*/**/*.parquet"
    # path = os.path.join(settings.BASE_DATA_PATH,"/trx/fold=*/**/*.parquet")
    # path = f"{settings.BASE_DATA_PATH}/detail/trx/fold=*/**/*.parquet"
        # Lazy scan parquet files
    df = pl.scan_parquet(path)
    # This will ensure event_time is datetime and will add a normalized date column
    df = df.with_columns([
        pl.col("event_time").cast(pl.Datetime).dt.date().alias("date")
    ])
    # Validate schema
    validate_columns(df, ["client_id", "amount", "event_type", "date"])
    context.log.info(f"Loaded dataset with {row_count(df)} rows")
    return df



def compute_daily_features(context, df: pl.LazyFrame, date: dt.date) -> pl.DataFrame:
    """Compute daily features per client"""
    daily_df = df.filter(pl.col("date") == date)
    result = daily_df.group_by("client_id").agg([
        pl.col("amount").mean().alias("mean_amount"),
        pl.col("amount").mean().over("event_type").alias("mean_amount_by_event")
    ]).collect()
    context.log.info(f"Computed daily features for {date}, {len(result)} rows")
    return result

def compute_backfill_features(context, df: pl.LazyFrame) -> pl.DataFrame:
    """Compute backfill features with rolling 30-day window"""
    df = df.sort("date")
    result = df.group_by("client_id").agg([
        pl.col("amount").mean().alias("mean_amount_daily"),
        pl.col("amount").rolling_mean(window_size=30, min_samples=1).over("client_id").alias("mean_amount_30d")
    ]).collect()
    context.log.info(f"Computed backfill features, {len(result)} rows")
    return result


