from huggingface_hub import hf_hub_download
import polars as pl
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



def validate_columns(df: pl.LazyFrame, required: list[str]) -> None:
    column_names = df.collect_schema().names()
    missing = [col for col in required if col not in column_names]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def load_parquet() -> pl.LazyFrame:
    """Creates a unified LazyFrame from the parquet files."""

    files_pattern = f"{settings.BASE_DATA_PATH}/detail/trx/fold=*/**/*.parquet"
    lf = pl.scan_parquet(files_pattern)

    # Cast and normalize date early in the lazy plan
    lf = lf.with_columns([
        pl.col("event_time").cast(pl.Datetime).alias("event_time"),
        pl.col("event_time").cast(pl.Datetime).dt.date().alias("date")
    ])

    validate_columns(lf, ["client_id", "amount", "event_type", "date", "event_time"])
    return lf


def compute_features(lf: pl.LazyFrame, start_date, end_date) -> pl.LazyFrame:
    """
    Unified compute logic for both Daily and Backfill modes.
    Returns a LazyFrame to be collected by the parent asset.
    """

    processed_lf = (
        lf.sort(["client_id", "event_time"])

        # Compute 30-day rolling mean per client
        .with_columns([
            pl.col("amount")
            .rolling_mean(window_size=30, min_samples=1)
            .over("client_id")
            .alias("mean_amount_30d"),

            # Compute mean per event type per client
            pl.col("amount")
            .mean()
            .over(["client_id", "event_type"])
            .alias("mean_amount_by_event")
        ])

        # Filter for the specific range requested by Dagster
        .filter(
            (pl.col("date") >= start_date) &
            (pl.col("date") <= end_date)
        )

        #  Aggregate to daily level
        .group_by(["client_id", "date"])
        .agg([
            pl.col("amount").mean().alias("daily_mean_amount"),
            pl.col("mean_amount_30d").last(),
            pl.col("mean_amount_by_event").mean()
        ])
    )

    return processed_lf