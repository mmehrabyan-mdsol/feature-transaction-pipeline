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
