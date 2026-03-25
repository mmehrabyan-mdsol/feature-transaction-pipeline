from dagster import Definitions
from .assets import transaction_features
from .parquet_io_manager import parquet_io_manager
from .config import settings

defs = Definitions(
    assets=[transaction_features],
    resources={
        "io_manager": parquet_io_manager.configured({
            "base_path": settings.OUTPUT_PATH
        })
    }
)