from dagster import Definitions
from feature_transaction_pipeline.defs.assets import transaction_features
from feature_transaction_pipeline.defs.resources import FeatureEngine
from feature_transaction_pipeline.defs.config import settings
from feature_transaction_pipeline.defs.schedules import job,schedule

defs = Definitions(
    assets=[transaction_features],
    jobs=[job],
    schedules=[schedule],
    resources={
        "engine": FeatureEngine(
            base_path=settings.BASE_DATA_PATH,
            num_chunks=settings.NUM_CHUNKS,
            output_dir=settings.OUTPUT_DIR,
        )
    }
)