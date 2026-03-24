import dagster as dg
from .assets import load_trx_data, daily_features, backfill_features

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        assets=[load_trx_data, daily_features, backfill_features],
        resources={}
    )
