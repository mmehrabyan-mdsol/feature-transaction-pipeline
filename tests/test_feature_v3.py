import polars as pl
import os
from dagster import build_asset_context
from src.feature_transaction_pipeline.defs.assets import transaction_features
from src.feature_transaction_pipeline.defs.resources import FeatureEngine


def test_transaction_features_logic(tmp_path):
    # 1. SETUP: Create physical mock data in a temp directory
    # FeatureEngine scans files, so we must provide actual files for a true integration test.
    base_dir = tmp_path/"input"
    out_dir = tmp_path/"output"
    os.makedirs(base_dir/"detail/trx/fold=0",exist_ok=True)
    os.makedirs(out_dir)

    mock_df = pl.DataFrame({
        "client_id": ["1", "1", "1", "2"],
        "amount": [100.0, 200.0, 300.0, 50.0],
        "event_type": [1, 1, 1, 2],
        "event_time": [
            "2025-01-01 10:00:00",
            "2025-01-02 10:00:00",
            "2025-01-03 10:00:00",
            "2025-01-03 11:00:00"
        ]
    }).with_columns(pl.col("event_time").str.to_datetime())

    mock_df.write_parquet(base_dir / "detail/trx/fold=0/test.parquet")

    # 2. Initialize the real engine with test paths
    test_engine = FeatureEngine(
        base_path=str(base_dir),
        output_dir=str(out_dir),
        num_chunks=1
    )

    # 3. Build Dagster Context
    context = build_asset_context(
        partition_key="2025-01-03",
        resources={"engine": test_engine}
    )

    # 4. EXECUTE
    result = transaction_features(context)

    # 5. ASSERTIONS
    assert result.metadata["status"] == "success"

    # Read back the written parquet to verify calculations
    output_path = os.path.join(out_dir, "date=2025-01-03-2025-01-03", "part_0.parquet")
    actual_df = pl.read_parquet(output_path)

    assert len(actual_df) > 0
    user1 = actual_df.filter(pl.col("client_id") == "1")
    # Mean of 100, 200, 300 is 200
    assert user1["mean_amount_30d"][0] == 200.0