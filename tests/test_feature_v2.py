import polars as pl
from datetime import date
from dagster import build_asset_context, PartitionKeyRange
from src.feature_transaction_pipeline.defs.assets import transaction_features


def test_transaction_features_logic(monkeypatch):
    # Create Mock Data
    mock_data = pl.LazyFrame({
        "client_id": [1, 1, 1, 2],
        "amount": [100.0, 200.0, 300.0, 50.0],
        "event_type": ["transfer", "transfer", "transfer", "pos"],
        "event_time": [
            "2025-01-01 10:00:00",
            "2025-01-02 10:00:00",
            "2025-01-03 10:00:00",
            "2025-01-03 11:00:00"
        ]
    }).with_columns([
        pl.col("event_time").str.to_datetime().alias("event_time"),
        pl.col("event_time").str.to_datetime().dt.date().alias("date")
    ])

    monkeypatch.setattr("src.feature_transaction_pipeline.defs.assets.load_parquet", lambda: mock_data)
    # Dagster Context for a Backfill range
    context = build_asset_context(
        partition_key_range=PartitionKeyRange(start="2025-01-01", end="2025-01-03")
    )

    # Execute the Asset
    result_df = transaction_features(context)

    # Assertions
    assert isinstance(result_df, pl.DataFrame)
    user1_jan3 = result_df.filter(
        (pl.col("client_id") == 1) & (pl.col("date") == date(2025, 1, 3))
    )
    assert user1_jan3["mean_amount_30d"][0] == 200.0

    def test_transaction_features_empty_data(mocker):
        # 1. Mock an empty LazyFrame WITH the correct schema
        # Polars needs the schema to know what columns to expect even if rows=0
        empty_lf = pl.LazyFrame([], schema={
            "client_id": pl.Int64,
            "amount": pl.Float64,
            "event_type": pl.String,
            "event_time": pl.Datetime,
            "date": pl.Date
        })

        mocker.patch("src.feature_transaction_pipeline.assets.load_parquet", return_value=empty_lf)

        # 2. Build context for a single day
        context = build_asset_context(partition_key="2025-01-01")

        # 3. Execute
        result = transaction_features(context)

        # 4. Validation
        # In V2, if no data matches the date, the result should be an empty DataFrame
        # but it should still have the expected output columns from the agg
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0
        assert "daily_mean_amount" in result.columns