import polars as pl
from feature_transaction_pipeline.defs.utils import compute_daily_features, compute_backfill_features
from types import SimpleNamespace
import datetime as dt


class DummyContext:
    log = SimpleNamespace(info=lambda x: print(f"INFO: {x}"), error=lambda x: print(f"ERROR: {x}"))

def test_compute_daily_features():
    ctx = DummyContext()
    # Mock data: 2 clients, Client 1 has 2 transactions
    df = pl.DataFrame({
        "client_id": [1, 1, 2],
        "amount": [10.0, 20.0, 5.0],
        "event_type": ["A", "A", "B"],
        "date": [dt.date(2023, 1, 1)] * 3
    }).lazy()

    result = compute_daily_features(ctx, df, dt.date(2023, 1, 1))

    # Assertions
    assert result.height == 2  # Should collapse to 2 unique clients
    assert "mean_amount" in result.columns
    assert "mean_amount_by_event" in result.columns
    client_1_mean = result.filter(pl.col("client_id") == 1)["mean_amount"][0]
    assert client_1_mean == 15.0

def test_compute_backfill_features():
    ctx = DummyContext()
    # Mock data with 30-day spread
    df = pl.DataFrame({
        "client_id": [1, 1, 1],
        "amount": [10.0, 20.0, 30.0],
        "event_type": ["A", "A", "A"],
        "date": [dt.date(2023, 1, 1), dt.date(2023, 1, 15), dt.date(2023, 2, 1)]
    }).lazy()

    result = compute_backfill_features(ctx, df)

    assert "mean_amount_daily" in result.columns
    assert "mean_amount_30d" in result.columns
    assert result.height > 0