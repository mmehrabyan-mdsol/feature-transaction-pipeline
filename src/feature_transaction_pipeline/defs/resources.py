import polars as pl
import os
import logging
import shutil
from dagster import ConfigurableResource
from datetime import date
from .config import settings
import concurrent.futures


class FeatureEngine(ConfigurableResource):
    base_path: str = settings.BASE_DATA_PATH
    num_chunks: int = settings.NUM_CHUNKS
    output_dir: str = settings.OUTPUT_DIR


    @staticmethod
    def _validate(lf: pl.LazyFrame) -> pl.LazyFrame:
        """Defensively casts types and handles schema alignment."""
        return lf.with_columns([
            pl.col("client_id").cast(pl.String),
            pl.col("amount").cast(pl.Float64),
            pl.col("event_type").cast(pl.Int64),
            # Convert to datetime and handle potential string formats
            pl.col("event_time").cast(pl.Datetime, strict=False),
        ]).drop_nulls(subset=["client_id", "event_time"])

    def _compute_internal(self, lf: pl.LazyFrame, start_date: date, end_date: date) -> pl.LazyFrame:
        """
        Encapsulated business logic.
        Note: sorted by time within each user group to ensure rolling windows work.
        """
        return (
            lf.sort(["client_id", "event_time"])
            .with_columns([
                # Rolling 1-month mean per client
                pl.col("amount")
                .rolling_mean_by(window_size="1mo", min_samples=1, by='event_time')
                .over("client_id")
                .alias("mean_amount_30d"),

                # Global mean per client/type (Window function)
                pl.col("amount")
                .mean()
                .over(["client_id", "event_type"])
                .alias("mean_amount_by_event")
            ])

            .filter(
                pl.col("event_time").dt.date().is_between(start_date, end_date)
            )
            # aggregation to daily grain
            .group_by(["client_id", pl.col("event_time").dt.date().alias("date")])
            .agg([
                pl.col("amount").mean().alias("daily_mean_amount"),
                pl.col("mean_amount_30d").last(),
                pl.col("mean_amount_by_event").mean()
            ])
        )

    def process_chunk(self, i: int, start_date: date, end_date: date, temp_dir: str):
        """Worker function for a single chunk."""
        chunk_file = os.path.join(temp_dir, f"chunk_{i}.parquet")
        # Each thread creates its own LazyFrame pointer
        pipeline = (
            pl.scan_parquet(f"{self.base_path}/detail/trx/fold=*/**/*.parquet")
            .pipe(self._validate)
            .filter((pl.col("client_id").hash() % self.num_chunks) == i)
            .pipe(self._compute_internal, start_date, end_date)
        )
        # Stream this specific chunk to disk
        pipeline.sink_parquet(chunk_file)
        return chunk_file

    def run_pipeline(self, start_date: date, end_date: date, log) -> str:
        target_dir = os.path.join(self.output_dir, str(start_date))
        temp_dir = os.path.join(target_dir, "_parallel_chunks")
        os.makedirs(temp_dir, exist_ok=True)
        final_file = os.path.join(target_dir, "data.parquet")

        log.info(f"Launching {self.num_chunks} parallel workers...")

        # ThreadPoolExecutor for Polars
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            # Create a list of futures
            futures = [
                executor.submit(self.process_chunk, i, start_date, end_date, temp_dir)
                for i in range(self.num_chunks)
            ]
            # Wait for completion
            concurrent.futures.wait(futures)

        # Merge the results
        log.info("Merging parallel chunks...")
        pl.scan_parquet(os.path.join(temp_dir, "*.parquet")).sink_parquet(final_file)

        shutil.rmtree(temp_dir)
        return final_file
