import polars as pl
import os
import logging
import shutil
from dagster import ConfigurableResource
from datetime import date
from .config import settings


class FeatureEngine(ConfigurableResource):
    base_path: str = settings.BASE_DATA_PATH
    num_chunks: int = settings.NUM_CHUNKS
    output_dir: str = settings.OUTPUT_DIR

    @staticmethod
    def _prepare_and_validate(lf: pl.LazyFrame) -> pl.LazyFrame:
        """Defensively casts types and handles schema alignment."""
        return lf.with_columns([
            pl.col("client_id").cast(pl.String),
            pl.col("amount").cast(pl.Float64),
            pl.col("event_type").cast(pl.Int64),
            # Convert to datetime and handle potential string formats
            pl.col("event_time").cast(pl.Datetime, strict=False),
        ]).drop_nulls(subset=["client_id", "event_time"])

    def run_backfill(self, start_date: date, end_date: date, log: logging.Logger):
        # 1. Define Paths
        # Hidden temp folder to avoid accidental use by other processes
        temp_dir = os.path.join(self.output_dir, f".tmp_{start_date}")
        final_dir = os.path.join(self.output_dir, str(start_date))

        # Initial Cleanup
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir, exist_ok=True)

        current_chunk = 0

        try:
            # 2. Scan Source
            # Using your specific glob pattern
            raw_lf = pl.scan_parquet(f"{self.base_path}/detail/trx/fold=*/**/*.parquet")
            lf = self._prepare_and_validate(raw_lf)

            # 3. Process in Chunks
            for i in range(self.num_chunks):
                current_chunk = i + 1
                log.info(f"Processing Hash-Chunk {current_chunk}/{self.num_chunks}")

                # Filter by User Hash
                chunk_lf = lf.filter(pl.col("client_id").hash() % self.num_chunks == i)

                # 4. Transform (Rolling logic)
                processed_df = (
                    chunk_lf.sort(["client_id", "event_time"])
                    .with_columns([
                        pl.col("amount")
                        .rolling_mean_by(window_size="1mo", min_samples=1, by='event_time')
                        .over("client_id")
                        .alias("mean_amount_30d"),
                        pl.col("amount")
                        .mean()
                        .over(["client_id", "event_type"])
                        .alias("mean_amount_by_event")
                    ])
                    .filter(
                        (pl.col("event_time").dt.date() >= start_date) &
                        (pl.col("event_time").dt.date() <= end_date)
                    )
                    .group_by(["client_id", pl.col("event_time").dt.date().alias("date")])
                    .agg([
                        pl.col("amount").mean().alias("daily_mean_amount"),
                        pl.col("mean_amount_30d").last(),
                        pl.col("mean_amount_by_event").mean()
                    ])
                    .collect(streaming=True)
                )

                # 5. Write Chunk to TEMP DIR
                chunk_path = os.path.join(temp_dir, f"chunk_{current_chunk}.parquet")
                processed_df.write_parquet(chunk_path)

                del processed_df
            # This automatically 'removes' the temp folder by renaming/moving it
            if os.path.exists(final_dir):
                shutil.rmtree(final_dir)

            shutil.move(temp_dir, final_dir)
            log.info(f"Successfully committed all chunks to {final_dir}")

        except Exception as e:
            log.error(f"Failure during chunk {current_chunk}. Cleaning up temp files.")
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise e