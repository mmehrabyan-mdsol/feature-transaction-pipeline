import polars as pl
import os
import shutil
import concurrent.futures
from datetime import date
from dagster import ConfigurableResource
from .config import settings


class FeatureEngine(ConfigurableResource):

    base_path: str = settings.BASE_DATA_PATH
    num_chunks: int = settings.NUM_CHUNKS
    output_dir: str = settings.OUTPUT_DIR

    # -------------------------
    # Validation
    # -------------------------
    @staticmethod
    def _validate(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns([
            pl.col("client_id").cast(pl.String),
            pl.col("amount").cast(pl.Float64),
            pl.col("event_type").cast(pl.Int64),
            pl.col("event_time").cast(pl.Datetime, strict=False),
        ]).drop_nulls(subset=["client_id", "event_time"])

    # -------------------------
    # Core Compute Logic
    # -------------------------
    def _compute_internal(
        self, lf: pl.LazyFrame, start_date: date, end_date: date
    ) -> pl.LazyFrame:

        return (
            lf.sort(["client_id", "event_time"])
            .with_columns([
                pl.col("amount")
                .rolling_mean_by(
                    window_size="1mo",
                    min_samples=1,
                    by="event_time",
                )
                .over("client_id")
                .alias("mean_amount_30d"),

                pl.col("amount")
                .mean()
                .over(["client_id", "event_type"])
                .alias("mean_amount_by_event"),
            ])
            .filter(
                pl.col("event_time").dt.date().is_between(start_date, end_date)
            )
            .group_by(
                ["client_id", pl.col("event_time").dt.date().alias("date")]
            )
            .agg([
                pl.col("amount").mean().alias("daily_mean_amount"),
                pl.col("mean_amount_30d").last(),
                pl.col("mean_amount_by_event").mean(),
            ])
        )

    # -------------------------
    # Process Single Chunk
    # -------------------------
    def _process_chunk(
            self, chunk_id: int, start_date: date, end_date: date, output_dir: str, log  # Added log here
    ) -> str:
        output_file = os.path.join(output_dir, f"part_{chunk_id}.parquet")
        try:
            # Build the LazyFrame
            lf = (
                pl.scan_parquet(f"{self.base_path}/detail/trx/fold=*/**/*.parquet")
                .pipe(self._validate)
                .filter((pl.col("client_id").hash() % self.num_chunks) == chunk_id)
                .pipe(self._compute_internal, start_date, end_date)
            )

            # SENIOR TIP: Instead of a separate row_count scan (which is slow),
            # just sink directly. If no data exists, Polars writes an empty schema file
            # or you can check the result after the sink.
            lf.sink_parquet(output_file)

            # Check if file was actually written and has size
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                log.info(f"Chunk {chunk_id} completed: {output_file}")
            else:
                # Clean up empty files if preferred
                if os.path.exists(output_file): os.remove(output_file)

            return output_file

        except Exception as e:
            log.error(f"Chunk {chunk_id} failed: {str(e)}")
            raise
    # def _process_chunk(
    #     self, chunk_id: int, start_date: date, end_date: date, output_dir: str
    # ) -> str:
    #
    #     output_file = os.path.join(output_dir, f"part_{chunk_id}.parquet")
    #     try:
    #         lf = (
    #             pl.scan_parquet(f"{self.base_path}/detail/trx/fold=*/**/*.parquet")
    #             .pipe(self._validate)
    #             .filter(
    #                 (pl.col("client_id").hash() % self.num_chunks) == chunk_id
    #             )
    #             .pipe(self._compute_internal, start_date, end_date)
    #         )
    #         # Check if this chunk has any rows before writing
    #         row_count = lf.select(pl.count()).collect().item()
    #         if row_count == 0:
    #             log.info(f"Skipping chunk {chunk_id}: no data for {start_date}–{end_date}")
    #         # Stream directly to final dataset
    #         else:
    #             lf.sink_parquet(output_file)
    #             log.info(f"Chunk {chunk_id} completed successfully.")
    #         return output_file
    #
    #     except Exception as e:
    #         log.error(f"Chunk {chunk_id} failed: {e}")
    #         raise
    # -------------------------
    # Parallel Execution
    # -------------------------
    def _run_parallel(
            self, start_date: date, end_date: date, output_dir: str, log
    ) -> list[str]:
        # Polars handles its own internal parallelism well,
        # so don't over-saturate with too many workers.
        max_workers = min(self.num_chunks, max(1, os.cpu_count() // 2))
        log.info(f"Launching {self.num_chunks} chunks across {max_workers} workers...")

        chunk_files = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_chunk = {
                executor.submit(
                    self._process_chunk, i, start_date, end_date, output_dir, log
                ): i
                for i in range(self.num_chunks)
            }

            for future in concurrent.futures.as_completed(future_to_chunk):
                try:
                    # Calling .result() is CRITICAL to see errors from the thread
                    path = future.result()
                    if os.path.exists(path):
                        chunk_files.append(path)
                except Exception as e:
                    chunk_id = future_to_chunk[future]
                    log.error(f"Thread for chunk {chunk_id} crashed: {e}")

        return chunk_files
    # def _run_parallel(
    #     self, start_date: date, end_date: date, output_dir: str, log
    # ) -> list[str]:
    #
    #     max_workers = min(self.num_chunks, max(1, os.cpu_count() // 2))
    #     log.info(f"Running with {max_workers} workers...")
    #
    #     chunk_files = []
    #     failed_chunks = []
    #
    #     with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    #         futures = {
    #             executor.submit(
    #                 self._process_chunk, i, start_date, end_date, output_dir, log
    #             ): i
    #             for i in range(self.num_chunks)
    #         }
    #
    #         for future in concurrent.futures.as_completed(futures):
    #             chunk_id = futures[future]
    #             try:
    #                 result = future.result()
    #                 chunk_files.append(result)
    #             except Exception:
    #                 failed_chunks.append(chunk_id)
    #
    #     if failed_chunks:
    #         log.warning(f"Failed chunks: {failed_chunks}.")
    #
    #     return chunk_files

    # -------------------------
    # Sequential Execution
    # -------------------------

    def _run_sequential(
            self, start_date: date, end_date: date, output_dir: str, log
    ) -> None:

        log.info(f"Processing {self.num_chunks} chunks sequentially using Polars engine...")

        for i in range(self.num_chunks):
            log.info(f"Processing chunk {i}/{self.num_chunks - 1}")

            self._process_chunk(
                chunk_id=i,
                start_date=start_date,
                end_date=end_date,
                output_dir=output_dir,
            )

    def _validate_final_dataset(self, output_dir: str, log) -> None:
        """
        Validate the final dataset by scanning all parquet files in the output directory.
        Logs row count, file size, and distinct client/date coverage.
        """
        parq_file=f"{output_dir}/data=*"
        parquet_files = [
            os.path.join(f"{output_dir}", f)
            for f in os.listdir(output_dir)
            if f.endswith(".parquet")
        ]

        if parquet_files:
            # return  # Exit gracefully
            lf = pl.scan_parquet(os.path.join(output_dir, "part_*.parquet"))

            # Row count
            row_count = lf.select(pl.count()).collect().item()
            log.info(f"Final dataset contains {row_count} rows.")

            # Distinct clients and dates
            n_clients = lf.select(pl.col("client_id").n_unique()).collect().item()
            n_dates = lf.select(pl.col("date").n_unique()).collect().item()
            log.info(f"Final dataset has {n_clients} clients across {n_dates} dates.")

            # File size check
            total_size = sum(
                os.path.getsize(os.path.join(output_dir, f))
                for f in os.listdir(output_dir)
                if f.endswith(".parquet")
            )
            log.info(f"Total parquet size: {total_size / 1e6:.2f} MB")
        else:
            log.warning(f"No parquet files found in {output_dir}. No data available for this date range.")

    # -------------------------
    # Merge [Optional not used currently]
    # -------------------------
    def _merge_chunks(self, chunk_files: list[str], final_file: str, log):
        log.info("Merging chunks (streaming)...")

        first = True
        for file in chunk_files:
            df = pl.read_parquet(file)

            if first:
                df.write_parquet(final_file)
                first = False
            else:
                df.write_parquet(final_file, append=True)


    # -------------------------
    # Pipeline Compute features
    # -------------------------
    def compute_features(
        self, start_date: date, end_date: date, log
    ) -> str:
        """
        Returns a directory (dataset).
        """

        target_dir = os.path.join(self.output_dir, f"date={str(start_date)}-{str(end_date)}")
        os.makedirs(target_dir, exist_ok=True)

        log.info(f"Writing dataset to {target_dir}")

        # Run parallel chunk processing
        self._run_parallel(start_date, end_date, target_dir, log)
        self._validate_final_dataset(target_dir, log)
        # Run sequential chunk processing
        # self._run_sequential(start_date, end_date, target_dir, log)

        # Return dataset path instead of single file
        return target_dir