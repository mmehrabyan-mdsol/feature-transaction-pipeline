from dagster import IOManager, io_manager
import polars as pl
import os

class ParquetIOManager(IOManager):

    def __init__(self, base_path: str):
        self.base_path = base_path

    def _get_path(self, context):
        return os.path.join(self.base_path, f"{context.asset_key.path[-1]}.parquet")

    def handle_output(self, context, obj):
        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        obj.write_parquet(path)

    def load_input(self, context):
        path = self._get_path(context)
        return pl.read_parquet(path)


@io_manager(config_schema={"base_path": str})
def parquet_io_manager(init_context):
    return ParquetIOManager(init_context.resource_config["base_path"])