from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_ignore_empty=True, extra="ignore"
    )
    # Data paths
    BASE_DATA_PATH: str = "/home/user/Desktop/git/feature-transaction-pipeline/data"
    NUM_CHUNKS: int =10
    OUTPUT_DIR: str = "data/output/features/"
    # DAILY_OUTPUT_PATH: str = "output/daily"
    # BACKFILL_OUTPUT_PATH: str = "output/backfill"
    DAILY_PARTITIONS_START_DATE: str="2020-12-31"

settings = Settings()
