from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    openrouter_api_key: str = ""
    google_application_credentials: str = ""
    google_credentials_base64: str = ""   # base64-encoded JSON for Railway/Cloud Run
    redis_url: str = "redis://redis:6379/0"
    cache_ttl_seconds: int = 300
    poll_interval_seconds: int = 180
    # Override with local project root when running without Docker
    vendor_dir: str = ""

    class Config:
        env_file = ".env"


settings = Settings()

# VENDOR_DIR: from env (local dev) or default Docker path
if settings.vendor_dir:
    VENDOR_DIR = Path(settings.vendor_dir)
else:
    VENDOR_DIR = Path(__file__).parents[2] / "vendor"
