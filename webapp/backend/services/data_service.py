import asyncio
from pathlib import Path
from typing import Optional

import pandas as pd


class DataService:
    """
    Wrapper around utils.DataLoader.
    mode='offline' uses google.cloud.bigquery.Client directly.
    BQ client is created lazily on first load_data() call to allow startup without credentials.
    """

    def __init__(self, vendor_dir: Path, sql_path: str = None):
        from utils import DataLoader  # noqa: PLC0415

        kwargs = {"mode": "offline"}
        if sql_path:
            kwargs["sql_path"] = sql_path
        self._loader = DataLoader(**kwargs)
        self._bq_client: Optional[object] = None

    def _get_bq_client(self):
        if self._bq_client is None:
            from google.cloud import bigquery  # noqa: PLC0415
            self._bq_client = bigquery.Client()
        return self._bq_client

    async def load_data(self) -> pd.DataFrame:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._loader, self._get_bq_client())
