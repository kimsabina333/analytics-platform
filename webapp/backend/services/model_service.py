import sys
import asyncio
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

FEATURES = [
    "utm_source", "geo", "device", "age", "gender", "payment_method",
    "card_type", "mid", "offer", "card_brand", "weekday", "bank_tier",
]


class ModelService:
    """
    Singleton wrapper around utils.ModelLoader.
    asyncio.Lock serializes pm.sample_posterior_predictive calls —
    PyMC model context is not thread-safe.
    """

    def __init__(self, encoder, q_map: dict, vendor_dir: Path, model_path: str = "idata/idata_sr_ts.nc"):
        self.encoder = encoder
        self.q_map = q_map
        self.features = FEATURES

        cat_map = {
            col: {i: cat for i, cat in enumerate(cats)}
            for col, cats in zip(FEATURES, encoder.categories_)
        }
        coords = {key: list(value.values()) for key, value in cat_map.items()}
        coords["feature_dim"] = FEATURES
        coords["obs_id"] = np.arange(5)

        # vendor/ already on sys.path from lifespan; import ModelLoader from utils.py
        from utils import ModelLoader  # noqa: PLC0415

        self._model = ModelLoader(coords=coords, model_path=model_path)
        self._lock = asyncio.Lock()

        self.categories: Dict[str, List[str]] = {
            col: list(cats) for col, cats in zip(FEATURES, encoder.categories_)
        }

    async def predict(self, cnt: np.ndarray, X_encoded: np.ndarray) -> np.ndarray:
        """Returns posterior samples of shape (4000, n_segments). Serialized by lock."""
        async with self._lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._model, cnt, X_encoded)

    def encode(self, df_features: pd.DataFrame) -> np.ndarray:
        return self.encoder.transform(df_features[self.features]).astype(int)
