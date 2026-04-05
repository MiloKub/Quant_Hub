# quant_data_hub/core/fetcher.py
"""Abstract base class for all data fetchers with caching and validation."""

import abc
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .config import load_config
from .storage import Storage
from .validator import DataValidator


class BaseFetcher(abc.ABC):
    """Abstract base for data fetchers providing consistent caching + validation.

    Production rationale: Centralizes common logic so each source only implements
    the raw fetch method; supports fallback patterns and idempotent runs.
    """

    def __init__(self, source_config: Dict | None = None):
        self.config = load_config() if source_config is None else {
            "data_sources": {source_config.get("name", ""): source_config}}
        self.storage = Storage()
        self.validator = DataValidator()

        # Anchor storage path to the package root (quant_data_hub/) so data/
        # always lands inside the package regardless of current working directory.
        # This fixes the files appearing in the parent directory.
        package_root = Path(__file__).parent.parent
        source_name = list(self.config.get("data_sources", {}).keys())[
            0] if "data_sources" in self.config else "default"
        relative_path = self.config.get("data_sources", {}) \
            .get(source_name, {}) \
            .get("storage_path", f"data/{source_name}")

        self.storage_path = package_root / relative_path

    @abc.abstractmethod
    def fetch(self, start_date: str, end_date: Optional[str] = None) -> pd.DataFrame:
        """Implement raw API or file fetch per source."""
        pass

    def get_data(self, start_date: str, end_date: Optional[str] = None, force_refresh: bool = False) -> pd.DataFrame:
        """Return validated data; prefer cache unless force_refresh=True."""
        if not force_refresh:
            cached = self.storage.load(self.storage_path, start_date, end_date)
            if cached is not None and not cached.empty:
                return cached

        raw_df = self.fetch(start_date, end_date)
        cleaned_df = self._clean(raw_df)
        validated_df = self.validator.validate(cleaned_df)
        self.storage.save(validated_df, self.storage_path)
        return validated_df

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic cleaning applied to all sources."""
        df = df.copy()
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        return df