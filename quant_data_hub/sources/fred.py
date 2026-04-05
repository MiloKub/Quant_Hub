# quant_data_hub/sources/fred.py
"""FRED (St. Luis Fed) data fetcher - SOFR and other economic series."""

import os
import pandas as pd
from typing import Optional

from fredapi import Fred

from ..core.fetcher import BaseFetcher
from ..utils.retry import retry_on_failure


class FredFetcher(BaseFetcher):
    """Fetcher for FRED series using official fredapi wrapper.

    Production rationale: FRED provides clean, consistent daily time series with
    built-in date handling; excellent for CCR calibration and exposure engines.
    """

    def __init__(self):
        super().__init__({"name": "sofr", "storage_path": "data/rates/sofr"})

    @retry_on_failure
    def fetch(self, start_date: str, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetch series from FRED API.

        Production rationale: fredapi handles authentication, date parsing,
        and returns a clean pandas Series that we convert to DataFrame.
        """
        # Prefer environment variable (standard for fredapi and risk platforms)
        api_key = os.getenv("FRED_API_KEY")

        # Fallback to config.yaml if env var is not set
        if not api_key or api_key == "your_actual_key":
            config = self.config
            api_key = config.get("api", {}).get("fred_api_key")

        if not api_key or api_key in ("your_key_here", "your_actual_key"):
            raise ValueError(
                "FRED_API_KEY not found. Set it as environment variable FRED_API_KEY "
                "or in config.yaml under api: fred_api_key. "
                "Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"
            )

        fred = Fred(api_key=api_key)

        series_id = self.config.get("data_sources", {}).get("sofr", {}).get("series_id_fred", "SOFR")

        series = fred.get_series(
            series_id=series_id,
            observation_start=start_date,
            observation_end=end_date
        )

        if series.empty:
            return pd.DataFrame()

        df = series.to_frame(name="SOFR")
        return df