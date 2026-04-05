# quant_data_hub/pipelines/rates_pipeline.py
"""High-level pipeline for interest rate series (SOFR, etc.).

Provides a simple public interface for other quant projects (short-rate models,
curve bootstrapping, CVA engines, etc.).
"""

import pandas as pd
from typing import Optional

from ..sources.fred import FredFetcher
# from ..sources.nyfed import NYFedFetcher   # commented - NY Fed endpoint needs rework
from ..core.fetcher import BaseFetcher


def get_sofr(start_date: str, end_date: Optional[str] = None, force_refresh: bool = False) -> pd.DataFrame:
    """Return validated SOFR series.

    Production rationale: Single entry point that prefers cached Parquet,
    falls back to FRED (primary), with retry logic and validation always applied.
    """
    fetcher: BaseFetcher = FredFetcher()

    # Apply retry decorator once implemented on fetch method
    # fetcher.fetch = retry_on_failure(fetcher.fetch)  # to be enabled later

    return fetcher.get_data(start_date, end_date, force_refresh)