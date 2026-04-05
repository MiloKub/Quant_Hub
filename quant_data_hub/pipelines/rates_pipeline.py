# quant_data_hub/pipelines/rates_pipeline.py
"""High-level pipeline for interest rate series (SOFR, Treasury yields, etc.).

Provides the public API used by short-rate models, curve bootstrapping,
CVA/EPE engines, and exposure simulation. All data is cached, validated,
and ready for regulatory IMM workflows.
"""

from __future__ import annotations

import pandas as pd
from typing import Optional

from ..core.config import load_config
from ..sources import get_fetcher_class
from ..core.fetcher import BaseFetcher


def get_rates(
    series_key: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Return validated rate series by config key.

    Production rationale: Single entry point that respects config.yaml
    primary_source / fallback_source. Guarantees consistent, cached data
    for discounting (DF(t) = exp(-∫ r(s) ds)) and Monte Carlo
    drivers in CCR exposure profiles (EE, PFE).
    """
    config = load_config()
    series_config = config["data_sources"].get(series_key)
    if not series_config:
        raise ValueError(f"Unknown series_key: {series_key}")

    primary_source = series_config.get("primary_source")
    fallback_source = series_config.get("fallback_source")

    # Primary fetch
    try:
        fetcher_cls = get_fetcher_class(primary_source)
        fetcher: BaseFetcher = fetcher_cls({"name": series_key})
        df = fetcher.get_data(start_date, end_date, force_refresh)
        if not df.empty:
            return df
    except Exception as e:  # noqa: BLE001
        # Log but continue to fallback (production resilience)
        print(f"Primary source {primary_source} failed for {series_key}: {e}")

    # Fallback (only if configured and primary returned empty)
    if fallback_source:
        try:
            fetcher_cls = get_fetcher_class(fallback_source)
            fetcher: BaseFetcher = fetcher_cls({"name": series_key})
            df = fetcher.get_data(start_date, end_date, force_refresh=True)
            if not df.empty:
                return df
        except Exception as e:  # noqa: BLE001
            print(f"Fallback source {fallback_source} also failed for {series_key}: {e}")

    # Final guard
    raise RuntimeError(f"Both primary and fallback failed for series {series_key}")


def get_sofr(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Convenience wrapper for SOFR (most common series in CCR discounting)."""
    return get_rates("sofr", start_date, end_date, force_refresh)


def get_treasury_yield_curve(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Placeholder for Treasury par yields (extend once TreasuryFetcher is added)."""
    return get_rates("treasury_yield_curve", start_date, end_date, force_refresh)