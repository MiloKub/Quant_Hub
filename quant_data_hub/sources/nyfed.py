# quant_data_hub/sources/nyfed.py
"""NY Fed reference rates fetcher (primary source for SOFR)."""

import requests
import pandas as pd
from typing import Optional

from ..core.fetcher import BaseFetcher
from ..utils.retry import retry_on_failure


class NYFedFetcher(BaseFetcher):
    """Fetcher for New York Fed SOFR and reference rates.

    Production rationale: NY Fed is the authoritative source for SOFR fixings.
    Direct JSON/CSV access; no authentication required for public series.
    """

    def __init__(self):
        super().__init__({"name": "sofr", "storage_path": "data/rates/sofr"})

    @retry_on_failure
    def fetch(self, start_date: str, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetch SOFR data from NY Fed Markets API."""
        base_url = self.config.get("api", {}).get("nyfed_base_url", "https://markets.newyorkfed.org/api")
        end_date_str = end_date or pd.Timestamp.today().strftime("%Y-%m-%d")

        url = f"{base_url}/rates/secured/sofr.json"  # correct public endpoint for SOFR
        params = {
            "startDate": start_date,
            "endDate": end_date_str,
        }

        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        records = data.get("rates") or data.get("data") or []
        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        if "effectiveDate" in df.columns and "rate" in df.columns:
            df = df.set_index("effectiveDate")["rate"].to_frame(name="SOFR")
        else:
            df = pd.DataFrame()  # fallback

        return df