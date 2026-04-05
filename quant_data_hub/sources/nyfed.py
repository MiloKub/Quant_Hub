# quant_data_hub/sources/nyfed.py
"""NY Fed reference rates fetcher (primary source for SOFR)."""

import requests
import pandas as pd
from typing import Optional

from ..core.fetcher import BaseFetcher
# from ..utils.retry import retry_on_failure   # commented until retry.py exists


class NYFedFetcher(BaseFetcher):
    """Fetcher for New York Fed SOFR and reference rates.

    Production rationale: NY Fed is the authoritative source for SOFR fixings.
    Direct JSON/CSV access; no authentication required for public series.
    """

    def __init__(self):
        super().__init__({"name": "sofr", "storage_path": "data/rates/sofr"})

    # @retry_on_failure   # decorator to be added once utils/retry.py exists
    def fetch(self, start_date: str, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetch SOFR data from NY Fed Markets API using /search.json endpoint.

        Production rationale: Uses official public endpoint; response parsed to date-indexed DataFrame.
        """
        # base_url = "https://markets.newyorkfed.org/api"
        # end_date_str = end_date or pd.Timestamp.today().strftime("%Y-%m-%d")
        #
        # url = f"{base_url}/reference-rates/sofr/search.json"
        # params = {
        #     "startDate": start_date,
        #     "endDate": end_date_str,
        #     "format": "json"
        # }
        #
        # resp = requests.get(url, params=params)
        # resp.raise_for_status()
        # data = resp.json()
        #
        # # Actual NY Fed response structure: top-level key often contains list of records
        # # Common keys: effectiveDate, rate (or similar)
        # records = data.get("refRates") or data.get("rates") or []
        # if not records:
        #     return pd.DataFrame()
        #
        # df = pd.DataFrame(records)
        # if "effectiveDate" in df.columns and "rate" in df.columns:
        #     df = df.set_index("effectiveDate")["rate"].to_frame(name="SOFR")
        # else:
        #     # Fallback parsing if column names differ slightly
        #     df = pd.DataFrame()
        #
        # return df

        # Disabled until full NY Fed integration
        return pd.DataFrame()