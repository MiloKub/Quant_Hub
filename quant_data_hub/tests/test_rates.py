# tests/test_rates.py
import pandas as pd
import pytest
from unittest.mock import patch

from quant_data_hub.pipelines.rates_pipeline import get_sofr


def test_get_sofr_returns_dataframe_with_dates():
    """Basic smoke test: returns a DataFrame with datetime index."""
    df = get_sofr(start_date="2024-01-01", end_date="2024-01-05", force_refresh=True)
    assert isinstance(df, pd.DataFrame)
    assert df.index.name == "date" or "date" in df.columns
    assert len(df) > 0


@patch("quant_data_hub.sources.fred.FredFetcher.get_data")
def test_get_sofr_uses_primary_fetcher(mock_get_data):
    """Verify the pipeline calls the correct fetcher (mocked)."""
    mock_df = pd.DataFrame({"SOFR": [5.3]}, index=pd.date_range("2024-01-01", periods=1))
    mock_get_data.return_value = mock_df

    df = get_sofr(start_date="2024-01-01", force_refresh=True)
    mock_get_data.assert_called_once()
    assert df.equals(mock_df)