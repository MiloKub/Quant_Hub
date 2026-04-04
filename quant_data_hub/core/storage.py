# quant_data_hub/core/storage.py
"""Parquet-based storage with year partitioning for efficient reads."""

from pathlib import Path
import pandas as pd


class Storage:
    """Handles saving and loading of validated DataFrames to partitioned Parquet.

    Production rationale: Year partitioning + zstd compression balances query speed
    and storage size for daily batch runs with multi-year histories.
    """

    def save(self, df: pd.DataFrame, base_path: Path) -> None:
        """Save DataFrame partitioned by year."""
        if df.empty:
            return
        base_path.mkdir(parents=True, exist_ok=True)
        for year, group in df.groupby(df.index.year):
            year_path = base_path / str(year)
            year_path.mkdir(parents=True, exist_ok=True)
            file_path = year_path / "data.parquet"
            # Atomic write pattern (temp file + rename) recommended for production
            group.to_parquet(
                file_path,
                compression="zstd",
                index=True,
                mode="overwrite" if not file_path.exists() else "append"
            )

    def load(self, base_path: Path, start_date: str, end_date: str | None = None) -> pd.DataFrame | None:
        """Load from Parquet files and filter by date range."""
        files = list(base_path.rglob("*.parquet"))
        if not files:
            return None

        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=False)
        df = df.sort_index()

        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date) if end_date else df.index.max()
        mask = (df.index >= start) & (df.index <= end)
        return df.loc[mask]