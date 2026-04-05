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
        """Save DataFrame partitioned by year using pyarrow engine.

        Production rationale: Skip empty DataFrames to prevent zero-byte files;
        explicit engine and compression for consistency. No 'mode' parameter
        (pyarrow does not support it).
        """
        if df.empty or len(df) == 0:
            return

        base_path.mkdir(parents=True, exist_ok=True)
        for year, group in df.groupby(df.index.year):
            year_path = base_path / str(year)
            year_path.mkdir(parents=True, exist_ok=True)
            file_path = year_path / "data.parquet"
            group.to_parquet(
                file_path,
                engine="pyarrow",
                compression="zstd",
                index=True
            )

    def load(self, base_path: Path, start_date: str, end_date: str | None = None) -> pd.DataFrame | None:
        """Load from Parquet files and filter by date range.

        Production rationale: Filter out zero-byte or invalid files to prevent
        pyarrow ArrowInvalid errors. Use explicit engine for consistency.
        """
        if not base_path.exists():
            return None

        files = list(base_path.rglob("*.parquet"))
        if not files:
            return None

        # Skip zero-byte files (common during early debugging)
        valid_files = [f for f in files if f.stat().st_size > 0]
        if not valid_files:
            return None

        dfs = []
        for f in valid_files:
            try:
                df_part = pd.read_parquet(f, engine="pyarrow")
                dfs.append(df_part)
            except Exception:  # Production: replace with specific exception + structured log later
                continue

        if not dfs:
            return None

        df = pd.concat(dfs, ignore_index=False)
        df = df.sort_index()

        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date) if end_date else df.index.max()
        mask = (df.index >= start) & (df.index <= end)
        return df.loc[mask]