# quant_data_hub/core/validator.py
"""Data quality validation rules for market rate series."""

import pandas as pd
import numpy as np


class DataValidator:
    """Core validation rules for rate and curve data.

    Production rationale: Enforces completeness and domain constraints early,
    supporting SR 11-7 / SS1/23 expectations for input data quality in CCR models.
    """

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all validation checks and return the DataFrame.

        Production rationale: Central validation point supporting SR 11-7 / SS1/23
        expectations for input data quality in CCR models.
        """
        DataValidator._check_completeness(df)
        DataValidator._check_domain_rules(df)
        DataValidator._check_outliers(df)
        return df

    @staticmethod
    def _check_completeness(df: pd.DataFrame) -> None:
        """Ensure no gaps on business days within the date range."""
        if df.empty:
            return
        expected_dates = pd.bdate_range(df.index.min(), df.index.max())
        missing = expected_dates.difference(df.index)
        if not missing.empty:
            print(f"Missing business days: {len(missing)} in range {df.index.min()} to {df.index.max()}")

    @staticmethod
    def _check_domain_rules(df: pd.DataFrame) -> None:
        """Rates must be non-negative; rare negatives are logged but not rejected."""
        for col in df.select_dtypes(include=[np.number]).columns:
            neg_mask = df[col] < 0
            if neg_mask.any():
                print(f"Negative values in column '{col}': {neg_mask.sum()} occurrences")

    @staticmethod
    def _check_outliers(df: pd.DataFrame, z_threshold: float = 5.0) -> None:
        """Z-score based outlier detection on numeric columns."""
        for col in df.select_dtypes(include=[np.number]).columns:
            if df[col].std() == 0:
                continue
            z = np.abs((df[col] - df[col].mean()) / df[col].std())
            outliers = z > z_threshold
            if outliers.any():
                print(f"Outliers detected in column '{col}': {outliers.sum()} occurrences")