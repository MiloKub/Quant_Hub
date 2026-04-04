# quant_data_hub/__init__.py
"""Quant Data Hub - market data pipeline for CCR and risk modeling.

Provides validated, cached access to rates, curves, equity, and FX data.
"""

__version__ = "0.1.0"

# Public API - expanded as modules are implemented
from .core.config import load_config

# from .pipelines.rates_pipeline import get_rates   # commented until pipeline exists

__all__ = ["load_config"]