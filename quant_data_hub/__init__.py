# quant_data_hub/__init__.py
"""Quant Data Hub -  market data pipeline for CCR and risk modeling.

Provides validated, cached access to rates, curves, equity, and FX data.
"""

from .core.config import load_config

__version__ = "0.1.0"
# Public API will be expanded once pipelines and fetchers are implemented
# For now we expose only the config loader directly
__all__ = ["load_config"]

# Optional: expose high-level pipeline functions once implemented
# from .pipelines.rates_pipeline import get_rates