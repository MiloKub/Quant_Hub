# quant_data_hub/__init__.py
"""Quant Data Hub

Production-grade market data pipeline for counterparty credit risk (CCR),
XVA calculations, short-rate models, and exposure simulation.

Provides cached, validated access to rates and curves used in EE/PFE,
discounting, and Monte Carlo path generation.
"""

from .core.config import load_config

# High-level public API (will be populated once pipelines are implemented)
# from .pipelines.rates_pipeline import (
#     get_rates,
#     get_sofr,
#     refresh_rates,
#     get_treasury_yield_curve,
# )

__version__ = "0.1.0"

__all__ = [
    "load_config",
    # "get_rates",
    # "get_sofr",
    # "refresh_rates",
    # "get_treasury_yield_curve",
]