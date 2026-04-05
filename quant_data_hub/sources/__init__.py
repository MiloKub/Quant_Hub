# quant_data_hub/sources/__init__.py
"""Data source implementations (one class per provider)."""

# from .nyfed import NYFedFetcher       # commented - nyfed.py needs rework
# from .treasury import TreasuryFetcher # commented
from .fred import FredFetcher         # commented

__all__ = ["FredFetcher"]