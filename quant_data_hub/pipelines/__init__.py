# quant_data_hub/pipelines/__init__.py
"""High-level pipelines that orchestrate multiple sources."""

from .rates_pipeline import get_sofr

__all__ = ["get_sofr"]