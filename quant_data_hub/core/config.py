# quant_data_hub/core/config.py
from pathlib import Path
import os
from typing import Any, Dict, Union

import yaml


def load_config(config_path: Union[str, Path, None] = None) -> Dict[str, Any]:
    """Load config.yaml with environment-variable overrides for secrets.

    Production rationale: environment variables keep secrets out of Git
    while a single YAML file remains the source of truth for non-secret settings.
    """
    if config_path is None:
        # Resolve relative to the package root (one level above core/)
        # Structure: quant_data_hub/core/config.py  →  quant_data_hub/config.yaml
        config_path = Path(__file__).parent.parent / "config.yaml"

    # Explicit conversion guarantees Path object; eliminates None for open() and Path()
    config_path = Path(config_path)

    with open(config_path, encoding="utf-8") as f:
        config: Dict[str, Any] = yaml.safe_load(f)

    # Override API keys from environment variables (standard in risk platforms)
    if "api" in config and isinstance(config["api"], dict):
        config["api"]["fred_api_key"] = os.getenv(
            "FRED_API_KEY", config["api"].get("fred_api_key")
        )

    # Future extension point: add schema validation (e.g. with pydantic) if config grows
    return config