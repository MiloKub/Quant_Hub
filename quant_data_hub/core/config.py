# quant_data_hub/core/config.py
from pathlib import Path
import os
from typing import Any, Dict

import yaml


def _resolve_config_path(config_path: str | Path | None) -> Path:
    """Resolve config path to absolute Path object.

    Production rationale: Guarantees a concrete Path before any filesystem
    operation, satisfying strict type checkers while keeping default
    behaviour at package root.
    """
    if config_path is None:
        # Structure: quant_data_hub/core/config.py → quant_data_hub/config.yaml
        return Path(__file__).parent.parent / "config.yaml"

    return Path(config_path)


def load_config(config_path: str | Path | None = None) -> Dict[str, Any]:
    """Load config.yaml with environment-variable overrides for secrets.

    Production rationale: Environment variables keep secrets out of Git
    while a single YAML file remains the source of truth for non-secret settings.
    """
    resolved_path = _resolve_config_path(config_path)

    with open(resolved_path, encoding="utf-8") as f:
        config: Dict[str, Any] = yaml.safe_load(f)

    # Override API keys from environment variables (standard in risk platforms)
    if "api" in config and isinstance(config["api"], dict):
        config["api"]["fred_api_key"] = os.getenv(
            "FRED_API_KEY", config["api"].get("fred_api_key", "")
        )

    # Future extension: add pydantic BaseModel validation when config grows
    # (ensures required keys and types for daily batch runs)
    return config