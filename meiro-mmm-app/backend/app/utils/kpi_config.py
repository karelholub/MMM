from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional


@dataclass
class KpiDefinition:
    """Definition of a KPI or micro-conversion."""

    id: str
    label: str
    type: str = "primary"  # "primary" or "micro"
    event_name: str = ""   # name or key of the event in raw data/CDP
    value_field: Optional[str] = None  # e.g. "revenue", "value"
    weight: float = 1.0
    lookback_days: Optional[int] = None


@dataclass
class KpiConfig:
    """Collection of KPI definitions and the designated primary KPI."""

    definitions: List[KpiDefinition] = field(default_factory=list)
    primary_kpi_id: Optional[str] = None


_BASE_DIR = Path(__file__).resolve().parent.parent


def _kpi_path() -> Path:
    data_dir = _BASE_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "kpi_config.json"


def default_kpi_config() -> KpiConfig:
    return KpiConfig(
        definitions=[
            KpiDefinition(id="purchase", label="Purchase", type="primary", event_name="purchase", value_field="revenue", weight=1.0),
            KpiDefinition(id="lead", label="Lead", type="primary", event_name="lead_submitted", value_field=None, weight=0.5),
            KpiDefinition(id="signup", label="Sign-up", type="primary", event_name="signup", value_field=None, weight=0.5),
            KpiDefinition(id="add_to_cart", label="Add to cart", type="micro", event_name="add_to_cart"),
            KpiDefinition(id="product_view", label="Product view", type="micro", event_name="product_view"),
        ],
        primary_kpi_id="purchase",
    )


def load_kpi_config() -> KpiConfig:
    path = _kpi_path()
    if not path.exists():
        cfg = default_kpi_config()
        save_kpi_config(cfg)
        return cfg
    try:
        raw = json.loads(path.read_text())
    except Exception:
        return default_kpi_config()
    defs = [KpiDefinition(**d) for d in raw.get("definitions", [])]
    return KpiConfig(definitions=defs, primary_kpi_id=raw.get("primary_kpi_id"))


def save_kpi_config(cfg: KpiConfig) -> None:
    path = _kpi_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "definitions": [asdict(d) for d in cfg.definitions],
        "primary_kpi_id": cfg.primary_kpi_id,
    }
    path.write_text(json.dumps(payload, indent=2))

