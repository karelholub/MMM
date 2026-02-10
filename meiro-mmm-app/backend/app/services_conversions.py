"""Helpers to apply versioned ModelConfig to journeys.

This module keeps attribution math unchanged, but:
  - Applies time windows from config.windows to prune touchpoints
  - Annotates journeys with a conversion key (kpi_type) derived from config.conversions

The goal is to make attribution *config-aware* while remaining backward compatible
when no ModelConfig is supplied.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, List

import pandas as pd


def _parse_ts(ts: Any):
  """Best-effort timestamp parsing. Returns pandas.Timestamp or None."""
  if not ts:
    return None
  try:
    return pd.to_datetime(ts, errors="coerce")
  except Exception:
    return None


def filter_journeys_by_windows(
  journeys: List[Dict[str, Any]],
  config_json: Dict[str, Any],
) -> List[Dict[str, Any]]:
  """Apply time windows from config.windows to touchpoints.

  Limitations:
    - We treat all touchpoints as "click-like" and apply click_lookback_days.
    - Impression windows would require event-type granularity that current paths lack.
  """
  windows = config_json.get("windows") or {}
  click_days = windows.get("click_lookback_days")
  if not click_days or click_days <= 0:
    return journeys

  max_delta = timedelta(days=float(click_days))
  out: List[Dict[str, Any]] = []

  for j in journeys:
    tps = j.get("touchpoints") or []
    if not tps:
      continue
    parsed = [_parse_ts(tp.get("timestamp")) for tp in tps]
    valid_ts = [p for p in parsed if p is not None]
    if not valid_ts:
      # No usable timestamps; keep journey as-is to avoid dropping data silently
      out.append(j)
      continue
    last_ts = max(valid_ts)
    min_ts = last_ts - max_delta
    kept_tps = []
    for tp, ts in zip(tps, parsed):
      if ts is None:
        # Keep touchpoints without timestamps; they're rare and carry some information
        kept_tps.append(tp)
      elif ts >= min_ts:
        kept_tps.append(tp)
    if not kept_tps:
      # If all touchpoints fall out of window, drop journey from attribution set
      continue
    new_j = dict(j)
    new_j["touchpoints"] = kept_tps
    out.append(new_j)

  return out


def annotate_journeys_with_conversion_key(
  journeys: List[Dict[str, Any]],
  config_json: Dict[str, Any],
) -> List[Dict[str, Any]]:
  """Attach kpi_type to journeys based on config.conversions.primary_conversion_key.

  If a journey already has kpi_type (e.g. from sample data), it is preserved.
  """
  conv_cfg = config_json.get("conversions") or {}
  primary_key = conv_cfg.get("primary_conversion_key")
  if not primary_key:
    return journeys

  out: List[Dict[str, Any]] = []
  for j in journeys:
    if j.get("kpi_type"):
      out.append(j)
      continue
    converted = j.get("converted", True)
    new_j = dict(j)
    if converted:
      new_j["kpi_type"] = primary_key
    out.append(new_j)
  return out


def apply_model_config_to_journeys(
  journeys: List[Dict[str, Any]],
  config_json: Dict[str, Any],
) -> List[Dict[str, Any]]:
  """Apply both time windows and conversion-key annotations to journeys."""
  tmp = filter_journeys_by_windows(journeys, config_json)
  tmp = annotate_journeys_with_conversion_key(tmp, config_json)
  return tmp

