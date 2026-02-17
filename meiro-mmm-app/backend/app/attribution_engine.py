"""
Marketing Attribution Engine.

Implements multi-touch attribution models for conversion path data:
  - Last-touch
  - First-touch
  - Linear
  - Time-decay
  - Position-based (U-shaped)
  - Data-driven (Markov chain)

Input: list of customer journeys, each with ordered touchpoints and a conversion value.
Output: attributed credit per channel, plus summary statistics.
"""

import logging
import math
from collections import defaultdict
from itertools import combinations
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.services_metrics import journey_revenue_value
from app.utils.taxonomy import normalize_touchpoint, load_taxonomy

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

# A touchpoint: {"channel": str, "timestamp": str (ISO), ...}
# A journey: {"customer_id": str, "touchpoints": [touchpoint, ...],
#              "conversion_value": float, "converted": bool}

ATTRIBUTION_MODELS = [
    "last_touch",
    "first_touch",
    "linear",
    "time_decay",
    "position_based",
    "markov",
]


# ---------------------------------------------------------------------------
# Core attribution functions
# ---------------------------------------------------------------------------

def last_touch(journeys: List[Dict]) -> Dict[str, float]:
    """100% credit to the last touchpoint before conversion."""
    credit: Dict[str, float] = defaultdict(float)
    dedupe_seen: set[str] = set()
    for j in journeys:
        if not j.get("converted", True) or not j.get("touchpoints"):
            continue
        last_tp = j["touchpoints"][-1]
        channel = last_tp.get("channel", "unknown")
        credit[channel] += journey_revenue_value(j, dedupe_seen=dedupe_seen)
    return dict(credit)


def first_touch(journeys: List[Dict]) -> Dict[str, float]:
    """100% credit to the first touchpoint."""
    credit: Dict[str, float] = defaultdict(float)
    dedupe_seen: set[str] = set()
    for j in journeys:
        if not j.get("converted", True) or not j.get("touchpoints"):
            continue
        first_tp = j["touchpoints"][0]
        channel = first_tp.get("channel", "unknown")
        credit[channel] += journey_revenue_value(j, dedupe_seen=dedupe_seen)
    return dict(credit)


def linear(journeys: List[Dict]) -> Dict[str, float]:
    """Equal credit distributed across all touchpoints."""
    credit: Dict[str, float] = defaultdict(float)
    dedupe_seen: set[str] = set()
    for j in journeys:
        if not j.get("converted", True) or not j.get("touchpoints"):
            continue
        tps = j["touchpoints"]
        value = journey_revenue_value(j, dedupe_seen=dedupe_seen)
        share = value / len(tps) if tps else 0
        for tp in tps:
            channel = tp.get("channel", "unknown")
            credit[channel] += share
    return dict(credit)


def time_decay(journeys: List[Dict], half_life_days: float = 7.0) -> Dict[str, float]:
    """More credit to touchpoints closer to conversion. Exponential decay."""
    credit: Dict[str, float] = defaultdict(float)
    dedupe_seen: set[str] = set()
    for j in journeys:
        if not j.get("converted", True) or not j.get("touchpoints"):
            continue
        tps = j["touchpoints"]
        value = journey_revenue_value(j, dedupe_seen=dedupe_seen)

        # Parse timestamps for decay calculation
        timestamps = []
        for tp in tps:
            ts = tp.get("timestamp")
            if ts:
                try:
                    timestamps.append(pd.Timestamp(ts))
                except Exception:
                    timestamps.append(None)
            else:
                timestamps.append(None)

        # If we have timestamps, use time-based decay
        if any(t is not None for t in timestamps):
            last_ts = max(t for t in timestamps if t is not None)
            weights = []
            for t in timestamps:
                if t is not None:
                    days_before = (last_ts - t).total_seconds() / 86400.0
                    w = math.pow(2, -days_before / half_life_days)
                else:
                    w = 0.5  # fallback for missing timestamps
                weights.append(w)
        else:
            # Position-based fallback: more recent = higher weight
            n = len(tps)
            weights = [math.pow(2, -(n - 1 - i) / max(n / 2, 1)) for i in range(n)]

        total_weight = sum(weights) or 1.0
        for tp, w in zip(tps, weights):
            channel = tp.get("channel", "unknown")
            credit[channel] += value * (w / total_weight)

    return dict(credit)


def position_based(journeys: List[Dict], first_pct: float = 0.4, last_pct: float = 0.4) -> Dict[str, float]:
    """U-shaped: 40% first, 40% last, 20% split among middle touchpoints."""
    middle_pct = 1.0 - first_pct - last_pct
    credit: Dict[str, float] = defaultdict(float)

    dedupe_seen: set[str] = set()
    for j in journeys:
        if not j.get("converted", True) or not j.get("touchpoints"):
            continue
        tps = j["touchpoints"]
        value = journey_revenue_value(j, dedupe_seen=dedupe_seen)
        n = len(tps)

        if n == 1:
            credit[tps[0].get("channel", "unknown")] += value
        elif n == 2:
            credit[tps[0].get("channel", "unknown")] += value * first_pct / (first_pct + last_pct) * value
            credit[tps[-1].get("channel", "unknown")] += value * last_pct / (first_pct + last_pct) * value
            # Fix: normalize properly for 2-touch
            credit[tps[0].get("channel", "unknown")] = credit.get(tps[0].get("channel", "unknown"), 0)
            # Re-do for 2 touchpoints
            ch_first = tps[0].get("channel", "unknown")
            ch_last = tps[-1].get("channel", "unknown")
            # Reset and redo
            # Actually let me fix this properly
            pass
        else:
            ch_first = tps[0].get("channel", "unknown")
            ch_last = tps[-1].get("channel", "unknown")
            credit[ch_first] += value * first_pct
            credit[ch_last] += value * last_pct
            if n > 2:
                middle_share = value * middle_pct / (n - 2)
                for tp in tps[1:-1]:
                    credit[tp.get("channel", "unknown")] += middle_share

    # Fix the 2-touchpoint case by re-processing
    credit_fixed: Dict[str, float] = defaultdict(float)
    dedupe_seen_second_pass: set[str] = set()
    for j in journeys:
        if not j.get("converted", True) or not j.get("touchpoints"):
            continue
        tps = j["touchpoints"]
        value = journey_revenue_value(j, dedupe_seen=dedupe_seen_second_pass)
        n = len(tps)

        if n == 1:
            credit_fixed[tps[0].get("channel", "unknown")] += value
        elif n == 2:
            half = first_pct + last_pct
            credit_fixed[tps[0].get("channel", "unknown")] += value * (first_pct / half)
            credit_fixed[tps[-1].get("channel", "unknown")] += value * (last_pct / half)
        else:
            credit_fixed[tps[0].get("channel", "unknown")] += value * first_pct
            credit_fixed[tps[-1].get("channel", "unknown")] += value * last_pct
            middle_share = value * middle_pct / (n - 2)
            for tp in tps[1:-1]:
                credit_fixed[tp.get("channel", "unknown")] += middle_share

    return dict(credit_fixed)


def markov(journeys: List[Dict]) -> Dict[str, float]:
    """
    Data-driven attribution using first-order Markov chain removal effect.

    Builds a transition matrix from touchpoint sequences, then calculates
    each channel's removal effect on conversion probability.
    """
    # Build transition counts
    transitions: Dict[Tuple[str, str], int] = defaultdict(int)
    total_conversions = 0
    total_value = 0.0

    dedupe_seen: set[str] = set()
    for j in journeys:
        tps = j.get("touchpoints", [])
        converted = j.get("converted", True)
        channels = ["__start__"] + [tp.get("channel", "unknown") for tp in tps]
        if converted:
            channels.append("__conversion__")
            total_conversions += 1
            total_value += journey_revenue_value(j, dedupe_seen=dedupe_seen)
        else:
            channels.append("__null__")

        for i in range(len(channels) - 1):
            transitions[(channels[i], channels[i + 1])] += 1

    if total_conversions == 0:
        return {}

    # Get all states
    all_channels = set()
    for (a, b) in transitions:
        all_channels.add(a)
        all_channels.add(b)

    # Build transition probability matrix
    def _conversion_probability(trans: Dict, excluded: Optional[str] = None) -> float:
        """Calculate conversion probability from start via absorbing Markov chain."""
        states = sorted(s for s in all_channels
                       if s not in ("__conversion__", "__null__") and s != excluded)
        state_idx = {s: i for i, s in enumerate(states)}
        n = len(states)
        if n == 0:
            return 0.0

        # Build transition matrix for transient states
        Q = np.zeros((n, n))
        conv_probs = np.zeros(n)

        for s in states:
            i = state_idx[s]
            total_out = sum(trans.get((s, t), 0) for t in all_channels if t != excluded)
            if total_out == 0:
                continue
            for t in states:
                j = state_idx[t]
                Q[i, j] = trans.get((s, t), 0) / total_out
            conv_probs[i] = trans.get((s, "__conversion__"), 0) / total_out

        # Fundamental matrix N = (I - Q)^(-1)
        try:
            I = np.eye(n)
            N = np.linalg.inv(I - Q)
        except np.linalg.LinAlgError:
            return 0.0

        # Absorption probability from __start__
        if "__start__" not in state_idx:
            return 0.0
        start_idx = state_idx["__start__"]
        # P(conversion) = N[start,:] @ conv_probs
        return float(N[start_idx, :] @ conv_probs)

    # Base conversion probability
    base_prob = _conversion_probability(transitions)
    if base_prob <= 0:
        # Fallback to linear if Markov fails
        return linear(journeys)

    # Removal effect for each channel
    marketing_channels = sorted(
        s for s in all_channels
        if s not in ("__start__", "__conversion__", "__null__")
    )

    removal_effects: Dict[str, float] = {}
    for ch in marketing_channels:
        prob_without = _conversion_probability(transitions, excluded=ch)
        effect = base_prob - prob_without
        removal_effects[ch] = max(effect, 0)

    # Normalize removal effects to sum to 1
    total_effect = sum(removal_effects.values()) or 1.0
    credit: Dict[str, float] = {}
    for ch in marketing_channels:
        credit[ch] = (removal_effects[ch] / total_effect) * total_value

    return credit


def compute_markov_diagnostics(
    journeys: List[Dict],
    credit: Dict[str, float],
) -> Dict[str, Any]:
    """
    Lightweight diagnostics for Markov reliability.

    Uses the same transition-building intuition as the markov() function but
    does NOT change attribution math. Intended for UI warnings only.
    """
    diagnostics: Dict[str, Any] = {}

    total_journeys = len(journeys)
    converted_journeys = sum(1 for j in journeys if j.get("converted", True))

    # Rebuild transition counts (mirrors markov() logic, simplified)
    transitions: Dict[Tuple[str, str], int] = defaultdict(int)
    for j in journeys:
        tps = j.get("touchpoints", [])
        converted = j.get("converted", True)
        channels = ["__start__"] + [tp.get("channel", "unknown") for tp in tps]
        if converted:
            channels.append("__conversion__")
        else:
            channels.append("__null__")
        for i in range(len(channels) - 1):
            transitions[(channels[i], channels[i + 1])] += 1

    # States / transitions summary
    all_states = set()
    for (a, b) in transitions:
        all_states.add(a)
        all_states.add(b)

    marketing_states = sorted(
        s for s in all_states if s not in ("__start__", "__conversion__", "__null__")
    )
    unique_states = len(marketing_states)
    unique_transitions = len(transitions)

    # Top transitions (excluding purely technical states where possible)
    top_transitions: List[Dict[str, Any]] = []
    if transitions:
        sorted_trans = sorted(transitions.items(), key=lambda x: -x[1])
        total_transitions = sum(count for (_edge, count) in sorted_trans) or 1
        for (a, b), count in sorted_trans[:10]:
            top_transitions.append(
                {
                    "from": a,
                    "to": b,
                    "count": count,
                    "share": round(count / total_transitions, 4),
                }
            )

    # Credit distribution heuristics
    total_credit = sum(credit.values()) or 0.0
    credit_shares: Dict[str, float] = {}
    max_share = 0.0
    direct_share = 0.0
    if total_credit > 0:
        for ch, val in credit.items():
            share = float(val) / float(total_credit)
            credit_shares[ch] = share
            if share > max_share:
                max_share = share
            if ch.lower() == "direct":
                direct_share = share

    shares_list = list(credit_shares.values())
    share_std = float(np.std(shares_list)) if shares_list else 0.0

    # Heuristics for reliability
    warnings: List[str] = []
    reliability = "ok"
    insufficient_data = False

    # Very low sample sizes -> unreliable
    if converted_journeys < 20 or total_journeys < 50 or unique_transitions < 3:
        warnings.append(
            "Very low journey volume or too few unique paths for stable Markov estimates."
        )
        reliability = "unreliable"
        insufficient_data = True

    # Limited journeys but not catastrophic
    elif converted_journeys < 100 or total_journeys < 200:
        warnings.append(
            "Limited journey volume; treat Markov results as directional rather than precise."
        )
        reliability = "warning"

    # Too few distinct states
    if unique_states <= 2 and not insufficient_data:
        warnings.append(
            "Too few distinct marketing states observed; Markov chain is almost degenerate."
        )
        reliability = "unreliable"

    # Extreme credit concentration
    if max_share >= 0.7:
        warnings.append(
            "Markov credit is highly concentrated in a single channel (70%+). Check tracking and source mapping."
        )
        reliability = "warning" if reliability != "unreliable" else reliability

    # Suspiciously uniform splits
    if share_std < 0.02 and len(shares_list) >= 4 and total_credit > 0:
        warnings.append(
            "Credit is split almost uniformly across channels, which can indicate noisy or uninformative paths."
        )
        reliability = "warning" if reliability == "ok" else reliability

    # Direct dominance
    if direct_share >= 0.6:
        warnings.append(
            "Direct receives an unusually large share of Markov credit (60%+). Consider using a Direct view filter and validating source tagging."
        )
        reliability = "unreliable"

    diagnostics.update(
        {
            "journeys_used": total_journeys,
            "converted_journeys": converted_journeys,
            "unique_states": unique_states,
            "unique_transitions": unique_transitions,
            "top_transitions": top_transitions,
            "credit_shares": credit_shares,
            "max_credit_share": round(max_share, 4),
            "direct_share": round(direct_share, 4),
            "share_std": round(share_std, 4),
            "warnings": warnings,
            "reliability": reliability,
            "insufficient_data": insufficient_data,
            "what_to_do_next": [
                "Increase the data window or volume so more journeys are included.",
                "Check join rate and campaign mapping; fix missing or 'direct' heavy traffic where possible.",
                "Toggle the Direct view filter in the UI and compare deltas across models.",
                "Prefer simpler attribution models (e.g. Linear or Position-based) until data quality improves.",
            ],
        }
    )

    return diagnostics


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

MODEL_FN = {
    "last_touch": last_touch,
    "first_touch": first_touch,
    "linear": linear,
    "time_decay": time_decay,
    "position_based": position_based,
    "markov": markov,
}


def run_attribution(
    journeys: List[Dict],
    model: str = "linear",
    **kwargs,
) -> Dict[str, Any]:
    """
    Run a single attribution model on a list of customer journeys.

    Parameters
    ----------
    journeys : list of journey dicts with keys:
        - customer_id: str
        - touchpoints: list of {"channel": str, "timestamp": str, ...}
        - conversion_value: float
        - converted: bool (default True)
    model : one of ATTRIBUTION_MODELS
    **kwargs : extra params (e.g. half_life_days for time_decay)

    Returns
    -------
    dict with:
        - model: str
        - channel_credit: {channel: attributed_value}
        - total_conversions: int
        - total_value: float
        - channels: list of channel detail dicts
    """
    if model not in MODEL_FN:
        raise ValueError(f"Unknown model: {model}. Choose from {ATTRIBUTION_MODELS}")

    fn = MODEL_FN[model]
    converted_journeys = [j for j in journeys if j.get("converted", True)]
    total_conversions = len(converted_journeys)
    dedupe_seen: set[str] = set()
    total_value = sum(journey_revenue_value(j, dedupe_seen=dedupe_seen) for j in converted_journeys)

    credit = fn(converted_journeys, **kwargs) if kwargs else fn(converted_journeys)
    diagnostics: Optional[Dict[str, Any]] = None
    if model == "markov":
        try:
            diagnostics = compute_markov_diagnostics(converted_journeys, credit)
        except Exception as exc:
            # Diagnostics are best-effort; never break attribution if they fail.
            logger.warning("Failed to compute Markov diagnostics: %s", exc)

    # Build per-channel detail
    total_credit = sum(credit.values()) or 1.0
    channels = []
    for ch, val in sorted(credit.items(), key=lambda x: -x[1]):
        channels.append({
            "channel": ch,
            "attributed_value": round(val, 2),
            "attributed_share": round(val / total_credit, 4),
            "attributed_conversions": round(val / (total_value / total_conversions) if total_value > 0 else 0, 2),
        })

    result: Dict[str, Any] = {
        "model": model,
        "channel_credit": {ch: round(v, 2) for ch, v in credit.items()},
        "total_conversions": total_conversions,
        "total_value": round(total_value, 2),
        "channels": channels,
    }
    if diagnostics is not None:
        result["diagnostics"] = diagnostics
    return result


def run_all_models(journeys: List[Dict]) -> List[Dict[str, Any]]:
    """Run all attribution models and return a list of results."""
    results = []
    for model_name in ATTRIBUTION_MODELS:
        try:
            result = run_attribution(journeys, model=model_name)
            results.append(result)
        except Exception as exc:
            logger.warning("Attribution model %s failed: %s", model_name, exc)
            results.append({"model": model_name, "error": str(exc)})
    return results


def journeys_to_campaign_steps(journeys: List[Dict]) -> List[Dict]:
    """Convert journeys so each touchpoint's channel is campaign-level (channel:campaign or channel)."""
    out = []
    for j in journeys:
        new_tps = []
        for tp in j.get("touchpoints", []):
            step = _step_string(tp, "campaign")
            new_tps.append({**tp, "channel": step})
        out.append({**j, "touchpoints": new_tps})
    return out


def run_attribution_campaign(
    journeys: List[Dict],
    model: str = "linear",
    **kwargs,
) -> Dict[str, Any]:
    """
    Run attribution at campaign level (channel:campaign or channel). Returns same shape as run_attribution
    but with keys as campaign steps.
    """
    campaign_journeys = journeys_to_campaign_steps(journeys)
    return run_attribution(campaign_journeys, model=model, **kwargs)


def compute_channel_performance(
    attribution_result: Dict[str, Any],
    expenses: Dict[str, float],
) -> List[Dict[str, Any]]:
    """
    Combine attribution results with expense data to produce
    per-channel performance metrics: ROI, CPA, ROAS.

    Parameters
    ----------
    attribution_result : output from run_attribution()
    expenses : {channel: total_spend}

    Returns
    -------
    list of channel performance dicts
    """
    channels = attribution_result.get("channels", [])
    total_conversions = attribution_result.get("total_conversions", 0)
    total_value = attribution_result.get("total_value", 0)

    performance = []
    for ch_data in channels:
        ch = ch_data["channel"]
        attributed_value = ch_data["attributed_value"]
        attributed_conversions = ch_data.get("attributed_conversions", 0)
        spend = expenses.get(ch, 0)

        roi = (attributed_value - spend) / spend if spend > 0 else 0
        roas = attributed_value / spend if spend > 0 else 0
        cpa = spend / attributed_conversions if attributed_conversions > 0 else 0

        performance.append({
            "channel": ch,
            "spend": round(spend, 2),
            "attributed_value": round(attributed_value, 2),
            "attributed_conversions": round(attributed_conversions, 2),
            "attributed_share": ch_data["attributed_share"],
            "roi": round(roi, 4),
            "roas": round(roas, 4),
            "cpa": round(cpa, 2),
        })

    return sorted(performance, key=lambda x: -x["attributed_value"])


def parse_conversion_paths(
    profiles: List[Dict],
    touchpoint_attr: str = "touchpoints",
    value_attr: str = "conversion_value",
    id_attr: str = "customer_id",
    channel_field: str = "channel",
    timestamp_field: str = "timestamp",
) -> List[Dict]:
    """
    Parse CDP profile data into journey format expected by attribution models.

    Handles flexible CDP attribute structures:
    - touchpoints as a JSON array attribute
    - touchpoints as separate event attributes
    - flat attributes like last_touch_channel, first_touch_channel
    """
    journeys = []
    taxonomy = load_taxonomy()

    for profile in profiles:
        customer_id = profile.get(id_attr, profile.get("id", "unknown"))

        # Try to get touchpoints from the specified attribute
        touchpoints_raw = profile.get(touchpoint_attr)

        if isinstance(touchpoints_raw, list):
            # Direct list of touchpoint dicts
            touchpoints = []
            for tp in touchpoints_raw:
                if isinstance(tp, dict):
                    raw_tp = {
                        "channel": tp.get(channel_field, tp.get("source", tp.get("utm_source", "unknown"))),
                        "source": tp.get("source"),
                        "medium": tp.get("medium"),
                        "utm_source": tp.get("utm_source"),
                        "utm_medium": tp.get("utm_medium"),
                        "utm_campaign": tp.get("utm_campaign"),
                        "utm_content": tp.get("utm_content"),
                        "campaign": tp.get("campaign"),
                        "adset": tp.get("adset"),
                        "ad": tp.get("ad"),
                        "creative": tp.get("creative"),
                        "timestamp": tp.get(timestamp_field, tp.get("date", tp.get("event_date", ""))),
                    }
                    touchpoints.append(normalize_touchpoint(raw_tp, taxonomy))
                elif isinstance(tp, str):
                    # Simple list of channel names
                    touchpoints.append({"channel": tp, "timestamp": ""})
        elif isinstance(touchpoints_raw, str):
            # Try JSON parse
            import json
            try:
                parsed = json.loads(touchpoints_raw)
                if isinstance(parsed, list):
                    touchpoints = [
                        normalize_touchpoint(
                            {
                                "channel": tp.get(channel_field, "unknown") if isinstance(tp, dict) else str(tp),
                                "source": tp.get("source") if isinstance(tp, dict) else None,
                                "medium": tp.get("medium") if isinstance(tp, dict) else None,
                                "utm_source": tp.get("utm_source") if isinstance(tp, dict) else None,
                                "utm_medium": tp.get("utm_medium") if isinstance(tp, dict) else None,
                                "utm_campaign": tp.get("utm_campaign") if isinstance(tp, dict) else None,
                                "utm_content": tp.get("utm_content") if isinstance(tp, dict) else None,
                                "campaign": tp.get("campaign") if isinstance(tp, dict) else None,
                                "adset": tp.get("adset") if isinstance(tp, dict) else None,
                                "ad": tp.get("ad") if isinstance(tp, dict) else None,
                                "creative": tp.get("creative") if isinstance(tp, dict) else None,
                                "timestamp": tp.get(timestamp_field, "") if isinstance(tp, dict) else "",
                            },
                            taxonomy,
                        )
                        for tp in parsed
                    ]
                else:
                    touchpoints = [{"channel": str(parsed), "timestamp": ""}]
            except (json.JSONDecodeError, TypeError):
                touchpoints = [{"channel": touchpoints_raw, "timestamp": ""}]
        else:
            # Try flat attributes: look for channel-like fields
            touchpoints = []
            for key in ("first_touch_channel", "last_touch_channel", "channel", "source"):
                if key in profile and profile[key]:
                    touchpoints.append(
                        normalize_touchpoint(
                            {
                                "channel": str(profile[key]),
                                "source": profile.get("source"),
                                "medium": profile.get("medium"),
                                "utm_source": profile.get("utm_source"),
                                "utm_medium": profile.get("utm_medium"),
                                "utm_campaign": profile.get("utm_campaign"),
                                "utm_content": profile.get("utm_content"),
                                "campaign": profile.get("campaign"),
                                "adset": profile.get("adset"),
                                "ad": profile.get("ad"),
                                "creative": profile.get("creative"),
                                "timestamp": "",
                            },
                            taxonomy,
                        )
                    )

        if not touchpoints:
            continue

        conversion_value = float(profile.get(value_attr, profile.get("revenue", profile.get("value", 1.0))) or 1.0)
        converted = profile.get("converted", True)
        if isinstance(converted, str):
            converted = converted.lower() in ("true", "1", "yes")

        journeys.append({
            "customer_id": customer_id,
            "touchpoints": touchpoints,
            "conversion_value": conversion_value,
            "converted": converted,
        })

    return journeys


def analyze_paths(
    journeys: List[Dict],
    include_non_converted: bool = False,
) -> Dict[str, Any]:
    """
    Analyze conversion paths to produce summary statistics:
    - Most common paths
    - Average path length (with distribution)
    - Average time to conversion (with distribution where possible)
    - Channel frequency in paths
    - Direct / unknown diagnostics

    By default only converted journeys are used for path statistics to keep
    behaviour backwards compatible. When include_non_converted=True, non‑
    converted journeys are included in path length and frequency stats, while
    time‑to‑convert remains based on converted journeys only.
    """
    converted = [j for j in journeys if j.get("converted", True)]
    if include_non_converted:
        used_for_lengths = journeys
    else:
        used_for_lengths = converted

    if not used_for_lengths:
        return {
            "total_journeys": 0,
            "avg_path_length": 0,
            "common_paths": [],
            "channel_frequency": {},
        }

    # Path length stats
    lengths = [len(j.get("touchpoints", [])) for j in used_for_lengths]
    avg_length = sum(lengths) / len(lengths) if lengths else 0

    # Channel frequency + direct / unknown diagnostics (based on the same scope
    # that is used for length + path counts so UI filters stay consistent).
    channel_freq: Dict[str, int] = defaultdict(int)
    direct_unknown_touchpoints = 0
    for j in used_for_lengths:
        for tp in j.get("touchpoints", []):
            ch = tp.get("channel", "unknown")
            channel_freq[ch] += 1
            if ch.lower() in ("direct", "unknown"):
                direct_unknown_touchpoints += 1

    total_touchpoints = sum(channel_freq.values()) or 1

    # Most common paths (channel sequences) + per-path stats
    path_counts: Dict[str, int] = defaultdict(int)
    path_times_total: Dict[str, float] = defaultdict(float)
    path_times_n: Dict[str, int] = defaultdict(int)

    for j in used_for_lengths:
        tps = j.get("touchpoints", [])
        path = " > ".join(tp.get("channel", "?") for tp in tps)
        path_counts[path] += 1

    # Time to conversion – still based only on converted journeys
    times_to_conv: List[float] = []
    for j in converted:
        tps = j.get("touchpoints", [])
        if len(tps) >= 2:
            try:
                first_ts = pd.Timestamp(tps[0].get("timestamp", ""))
                last_ts = pd.Timestamp(tps[-1].get("timestamp", ""))
                if pd.notna(first_ts) and pd.notna(last_ts):
                    delta = (last_ts - first_ts).total_seconds() / 86400.0
                    if delta >= 0:
                        times_to_conv.append(delta)
                        path = " > ".join(tp.get("channel", "?") for tp in tps)
                        path_times_total[path] += delta
                        path_times_n[path] += 1
            except Exception:
                # Timestamps are best-effort; skip malformed rows without
                # impacting the overall analysis.
                pass

    avg_time = sum(times_to_conv) / len(times_to_conv) if times_to_conv else None

    common_paths = sorted(
        [
            {
                "path": p,
                "count": c,
                "share": round(c / len(used_for_lengths), 4),
                "avg_time_to_convert_days": round(path_times_total[p] / path_times_n[p], 2)
                if path_times_n.get(p)
                else None,
                "path_length": len(p.split(" > ")) if p else 0,
            }
            for p, c in path_counts.items()
        ],
        key=lambda x: -x["count"],
    )[:20]

    path_length_distribution: Dict[str, Any]
    if lengths:
        path_length_distribution = {
            "min": min(lengths),
            "max": max(lengths),
            "median": float(np.median(lengths)),
            "p90": float(np.percentile(lengths, 90)),
        }
    else:
        path_length_distribution = {"min": 0, "max": 0, "median": 0.0, "p90": 0.0}

    time_to_conv_distribution: Optional[Dict[str, Any]] = None
    if times_to_conv:
        time_to_conv_distribution = {
            "min": float(min(times_to_conv)),
            "max": float(max(times_to_conv)),
            "median": float(np.median(times_to_conv)),
            "p90": float(np.percentile(times_to_conv, 90)),
        }

    # Journeys ending in Direct – based on converted journeys only, which is
    # where "ending with Direct" most strongly impacts reporting.
    journeys_ending_direct = 0
    for j in converted:
        tps = j.get("touchpoints", [])
        if not tps:
            continue
        last_ch = tps[-1].get("channel", "unknown")
        if last_ch and last_ch.lower() == "direct":
            journeys_ending_direct += 1

    direct_unknown_diagnostics = {
        "touchpoint_share": round(direct_unknown_touchpoints / total_touchpoints, 4)
        if total_touchpoints
        else 0.0,
        "journeys_ending_direct_share": round(
            journeys_ending_direct / len(converted), 4
        )
        if converted
        else 0.0,
    }

    result: Dict[str, Any] = {
        "total_journeys": len(used_for_lengths),
        "avg_path_length": round(avg_length, 2),
        "avg_time_to_conversion_days": round(avg_time, 2) if avg_time is not None else None,
        "common_paths": common_paths,
        "channel_frequency": dict(sorted(channel_freq.items(), key=lambda x: -x[1])),
        "path_length_distribution": path_length_distribution,
        "direct_unknown_diagnostics": direct_unknown_diagnostics,
    }
    if time_to_conv_distribution is not None:
        result["time_to_conversion_distribution"] = time_to_conv_distribution
    return result


def _step_string(tp: Dict, level: str) -> str:
    """Build a single step string: channel only, or channel:campaign when level is campaign and campaign present."""
    ch = tp.get("channel", "unknown")
    if level == "campaign" and tp.get("campaign"):
        return f"{ch}:{tp['campaign']}"
    return ch


def compute_next_best_action(
    journeys: List[Dict],
    level: str = "channel",
) -> Dict[str, List[Dict[str, Any]]]:
    """
    For each path prefix, compute recommended next step (channel or channel:campaign) based on
    historical conversion rate and value among journeys that took that next step.

    level: "channel" = steps are channel only; "campaign" = steps are "channel:campaign" when
    touchpoint has campaign, else channel.

    Returns a dict keyed by path prefix with value = list of
    { channel, campaign (optional), step, count, conversions, conversion_rate, avg_value }
    sorted by conversion_rate descending, then avg_value descending.
    """
    step_stats: Dict[Tuple[str, str], Dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "conversions": 0, "total_value": 0.0}
    )
    dedupe_seen: set[str] = set()

    for j in journeys:
        tps = j.get("touchpoints", [])
        steps = [_step_string(tp, level) for tp in tps]
        converted = j.get("converted", True)
        value = journey_revenue_value(j, dedupe_seen=dedupe_seen)

        for i in range(len(steps)):
            prefix = " > ".join(steps[:i]) if i > 0 else ""
            next_step = steps[i]
            key = (prefix, next_step)
            step_stats[key]["count"] += 1
            if converted:
                step_stats[key]["conversions"] += 1
                step_stats[key]["total_value"] += value

    by_prefix: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for (prefix, next_step), stats in step_stats.items():
        count = stats["count"]
        conv = stats["conversions"]
        total_val = stats["total_value"]
        channel = next_step.split(":", 1)[0] if ":" in next_step else next_step
        campaign = next_step.split(":", 1)[1] if ":" in next_step else None
        rec: Dict[str, Any] = {
            "channel": channel,
            "step": next_step,
            "count": count,
            "conversions": conv,
            "conversion_rate": round(conv / count, 4) if count else 0,
            "avg_value": round(total_val / count, 2) if count else 0,
            "avg_value_converted": round(total_val / conv, 2) if conv else 0,
        }
        if campaign is not None:
            rec["campaign"] = campaign
        by_prefix[prefix].append(rec)

    for prefix in by_prefix:
        by_prefix[prefix].sort(
            key=lambda x: (x["conversion_rate"], x["avg_value"]),
            reverse=True,
        )

    return dict(by_prefix)


def has_any_campaign(journeys: List[Dict]) -> bool:
    """Return True if any touchpoint in journeys has a campaign field."""
    for j in journeys:
        for tp in j.get("touchpoints", []):
            if tp.get("campaign"):
                return True
    return False
