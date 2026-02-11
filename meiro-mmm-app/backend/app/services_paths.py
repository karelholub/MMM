"""Helpers for path archetypes (clustering) and path anomaly detection.

This module is intentionally *stateless*: it computes clusters/anomalies from the
current in-memory journeys payload. The API can later persist results into the
`path_clusters` / `path_anomalies` tables if/when scheduling is enabled.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from collections import Counter, defaultdict

import hashlib
import math
import logging

import pandas as pd

# Optional ML stack. The app can run in "minimal" mode without clustering.
try:
    import numpy as np  # type: ignore
    from sklearn.cluster import MiniBatchKMeans  # type: ignore
    from sklearn.feature_extraction import DictVectorizer  # type: ignore
    from sklearn.feature_extraction.text import TfidfTransformer  # type: ignore
    from sklearn.metrics import adjusted_rand_score, silhouette_score  # type: ignore
    from sklearn.preprocessing import normalize  # type: ignore

    _HAS_ML = True
except Exception:  # pragma: no cover
    np = None  # type: ignore
    MiniBatchKMeans = None  # type: ignore
    DictVectorizer = None  # type: ignore
    TfidfTransformer = None  # type: ignore
    silhouette_score = None  # type: ignore
    adjusted_rand_score = None  # type: ignore
    normalize = None  # type: ignore
    _HAS_ML = False


logger = logging.getLogger(__name__)


@dataclass
class PathRecord:
    path: str
    count: int
    avg_length: float
    avg_time_to_conv_days: Optional[float]
    top_channels: List[str]


def _safe_channel(v: Any) -> str:
    s = str(v) if v is not None else "unknown"
    s = s.strip() or "unknown"
    return s


def _parse_ts(v: Any) -> Optional[pd.Timestamp]:
    if not v:
        return None
    try:
        ts = pd.to_datetime(v, errors="coerce", utc=True)
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None


def _path_hash(path_str: str) -> str:
    return hashlib.sha256(path_str.encode("utf-8")).hexdigest()


def _journey_to_path_row(j: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract a path-level row from a single journey (converted only)."""
    if not j.get("converted", True):
        return None
    tps = j.get("touchpoints") or []
    if not tps:
        return None

    channels = [_safe_channel(tp.get("channel")) for tp in tps]
    path_str = " > ".join(channels)
    path_h = _path_hash(path_str)

    parsed_ts = [_parse_ts(tp.get("timestamp")) for tp in tps]
    valid_ts = [ts for ts in parsed_ts if ts is not None]

    time_to_conv_days: Optional[float] = None
    conv_ts: Optional[pd.Timestamp] = None
    if valid_ts:
        first = min(valid_ts)
        last = max(valid_ts)
        if last >= first:
            time_to_conv_days = float((last - first).total_seconds() / 86400.0)
        conv_ts = last

    # repeated steps (consecutive)
    consecutive_repeats = 0
    for a, b in zip(channels, channels[1:]):
        if a == b:
            consecutive_repeats += 1

    return {
        "path": path_str,
        "path_hash": path_h,
        "channels": channels,
        "length": len(channels),
        "unique_channels": len(set(channels)),
        "time_to_conv": time_to_conv_days,
        "missing_ts_touchpoints": int(sum(1 for ts in parsed_ts if ts is None)),
        "touchpoints": len(tps),
        "non_monotonic_ts": int(_is_non_monotonic(valid_ts, parsed_ts)),
        "consecutive_repeats": consecutive_repeats,
        # Last valid timestamp used as a proxy for "conversion date" for
        # period-over-period comparisons and cluster-level timing stats.
        "conversion_ts": conv_ts,
        "start_channel": channels[0] if channels else "unknown",
        "end_channel": channels[-1] if channels else "unknown",
    }


def _is_non_monotonic(valid_ts: List[pd.Timestamp], parsed_ts: List[Optional[pd.Timestamp]]) -> bool:
    """True if timestamps exist but are not non-decreasing in order."""
    if not valid_ts:
        return False
    prev: Optional[pd.Timestamp] = None
    for ts in parsed_ts:
        if ts is None:
            continue
        if prev is not None and ts < prev:
            return True
        prev = ts
    return False


def _extract_path_rows(journeys: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for j in journeys or []:
        r = _journey_to_path_row(j)
        if r is not None:
            rows.append(r)
    if not rows:
        return pd.DataFrame(columns=["path", "path_hash", "channels", "length"])
    return pd.DataFrame(rows)


def _aggregate_paths(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-journey rows into per-path rows with counts and summary stats."""
    if df.empty:
        return df
    # Keep one path_hash per path (stable by definition)
    grouped = df.groupby("path", dropna=False)
    out = grouped.agg(
        count=("path", "size"),
        path_hash=("path_hash", "first"),
        length=("length", "mean"),
        time_to_conv=("time_to_conv", "mean"),
        consecutive_repeats=("consecutive_repeats", "mean"),
    ).reset_index()
    # Ensure proper types
    out["count"] = out["count"].astype(int)
    return out


def _top_channels_for_paths(df: pd.DataFrame, paths: Iterable[str], top_n: int = 5) -> List[str]:
    if df.empty:
        return []
    mask = df["path"].isin(list(paths))
    if not mask.any():
        return []
    all_channels: List[str] = []
    for ch_list in df.loc[mask, "channels"].tolist():
        if isinstance(ch_list, list):
            all_channels.extend([_safe_channel(x) for x in ch_list])
    if not all_channels:
        return []
    return list(pd.Series(all_channels).value_counts().head(top_n).index.astype(str))


def _path_features(steps: List[str], max_pos: int = 6) -> Dict[str, float]:
    """Order-aware sparse features for a path."""
    feats = defaultdict(float)
    if not steps:
        feats["len"] = 0.0
        feats["uniq"] = 0.0
        feats["start=unknown"] = 1.0
        feats["end=unknown"] = 1.0
        return dict(feats)

    feats["len"] = float(len(steps))
    feats["uniq"] = float(len(set(steps)))
    feats[f"start={steps[0]}"] = 1.0
    feats[f"end={steps[-1]}"] = 1.0

    # unigrams + bigrams (transitions)
    for ch in steps:
        feats[f"ch={ch}"] += 1.0
    for a, b in zip(steps, steps[1:]):
        feats[f"tr={a}->{b}"] += 1.0

    # positional (first few)
    for i, ch in enumerate(steps[:max_pos]):
        feats[f"pos{i}={ch}"] = 1.0

    # repetition features
    consecutive_repeats = 0
    for a, b in zip(steps, steps[1:]):
        if a == b:
            consecutive_repeats += 1
    feats["consecutive_repeats"] = float(consecutive_repeats)
    feats["repeat_ratio"] = float(consecutive_repeats) / float(max(1, len(steps) - 1))
    return dict(feats)


def _auto_select_k(X, k_min: int, k_max: int, random_state: int = 42) -> Tuple[int, Optional[float]]:
    """Pick k by silhouette score on a capped sample."""
    n = X.shape[0]
    if n < (k_min + 1):
        return max(1, min(3, n)), None

    k_min = max(2, int(k_min))
    k_max = max(k_min, int(k_max))
    k_max = min(k_max, n - 1)  # silhouette requires at least 2 clusters and <= n-1

    best_k = k_min
    best_score: Optional[float] = None

    # Cap evaluation to keep this endpoint fast.
    sample_n = min(2000, n)
    if sample_n < n:
        rng = np.random.default_rng(seed=random_state)
        idx = rng.choice(n, size=sample_n, replace=False)
        X_eval = X[idx]
    else:
        X_eval = X

    for k in range(k_min, k_max + 1):
        try:
            model = MiniBatchKMeans(n_clusters=k, random_state=random_state, n_init="auto", batch_size=1024)
            labels = model.fit_predict(X_eval)
            # silhouette can fail if a cluster has 1 sample
            score = float(silhouette_score(X_eval, labels, metric="cosine"))
            if best_score is None or score > best_score:
                best_score = score
                best_k = k
        except Exception:
            continue

    return best_k, best_score


def compute_path_archetypes(
    journeys: List[Dict[str, Any]],
    conversion_key: Optional[str] = None,
    *,
    k_mode: str = "auto",  # "auto" | "fixed"
    k: Optional[int] = None,
    k_min: int = 3,
    k_max: int = 10,
    top_paths_per_cluster: int = 5,
    max_clusters_returned: int = 12,
    random_state: int = 42,
    enable_stability: bool = True,
    enable_compare_previous: bool = False,
) -> Dict[str, Any]:
    """Cluster conversion paths into archetypes.

    Algorithm:
    - Aggregate journeys into unique paths with counts
    - Vectorize paths using order-aware sparse features (unigrams, transitions, positions)
    - Cluster with MiniBatchKMeans
    - Summarize clusters with top paths/channels and simple stats
    """
    df_j = _extract_path_rows(journeys)
    if df_j.empty:
        return {
            "clusters": [],
            "total_converted": 0,
            "diagnostics": {"algorithm": "kmeans", "reason": "no_paths"},
        }

    df_p = _aggregate_paths(df_j)
    if df_p.empty:
        return {
            "clusters": [],
            "total_converted": 0,
            "diagnostics": {"algorithm": "kmeans", "reason": "no_paths"},
        }

    total_converted = int(df_p["count"].sum())
    n_paths = int(df_p.shape[0])

    # Very small: keep "one path per cluster" but still return the richer shape.
    if n_paths <= 2:
        clusters = []
        for idx, row in df_p.sort_values("count", ascending=False).iterrows():
            path = str(row["path"])
            count = int(row["count"])
            clusters.append(
                {
                    "id": len(clusters) + 1,
                    "name": path,
                    "size": count,
                    "share": count / total_converted if total_converted else 0.0,
                    "avg_length": float(row["length"]),
                    "avg_time_to_conversion_days": float(row["time_to_conv"]) if not pd.isna(row["time_to_conv"]) else None,
                    "top_channels": _top_channels_for_paths(df_j, [path], top_n=5),
                    "top_paths": [{"path": path, "count": count, "share": count / total_converted if total_converted else 0.0}],
                    "representative_path": path,
                }
            )
        return {
            "clusters": clusters,
            "total_converted": total_converted,
            "diagnostics": {"algorithm": "distinct_paths", "n_unique_paths": n_paths},
        }

    # Helper: fallback to simple distinct-path archetypes (no clustering).
    def _distinct_paths_fallback(reason: str) -> Dict[str, Any]:
        top_n = max(1, int(max_clusters_returned))
        clusters = []
        for _, row in df_p.sort_values("count", ascending=False).head(top_n).iterrows():
            path = str(row["path"])
            count = int(row["count"])
            clusters.append(
                {
                    "id": len(clusters) + 1,
                    "name": path,
                    "size": count,
                    "share": count / total_converted if total_converted else 0.0,
                    "avg_length": float(row["length"]),
                    "avg_time_to_conversion_days": float(row["time_to_conv"])
                    if not pd.isna(row["time_to_conv"])
                    else None,
                    "top_channels": _top_channels_for_paths(df_j, [path], top_n=5),
                    "top_paths": [
                        {
                            "path": path,
                            "count": count,
                            "share": count / total_converted if total_converted else 0.0,
                        }
                    ],
                    "representative_path": path,
                }
            )
        return {
            "clusters": clusters,
            "total_converted": total_converted,
            "diagnostics": {
                "algorithm": "distinct_paths",
                "n_unique_paths": n_paths,
                "reason": reason,
            },
        }

    # If the ML stack is unavailable, fall back to "top distinct paths" archetypes.
    if not _HAS_ML:
        return _distinct_paths_fallback("ml_unavailable")

    try:
        # Build features for each unique path
        path_steps: List[List[str]] = [str(p).split(" > ") for p in df_p["path"].tolist()]
        feature_dicts = [_path_features([_safe_channel(s) for s in steps]) for steps in path_steps]

        vec = DictVectorizer(sparse=True)
        X = vec.fit_transform(feature_dicts)
        # tf-idf + l2 norm improves cosine distance behavior
        X = TfidfTransformer().fit_transform(X)
        X = normalize(X, norm="l2", copy=False)

        weights = df_p["count"].astype(float).to_numpy()

        if k_mode == "fixed" and k is not None:
            k_selected = int(max(2, min(int(k), n_paths)))
            sil = None
        else:
            k_selected, sil = _auto_select_k(
                X, k_min=k_min, k_max=min(k_max, n_paths - 1), random_state=random_state
            )
            k_selected = int(max(2, min(k_selected, n_paths)))

        model = MiniBatchKMeans(
            n_clusters=k_selected, random_state=random_state, n_init="auto", batch_size=2048
        )
        try:
            labels = model.fit_predict(X, sample_weight=weights)
        except TypeError:
            # Older scikit-learn: no sample_weight
            labels = model.fit_predict(X)

        df_p = df_p.copy()
        df_p["cluster"] = labels.astype(int)
        df_p_with_clusters = df_p.copy()

        # Optional stability: run a second clustering with a different seed and
        # compare assignments using adjusted Rand index.
        stability_score: float | None = None
        if enable_stability and _HAS_ML and adjusted_rand_score is not None:
            try:
                model2 = MiniBatchKMeans(
                    n_clusters=k_selected,
                    random_state=random_state + 17,
                    n_init="auto",
                    batch_size=2048,
                )
                try:
                    labels2 = model2.fit_predict(X, sample_weight=weights)
                except TypeError:
                    labels2 = model2.fit_predict(X)
                ari = float(adjusted_rand_score(labels, labels2))
                # Clamp to [0, 1] for interpretability.
                stability_score = max(0.0, min(1.0, ari))
            except Exception:
                stability_score = None

    except Exception as exc:
        # If anything in the ML stack fails (missing deps, numerical issues, etc.),
        # fall back to a simple distinct-path summary instead of raising 500s.
        logger.warning("compute_path_archetypes: ML pipeline failed, falling back to distinct_paths: %s", exc)
        return _distinct_paths_fallback("ml_failed")

    # Map paths to clusters for downstream stats.
    path_to_cluster: Dict[str, int] = {}
    for _, row in df_p_with_clusters.iterrows():
        path_to_cluster[str(row["path"])] = int(row["cluster"])

    # Attach cluster id to per-journey rows where possible.
    df_j_with_cluster = df_j.copy()
    df_j_with_cluster["cluster"] = df_j_with_cluster["path"].map(path_to_cluster).astype("Int64")

    # Optional period-over-period comparison on journey conversion timestamps.
    per_cluster_compare: Dict[int, Dict[str, Any]] = {}
    compare_available = False
    if enable_compare_previous and "conversion_ts" in df_j_with_cluster.columns:
        conv_series = df_j_with_cluster["conversion_ts"].dropna()
        if not conv_series.empty:
            conv_series_sorted = conv_series.sort_values()
            # Require a minimal number of journeys with timestamps to make a split meaningful.
            if len(conv_series_sorted) >= max(40, k_selected * 4):
                split_idx = len(conv_series_sorted) // 2
                split_ts = conv_series_sorted.iloc[split_idx]
                prev_mask = df_j_with_cluster["conversion_ts"] < split_ts
                curr_mask = df_j_with_cluster["conversion_ts"] >= split_ts
                df_prev = df_j_with_cluster[prev_mask].dropna(subset=["cluster"])
                df_curr = df_j_with_cluster[curr_mask].dropna(subset=["cluster"])
                if not df_prev.empty and not df_curr.empty:
                    total_prev = float(len(df_prev))
                    total_curr = float(len(df_curr))
                    # Shares by cluster
                    prev_sizes: Dict[int, int] = (
                        df_prev.groupby("cluster")["path"]
                        .size()
                        .astype(int)
                        .to_dict()  # type: ignore[assignment]
                    )
                    curr_sizes: Dict[int, int] = (
                        df_curr.groupby("cluster")["path"]
                        .size()
                        .astype(int)
                        .to_dict()  # type: ignore[assignment]
                    )

                    # Medians by cluster for length and time-to-conversion.
                    prev_len_med: Dict[int, float] = {}
                    curr_len_med: Dict[int, float] = {}
                    prev_ttc_med: Dict[int, Optional[float]] = {}
                    curr_ttc_med: Dict[int, Optional[float]] = {}

                    for cl, group in df_prev.groupby("cluster"):
                        lens = [float(x) for x in group.get("length", []) if x is not None]
                        ttcs = [
                            float(x)
                            for x in group.get("time_to_conv", []).dropna().tolist()
                            if x is not None
                        ]
                        prev_len_med[int(cl)] = _median(lens) if lens else 0.0
                        prev_ttc_med[int(cl)] = _median(ttcs) if ttcs else None

                    for cl, group in df_curr.groupby("cluster"):
                        lens = [float(x) for x in group.get("length", []) if x is not None]
                        ttcs = [
                            float(x)
                            for x in group.get("time_to_conv", []).dropna().tolist()
                            if x is not None
                        ]
                        curr_len_med[int(cl)] = _median(lens) if lens else 0.0
                        curr_ttc_med[int(cl)] = _median(ttcs) if ttcs else None

                    for cl in sorted(
                        set(prev_sizes.keys()) | set(curr_sizes.keys())
                    ):
                        cl_int = int(cl)
                        prev_share = prev_sizes.get(cl_int, 0) / (total_prev or 1.0)
                        curr_share = curr_sizes.get(cl_int, 0) / (total_curr or 1.0)
                        per_cluster_compare[cl_int] = {
                            "share_previous": prev_share,
                            "share_current": curr_share,
                            "share_delta": curr_share - prev_share,
                            "median_length_previous": prev_len_med.get(cl_int),
                            "median_length_current": curr_len_med.get(cl_int),
                            "median_length_delta": (
                                (curr_len_med.get(cl_int) or 0.0)
                                - (prev_len_med.get(cl_int) or 0.0)
                            ),
                            "median_ttc_previous_days": prev_ttc_med.get(cl_int),
                            "median_ttc_current_days": curr_ttc_med.get(cl_int),
                            "median_ttc_delta_days": (
                                (curr_ttc_med.get(cl_int) or 0.0)
                                - (prev_ttc_med.get(cl_int) or 0.0)
                            ),
                        }
                    compare_available = True

    # Cluster summaries (weighted by count)
    clusters: List[Dict[str, Any]] = []
    channel_counts_overall: Counter[str] = Counter()
    for ch_list in df_j.get("channels", []):
        if isinstance(ch_list, list):
            channel_counts_overall.update(_safe_channel(ch) for ch in ch_list)
    overall_total_channels = float(sum(channel_counts_overall.values()) or 1.0)
    overall_channel_dist = {
        ch: cnt / overall_total_channels for ch, cnt in channel_counts_overall.items()
    }

    # Pre-compute basic journey-level stats by cluster.
    cluster_lengths: Dict[int, List[float]] = defaultdict(list)
    cluster_ttc: Dict[int, List[float]] = defaultdict(list)
    cluster_channel_counts: Dict[int, Counter[str]] = defaultdict(Counter)
    cluster_transitions: Dict[int, Counter[tuple[str, str]]] = defaultdict(Counter)
    cluster_value_sums: Dict[int, float] = defaultdict(float)
    cluster_value_counts: Dict[int, int] = defaultdict(int)

    for _, row in df_j_with_cluster.dropna(subset=["cluster"]).iterrows():
        cl = int(row["cluster"])
        length = float(row.get("length") or 0.0)
        if length:
            cluster_lengths[cl].append(length)
        ttc = row.get("time_to_conv")
        if pd.notna(ttc):
            cluster_ttc[cl].append(float(ttc))
        ch_list = row.get("channels") or []
        if isinstance(ch_list, list):
            safe_channels = [_safe_channel(c) for c in ch_list]
            cluster_channel_counts[cl].update(safe_channels)
            for a, b in zip(safe_channels, safe_channels[1:]):
                cluster_transitions[cl][(a, b)] += 1

    # Conversion value (if present on journeys) – best-effort cluster summary.
    for j in journeys or []:
        tps = j.get("touchpoints") or []
        channels = [_safe_channel(tp.get("channel")) for tp in tps] or ["unknown"]
        path = " > ".join(channels)
        cl = path_to_cluster.get(path)
        if cl is None:
            continue
        value = j.get("conversion_value")
        if isinstance(value, (int, float)):
            cluster_value_sums[cl] += float(value)
            cluster_value_counts[cl] += 1

    # Per-path counts for representativeness and variants/outliers.
    path_counts: Dict[str, int] = {
        str(row["path"]): int(row["count"]) for _, row in df_p_with_clusters.iterrows()
    }

    for cl in sorted(df_p_with_clusters["cluster"].unique().tolist()):
        g = df_p_with_clusters[df_p_with_clusters["cluster"] == cl].sort_values(
            "count", ascending=False
        )
        size = int(g["count"].sum())
        if size <= 0:
            continue
        share = size / total_converted if total_converted else 0.0

        # Weighted averages
        avg_len = float(np.average(g["length"].to_numpy(), weights=g["count"].to_numpy()))
        if g["time_to_conv"].notna().any():
            # weight only on non-null
            g_tt = g[g["time_to_conv"].notna()]
            avg_time = float(
                np.average(g_tt["time_to_conv"].to_numpy(), weights=g_tt["count"].to_numpy())
            )
        else:
            avg_time = None

        top_paths: List[Dict[str, Any]] = []
        for _, r in g.head(top_paths_per_cluster).iterrows():
            p = str(r["path"])
            c = int(r["count"])
            top_paths.append(
                {
                    "path": p,
                    "count": c,
                    "share": c / total_converted if total_converted else 0.0,
                    "avg_time_to_conversion_days": float(r["time_to_conv"])
                    if not pd.isna(r["time_to_conv"])
                    else None,
                    "avg_length": float(r["length"]),
                }
            )
        rep_path = top_paths[0]["path"] if top_paths else str(g.iloc[0]["path"])

        # Cluster "name": short and readable (first ~3 steps of representative path)
        rep_steps = rep_path.split(" > ")
        short = " → ".join(rep_steps[:3]) + (" → …" if len(rep_steps) > 3 else "")

        top_channels = _top_channels_for_paths(df_j, g["path"].tolist(), top_n=5)

        # Journey-level distribution stats for this cluster.
        lengths = cluster_lengths.get(cl, [])
        ttc_vals = cluster_ttc.get(cl, [])
        length_median = _median(lengths)
        length_p90 = _percentile(lengths, 90.0)
        ttc_median = _median(ttc_vals) if ttc_vals else None
        ttc_p90 = _percentile(ttc_vals, 90.0) if ttc_vals else None

        # Distinctiveness vs global channel distribution.
        cl_channels = cluster_channel_counts.get(cl, Counter())
        cl_total_channels = float(sum(cl_channels.values()) or 1.0)
        l1 = 0.0
        for ch in set(overall_channel_dist.keys()) | set(cl_channels.keys()):
            p_all = overall_channel_dist.get(ch, 0.0)
            p_cl = cl_channels.get(ch, 0) / cl_total_channels
            l1 += abs(p_all - p_cl)
        distinctiveness_score = int(round(min(1.0, l1 / 2.0) * 100.0))

        # Simple human-readable label + defining traits.
        traits: List[str] = []
        first_ch = rep_steps[0] if rep_steps else ""
        last_ch = rep_steps[-1] if rep_steps else ""
        if first_ch:
            traits.append(f"common first touch: {first_ch}")
        if last_ch:
            traits.append(f"common last touch: {last_ch}")
        if length_p90 >= max(5.0, length_median + 2.0):
            traits.append("long paths (P90 high)")
        if ttc_p90 is not None and ttc_p90 >= max(14.0, (ttc_median or 0.0) + 7.0):
            traits.append("slow time-to-convert tail")
        if any("email" in ch.lower() for ch in top_channels):
            traits.append("email-assisted")
        if any("retarget" in ch.lower() or "remarket" in ch.lower() for ch in top_channels):
            traits.append("retargeting-heavy")

        label = "Mixed journey"
        # Heuristic: paths that start with paid-like channels (search/social).
        paid_like = any(k in first_ch.lower() for k in ["google", "meta", "facebook", "paid", "search"])
        if "email" in first_ch.lower():
            label = "Email-led journeys"
        elif paid_like and last_ch.lower() == "direct":
            label = "Paid → Direct close"
        elif any("retarget" in ch.lower() or "remarket" in ch.lower() for ch in top_channels):
            label = "Retargeting-heavy"
        elif length_median >= 5:
            label = "Multi-touch long tail"

        # Representativeness of the representative path.
        rep_count = path_counts.get(rep_path, 0)
        representativeness_score = rep_count / float(size or 1.0)

        # Top transitions within this cluster.
        trans_counter = cluster_transitions.get(cl, Counter())
        total_trans = float(sum(trans_counter.values()) or 1.0)
        top_transitions = [
            {
                "from": a,
                "to": b,
                "count": cnt,
                "share": cnt / total_trans,
            }
            for (a, b), cnt in trans_counter.most_common(10)
        ]

        # Variants & outliers (within-cluster paths).
        variants: List[Dict[str, Any]] = []
        for _, r in g.head(5).iterrows():
            p = str(r["path"])
            c = int(r["count"])
            variants.append(
                {
                    "path": p,
                    "count": c,
                    "share": c / float(size or 1.0),
                    "avg_time_to_conversion_days": float(r["time_to_conv"])
                    if not pd.isna(r["time_to_conv"])
                    else None,
                    "avg_length": float(r["length"]),
                }
            )
        outlier_candidates = g.sort_values("count", ascending=True).head(5)
        outliers: List[Dict[str, Any]] = []
        for _, r in outlier_candidates.iterrows():
            p = str(r["path"])
            c = int(r["count"])
            outliers.append(
                {
                    "path": p,
                    "count": c,
                    "share": c / float(size or 1.0),
                    "avg_time_to_conversion_days": float(r["time_to_conv"])
                    if not pd.isna(r["time_to_conv"])
                    else None,
                    "avg_length": float(r["length"]),
                }
            )

        # Next-best-action style suggestions within this archetype:
        # use the prefix of the representative path (all but last step).
        actions: List[Dict[str, Any]] = []
        if len(rep_steps) >= 2:
            prefix_steps = rep_steps[:-1]
            prefix = " > ".join(prefix_steps)
            support_counts: Counter[str] = Counter()
            for _, row in df_j_with_cluster[df_j_with_cluster["cluster"] == cl].iterrows():
                ch_list = row.get("channels") or []
                if not isinstance(ch_list, list):
                    continue
                safe_channels = [_safe_channel(c) for c in ch_list]
                if len(safe_channels) < len(prefix_steps):
                    continue
                if safe_channels[: len(prefix_steps)] == prefix_steps:
                    last = safe_channels[-1]
                    support_counts[last] += 1
            total_support = float(sum(support_counts.values()) or 1.0)
            for ch, cnt in support_counts.most_common(3):
                actions.append(
                    {
                        "channel": ch,
                        "support": cnt,
                        "support_share": cnt / total_support,
                        "low_sample": cnt < 10,
                    }
                )

        # Simple confidence heuristic.
        confidence = "medium"
        if size >= 200 and distinctiveness_score >= 40:
            confidence = "high"
        if size < 50 or distinctiveness_score <= 20:
            confidence = "low"

        avg_value = None
        total_value = None
        if cluster_value_counts.get(cl):
            total_value = cluster_value_sums[cl]
            avg_value = total_value / float(cluster_value_counts[cl] or 1)

        compare_info = per_cluster_compare.get(cl)

        clusters.append(
            {
                "cluster_label": int(cl),  # raw label (diagnostic)
                "name": short,
                "size": size,
                "share": share,
                "avg_length": avg_len,
                "avg_time_to_conversion_days": avg_time,
                "top_channels": top_channels,
                "top_paths": top_paths,
                "representative_path": rep_path,
                "length_median": length_median,
                "length_p90": length_p90,
                "time_to_conversion_median_days": ttc_median,
                "time_to_conversion_p90_days": ttc_p90,
                "distinctiveness_score": distinctiveness_score,
                "human_label": label,
                "defining_traits": traits,
                "representativeness_score": representativeness_score,
                "top_transitions": top_transitions,
                "variants": variants,
                "outlier_paths": outliers,
                "actions": actions,
                "confidence": confidence,
                "avg_conversion_value": avg_value,
                "total_conversion_value": total_value,
                "compare": compare_info,
            }
        )

    # Stable IDs: sort by size desc, then assign id 1..N
    clusters.sort(key=lambda c: c["size"], reverse=True)
    clusters = clusters[: max(1, int(max_clusters_returned))]
    for i, c in enumerate(clusters, start=1):
        c["id"] = i

    # Cluster size distribution
    sizes = [float(c["size"]) for c in clusters]
    size_min = int(min(sizes)) if sizes else 0
    size_median = int(_median(sizes)) if sizes else 0
    size_max = int(max(sizes)) if sizes else 0

    # Overall direct/unknown diagnostics (best-effort).
    direct_unknown_touchpoints = 0
    total_touchpoints = 0
    journeys_ending_direct = 0
    total_journeys_converted = 0
    for _, row in df_j.iterrows():
        ch_list = row.get("channels") or []
        if isinstance(ch_list, list):
            safe = [_safe_channel(c) for c in ch_list]
            total_touchpoints += len(safe)
            direct_unknown_touchpoints += sum(
                1 for c in safe if c.lower() in ("direct", "unknown")
            )
            total_journeys_converted += 1
            if safe and safe[-1].lower() in ("direct", "unknown"):
                journeys_ending_direct += 1
    direct_unknown_share = (
        direct_unknown_touchpoints / float(total_touchpoints or 1.0)
    )
    ending_direct_share = journeys_ending_direct / float(
        total_journeys_converted or 1.0
    )

    # Quality and stability badges + warnings.
    quality_badge = "ok"
    warnings: List[Dict[str, Any]] = []
    if sil is None or sil < 0.1:
        quality_badge = "weak"
        warnings.append(
            {
                "code": "low_separation",
                "severity": "warn",
                "message": "Low separation: clusters overlap (silhouette is low or unavailable).",
            }
        )
    elif sil is not None and sil < 0.2:
        quality_badge = "warning"
        warnings.append(
            {
                "code": "moderate_separation",
                "severity": "info",
                "message": "Clusters are only moderately separated; treat archetypes as directional.",
            }
        )

    if direct_unknown_share >= 0.4 or ending_direct_share >= 0.4:
        warnings.append(
            {
                "code": "direct_unknown_dominates",
                "severity": "warn",
                "message": "Direct/Unknown channels dominate a large share of touchpoints or last touches. Tracking or taxonomy may be incomplete.",
            }
        )

    singleton_paths_ratio = float((df_p["count"] == 1).sum()) / float(n_paths or 1)
    if singleton_paths_ratio >= 0.5:
        warnings.append(
            {
                "code": "too_many_singletons",
                "severity": "info",
                "message": "Many paths occur only once. Clusters may be noisy; consider aggregating channels or relaxing filters.",
            }
        )

    if total_converted < max(100, k_selected * 10):
        warnings.append(
            {
                "code": "low_sample_size",
                "severity": "warn",
                "message": "Not enough converted journeys for very stable archetypes. Treat results as directional.",
            }
        )

    stability_pct = int(round(stability_score * 100.0)) if stability_score is not None else None
    stability_label = None
    if stability_pct is not None:
        if stability_pct >= 80:
            stability_label = "stable"
        elif stability_pct >= 50:
            stability_label = "moderate"
        else:
            stability_label = "unstable"

    emerging_cluster_id: Optional[int] = None
    declining_cluster_id: Optional[int] = None
    if compare_available:
        best_inc = None
        best_dec = None
        for c in clusters:
            cmp = c.get("compare") or {}
            delta = float(cmp.get("share_delta") or 0.0)
            cid = int(c.get("id"))
            if best_inc is None or delta > best_inc[1]:
                best_inc = (cid, delta)
            if best_dec is None or delta < best_dec[1]:
                best_dec = (cid, delta)
        if best_inc is not None:
            emerging_cluster_id = best_inc[0]
        if best_dec is not None:
            declining_cluster_id = best_dec[0]

    diagnostics = {
        "algorithm": "kmeans",
        "k_mode": k_mode,
        "k_selected": int(k_selected),
        "silhouette_cosine": sil,
        "n_unique_paths": n_paths,
        "total_converted": total_converted,
        "conversion_key": conversion_key,
        "cluster_size_stats": {
            "min": size_min,
            "median": size_median,
            "max": size_max,
        },
        "direct_unknown_share": direct_unknown_share,
        "journeys_ending_direct_share": ending_direct_share,
        "quality_badge": quality_badge,
        "warnings": warnings,
        "stability_score": stability_score,
        "stability_score_pct": stability_pct,
        "stability_label": stability_label,
        "compare_available": compare_available,
        "emerging_cluster_id": emerging_cluster_id,
        "declining_cluster_id": declining_cluster_id,
    }
    return {"clusters": clusters, "total_converted": total_converted, "diagnostics": diagnostics}


def _median(vals: List[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return float((s[mid - 1] + s[mid]) / 2.0)


def _percentile(vals: List[float], pct: float) -> float:
    """Linear-interpolated percentile for small lists."""
    if not vals:
        return 0.0
    if pct <= 0:
        return float(min(vals))
    if pct >= 100:
        return float(max(vals))
    s = sorted(vals)
    k = (len(s) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(s[int(k)])
    d0 = s[int(f)] * (c - k)
    d1 = s[int(c)] * (k - f)
    return float(d0 + d1)


def _mad(vals: List[float]) -> float:
    if not vals:
        return 0.0
    med = _median(vals)
    devs = [abs(v - med) for v in vals]
    return _median(devs)


def _robust_z(x: float, baseline: float, mad: float, eps: float = 1e-9) -> float:
    # 1.4826*MAD approximates std under normality
    denom = 1.4826 * mad + eps
    return float((x - baseline) / denom)


def compute_path_anomalies(
    journeys: List[Dict[str, Any]],
    conversion_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Compute anomaly hints for pathing quality and distribution issues.

    Returned items are UI-friendly and explainable; additional optional fields
    (`baseline_value`, `z_score`, `details`, `suggestion`) help richer UIs.
    """
    df = _extract_path_rows(journeys)
    if df.empty:
        return []

    anomalies: List[Dict[str, Any]] = []

    # Overall touchpoint-level quality
    total_touchpoints = int(df["touchpoints"].sum()) if "touchpoints" in df.columns else 0
    unknown_touchpoints = 0
    if total_touchpoints > 0:
        for ch_list in df["channels"].tolist():
            if isinstance(ch_list, list):
                unknown_touchpoints += sum(1 for ch in ch_list if _safe_channel(ch) == "unknown")
        share_unknown = unknown_touchpoints / float(total_touchpoints)
        if share_unknown >= 0.25:
            sev = "critical" if share_unknown >= 0.45 else "warn"
            anomalies.append(
                {
                    "type": "high_unknown_share",
                    "severity": sev,
                    "metric_key": "unknown_channel_share",
                    "metric_value": share_unknown,
                    "message": f"{share_unknown:.1%} of touchpoints are labeled 'unknown'. This usually indicates missing mapping or inconsistent taxonomy.",
                    "suggestion": "Audit channel mapping rules and ensure touchpoints emit a stable channel field.",
                }
            )

        missing_ts = int(df["missing_ts_touchpoints"].sum()) if "missing_ts_touchpoints" in df.columns else 0
        share_missing_ts = missing_ts / float(total_touchpoints) if total_touchpoints else 0.0
        if share_missing_ts >= 0.2:
            sev = "critical" if share_missing_ts >= 0.5 else "warn"
            anomalies.append(
                {
                    "type": "missing_timestamps",
                    "severity": sev,
                    "metric_key": "missing_timestamp_share",
                    "metric_value": share_missing_ts,
                    "message": f"{share_missing_ts:.1%} of touchpoints are missing timestamps. Windowing and time-to-conversion metrics may be unreliable.",
                    "suggestion": "Backfill event timestamps (UTC) and ensure the pipeline always populates them.",
                }
            )

    # Journey-level timestamp ordering
    non_mono = int(df["non_monotonic_ts"].sum()) if "non_monotonic_ts" in df.columns else 0
    share_non_mono = non_mono / float(max(1, int(df.shape[0])))
    if share_non_mono >= 0.05:
        anomalies.append(
            {
                "type": "non_monotonic_timestamps",
                "severity": "warn",
                "metric_key": "non_monotonic_journeys_share",
                "metric_value": share_non_mono,
                "message": f"{share_non_mono:.1%} of journeys have out-of-order timestamps. This can break time-based attribution/windowing assumptions.",
                "suggestion": "Sort touchpoints by timestamp before analysis; fix upstream ordering issues.",
            }
        )

    # Distribution anomalies (path length)
    lengths = [float(x) for x in df["length"].astype(float).tolist() if x is not None]
    if lengths:
        q50 = _percentile(lengths, 50)
        q75 = _percentile(lengths, 75)
        q95 = _percentile(lengths, 95)
        q99 = _percentile(lengths, 99)
        iqr = max(0.0, q75 - q50)  # not true IQR, but stable with small n
        if q99 >= max(12.0, q75 + 3.0 * max(1.0, iqr)):
            anomalies.append(
                {
                    "type": "extreme_path_lengths",
                    "severity": "info" if q99 < 20 else "warn",
                    "metric_key": "path_length_p99",
                    "metric_value": q99,
                    "message": f"Long tail in path length (p99={q99:.0f}, p95={q95:.0f}, median={q50:.0f}). Consider applying a lookback window or deduplicating repeated touches.",
                    "details": {"p50": q50, "p75": q75, "p95": q95, "p99": q99},
                }
            )

    # Concentration anomaly: dominant single path
    df_p = _aggregate_paths(df)
    if not df_p.empty:
        total = int(df_p["count"].sum())
        top = df_p.sort_values("count", ascending=False).head(1)
        if total > 0 and not top.empty:
            top_count = int(top.iloc[0]["count"])
            top_share = top_count / float(total)
            if top_share >= 0.2 and total >= 50:
                sev = "warn" if top_share < 0.35 else "critical"
                top_path = str(top.iloc[0]["path"])
                anomalies.append(
                    {
                        "type": "path_concentration",
                        "severity": sev,
                        "metric_key": "top_path_share",
                        "metric_value": top_share,
                        "message": f"A single path accounts for {top_share:.1%} of converted journeys. This may indicate overly coarse channel mapping or missing touches.",
                        "details": {"top_path": top_path, "top_count": top_count, "total_converted": total},
                        "suggestion": "Validate upstream event join and ensure multiple channels/campaigns are captured.",
                    }
                )

    # Time-to-conversion outliers (if available)
    if "time_to_conv" in df.columns and df["time_to_conv"].notna().any():
        ttc = [float(x) for x in df["time_to_conv"].dropna().astype(float).tolist() if x is not None]
        med = _median(ttc)
        mad = _mad(ttc)
        p95 = _percentile(ttc, 95)
        if p95 >= max(30.0, med + 8.0 * (1.4826 * mad + 1e-9)):
            anomalies.append(
                {
                    "type": "slow_conversions_tail",
                    "severity": "info",
                    "metric_key": "time_to_conversion_p95_days",
                    "metric_value": p95,
                    "message": f"Time-to-conversion has a long tail (p95={p95:.1f}d, median={med:.1f}d). Consider segmenting by channel or tightening the lookback window.",
                    "details": {"median_days": med, "p95_days": p95},
                }
            )

    # Optional: day-over-day drift detection (if enough dated data)
    # We only use touchpoint timestamps; take last valid timestamp per journey as "conversion day".
    day_series: List[datetime] = []
    for ch_list, tps in zip(df.get("channels", []), journeys or []):
        _ = ch_list  # unused but keeps zip length aligned if journeys differ
        tps2 = (tps or {}).get("touchpoints") if isinstance(tps, dict) else None
        if not tps2:
            continue
        parsed = [_parse_ts(tp.get("timestamp")) for tp in (tps2 or [])]
        valid = [ts for ts in parsed if ts is not None]
        if not valid:
            continue
        last = max(valid)
        day_series.append(last.to_pydatetime().replace(tzinfo=timezone.utc).date())

    if len(day_series) >= 200:
        # compute last-day volume drift as a lightweight sanity check
        s = pd.Series(day_series)
        counts = s.value_counts().sort_index()
        if len(counts) >= 10:
            last_day = counts.index[-1]
            last_val = float(counts.iloc[-1])
            baseline_vals = [float(x) for x in counts.iloc[-8:-1].to_list()]
            baseline = _median(baseline_vals)
            mad = _mad(baseline_vals)
            z = _robust_z(last_val, baseline, mad)
            if abs(z) >= 4.0:
                anomalies.append(
                    {
                        "type": "volume_drift",
                        "severity": "warn",
                        "metric_key": "converted_journeys_last_day",
                        "metric_value": last_val,
                        "baseline_value": baseline,
                        "z_score": z,
                        "message": f"Converted journey volume on {last_day} is unusual vs recent baseline (z={z:.1f}).",
                        "details": {"day": str(last_day), "baseline_median": baseline},
                    }
                )

    # Sort by severity
    sev_rank = {"critical": 0, "warn": 1, "info": 2}
    anomalies.sort(key=lambda a: (sev_rank.get(str(a.get("severity")), 3), str(a.get("type"))))
    return anomalies
