from __future__ import annotations

from typing import Any

from app.mmm_version import CURRENT_MMM_ENGINE_VERSION


def _finite_numbers(values: list[Any]) -> list[float]:
    numbers: list[float] = []
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number == number and number not in (float("inf"), float("-inf")):
            numbers.append(number)
    return numbers


def _all_near_zero(values: list[float], epsilon: float = 1e-9) -> bool:
    return bool(values) and all(abs(value) <= epsilon for value in values)


def evaluate_mmm_run_quality(
    run: dict[str, Any],
    *,
    dataset_available: bool | None = None,
    weeks: int | None = None,
    channels_modeled: int | None = None,
    total_spend: float | None = None,
) -> dict[str, Any]:
    """Classify whether a saved MMM run is safe for results and budget actions."""
    status = str(run.get("status") or "").lower()
    if status and status != "finished":
        return {
            "level": "pending",
            "label": status,
            "tone": "danger" if status == "error" else "warning",
            "reasons": ["Model run ended in an error state." if status == "error" else "Model run is not finished yet."],
            "can_use_results": False,
            "can_use_budget": False,
        }

    reasons: list[str] = []
    warnings: list[str] = []
    roi_values = _finite_numbers([row.get("roi") for row in run.get("roi") or [] if isinstance(row, dict)])
    contrib_shares = _finite_numbers([row.get("mean_share") for row in run.get("contrib") or [] if isinstance(row, dict)])
    has_output = bool(roi_values) and bool(contrib_shares)
    output_all_zero = has_output and _all_near_zero(roi_values) and _all_near_zero(contrib_shares)
    is_legacy_engine = status == "finished" and run.get("engine_version") != CURRENT_MMM_ENGINE_VERSION

    if not has_output:
        reasons.append("Model output is incomplete: ROI or contribution rows are missing.")
    if dataset_available is not False and total_spend is not None and total_spend <= 0:
        reasons.append("Linked dataset has no mapped spend for selected model channels.")
    if output_all_zero:
        reasons.append("All modeled ROI and contribution values are zero, so the run has no usable media signal.")

    try:
        r2 = float(run.get("r2"))
    except (TypeError, ValueError):
        r2 = None
    if r2 is not None:
        if r2 < 0.05:
            message = f"Model fit is effectively flat (R2 {r2:.3f})."
            if output_all_zero:
                reasons.append(message)
            else:
                warnings.append(message)
        elif r2 < 0.3:
            warnings.append(f"Model fit is weak (R2 {r2:.3f}).")

    if dataset_available is False:
        warnings.append("Linked dataset preview is unavailable; source-row checks are disabled.")
    if is_legacy_engine:
        warnings.append("This run was created before the current MMM calculation contract. Re-run with the same setup before using it for new budget decisions.")
    if weeks is not None and weeks > 0 and weeks < 20:
        warnings.append(f"Short history: {weeks:,} modeled weeks.")
    if channels_modeled is not None and channels_modeled > 0 and weeks is not None and weeks > 0 and channels_modeled > weeks / 2:
        warnings.append("Many modeled channels compared to available weeks.")

    diagnostics = run.get("diagnostics") if isinstance(run.get("diagnostics"), dict) else {}
    if diagnostics.get("rhat_max") is not None and float(diagnostics["rhat_max"]) > 1.1:
        warnings.append(f"R-hat {float(diagnostics['rhat_max']):.2f} suggests convergence risk.")
    if diagnostics.get("ess_bulk_min") is not None and float(diagnostics["ess_bulk_min"]) < 200:
        warnings.append(f"Effective sample size is low ({float(diagnostics['ess_bulk_min']):.0f}).")
    if diagnostics.get("divergences") is not None and float(diagnostics["divergences"]) > 0:
        warnings.append(f"{float(diagnostics['divergences']):,.0f} MCMC divergences detected.")
    if any(value < 0 for value in roi_values):
        warnings.append("Some channels have negative ROI.")

    if reasons:
        return {
            "level": "not_usable",
            "label": "Not usable",
            "tone": "danger",
            "reasons": list(dict.fromkeys([*reasons, *warnings])),
            "can_use_results": False,
            "can_use_budget": False,
        }
    if warnings:
        return {
            "level": "directional",
            "label": "Readout only" if dataset_available is False else "Refresh needed" if is_legacy_engine else "Directional",
            "tone": "warning",
            "reasons": list(dict.fromkeys(warnings)),
            "can_use_results": True,
            "can_use_budget": dataset_available is not False and not is_legacy_engine,
        }
    return {
        "level": "ready",
        "label": "Decision ready",
        "tone": "success",
        "reasons": [],
        "can_use_results": True,
        "can_use_budget": True,
    }
