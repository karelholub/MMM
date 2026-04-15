from app.services_mmm_quality import evaluate_mmm_run_quality
from app.mmm_version import CURRENT_MMM_ENGINE_VERSION


def test_mmm_quality_blocks_all_zero_outputs():
    quality = evaluate_mmm_run_quality(
        {
            "status": "finished",
            "engine_version": CURRENT_MMM_ENGINE_VERSION,
            "r2": 0.0,
            "roi": [{"channel": "paid_search", "roi": 0.0}],
            "contrib": [{"channel": "paid_search", "mean_share": 0.0}],
            "channel_summary": [{"channel": "paid_search", "spend": 1000.0}],
        },
        dataset_available=True,
        total_spend=1000.0,
    )

    assert quality["level"] == "not_usable"
    assert quality["can_use_results"] is False
    assert quality["can_use_budget"] is False
    assert any("no usable media signal" in reason for reason in quality["reasons"])


def test_mmm_quality_marks_missing_dataset_as_readout_only():
    quality = evaluate_mmm_run_quality(
        {
            "status": "finished",
            "r2": 0.71,
            "roi": [{"channel": "paid_search", "roi": 1.2}],
            "contrib": [{"channel": "paid_search", "mean_share": 1.0}],
        },
        dataset_available=False,
    )

    assert quality["level"] == "directional"
    assert quality["label"] == "Readout only"
    assert quality["can_use_results"] is True
    assert quality["can_use_budget"] is False


def test_mmm_quality_marks_legacy_run_without_current_engine_version_as_refresh_needed():
    quality = evaluate_mmm_run_quality(
        {
            "status": "finished",
            "r2": 0.52,
            "roi": [{"channel": "paid_search", "roi": 1.2}],
            "contrib": [{"channel": "paid_search", "mean_share": 1.0}],
        },
        dataset_available=True,
        total_spend=None,
    )

    assert quality["level"] == "directional"
    assert quality["label"] == "Refresh needed"
    assert quality["can_use_results"] is True
    assert quality["can_use_budget"] is False


def test_mmm_quality_keeps_current_run_without_spend_summary_usable():
    quality = evaluate_mmm_run_quality(
        {
            "status": "finished",
            "engine_version": CURRENT_MMM_ENGINE_VERSION,
            "r2": 0.52,
            "roi": [{"channel": "paid_search", "roi": 1.2}],
            "contrib": [{"channel": "paid_search", "mean_share": 1.0}],
        },
        dataset_available=True,
        total_spend=None,
    )

    assert quality["level"] == "ready"
    assert quality["can_use_results"] is True
    assert quality["can_use_budget"] is True
