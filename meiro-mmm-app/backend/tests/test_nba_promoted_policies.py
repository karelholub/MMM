from app.modules.settings.schemas import NBASettings
from app.services_nba_defaults import filter_nba_recommendations


def test_promoted_journey_policy_is_retained_and_ranked_first():
    raw = {
        "Paid Social > Product View": [
            {
                "channel": "Exit",
                "step": "Exit",
                "count": 120,
                "conversions": 18,
                "conversion_rate": 0.15,
                "avg_value": 100.0,
            },
            {
                "channel": "Checkout",
                "step": "Checkout",
                "count": 4,
                "conversions": 0,
                "conversion_rate": 0.0,
                "avg_value": 0.0,
            },
        ]
    }
    settings = NBASettings(
        min_prefix_support=5,
        min_conversion_rate=0.05,
        max_prefix_depth=5,
        min_next_support=10,
        max_suggestions_per_prefix=3,
        min_uplift_pct=0.1,
        excluded_channels=["direct"],
        promoted_journey_policies=[
            {
                "hypothesis_id": "hyp-1",
                "title": "Promote checkout recovery",
                "journey_definition_id": "jd-1",
                "prefix": "Paid Social > Product View",
                "prefix_steps": ["Paid Social", "Product View"],
                "step": "Checkout",
                "channel": "Checkout",
            }
        ],
    )

    filtered, stats = filter_nba_recommendations(raw, settings)

    kept = filtered["Paid Social > Product View"]
    assert len(kept) == 1
    assert kept[0]["step"] == "Checkout"
    assert kept[0]["is_promoted_policy"] is True
    assert kept[0]["promoted_policy_hypothesis_id"] == "hyp-1"
    assert kept[0]["promoted_policy_journey_definition_id"] == "jd-1"
    assert stats["filtered_support"] == 0
    assert stats["filtered_uplift"] == 1
