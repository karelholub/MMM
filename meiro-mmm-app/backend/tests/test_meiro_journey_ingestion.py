from app.services_journey_ingestion import canonicalize_meiro_profiles
from app.services_metrics import journey_revenue_value


def test_canonicalize_meiro_profiles_builds_v2_journey_from_flat_profile():
    result = canonicalize_meiro_profiles(
        [
            {
                "user": {"id": "cust-1"},
                "journey_touchpoints": [
                    {
                        "occurred_at": "2026-03-01T10:00:00Z",
                        "channel_name": "facebook",
                        "source": {"platform": "fb"},
                        "medium": "paid_social",
                        "campaign": {"name": "Spring Launch"},
                    }
                ],
                "converted": True,
                "kpi_type": "form_submit",
                "conversion_meta": {"currency_code": "USD"},
            }
        ],
        mapping={
            "touchpoint_attr": "journey_touchpoints",
            "id_attr": "user.id",
            "channel_field": "channel_name",
            "timestamp_field": "occurred_at",
            "source_field": "source.platform",
            "medium_field": "medium",
            "campaign_field": "campaign.name",
            "currency_field": "conversion_meta.currency_code",
        },
        revenue_config={
            "conversion_names": ["form_submit"],
            "default_value": 25,
            "default_value_mode": "missing_only",
            "base_currency": "EUR",
            "fx_enabled": False,
        },
    )

    assert result["import_summary"]["valid"] == 1
    journey = result["valid_journeys"][0]
    assert journey["customer"]["id"] == "cust-1"
    assert journey["touchpoints"][0]["channel"]
    assert journey["touchpoints"][0]["source"] == "fb"
    assert journey["touchpoints"][0]["campaign"]["name"] == "Spring Launch"
    assert journey["conversions"][0]["name"] == "form_submit"
    assert journey["conversions"][0]["currency"] == "USD"
    assert journey["_revenue_entries"][0]["default_applied"] is True
    assert journey["meta"]["parser"]["used_inferred_mapping"] is True
    assert journey["meta"]["parser"]["inferred_items"] > 0
    assert journey["meta"]["parser"]["confidence"] < 1.0
    assert journey_revenue_value(journey) == 25.0


def test_canonicalize_meiro_profiles_handles_mixed_flat_and_v2_items():
    result = canonicalize_meiro_profiles(
        [
            {
                "customer_id": "flat-1",
                "touchpoints": [
                    {
                        "timestamp": "2026-03-02T08:00:00Z",
                        "source": "newsletter",
                        "medium": "email",
                        "campaign": "Welcome",
                    }
                ],
                "conversion_value": 99,
                "kpi_type": "purchase",
                "currency": "EUR",
            },
            {
                "journey_id": "j-existing",
                "customer": {"id": "v2-1", "type": "profile_id"},
                "touchpoints": [
                    {"id": "tp-1", "ts": "2026-03-03T09:00:00Z", "channel": "direct"}
                ],
                "conversions": [
                    {"id": "cv-1", "ts": "2026-03-03T10:00:00Z", "name": "purchase", "value": 50, "currency": "EUR"}
                ],
            },
        ],
        revenue_config={"conversion_names": ["purchase"]},
    )

    assert result["import_summary"]["valid"] == 2
    journeys = result["valid_journeys"]
    assert journeys[0]["customer"]["id"] == "flat-1"
    assert journeys[0]["conversions"][0]["value"] == 99
    assert journey_revenue_value(journeys[0]) == 99.0
    assert journeys[1]["journey_id"] == "j-existing"
    assert journey_revenue_value(journeys[1]) == 50.0
