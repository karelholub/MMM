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
    assert journey["touchpoints"][0]["source"] == "facebook"
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


def test_canonicalize_meiro_profiles_applies_webhook_dedup_settings():
    result = canonicalize_meiro_profiles(
        [
            {
                "customer_id": "dup-1",
                "touchpoints": [
                    {
                        "timestamp": "2026-03-02T08:00:00Z",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "Brand",
                    },
                    {
                        "timestamp": "2026-03-02T08:02:00Z",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "Brand",
                    },
                ],
                "conversions": [
                    {
                        "conversion_id": "ord-1",
                        "order_id": "ord-1",
                        "event_id": "evt-1",
                        "timestamp": "2026-03-02T09:00:00Z",
                        "name": "purchase",
                        "value": 50,
                        "currency": "EUR",
                    }
                ],
            },
            {
                "customer_id": "dup-2",
                "touchpoints": [
                    {
                        "timestamp": "2026-03-02T10:00:00Z",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "Brand",
                    }
                ],
                "conversions": [
                    {
                        "conversion_id": "ord-1",
                        "order_id": "ord-1",
                        "event_id": "evt-2",
                        "timestamp": "2026-03-02T10:05:00Z",
                        "name": "purchase",
                        "value": 50,
                        "currency": "EUR",
                    }
                ],
            },
        ],
        revenue_config={"conversion_names": ["purchase"]},
        dedup_config={
            "dedup_mode": "balanced",
            "dedup_interval_minutes": 5,
            "primary_dedup_key": "order_id",
            "fallback_dedup_keys": ["event_id"],
        },
    )

    assert result["import_summary"]["valid"] == 2
    first, second = result["valid_journeys"]
    assert len(first["touchpoints"]) == 1
    assert first["meta"]["parser"]["dedup_removed_touchpoints"] == 1
    assert len(first["conversions"]) == 1
    assert len(second["conversions"]) == 0
    assert second["meta"]["parser"]["dedup_removed_conversions"] == 1


def test_canonicalize_meiro_profiles_quarantines_missing_timestamp_when_strict():
    result = canonicalize_meiro_profiles(
        [
            {
                "customer_id": "strict-1",
                "touchpoints": [
                    {
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "Brand",
                    }
                ],
                "conversions": [
                    {
                        "conversion_id": "conv-1",
                        "timestamp": "2026-03-02T09:00:00Z",
                        "name": "purchase",
                        "value": 50,
                        "currency": "EUR",
                    }
                ],
            }
        ],
        revenue_config={"conversion_names": ["purchase"]},
        dedup_config={"timestamp_fallback_policy": "quarantine"},
    )

    assert result["import_summary"]["valid"] == 0
    assert result["import_summary"]["quarantined"] == 1
    assert result["quarantine_records"][0]["reason_codes"] == ["missing_touchpoint_timestamp"]


def test_canonicalize_meiro_profiles_applies_conversion_aliases_and_currency_quarantine():
    result = canonicalize_meiro_profiles(
        [
            {
                "customer_id": "alias-1",
                "touchpoints": [
                    {
                        "timestamp": "2026-03-02T08:00:00Z",
                        "source": "fb",
                        "medium": "paid_social",
                        "campaign": "Launch",
                    }
                ],
                "conversions": [
                    {
                        "conversion_id": "conv-2",
                        "timestamp": "2026-03-02T09:00:00Z",
                        "event_name": "Order Completed",
                        "value": 80,
                    }
                ],
            }
        ],
        revenue_config={"conversion_names": ["purchase"], "base_currency": "EUR"},
        dedup_config={
            "conversion_event_aliases": {"order completed": "purchase"},
            "currency_fallback_policy": "quarantine",
        },
    )

    assert result["import_summary"]["valid"] == 0
    assert result["quarantine_records"][0]["reason_codes"] == ["missing_conversion_currency"]


def test_canonicalize_meiro_profiles_quarantines_duplicate_profile_ids():
    result = canonicalize_meiro_profiles(
        [
            {
                "customer_id": "dup-profile",
                "touchpoints": [
                    {"timestamp": "2026-03-02T08:00:00Z", "source": "google", "medium": "cpc", "campaign": "Brand"}
                ],
                "conversions": [
                    {"conversion_id": "conv-a", "timestamp": "2026-03-02T09:00:00Z", "name": "purchase", "value": 50, "currency": "EUR"}
                ],
            },
            {
                "customer_id": "dup-profile",
                "touchpoints": [
                    {"timestamp": "2026-03-03T08:00:00Z", "source": "google", "medium": "cpc", "campaign": "Brand"}
                ],
                "conversions": [
                    {"conversion_id": "conv-b", "timestamp": "2026-03-03T09:00:00Z", "name": "purchase", "value": 60, "currency": "EUR"}
                ],
            },
        ],
        revenue_config={"conversion_names": ["purchase"]},
        dedup_config={"quarantine_duplicate_profiles": True},
    )

    assert result["import_summary"]["valid"] == 0
    assert result["import_summary"]["quarantined"] == 2
    assert result["import_summary"]["cleaning_report"]["duplicate_profiles"] == 1
    assert "duplicate_profile_id" in result["quarantine_records"][0]["reason_codes"]


def test_canonicalize_meiro_profiles_attaches_quality_score():
    result = canonicalize_meiro_profiles(
        [
            {
                "customer_id": "quality-1",
                "touchpoints": [
                    {"timestamp": "2026-03-02T08:00:00Z", "source": "newsletter", "medium": "email", "campaign": "Welcome"}
                ],
                "conversions": [
                    {"conversion_id": "conv-q", "timestamp": "2026-03-02T09:00:00Z", "name": "purchase", "value": 10, "currency": "EUR"}
                ],
            }
        ],
        revenue_config={"conversion_names": ["purchase"]},
    )

    journey = result["valid_journeys"][0]
    quality = journey["meta"]["quality"]
    assert isinstance(quality["score"], int)
    assert quality["band"] in {"high", "medium", "low"}
