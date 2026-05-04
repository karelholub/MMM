import pytest

from app.services_activation_measurement import (
    build_activation_object_registry,
    build_activation_measurement_evidence,
    build_activation_measurement_summary,
)


def _journeys():
    return [
        {
            "journey_id": "j-1",
            "customer": {"id": "cust-1"},
            "conversion_value": 50,
            "touchpoints": [
                {
                    "ts": "2026-04-30T10:00:00Z",
                    "channel": "web_inapp",
                    "campaign_id": "spring_hero",
                    "meta": {
                        "activation": {
                            "schema_version": "activation_measurement.v1",
                            "source_system": "deciEngine",
                            "activation_campaign_id": "spring_hero",
                            "native_meiro_campaign_id": "meiro-native-spring",
                            "decision_key": "homepage_offer_decision",
                            "decision_stack_key": "homepage_stack",
                            "placement_key": "homepage_hero",
                            "template_key": "hero_banner_v1",
                            "content_block_id": "spring_copy",
                            "creative_asset_id": "meiro-creative-spring",
                            "native_meiro_asset_id": "meiro-asset-spring",
                            "offer_id": "spring_discount_10",
                            "experiment_key": "hero_test",
                            "variant_key": "default",
                        }
                    },
                }
            ],
            "conversions": [
                {
                    "id": "ord-1",
                    "ts": "2026-04-30T11:00:00Z",
                    "name": "purchase",
                    "value": 50,
                    "currency": "EUR",
                }
            ],
        },
        {
            "journey_id": "j-2",
            "customer": {"id": "cust-2"},
            "touchpoints": [
                {
                    "ts": "2026-04-30T12:00:00Z",
                    "channel": "web_inapp",
                    "campaign_id": "spring_hero",
                    "meta": {
                        "activation": {
                            "schema_version": "activation_measurement.v1",
                            "source_system": "deciEngine",
                            "activation_campaign_id": "spring_hero",
                            "native_meiro_campaign_id": "meiro-native-spring",
                            "decision_key": "homepage_offer_decision",
                            "placement_key": "homepage_hero",
                            "offer_id": "spring_discount_10",
                            "variant_key": "holdout",
                        }
                    },
                }
            ],
            "conversions": [],
        },
    ]


def test_activation_measurement_summarizes_campaign_evidence():
    summary = build_activation_measurement_summary(
        journeys=_journeys(),
        object_type="campaign",
        object_id="spring_hero",
        date_from="2026-04-30",
        date_to="2026-04-30",
    )

    assert summary["summary"]["matched_touchpoints"] == 2
    assert summary["summary"]["matched_journeys"] == 2
    assert summary["summary"]["matched_profiles"] == 2
    assert summary["summary"]["conversions"] == 1
    assert summary["summary"]["revenue"] == 50
    assert summary["summary"]["variants"] == ["default", "holdout"]
    assert summary["summary"]["placements"] == ["homepage_hero"]
    assert summary["evidence"]["attribution"]["available"] is True
    assert summary["evidence"]["incrementality"]["available"] is False
    assert "not causal lift" in summary["evidence"]["attribution"]["limitations"][0]


def test_activation_measurement_matches_native_meiro_campaign_alias():
    summary = build_activation_measurement_summary(
        journeys=_journeys(),
        object_type="campaign",
        object_id="local_imported_campaign",
        match_aliases=["meiro-native-spring"],
    )

    assert summary["object"]["id"] == "local_imported_campaign"
    assert summary["object"]["match_aliases"] == ["meiro-native-spring"]
    assert summary["summary"]["matched_touchpoints"] == 2
    assert summary["summary"]["conversions"] == 1


def test_activation_measurement_matches_creative_asset_alias():
    summary = build_activation_measurement_summary(
        journeys=_journeys(),
        object_type="asset",
        object_id="local_imported_asset",
        match_aliases=["meiro-creative-spring"],
    )

    assert summary["summary"]["matched_touchpoints"] == 1

    evidence = build_activation_measurement_evidence(
        journeys=_journeys(),
        object_type="asset",
        object_id="local_imported_asset",
        match_aliases=["meiro-creative-spring"],
    )

    assert evidence["total_matches"] == 1
    assert evidence["items"][0]["activation"]["creative_asset_id"] == "meiro-creative-spring"


def test_activation_measurement_matches_offer_and_content_ids():
    offer = build_activation_measurement_summary(
        journeys=_journeys(),
        object_type="offer",
        object_id="spring_discount_10",
    )
    content = build_activation_measurement_summary(
        journeys=_journeys(),
        object_type="content",
        object_id="spring_copy",
    )

    assert offer["summary"]["matched_touchpoints"] == 2
    assert content["summary"]["matched_touchpoints"] == 1


def test_activation_measurement_matches_decision_ids():
    decision = build_activation_measurement_summary(
        journeys=_journeys(),
        object_type="decision",
        object_id="homepage_offer_decision",
    )
    stack = build_activation_measurement_summary(
        journeys=_journeys(),
        object_type="decision_stack",
        object_id="homepage_stack",
    )

    assert decision["summary"]["matched_touchpoints"] == 2
    assert decision["summary"]["conversions"] == 1
    assert stack["summary"]["matched_touchpoints"] == 1


def test_activation_measurement_evidence_returns_capped_rows():
    evidence = build_activation_measurement_evidence(
        journeys=_journeys(),
        object_type="offer",
        object_id="spring_discount_10",
        limit=1,
    )

    assert evidence["total_matches"] == 2
    assert evidence["limit"] == 1
    assert len(evidence["items"]) == 1
    item = evidence["items"][0]
    assert item["journey_id"] == "j-1"
    assert item["profile_id"] == "cust-1"
    assert item["converted"] is True
    assert item["revenue"] == 50
    assert item["activation"]["activation_campaign_id"] == "spring_hero"
    assert item["activation"]["decision_key"] == "homepage_offer_decision"
    assert item["activation"]["offer_id"] == "spring_discount_10"


def test_activation_object_registry_discovers_measurable_objects():
    registry = build_activation_object_registry(journeys=_journeys(), limit=20)

    campaign = next(item for item in registry["items"] if item["object_type"] == "campaign" and item["object_id"] == "spring_hero")
    assert campaign["matched_touchpoints"] == 2
    assert campaign["matched_journeys"] == 2
    assert campaign["matched_profiles"] == 2
    assert campaign["conversions"] == 1
    assert campaign["revenue"] == 50
    assert campaign["aliases"] == ["meiro-native-spring"]
    assert campaign["source_systems"] == ["deciEngine"]

    asset = next(item for item in registry["items"] if item["object_type"] == "asset" and item["object_id"] == "meiro-creative-spring")
    assert asset["matched_touchpoints"] == 1
    assert asset["aliases"] == ["meiro-asset-spring", "spring_copy", "spring_discount_10"]


def test_activation_object_registry_filters_by_type_and_query():
    decisions = build_activation_object_registry(journeys=_journeys(), object_type="decision")
    assert [item["object_id"] for item in decisions["items"]] == ["homepage_offer_decision"]

    native_campaigns = build_activation_object_registry(journeys=_journeys(), q="native-spring")
    assert [
        (item["object_type"], item["object_id"])
        for item in native_campaigns["items"]
        if item["object_type"] == "campaign"
    ] == [("campaign", "spring_hero")]


def test_activation_measurement_returns_unavailable_for_no_match():
    summary = build_activation_measurement_summary(
        journeys=_journeys(),
        object_type="campaign",
        object_id="missing_campaign",
    )

    assert summary["summary"]["matched_touchpoints"] == 0
    assert summary["evidence"]["attribution"]["available"] is False
    assert summary["evidence"]["data_quality"]["status"] == "unavailable"
    assert summary["recommended_actions"][0]["id"] == "verify_activation_event_mapping"


def test_activation_measurement_rejects_unsupported_object_type():
    with pytest.raises(ValueError):
        build_activation_measurement_summary(
            journeys=_journeys(),
            object_type="unsupported",
            object_id="spring_hero",
        )
