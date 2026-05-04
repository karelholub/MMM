from app.services_deciengine_events import deciengine_inapp_events_to_v2_journeys
from app.services_journey_ingestion import validate_and_normalize


def test_deciengine_events_convert_to_v2_journeys_with_activation_metadata():
    envelope = deciengine_inapp_events_to_v2_journeys(
        {
            "items": [
                {
                    "id": "evt-1",
                    "eventType": "IMPRESSION",
                    "ts": "2026-04-30T10:00:00.000Z",
                    "appKey": "meiro_store",
                    "placement": "home_top",
                    "campaignKey": "prism_campaign_push_store_order",
                    "variantKey": "default",
                    "messageId": "msg-prism-1",
                    "profileId": "profile-1",
                    "context": {
                        "activationMeasurement": {
                            "schema_version": "activation_measurement.v1",
                            "source_system": "deciEngine",
                            "activation_campaign_id": "prism_campaign_push_store_order",
                            "native_meiro_campaign_id": "520d7a4d-d2bf-4d3a-9560-bb10f6477ef3",
                            "creative_asset_id": "prism_asset_push",
                            "native_meiro_asset_id": "meiro-asset-push",
                            "offer_catalog_id": "catalog-push",
                            "native_meiro_catalog_id": "meiro-catalog-push",
                            "prism_source_id": "520d7a4d-d2bf-4d3a-9560-bb10f6477ef3",
                            "imported_from": "pipes_prism_preview",
                            "channel": "push",
                        }
                    },
                },
                {
                    "id": "evt-2",
                    "eventType": "CLICK",
                    "ts": "2026-04-30T10:05:00.000Z",
                    "appKey": "meiro_store",
                    "placement": "home_top",
                    "campaignKey": "prism_campaign_push_store_order",
                    "variantKey": "default",
                    "messageId": "msg-prism-1",
                    "profileId": "profile-1",
                    "context": {
                        "activationMeasurement": {
                            "activation_campaign_id": "prism_campaign_push_store_order",
                            "native_meiro_campaign_id": "520d7a4d-d2bf-4d3a-9560-bb10f6477ef3",
                            "channel": "push",
                        }
                    },
                },
            ]
        }
    )

    assert envelope["schema_version"] == "2.0"
    assert len(envelope["journeys"]) == 1
    journey = envelope["journeys"][0]
    assert journey["customer"]["id"] == "profile-1"
    assert len(journey["touchpoints"]) == 2
    first = journey["touchpoints"][0]
    assert first["campaign_id"] == "prism_campaign_push_store_order"
    assert first["interaction_type"] == "impression"
    activation = first["meta"]["activation"]
    assert activation["native_meiro_campaign_id"] == "520d7a4d-d2bf-4d3a-9560-bb10f6477ef3"
    assert activation["creative_asset_id"] == "prism_asset_push"
    assert activation["native_meiro_asset_id"] == "meiro-asset-push"
    assert activation["offer_catalog_id"] == "catalog-push"
    assert activation["native_meiro_catalog_id"] == "meiro-catalog-push"

    normalized = validate_and_normalize(envelope)
    assert normalized["import_summary"]["valid"] == 1
    normalized_activation = normalized["valid_journeys"][0]["touchpoints"][0]["meta"]["activation"]
    assert normalized_activation["native_meiro_campaign_id"] == "520d7a4d-d2bf-4d3a-9560-bb10f6477ef3"
