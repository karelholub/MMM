from typing import Any, Callable, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request

from app.modules.settings.schemas import KpiConfigModel, KpiTestPayload, RevenueConfig, Settings
from app.modules.settings.schemas import (
    NotificationChannelCreate,
    NotificationChannelUpdate,
    NotificationPrefUpdate,
    NotificationPrefUpsert,
)
from app.services_notifications import (
    create_channel as create_notification_channel,
    delete_channel as delete_notification_channel,
    delete_pref as delete_notification_pref,
    get_channel as get_notification_channel,
    get_pref as get_notification_pref,
    list_channels as list_notification_channels,
    list_prefs as list_notification_prefs,
    update_channel as update_notification_channel,
    update_pref as update_notification_pref,
    upsert_pref as upsert_notification_pref,
)
from app.services_revenue_config import normalize_revenue_config
from app.utils.taxonomy import (
    ChannelRule,
    MatchExpression,
    Taxonomy,
    is_catch_all_rule,
    load_taxonomy,
    save_taxonomy,
)


def _serialize_taxonomy() -> Dict[str, Any]:
    tax = load_taxonomy()

    def _serialize_expression(expr):
        return {
            "operator": expr.normalize_operator(),
            "value": expr.value or "",
        }

    return {
        "channel_rules": [
            {
                "name": r.name,
                "channel": r.channel,
                "priority": r.priority,
                "enabled": r.enabled,
                "source": _serialize_expression(r.source),
                "medium": _serialize_expression(r.medium),
                "campaign": _serialize_expression(r.campaign),
            }
            for r in tax.channel_rules
        ],
        "source_aliases": tax.source_aliases,
        "medium_aliases": tax.medium_aliases,
    }


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
    get_settings_obj: Callable[[], Settings],
    replace_settings_fn: Callable[[Settings], Settings],
    replace_revenue_config_fn: Callable[[RevenueConfig], Dict[str, Any]],
    get_kpi_config_model_fn: Callable[[], KpiConfigModel],
    replace_kpi_config_fn: Callable[[KpiConfigModel], KpiConfigModel],
    ensure_journeys_loaded_fn: Callable[[Any], list[dict]],
    resolve_current_user_id_fn: Callable[[Request], str],
) -> APIRouter:
    router = APIRouter(tags=["settings"])

    @router.get("/api/settings")
    def get_settings(_ctx=Depends(require_permission_dependency("settings.view"))):
        return get_settings_obj()

    @router.post("/api/settings")
    def update_settings(
        new_settings: Settings,
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        return replace_settings_fn(new_settings)

    @router.get("/api/settings/revenue-config")
    def get_revenue_config_settings(_ctx=Depends(require_permission_dependency("settings.view"))):
        settings = get_settings_obj()
        revenue = getattr(settings, "revenue_config", None)
        return normalize_revenue_config(revenue.model_dump() if revenue else None)

    @router.put("/api/settings/revenue-config")
    def update_revenue_config_settings(
        payload: RevenueConfig,
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        return replace_revenue_config_fn(payload)

    @router.get("/api/taxonomy")
    def get_taxonomy():
        return _serialize_taxonomy()

    @router.post("/api/taxonomy")
    def update_taxonomy(payload: Dict[str, Any]):
        def _parse_expression(data: Optional[Dict[str, Any]], fallback_regex: Optional[str] = None):
            if isinstance(data, dict):
                return {
                    "operator": data.get("operator", "any"),
                    "value": data.get("value", ""),
                }
            if fallback_regex:
                return {"operator": "regex", "value": fallback_regex}
            return {"operator": "any", "value": ""}

        rules: list[ChannelRule] = []
        channel_rules_payload = payload.get("channel_rules", [])
        for idx, rule_payload in enumerate(channel_rules_payload):
            expr_source = _parse_expression(rule_payload.get("source"), rule_payload.get("source_regex"))
            expr_medium = _parse_expression(rule_payload.get("medium"), rule_payload.get("medium_regex"))
            expr_campaign = _parse_expression(rule_payload.get("campaign"))
            rules.append(
                ChannelRule(
                    name=rule_payload.get("name", ""),
                    channel=rule_payload.get("channel", ""),
                    priority=int(rule_payload.get("priority", (idx + 1) * 10)),
                    enabled=bool(rule_payload.get("enabled", True)),
                    source=MatchExpression(**expr_source),
                    medium=MatchExpression(**expr_medium),
                    campaign=MatchExpression(**expr_campaign),
                )
            )

        rules.sort(key=lambda r: (r.priority, r.name))
        invalid_rules = [
            rule.name or rule.channel or f"Rule {idx + 1}"
            for idx, rule in enumerate(rules)
            if rule.enabled and is_catch_all_rule(rule)
        ]
        if invalid_rules:
            raise HTTPException(
                status_code=400,
                detail=f"Enabled taxonomy rules must include at least one match condition. Invalid: {', '.join(invalid_rules[:5])}",
            )

        save_taxonomy(
            Taxonomy(
                channel_rules=rules,
                source_aliases=payload.get("source_aliases", {}),
                medium_aliases=payload.get("medium_aliases", {}),
            )
        )
        return _serialize_taxonomy()

    @router.post("/api/taxonomy/validate-utm")
    def validate_utm_endpoint(params: Dict[str, Any]):
        from app.services_taxonomy import validate_utm_params

        result = validate_utm_params(params)
        return {
            "is_valid": result.is_valid,
            "warnings": result.warnings,
            "errors": result.errors,
            "normalized": result.normalized,
            "confidence": result.confidence,
        }

    @router.post("/api/taxonomy/map-channel")
    def map_channel_endpoint(body: Dict[str, Any]):
        from app.services_taxonomy import map_to_channel

        mapping = map_to_channel(body.get("source"), body.get("medium"), body.get("campaign"))
        return {
            "channel": mapping.channel,
            "matched_rule": mapping.matched_rule,
            "confidence": mapping.confidence,
            "source": mapping.source,
            "medium": mapping.medium,
            "fallback_reason": mapping.fallback_reason,
        }

    @router.get("/api/taxonomy/unknown-share")
    def get_unknown_share(limit: int = 20, db=Depends(get_db_dependency)):
        from app.services_conversions import load_journeys_from_db
        from app.services_taxonomy import compute_unknown_share

        journeys = load_journeys_from_db(db, limit=10000)
        if not journeys:
            return {
                "total_touchpoints": 0,
                "unknown_count": 0,
                "unknown_share": 0.0,
                "by_source": {},
                "by_medium": {},
                "top_unmapped_patterns": [],
                "sample_unmapped": [],
            }

        report = compute_unknown_share(journeys, sample_size=limit)
        top_patterns = sorted(report.by_source_medium.items(), key=lambda item: -item[1])[:limit]
        return {
            "total_touchpoints": report.total_touchpoints,
            "unknown_count": report.unknown_count,
            "unknown_share": report.unknown_share,
            "by_source": report.by_source,
            "by_medium": report.by_medium,
            "top_unmapped_patterns": [
                {"source": source, "medium": medium, "count": count}
                for (source, medium), count in top_patterns
            ],
            "sample_unmapped": report.sample_unmapped,
        }

    @router.get("/api/taxonomy/coverage")
    def get_taxonomy_coverage(db=Depends(get_db_dependency)):
        from app.services_conversions import load_journeys_from_db
        from app.services_taxonomy import compute_taxonomy_coverage

        journeys = load_journeys_from_db(db, limit=10000)
        if not journeys:
            return {
                "channel_distribution": {},
                "source_coverage": 0.0,
                "medium_coverage": 0.0,
                "rule_usage": {},
                "top_unmapped_patterns": [],
            }
        return compute_taxonomy_coverage(journeys)

    @router.get("/api/taxonomy/suggestions")
    def get_taxonomy_suggestions(limit: int = 12, db=Depends(get_db_dependency)):
        from app.services_conversions import load_journeys_from_db
        from app.services_taxonomy_suggestions import generate_taxonomy_suggestions

        journeys = load_journeys_from_db(db, limit=10000)
        return generate_taxonomy_suggestions(journeys, limit=max(1, min(limit, 30)))

    @router.get("/api/taxonomy/channel-confidence")
    def get_channel_confidence(channel: str, db=Depends(get_db_dependency)):
        from app.services_conversions import load_journeys_from_db
        from app.services_taxonomy import compute_channel_confidence

        journeys = load_journeys_from_db(db, limit=10000)
        if not journeys:
            return {
                "mean_confidence": 0.0,
                "touchpoint_count": 0,
                "low_confidence_count": 0,
                "low_confidence_share": 0.0,
                "sample_low_confidence": [],
            }
        return compute_channel_confidence(journeys, channel)

    @router.post("/api/taxonomy/compute-dq")
    def compute_taxonomy_dq(db=Depends(get_db_dependency)):
        from app.services_conversions import load_journeys_from_db
        from app.services_taxonomy import persist_taxonomy_dq_snapshots

        journeys = load_journeys_from_db(db, limit=10000)
        if not journeys:
            return {"computed": 0, "message": "No journeys found"}
        snapshots = persist_taxonomy_dq_snapshots(db, journeys)
        return {
            "computed": len(snapshots),
            "metrics": [
                {
                    "source": snapshot.source,
                    "metric_key": snapshot.metric_key,
                    "metric_value": snapshot.metric_value,
                    "ts_bucket": snapshot.ts_bucket,
                }
                for snapshot in snapshots
            ],
        }

    @router.get("/api/kpis", response_model=KpiConfigModel)
    def get_kpis():
        return get_kpi_config_model_fn()

    @router.post("/api/kpis/test")
    def test_kpi_definition(payload: KpiTestPayload, db=Depends(get_db_dependency)):
        journeys = ensure_journeys_loaded_fn(db)
        if not journeys:
            return {
                "testAvailable": False,
                "eventsMatched": 0,
                "journeysMatched": 0,
                "journeysTotal": 0,
                "journeysPct": 0.0,
                "missingValueChecks": 0,
                "missingValueCount": 0,
                "missingValuePct": None,
                "message": None,
                "reason": "Load sample data to test",
            }

        definition = payload.definition
        total_journeys = len(journeys)
        target_event = (definition.event_name or definition.id or "").strip().lower()
        matched_events = 0
        journeys_matched = 0
        missing_value_checks = 0
        missing_value_count = 0
        fallback_used = False

        def _record_value(source: Dict[str, Any]) -> None:
            nonlocal missing_value_checks, missing_value_count
            if definition.value_field:
                missing_value_checks += 1
                value = source.get(definition.value_field)
                if value in (None, "", []):
                    missing_value_count += 1

        for journey in journeys:
            journey_matches = False
            events = journey.get("events") or []
            if target_event:
                for event in events:
                    name = str(event.get("name") or event.get("event_name") or "").strip().lower()
                    if name and name == target_event:
                        matched_events += 1
                        journey_matches = True
                        _record_value(event)

            if not journey_matches:
                journey_type = str(journey.get("kpi_type") or "").strip().lower()
                if definition.id and journey_type == definition.id.strip().lower():
                    matched_events += 1
                    journey_matches = True
                    _record_value(journey)
                    fallback_used = True
                elif not target_event and definition.event_name == "":
                    if journey.get("converted", False):
                        matched_events += 1
                        journey_matches = True
                        _record_value(journey)
                        fallback_used = True

            if journey_matches:
                journeys_matched += 1

        journeys_pct = (journeys_matched / total_journeys) * 100.0 if total_journeys else 0.0
        missing_value_pct = (
            (missing_value_count / missing_value_checks) * 100.0 if missing_value_checks else None
        )
        return {
            "testAvailable": True,
            "eventsMatched": matched_events,
            "journeysMatched": journeys_matched,
            "journeysTotal": total_journeys,
            "journeysPct": round(journeys_pct, 2),
            "missingValueChecks": missing_value_checks,
            "missingValueCount": missing_value_count,
            "missingValuePct": round(missing_value_pct, 2) if missing_value_pct is not None else None,
            "message": "Matched using KPI ID fallback; event stream not found." if fallback_used and target_event else None,
            "reason": None,
        }

    @router.post("/api/kpis", response_model=KpiConfigModel)
    def update_kpis(cfg: KpiConfigModel):
        return replace_kpi_config_fn(cfg)

    @router.get("/api/settings/notification-channels")
    def api_list_notification_channels(
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        return list_notification_channels(db)

    @router.post("/api/settings/notification-channels")
    def api_create_notification_channel(
        body: NotificationChannelCreate,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        try:
            return create_notification_channel(
                db,
                body.type,
                body.config,
                slack_webhook_url=body.slack_webhook_url if body.type == "slack_webhook" else None,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @router.get("/api/settings/notification-channels/{channel_id}")
    def api_get_notification_channel(
        channel_id: int,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        out = get_notification_channel(db, channel_id)
        if out is None:
            raise HTTPException(status_code=404, detail="Channel not found")
        return out

    @router.put("/api/settings/notification-channels/{channel_id}")
    def api_update_notification_channel(
        channel_id: int,
        body: NotificationChannelUpdate,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        out = update_notification_channel(
            db,
            channel_id,
            config=body.config,
            slack_webhook_url=body.slack_webhook_url,
        )
        if out is None:
            raise HTTPException(status_code=404, detail="Channel not found")
        return out

    @router.delete("/api/settings/notification-channels/{channel_id}")
    def api_delete_notification_channel(
        channel_id: int,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        if not delete_notification_channel(db, channel_id):
            raise HTTPException(status_code=404, detail="Channel not found")
        return {"ok": True}

    @router.get("/api/settings/notification-preferences")
    def api_list_notification_preferences(
        request: Request,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        return list_notification_prefs(db, resolve_current_user_id_fn(request))

    @router.post("/api/settings/notification-preferences")
    def api_upsert_notification_preference(
        body: NotificationPrefUpsert,
        request: Request,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        return upsert_notification_pref(
            db,
            resolve_current_user_id_fn(request),
            body.channel_id,
            severities=body.severities,
            digest_mode=body.digest_mode,
            quiet_hours=body.quiet_hours,
            is_enabled=body.is_enabled,
        )

    @router.get("/api/settings/notification-preferences/{pref_id}")
    def api_get_notification_preference(
        pref_id: int,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        out = get_notification_pref(db, pref_id)
        if out is None:
            raise HTTPException(status_code=404, detail="Preference not found")
        return out

    @router.put("/api/settings/notification-preferences/{pref_id}")
    def api_update_notification_preference(
        pref_id: int,
        body: NotificationPrefUpdate,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        out = update_notification_pref(
            db,
            pref_id,
            severities=body.severities,
            digest_mode=body.digest_mode,
            quiet_hours=body.quiet_hours,
            is_enabled=body.is_enabled,
        )
        if out is None:
            raise HTTPException(status_code=404, detail="Preference not found")
        return out

    @router.delete("/api/settings/notification-preferences/{pref_id}")
    def api_delete_notification_preference(
        pref_id: int,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        if not delete_notification_pref(db, pref_id):
            raise HTTPException(status_code=404, detail="Preference not found")
        return {"ok": True}

    return router
