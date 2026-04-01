from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from .models_config_dq import SilverConversionFact, SilverTouchpointFact


def _aware_dt(value: Any) -> Optional[datetime]:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _touchpoint_payload(ts: Any, channel: Any, source: Any, medium: Any, campaign: Any, event_name: Any, interaction_type: Any) -> Dict[str, Any]:
    ts_value = _aware_dt(ts)
    iso_ts = ts_value.isoformat() if ts_value is not None else None
    return {
        "ts": iso_ts,
        "timestamp": iso_ts,
        "channel": channel,
        "source": source,
        "medium": medium,
        "campaign": campaign,
        "event_name": event_name,
        "interaction_type": interaction_type,
    }


def load_silver_journeys(
    db: Session,
    *,
    start_dt: datetime,
    end_dt: datetime,
    conversion_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    q = db.query(
        SilverConversionFact.conversion_id,
        SilverConversionFact.profile_id,
        SilverConversionFact.conversion_key,
        SilverConversionFact.conversion_ts,
        SilverConversionFact.interaction_path_type,
        SilverConversionFact.gross_conversions_total,
        SilverConversionFact.net_conversions_total,
        SilverConversionFact.gross_revenue_total,
        SilverConversionFact.net_revenue_total,
        SilverConversionFact.refunded_value,
        SilverConversionFact.cancelled_value,
        SilverConversionFact.invalid_leads,
        SilverConversionFact.valid_leads,
        SilverConversionFact.device,
        SilverConversionFact.country,
        SilverConversionFact.browser,
        SilverConversionFact.consent_opt_out,
        SilverConversionFact.landing_page_group,
        SilverConversionFact.has_error_event,
    ).filter(
        SilverConversionFact.conversion_ts >= start_dt,
        SilverConversionFact.conversion_ts < end_dt,
    )
    if conversion_key:
        q = q.filter(SilverConversionFact.conversion_key == conversion_key)
    rows = q.all()
    if not rows:
        return []

    conversion_ids = [str(row[0] or "") for row in rows if str(row[0] or "")]
    touchpoints_by_conversion: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    if conversion_ids:
        for conversion_id, touchpoint_ts, channel, source, medium, campaign, event_name, interaction_type, _ordinal in (
            db.query(
                SilverTouchpointFact.conversion_id,
                SilverTouchpointFact.touchpoint_ts,
                SilverTouchpointFact.channel,
                SilverTouchpointFact.source,
                SilverTouchpointFact.medium,
                SilverTouchpointFact.campaign,
                SilverTouchpointFact.event_name,
                SilverTouchpointFact.interaction_type,
                SilverTouchpointFact.ordinal,
            )
            .filter(SilverTouchpointFact.conversion_id.in_(conversion_ids))
            .order_by(SilverTouchpointFact.conversion_id.asc(), SilverTouchpointFact.ordinal.asc())
            .all()
        ):
            touchpoints_by_conversion[str(conversion_id or "")].append(
                _touchpoint_payload(touchpoint_ts, channel, source, medium, campaign, event_name, interaction_type)
            )

    out: List[Dict[str, Any]] = []
    for row in rows:
        conversion_id = str(row[0] or "")
        profile_id = str(row[1] or "").strip() or conversion_id
        conversion_key_value = str(row[2] or "").strip() or None
        conversion_ts = _aware_dt(row[3])
        if conversion_ts is None:
            continue
        interaction_path_type = row[4]
        gross_conversions_total = float(row[5] or 0.0)
        net_conversions_total = float(row[6] or 0.0)
        gross_revenue_total = float(row[7] or 0.0)
        net_revenue_total = float(row[8] or 0.0)
        refunded_value = float(row[9] or 0.0)
        cancelled_value = float(row[10] or 0.0)
        invalid_leads = float(row[11] or 0.0)
        valid_leads = float(row[12] or 0.0)
        payload = {
            "journey_id": conversion_id,
            "customer": {"id": profile_id},
            "kpi_type": conversion_key_value,
            "touchpoints": list(touchpoints_by_conversion.get(conversion_id) or []),
            "conversions": [
                {
                    "id": conversion_id,
                    "name": conversion_key_value,
                    "ts": conversion_ts.isoformat(),
                    "value": gross_revenue_total,
                }
            ],
            "device": row[13],
            "country": row[14],
            "browser": row[15],
            "consent_opt_out": row[16],
            "landing_page_group": row[17],
            "has_error_event": row[18],
            "interaction_path_type": interaction_path_type,
            "meta": {"interaction_summary": {"path_type": interaction_path_type}},
            "_revenue_entries": [
                {
                    "value_in_base": gross_revenue_total,
                    "net_value_in_base": net_revenue_total,
                    "gross_value": gross_revenue_total,
                    "net_value": net_revenue_total,
                    "refunded_value": refunded_value,
                    "cancelled_value": cancelled_value,
                    "gross_conversions": gross_conversions_total,
                    "net_conversions": net_conversions_total,
                    "invalid_leads": invalid_leads,
                    "valid_leads": valid_leads,
                }
            ],
        }
        out.append(
            {
                "conversion_id": conversion_id,
                "profile_id": profile_id,
                "conversion_key": conversion_key_value,
                "conversion_ts": conversion_ts,
                "interaction_path_type": interaction_path_type,
                "gross_conversions_total": gross_conversions_total,
                "net_conversions_total": net_conversions_total,
                "gross_revenue_total": gross_revenue_total,
                "net_revenue_total": net_revenue_total,
                "refunded_value": refunded_value,
                "cancelled_value": cancelled_value,
                "invalid_leads": invalid_leads,
                "valid_leads": valid_leads,
                "device": row[13],
                "country": row[14],
                "browser": row[15],
                "consent_opt_out": row[16],
                "landing_page_group": row[17],
                "has_error_event": row[18],
                "payload": payload,
            }
        )
    return out


def load_recent_silver_journeys(
    db: Session,
    *,
    limit: int = 10000,
) -> List[Dict[str, Any]]:
    rows = (
        db.query(SilverConversionFact.conversion_ts)
        .order_by(SilverConversionFact.conversion_ts.desc())
        .limit(max(1, int(limit)))
        .all()
    )
    if not rows:
        return []
    end_dt = _aware_dt(rows[0][0])
    start_dt = _aware_dt(rows[-1][0])
    if end_dt is None or start_dt is None:
        return []
    journeys = load_silver_journeys(
        db,
        start_dt=start_dt,
        end_dt=end_dt + timedelta(microseconds=1),
    )
    journeys.sort(key=lambda row: row["conversion_ts"], reverse=True)
    return journeys[: max(1, int(limit))]
