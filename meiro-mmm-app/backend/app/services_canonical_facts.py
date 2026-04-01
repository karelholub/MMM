from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any, List, Optional, Tuple

from sqlalchemy.orm import Session

from .models_config_dq import JourneyInstanceFact, SilverConversionFact
from .services_journey_instance_facts import load_journey_instance_sequences
from .services_silver_journeys import load_silver_journeys
from .services_visit_facts import iter_touchpoint_visit_rows


def iter_canonical_conversion_rows(
    db: Session,
    *,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    conversion_key: Optional[str] = None,
):
    silver_q = db.query(
        SilverConversionFact.conversion_id,
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
    )
    if date_from is not None:
        silver_q = silver_q.filter(SilverConversionFact.conversion_ts >= date_from)
    if date_to is not None:
        silver_q = silver_q.filter(SilverConversionFact.conversion_ts <= date_to)
    if conversion_key:
        silver_q = silver_q.filter(SilverConversionFact.conversion_key == conversion_key)
    if silver_q.limit(1).all():
        for row in silver_q.order_by(SilverConversionFact.conversion_ts.desc()).yield_per(1000):
            yield SimpleNamespace(
                conversion_id=row[0],
                conversion_key=row[1],
                conversion_ts=row[2],
                interaction_path_type=row[3],
                gross_conversions_total=float(row[4] or 0.0),
                net_conversions_total=float(row[5] or 0.0),
                gross_revenue_total=float(row[6] or 0.0),
                net_revenue_total=float(row[7] or 0.0),
                refunded_value=float(row[8] or 0.0),
                cancelled_value=float(row[9] or 0.0),
                invalid_leads=float(row[10] or 0.0),
                valid_leads=float(row[11] or 0.0),
            )
        return

    instance_q = db.query(
        JourneyInstanceFact.conversion_id,
        JourneyInstanceFact.conversion_key,
        JourneyInstanceFact.conversion_ts,
        JourneyInstanceFact.interaction_path_type,
        JourneyInstanceFact.gross_conversions_total,
        JourneyInstanceFact.net_conversions_total,
        JourneyInstanceFact.gross_revenue_total,
        JourneyInstanceFact.net_revenue_total,
    )
    if date_from is not None:
        instance_q = instance_q.filter(JourneyInstanceFact.conversion_ts >= date_from)
    if date_to is not None:
        instance_q = instance_q.filter(JourneyInstanceFact.conversion_ts <= date_to)
    if conversion_key:
        instance_q = instance_q.filter(JourneyInstanceFact.conversion_key == conversion_key)
    for row in instance_q.order_by(JourneyInstanceFact.conversion_ts.desc()).yield_per(1000):
        yield SimpleNamespace(
            conversion_id=row[0],
            conversion_key=row[1],
            conversion_ts=row[2],
            interaction_path_type=row[3],
            gross_conversions_total=float(row[4] or 0.0),
            net_conversions_total=float(row[5] or 0.0),
            gross_revenue_total=float(row[6] or 0.0),
            net_revenue_total=float(row[7] or 0.0),
            refunded_value=0.0,
            cancelled_value=0.0,
            invalid_leads=0.0,
            valid_leads=float(row[5] or 0.0),
        )


def load_preferred_journey_rows(
    db: Session,
    *,
    start_dt: datetime,
    end_dt: datetime,
    conversion_key: Optional[str] = None,
) -> Tuple[Optional[str], List[dict]]:
    instance_rows = load_journey_instance_sequences(
        db,
        start_dt=start_dt,
        end_dt=end_dt,
        conversion_key=conversion_key,
    )
    if instance_rows:
        return "instance", instance_rows
    silver_rows = load_silver_journeys(
        db,
        start_dt=start_dt,
        end_dt=end_dt,
        conversion_key=conversion_key,
    )
    if silver_rows:
        return "silver", silver_rows
    return None, []


def load_channel_canonical_source(
    db: Session,
    *,
    start_dt: datetime,
    end_dt: datetime,
    conversion_key: Optional[str] = None,
) -> Tuple[List[Any], List[Any]]:
    touchpoint_rows = list(
        iter_touchpoint_visit_rows(
            db,
            touchpoint_from=start_dt,
            touchpoint_to=end_dt,
        )
    )
    conversion_rows = list(
        iter_canonical_conversion_rows(
            db,
            date_from=start_dt,
            date_to=end_dt,
            conversion_key=conversion_key,
        )
    )
    return touchpoint_rows, conversion_rows
