from __future__ import annotations

from datetime import datetime
from typing import List

from .models_config_dq import JourneyInstanceFact, JourneyRoleFact, JourneyStepFact

_CONVERSION_STEP = "Purchase / Lead Won (conversion)"


def build_journey_role_facts(
    *,
    instance_row: JourneyInstanceFact,
    step_rows: List[JourneyStepFact],
    created_at: datetime,
) -> List[JourneyRoleFact]:
    pre_conversion_steps = [row for row in step_rows if str(row.step_name or "") != _CONVERSION_STEP]
    rows: List[JourneyRoleFact] = []
    if not pre_conversion_steps:
        return rows

    def _make_row(*, role_key: str, ordinal: int, step_row: JourneyStepFact) -> JourneyRoleFact:
        return JourneyRoleFact(
            conversion_id=instance_row.conversion_id,
            profile_id=instance_row.profile_id,
            conversion_key=instance_row.conversion_key,
            conversion_ts=instance_row.conversion_ts,
            role_key=role_key,
            ordinal=ordinal,
            path_hash=instance_row.path_hash,
            channel_group=instance_row.channel_group,
            channel=step_row.channel,
            campaign=step_row.campaign or instance_row.campaign_id,
            device=instance_row.device,
            country=instance_row.country,
            interaction_path_type=instance_row.interaction_path_type,
            gross_conversions_total=float(instance_row.gross_conversions_total or 0.0),
            net_conversions_total=float(instance_row.net_conversions_total or 0.0),
            gross_revenue_total=float(instance_row.gross_revenue_total or 0.0),
            net_revenue_total=float(instance_row.net_revenue_total or 0.0),
            import_batch_id=instance_row.import_batch_id,
            import_source=instance_row.import_source,
            source_snapshot_id=instance_row.source_snapshot_id,
            created_at=created_at,
            updated_at=created_at,
        )

    rows.append(_make_row(role_key="first_touch", ordinal=0, step_row=pre_conversion_steps[0]))
    rows.append(_make_row(role_key="last_touch", ordinal=0, step_row=pre_conversion_steps[-1]))

    if len(pre_conversion_steps) > 2:
        for assist_idx, step_row in enumerate(pre_conversion_steps[1:-1]):
            rows.append(_make_row(role_key="assist", ordinal=assist_idx, step_row=step_row))

    return rows
