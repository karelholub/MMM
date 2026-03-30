from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import ConversionPath, Experiment, ExperimentAssignment
from app.services_incrementality import auto_assign_from_conversion_paths


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_auto_assign_from_conversion_paths_uses_shared_touchpoint_adapter():
    db = _unit_db_session()
    try:
        experiment = Experiment(
            name="Paid holdout",
            channel="google_ads",
            start_at=datetime(2026, 2, 1, 0, 0),
            end_at=datetime(2026, 2, 10, 23, 59),
            status="running",
        )
        db.add(experiment)
        db.flush()
        db.add_all(
            [
                ConversionPath(
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 2, 5, 12, 0),
                    path_json={
                        "touchpoints": [
                            {"ts": "2026-02-03T09:00:00Z", "channel": "google_ads"},
                            {"ts": "2026-02-05T09:00:00Z", "channel": "direct"},
                        ],
                        "conversions": [{"name": "purchase", "value": 100.0}],
                    },
                    path_hash="hash-1",
                    length=2,
                    first_touch_ts=datetime(2026, 2, 3, 9, 0),
                    last_touch_ts=datetime(2026, 2, 5, 9, 0),
                ),
                ConversionPath(
                    conversion_id="conv-2",
                    profile_id="p-2",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 2, 5, 12, 0),
                    path_json={
                        "touchpoints": [
                            {"ts": "2026-02-03T09:00:00Z", "channel": "email"},
                        ],
                        "conversions": [{"name": "purchase", "value": 50.0}],
                    },
                    path_hash="hash-2",
                    length=1,
                    first_touch_ts=datetime(2026, 2, 3, 9, 0),
                    last_touch_ts=datetime(2026, 2, 3, 9, 0),
                ),
            ]
        )
        db.commit()

        counts = auto_assign_from_conversion_paths(
            db=db,
            experiment_id=experiment.id,
            channel="google_ads",
            start_date=datetime(2026, 2, 1, 0, 0),
            end_date=datetime(2026, 2, 10, 23, 59),
            treatment_rate=0.5,
        )

        assignments = db.query(ExperimentAssignment).filter(ExperimentAssignment.experiment_id == experiment.id).all()

        assert sum(counts.values()) == 1
        assert len(assignments) == 1
        assert assignments[0].profile_id == "p-1"
    finally:
        db.close()
