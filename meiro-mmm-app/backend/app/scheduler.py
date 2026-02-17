"""
Scheduler for automated incrementality experiment reporting.

This module provides scheduled tasks for:
- Nightly experiment result computation
- Alert generation
- Email/webhook notifications

Usage:
    # Run as a cron job
    python -m app.scheduler --task nightly-report

    # Or use APScheduler for in-process scheduling
    from app.scheduler import start_scheduler
    start_scheduler()
"""

import argparse
import logging
import sys
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from .db import Base, engine, get_db
from .services_alerts_engine import run_alerts_engine
from .services_delivery import run_daily_digest, trigger_realtime_for_new_open_events
from .services_incrementality import run_nightly_report
from .services_journey_aggregates import run_daily_journey_aggregates
from .services_journey_alerts import evaluate_alert_definitions as run_journey_alerts_evaluator
from .services_journey_settings import get_active_journey_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Ensure local SQLite/dev runs can execute scheduled tasks without a separate bootstrap.
Base.metadata.create_all(bind=engine)


def run_nightly_report_task(db: Session) -> None:
    """
    Run nightly report for all active experiments.
    
    Computes results and logs alerts.
    """
    logger.info("Starting nightly experiment report")
    
    try:
        report = run_nightly_report(db)
        
        logger.info(f"Nightly report completed: {len(report['experiments'])} experiments processed")
        
        # Log alerts
        for alert in report.get("alerts", []):
            severity = alert.get("severity", "info")
            message = alert.get("message", "")
            exp_id = alert.get("experiment_id")
            
            log_fn = {
                "info": logger.info,
                "success": logger.info,
                "warning": logger.warning,
                "error": logger.error,
            }.get(severity, logger.info)
            
            log_fn(f"Experiment {exp_id}: {message}")
        
        # TODO: Send notifications (email, Slack, webhook)
        # send_notifications(report)
        
    except Exception as e:
        logger.error(f"Nightly report failed: {e}", exc_info=True)
        raise


def run_alerts_task(db: Session, scope: str = "default", base_url: str = "") -> dict:
    """
    Run alerts engine for the given scope, then trigger realtime notification delivery
    for newly created open alert_events.
    Returns metrics (rules_evaluated, events_created, events_updated, events_resolved, delivery stats).
    """
    logger.info("Starting alerts engine for scope=%s", scope)
    try:
        metrics = run_alerts_engine(db, scope)
        logger.info(
            "Alerts engine completed: scope=%s rules_evaluated=%s events_created=%s events_updated=%s events_resolved=%s",
            scope,
            metrics.get("rules_evaluated", 0),
            metrics.get("events_created", 0),
            metrics.get("events_updated", 0),
            metrics.get("events_resolved", 0),
        )
        created_ids = metrics.get("created_event_ids") or []
        if created_ids:
            try:
                delivery = trigger_realtime_for_new_open_events(db, created_ids, base_url=base_url)
                metrics["delivery"] = delivery
                logger.info(
                    "Realtime delivery: delivered=%s skipped=%s failed=%s",
                    delivery.get("delivered", 0),
                    delivery.get("skipped", 0),
                    delivery.get("failed", 0),
                )
            except Exception as e:
                logger.error("Realtime notification delivery failed: %s", e, exc_info=True)
        return metrics
    except Exception as e:
        logger.error("Alerts engine failed: %s", e, exc_info=True)
        raise


def run_daily_digest_task(db: Session, base_url: str = "") -> dict:
    """
    Send daily alert digest to users with digest_mode=daily, respecting quiet hours.
    """
    logger.info("Starting daily alert digest")
    try:
        result = run_daily_digest(db, base_url=base_url)
        logger.info(
            "Daily digest completed: digests_sent=%s failed=%s",
            result.get("digests_sent", 0),
            result.get("failed", 0),
        )
        return result
    except Exception as e:
        logger.error("Daily digest failed: %s", e, exc_info=True)
        raise


def run_journey_daily_aggregates_task(db: Session, reprocess_days: int = 3) -> dict:
    """Compute daily journey path/transition aggregates with incremental backfill."""
    active = get_active_journey_settings(db, use_cache=True)
    configured_reprocess = (
        ((active.get("settings_json") or {}).get("performance_guardrails") or {}).get(
            "aggregation_reprocess_window_days",
            reprocess_days,
        )
    )
    effective_reprocess_days = max(1, int(configured_reprocess or reprocess_days))
    logger.info(
        "Starting journey daily aggregates (reprocess_days=%s)",
        effective_reprocess_days,
    )
    try:
        metrics = run_daily_journey_aggregates(db, reprocess_days=effective_reprocess_days)
        logger.info(
            "Journey daily aggregates completed: definitions=%s days_processed=%s source_rows=%s lag_minutes=%s duration_ms=%s",
            metrics.get("definitions", 0),
            metrics.get("days_processed", 0),
            metrics.get("source_rows_processed", 0),
            metrics.get("lag_minutes"),
            metrics.get("duration_ms", 0),
        )
        return metrics
    except Exception as e:
        logger.error("Journey daily aggregates failed: %s", e, exc_info=True)
        raise


def run_journey_alerts_task(db: Session, domain: Optional[str] = None) -> dict:
    """Evaluate enabled journey/funnel alerts and emit alert events."""
    logger.info("Starting journey alerts evaluator (domain=%s)", domain or "all")
    try:
        metrics = run_journey_alerts_evaluator(db, domain=domain)
        logger.info(
            "Journey alerts evaluator completed: evaluated=%s fired=%s skipped_cooldown=%s errors=%s duration_ms=%s",
            metrics.get("evaluated", 0),
            metrics.get("fired", 0),
            metrics.get("skipped_cooldown", 0),
            metrics.get("errors", 0),
            metrics.get("duration_ms", 0),
        )
        return metrics
    except Exception as e:
        logger.error("Journey alerts evaluator failed: %s", e, exc_info=True)
        raise


def send_notifications(report: dict) -> None:
    """
    Send notifications for experiment alerts.
    
    TODO: Implement email, Slack, or webhook notifications.
    """
    # Placeholder for notification logic
    # Could integrate with:
    # - SendGrid for email
    # - Slack webhook for Slack notifications
    # - Generic webhook for custom integrations
    pass


def start_scheduler() -> None:
    """
    Start APScheduler for in-process scheduling.
    
    Useful for development or single-instance deployments.
    For production, prefer external cron or task queue (Celery, etc.).
    """
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        logger.error("APScheduler not installed. Install with: pip install apscheduler")
        return
    
    scheduler = BackgroundScheduler()
    
    # Schedule nightly report at 2 AM
    scheduler.add_job(
        func=lambda: run_nightly_report_task(next(get_db())),
        trigger=CronTrigger(hour=2, minute=0),
        id="nightly_experiment_report",
        name="Nightly experiment report",
        replace_existing=True,
    )

    # Schedule alerts engine hourly (default scope)
    scheduler.add_job(
        func=lambda: run_alerts_task(next(get_db()), "default"),
        trigger=CronTrigger(minute=5),  # every hour at :05
        id="alerts_engine",
        name="Alerts engine",
        replace_existing=True,
    )

    # Daily digest at 9 AM (respects quiet_hours per user pref)
    scheduler.add_job(
        func=lambda: run_daily_digest_task(next(get_db())),
        trigger=CronTrigger(hour=9, minute=0),
        id="alert_daily_digest",
        name="Alert daily digest",
        replace_existing=True,
    )

    # Daily journey path/transition aggregates shortly after midnight
    scheduler.add_job(
        func=lambda: run_journey_daily_aggregates_task(next(get_db()), reprocess_days=3),
        trigger=CronTrigger(hour=0, minute=20),
        id="journey_daily_aggregates",
        name="Journey daily aggregates",
        replace_existing=True,
    )

    # Daily journey/funnel alert evaluation after aggregate build.
    scheduler.add_job(
        func=lambda: run_journey_alerts_task(next(get_db())),
        trigger=CronTrigger(hour=0, minute=35),
        id="journey_alerts_evaluator",
        name="Journey alerts evaluator",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started. Nightly report at 2 AM; alerts engine hourly; digest at 9 AM; journey aggregates at 00:20; journey alerts at 00:35.")


def main():
    """CLI entry point for scheduled tasks."""
    parser = argparse.ArgumentParser(description="Run scheduled tasks for incrementality experiments")
    parser.add_argument(
        "--task",
        choices=["nightly-report", "run-alerts", "alert-daily-digest", "journey-daily-aggs", "journey-alerts"],
        required=True,
        help="Task to run",
    )
    parser.add_argument("--scope", default="default", help="Scope for run-alerts (default: default)")
    parser.add_argument("--domain", default="", help="Domain for journey-alerts: journeys|funnels|empty")
    parser.add_argument("--base-url", default="", help="Base URL for alert links (e.g. https://app.example.com)")
    parser.add_argument("--reprocess-days", type=int, default=3, help="Days to reprocess for journey-daily-aggs (default: 3)")

    args = parser.parse_args()

    db = next(get_db())

    try:
        if args.task == "nightly-report":
            run_nightly_report_task(db)
        elif args.task == "run-alerts":
            run_alerts_task(db, scope=args.scope, base_url=args.base_url)
        elif args.task == "alert-daily-digest":
            run_daily_digest_task(db, base_url=args.base_url)
        elif args.task == "journey-daily-aggs":
            run_journey_daily_aggregates_task(db, reprocess_days=max(1, args.reprocess_days))
        elif args.task == "journey-alerts":
            dom = (args.domain or "").strip().lower() or None
            run_journey_alerts_task(db, domain=dom)
        else:
            logger.error("Unknown task: %s", args.task)
            sys.exit(1)
    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
