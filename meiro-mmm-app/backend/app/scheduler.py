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

from .db import get_db
from .services_alerts_engine import run_alerts_engine
from .services_delivery import run_daily_digest, trigger_realtime_for_new_open_events
from .services_incrementality import run_nightly_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


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

    scheduler.start()
    logger.info("Scheduler started. Nightly report at 2 AM; alerts engine hourly; digest at 9 AM.")


def main():
    """CLI entry point for scheduled tasks."""
    parser = argparse.ArgumentParser(description="Run scheduled tasks for incrementality experiments")
    parser.add_argument(
        "--task",
        choices=["nightly-report", "run-alerts", "alert-daily-digest"],
        required=True,
        help="Task to run",
    )
    parser.add_argument("--scope", default="default", help="Scope for run-alerts (default: default)")
    parser.add_argument("--base-url", default="", help="Base URL for alert links (e.g. https://app.example.com)")

    args = parser.parse_args()

    db = next(get_db())

    try:
        if args.task == "nightly-report":
            run_nightly_report_task(db)
        elif args.task == "run-alerts":
            run_alerts_task(db, scope=args.scope, base_url=args.base_url)
        elif args.task == "alert-daily-digest":
            run_daily_digest_task(db, base_url=args.base_url)
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
