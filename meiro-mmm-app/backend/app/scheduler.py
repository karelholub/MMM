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
    
    scheduler.start()
    logger.info("Scheduler started. Nightly report scheduled for 2 AM daily.")


def main():
    """CLI entry point for scheduled tasks."""
    parser = argparse.ArgumentParser(description="Run scheduled tasks for incrementality experiments")
    parser.add_argument(
        "--task",
        choices=["nightly-report"],
        required=True,
        help="Task to run",
    )
    
    args = parser.parse_args()
    
    db = next(get_db())
    
    try:
        if args.task == "nightly-report":
            run_nightly_report_task(db)
        else:
            logger.error(f"Unknown task: {args.task}")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
