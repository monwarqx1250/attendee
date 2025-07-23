import logging

from django.core.management.base import BaseCommand

from bots.tasks import run_meeting_app_session  # Import your task

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs the celery task synchronously on a given bot that is already created"

    def add_arguments(self, parser):
        # Add any arguments you need
        parser.add_argument("--meeting_app_session_id", type=int, help="Meeting app session ID")

    def handle(self, *args, **options):
        logger.info("Running run meeting app session task...")

        # Call your task directly
        result = run_meeting_app_session.run(options["meeting_app_session_id"])

        logger.info(f"Run meeting app session task completed with result: {result}")
