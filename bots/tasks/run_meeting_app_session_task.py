import logging

from celery import shared_task

from bots.meeting_app_session_controller import MeetingAppSessionController

logger = logging.getLogger(__name__)


@shared_task(bind=True, soft_time_limit=3600)
def run_meeting_app_session(self, meeting_app_session_id):
    logger.info(f"Running meeting app session {meeting_app_session_id}")
    meeting_app_session_controller = MeetingAppSessionController(meeting_app_session_id)
    meeting_app_session_controller.run()
