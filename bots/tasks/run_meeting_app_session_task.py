import logging
import os
import signal

from celery import shared_task
from celery.signals import worker_shutting_down

from bots.meeting_app_session_controller import MeetingAppSessionController

logger = logging.getLogger(__name__)


@shared_task(bind=True, soft_time_limit=3600)
def run_meeting_app_session(self, meeting_app_session_id):
    logger.info(f"Running meeting app session {meeting_app_session_id}")
    meeting_app_session_controller = MeetingAppSessionController(meeting_app_session_id)
    meeting_app_session_controller.run()


def kill_child_processes():
    # Get the process group ID (PGID) of the current process
    pgid = os.getpgid(os.getpid())

    try:
        # Send SIGTERM to all processes in the process group
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass  # Process group may no longer exist


@worker_shutting_down.connect
def shutting_down_handler(sig, how, exitcode, **kwargs):
    # Just adding this code so we can see how to shut down all the tasks
    # when the main process is terminated.
    # It's likely overkill.
    logger.info("Celery worker shutting down, sending SIGTERM to all child processes")
    kill_child_processes()
