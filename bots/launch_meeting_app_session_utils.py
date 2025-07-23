import json
import logging
import os

from bots.models import MeetingAppSessionEventManager, MeetingAppSessionEventSubTypes, MeetingAppSessionEventTypes

logger = logging.getLogger(__name__)


def launch_meeting_app_session(meeting_app_session):
    # If this instance is running in Kubernetes, use the Kubernetes pod creator
    # which spins up a new pod for the bot
    if os.getenv("LAUNCH_BOT_METHOD") == "kubernetes":
        from .task_pod_creator import TaskPodCreator

        task_pod_creator = TaskPodCreator()
        create_pod_result = task_pod_creator.create_task_pod(id=meeting_app_session.id, name=meeting_app_session.k8s_pod_name(), cpu_request=meeting_app_session.cpu_request(), command=meeting_app_session.k8s_run_command())
        logger.info(f"Meeting app session {meeting_app_session.object_id} ({meeting_app_session.id}) launched via Kubernetes: {create_pod_result}")
        if not create_pod_result.get("created"):
            logger.error(f"Meeting app session {meeting_app_session.object_id} ({meeting_app_session.id}) failed to launch via Kubernetes.")
            try:
                MeetingAppSessionEventManager.create_event(
                    meeting_app_session=meeting_app_session,
                    event_type=MeetingAppSessionEventTypes.FATAL_ERROR,
                    event_sub_type=MeetingAppSessionEventSubTypes.FATAL_ERROR_MEETING_APP_SESSION_NOT_LAUNCHED,
                    event_metadata={
                        "create_pod_result": json.dumps(create_pod_result),
                    },
                )
            except Exception as e:
                logger.error(f"Failed to create fatal error meeting app session not launched event for meeting app session {meeting_app_session.object_id} ({meeting_app_session.id}): {str(e)}")
    else:
        # Default to launching bot via celery
        from .tasks.run_meeting_app_session_task import run_meeting_app_session

        run_meeting_app_session.delay(meeting_app_session.id)
