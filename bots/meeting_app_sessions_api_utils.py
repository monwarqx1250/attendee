import json
import logging
import os

import redis
from django.core.exceptions import ValidationError
from django.db import transaction

from .models import (
    MeetingAppSession,
    MeetingAppSessionEventManager,
    MeetingAppSessionEventTypes,
    Project,
    Recording,
    RecordingTypes,
    TranscriptionProviders,
    TranscriptionTypes,
)
from .serializers import CreateMeetingAppSessionSerializer

logger = logging.getLogger(__name__)


def send_sync_command(meeting_app_session, command="sync"):
    redis_url = os.getenv("REDIS_URL") + ("?ssl_cert_reqs=none" if os.getenv("DISABLE_REDIS_SSL") else "")
    redis_client = redis.from_url(redis_url)
    channel = f"meeting_app_session_{meeting_app_session.id}"
    message = {"command": command}
    redis_client.publish(channel, json.dumps(message))


def create_meeting_app_session(data: dict, project: Project) -> tuple[MeetingAppSession | None, dict | None]:
    """
    Creates a new meeting app session with the provided data.

    Args:
        data: Dictionary containing the meeting app session creation data
        project: The Project instance

    Returns:
        tuple: (MeetingAppSession instance, None) if successful, (None, error dict) if failed
    """
    # Validate the request data
    serializer = CreateMeetingAppSessionSerializer(data=data)
    if not serializer.is_valid():
        return None, serializer.errors

    validated_data = serializer.validated_data

    # Combine the three fields into a single settings object
    settings = {
        "zoom_rtms": validated_data["zoom_rtms"],
        "transcription_settings": validated_data["transcription_settings"],
        "recording_settings": validated_data["recording_settings"],
    }

    metadata = {}

    try:
        with transaction.atomic():
            meeting_app_session = MeetingAppSession.objects.create(
                project=project,
                zoom_rtms_stream_id=validated_data["zoom_rtms"]["rtms_stream_id"],
                settings=settings,
                metadata=metadata,
            )

            Recording.objects.create(
                meeting_app_session=meeting_app_session,
                recording_type=RecordingTypes.AUDIO_AND_VIDEO,
                transcription_type=TranscriptionTypes.NON_REALTIME,
                transcription_provider=TranscriptionProviders.DEEPGRAM,
                is_default_recording=True,
            )

            MeetingAppSessionEventManager.create_event(
                meeting_app_session=meeting_app_session,
                event_type=MeetingAppSessionEventTypes.CONNECTION_REQUESTED,
                event_metadata={"source": "api"},
            )

        logger.info(f"Created meeting app session {meeting_app_session.object_id}")
        return meeting_app_session, None

    except ValidationError as e:
        logger.error(f"ValidationError creating meeting app session: {e}")
        return None, {"error": e.messages[0]}
    except Exception as e:
        logger.error(f"Error creating meeting app session: {e}")
        return None, {"error": str(e)}
