import json
import logging
import os
import signal
import threading
import time

import redis
from gi.repository import GLib

from bots.bot_controller.pipeline_configuration import PipelineConfiguration
from bots.models import Credentials, MeetingAppSession, MeetingAppSessionEventManager, MeetingAppSessionEventSubTypes, MeetingAppSessionEventTypes, MeetingAppSessionStates, Recording
from bots.zoom_rtms_meeting_app_session_adapter import ZoomRtmsMeetingAppSessionAdapter

logger = logging.getLogger(__name__)


class MeetingAppSessionController:
    def get_pipeline_configuration(self):
        return PipelineConfiguration.recorder_bot()

    def __init__(self, meeting_app_session_id):
        self.meeting_app_session_in_db = MeetingAppSession.objects.get(id=meeting_app_session_id)
        self.cleanup_called = False
        self.run_called = False

        self.redis_client = None
        self.pubsub = None
        self.pubsub_channel = f"meeting_app_session_{self.meeting_app_session_in_db.id}"

        self.pipeline_configuration = self.get_pipeline_configuration()

    def connect_to_redis(self):
        # Close both pubsub and client if they exist
        if self.pubsub:
            self.pubsub.close()
        if self.redis_client:
            self.redis_client.close()

        redis_url = os.getenv("REDIS_URL") + ("?ssl_cert_reqs=none" if os.getenv("DISABLE_REDIS_SSL") else "")
        self.redis_client = redis.from_url(redis_url)
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe(self.pubsub_channel)
        logger.info(f"Redis connection established for meeting app session {self.meeting_app_session_in_db.id}")

    def get_zoom_oauth_credentials(self):
        zoom_oauth_credentials_record = self.meeting_app_session_in_db.project.credentials.filter(credential_type=Credentials.CredentialTypes.ZOOM_OAUTH).first()
        if not zoom_oauth_credentials_record:
            raise Exception("Zoom OAuth credentials not found")

        zoom_oauth_credentials = zoom_oauth_credentials_record.get_credentials()
        if not zoom_oauth_credentials:
            raise Exception("Zoom OAuth credentials data not found")

        return zoom_oauth_credentials

    def get_meeting_app_session_adapter(self):
        zoom_oauth_credentials = self.get_zoom_oauth_credentials()
        return ZoomRtmsMeetingAppSessionAdapter(
            rtms_join_payload=self.meeting_app_session_in_db.zoom_rtms(),
            zoom_client_id=zoom_oauth_credentials["client_id"],
            zoom_client_secret=zoom_oauth_credentials["client_secret"],
            recording_file_location=self.get_recording_file_location(),
        )

    def get_recording_file_location(self):
        if self.pipeline_configuration.rtmp_stream_audio or self.pipeline_configuration.rtmp_stream_video:
            return None
        else:
            return os.path.join("/tmp", self.get_recording_filename())

    def get_recording_filename(self):
        recording = Recording.objects.get(meeting_app_session=self.meeting_app_session_in_db, is_default_recording=True)
        return f"{self.meeting_app_session_in_db.object_id}-{recording.object_id}.mp4"

    def run(self):
        if self.run_called:
            raise Exception("Run already called, exiting")
        self.run_called = True

        self.connect_to_redis()

        self.adapter = self.get_meeting_app_session_adapter()
        self.adapter_initialized = False

        # Create GLib main loop
        self.main_loop = GLib.MainLoop()

        def repeatedly_try_to_reconnect_to_redis():
            reconnect_delay_seconds = 1
            num_attempts = 0
            while True:
                try:
                    self.connect_to_redis()
                    break
                except Exception as e:
                    logger.info(f"Error reconnecting to Redis: {e} Attempt {num_attempts} / 30.")
                    time.sleep(reconnect_delay_seconds)
                    num_attempts += 1
                    if num_attempts > 30:
                        raise Exception("Failed to reconnect to Redis after 30 attempts")

        def redis_listener():
            while True:
                try:
                    message = self.pubsub.get_message(timeout=1.0)
                    if message:
                        # Schedule Redis message handling in the main GLib loop
                        GLib.idle_add(lambda: self.handle_redis_message(message))
                except Exception as e:
                    # If this is a certain type of exception, we can attempt to reconnect
                    if isinstance(e, redis.exceptions.ConnectionError) and "Connection closed by server." in str(e):
                        logger.info("Redis connection closed by server. Attempting to reconnect...")
                        repeatedly_try_to_reconnect_to_redis()

                    else:
                        # log the type of exception
                        logger.info(f"Error in Redis listener: {type(e)} {e}")
                        break

        redis_thread = threading.Thread(target=redis_listener, daemon=True)
        redis_thread.start()

        # Add timeout just for audio processing
        self.first_timeout_call = True
        GLib.timeout_add(100, self.on_main_loop_timeout)

        # Add signal handlers so that when we get a SIGTERM or SIGINT, we can clean up the meeting app session
        GLib.unix_signal_add(GLib.PRIORITY_HIGH, signal.SIGTERM, self.handle_glib_shutdown)
        GLib.unix_signal_add(GLib.PRIORITY_HIGH, signal.SIGINT, self.handle_glib_shutdown)

        # Run the main loop
        try:
            self.main_loop.run()
        except Exception as e:
            logger.info(f"Error in meeting app session {self.meeting_app_session_in_db.id} ({self.meeting_app_session_in_db.object_id}): {str(e)}")
            self.cleanup()

    def take_action_based_on_meeting_app_session_in_db(self):
        if self.meeting_app_session_in_db.state == MeetingAppSessionStates.CONNECTING:
            logger.info("take_action_based_on_meeting_app_session_in_db - CONNECTING")
            MeetingAppSessionEventManager.set_requested_meeting_app_session_action_taken_at(self.meeting_app_session_in_db)
            self.adapter.init()
        if self.meeting_app_session_in_db.state == MeetingAppSessionStates.POST_PROCESSING:
            logger.info("take_action_based_on_meeting_app_session_in_db - POST_PROCESSING")
            MeetingAppSessionEventManager.set_requested_meeting_app_session_action_taken_at(self.meeting_app_session_in_db)
            self.adapter.leave()

    def handle_redis_message(self, message):
        if message and message["type"] == "message":
            data = json.loads(message["data"].decode("utf-8"))
            command = data.get("command")

            if command == "sync":
                logger.info(f"Syncing meeting app session {self.meeting_app_session_in_db.object_id}")
                self.meeting_app_session_in_db.refresh_from_db()
                self.take_action_based_on_meeting_app_session_in_db()
            else:
                logger.info(f"Unknown command: {command}")

    def cleanup(self):
        pass

    def on_main_loop_timeout(self):
        if not self.adapter_initialized:
            self.adapter_initialized = True
            self.adapter.init()

        return True

    def handle_glib_shutdown(self):
        logger.info("handle_glib_shutdown called")

        try:
            MeetingAppSessionEventManager.create_event(
                meeting_app_session=self.meeting_app_session_in_db,
                event_type=MeetingAppSessionEventTypes.FATAL_ERROR,
                event_sub_type=MeetingAppSessionEventSubTypes.FATAL_ERROR_PROCESS_TERMINATED,
            )
        except Exception as e:
            logger.info(f"Error creating FATAL_ERROR event: {e}")

        self.cleanup()
        return False
