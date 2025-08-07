import json
import subprocess

import gi

from bots.bot_adapter import BotAdapter

gi.require_version("GLib", "2.0")
from gi.repository import GLib
import logging

from bots.automatic_leave_configuration import AutomaticLeaveConfiguration

logger = logging.getLogger(__name__)


class ZoomRTMSAdapter(BotAdapter):
    def __init__(
        self,
        *,
        use_one_way_audio,
        use_mixed_audio,
        use_video,
        send_message_callback,
        add_audio_chunk_callback,
        zoom_rtms,
        add_video_frame_callback,
        wants_any_video_frames_callback,
        add_mixed_audio_chunk_callback,
        zoom_client_id,
        zoom_client_secret,
        recording_file_path,
        automatic_leave_configuration: AutomaticLeaveConfiguration,
        upsert_chat_message_callback,
        add_participant_event_callback,
        video_frame_size: tuple[int, int],
    ):
        self.zoom_rtms = zoom_rtms
        self.use_one_way_audio = use_one_way_audio
        self.use_mixed_audio = use_mixed_audio
        self.use_video = use_video
        self.send_message_callback = send_message_callback
        self.add_audio_chunk_callback = add_audio_chunk_callback
        self.add_mixed_audio_chunk_callback = add_mixed_audio_chunk_callback
        self.add_video_frame_callback = add_video_frame_callback
        self.wants_any_video_frames_callback = wants_any_video_frames_callback

        self.zoom_client_id = zoom_client_id
        self.zoom_client_secret = zoom_client_secret
        self.recording_file_path = recording_file_path

        self.meeting_service = None
        self.setting_service = None
        self.auth_service = None

        self.auth_event = None
        self.recording_event = None
        self.meeting_service_event = None

        self.audio_source = None
        self.audio_helper = None

        self.audio_settings = None

        self.use_raw_recording = True
        self.recording_permission_granted = False

        self.reminder_controller = None

        self.recording_ctrl = None

        self.audio_raw_data_sender = None
        self.virtual_audio_mic_event_passthrough = None

        self.my_participant_id = None
        self.participants_ctrl = None
        self.meeting_reminder_event = None
        self.on_mic_start_send_callback_called = False
        self.on_virtual_camera_start_send_callback_called = False

        self.meeting_video_controller = None
        self.video_sender = None
        self.virtual_camera_video_source = None
        self.video_source_helper = None
        self.video_frame_size = video_frame_size
        self.send_image_timeout_id = None

        self.automatic_leave_configuration = automatic_leave_configuration

        self.only_one_participant_in_meeting_at = None
        self.last_audio_received_at = None
        self.silence_detection_activated = False
        self.cleaned_up = False
        self.requested_leave = False
        self.joined_at = None
        self.left_meeting = False

        if self.use_video:
            self.video_input_manager = None
        else:
            self.video_input_manager = None

        self.meeting_sharing_controller = None
        self.meeting_share_ctrl_event = None

        self.active_speaker_id = None
        self.active_sharer_id = None
        self.active_sharer_source_id = None

        self._participant_cache = {}

        self.meeting_status = None

        self.suggested_video_cap = None

        self.upsert_chat_message_callback = upsert_chat_message_callback
        self.add_participant_event_callback = add_participant_event_callback

        # Stdout IO watch
        self.stdout_watch_id = None

    def cleanup(self):
        logger.info("cleanup called")
        self.cleaned_up = True
        
        # Remove stdout IO watch
        if self.stdout_watch_id:
            GLib.source_remove(self.stdout_watch_id)
            self.stdout_watch_id = None

    def init(self):
        logger.info("init called")
        self.initialize_rtms_connection()

        return

    def initialize_rtms_connection(self):
        logger.info("Initializing RTMS connection...")

        # Define recording file path - you may want to customize this
        recording_file_path = self.recording_file_path

        # Construct the command
        cmd_env = {
            "ZM_RTMS_CLIENT": self.zoom_client_id,
            "ZM_RTMS_SECRET": self.zoom_client_secret,
        }

        cmd = ["node", "/home/nduncan/Documents/attendee_stuff/rtms-developer-preview-js/index.js", "--", f"--recording_file_path={recording_file_path}", f"--join_payload={json.dumps(self.zoom_rtms)}"]

        logger.info(f"Executing RTMS client with command: {' '.join(cmd)}")

        try:
            # Start the subprocess with stdin and stdout pipes opened
            process = subprocess.Popen(
                cmd, 
                env=cmd_env, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                stdin=subprocess.PIPE, 
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True
            )

            # You might want to store the process to interact with it later
            self.rtms_process = process

            # Set up stdout monitoring
            self.setup_stdout_monitoring()

            logger.info("RTMS client started successfully")
            self.send_message_callback({"message": self.Messages.APP_SESSION_CONNECTED})
        except Exception as e:
            logger.error(f"Failed to start RTMS client: {e}")

        return

    def setup_stdout_monitoring(self):
        """Set up GLib IO watch to monitor stdout from the RTMS process."""
        if not self.rtms_process or not self.rtms_process.stdout:
            logger.warning("No stdout available to monitor")
            return

        # Get the file descriptor for stdout
        stdout_fd = self.rtms_process.stdout.fileno()
        
        # Set up GLib IO watch
        self.stdout_watch_id = GLib.io_add_watch(
            stdout_fd,
            GLib.IO_IN | GLib.IO_HUP,
            self._on_stdout_data_available
        )
        logger.info("Set up stdout monitoring with GLib IO watch")

    def _on_stdout_data_available(self, fd, condition):
        """Callback called when stdout data is available."""
        if condition & GLib.IO_HUP:
            # Process has closed stdout
            logger.info("RTMS process stdout closed")
            return False  # Remove the watch
        
        if condition & GLib.IO_IN:
            try:
                # Read a line from stdout
                line = self.rtms_process.stdout.readline()
                if line:
                    # Remove trailing newline and call the callback
                    data = line.rstrip('\n')
                    logger.debug(f"Read from RTMS stdout: {data}")
                    # Handle stdout data internally
                    self.on_stdout_data_received(data)
                else:
                    # EOF reached
                    logger.info("RTMS process stdout EOF")
                    return False  # Remove the watch
            except Exception as e:
                logger.error(f"Error reading stdout: {e}")
                return False  # Remove the watch
        
        return True  # Continue watching

    def on_stdout_data_received(self, data):
        """
        Handle stdout data received from the RTMS process.
        Override this method or modify it to handle stdout data as needed.
        
        Args:
            data (str): The stdout line received from the RTMS process
        """
        logger.info(f"RTMS stdout: {data}")
        

    def send_to_rtms_stdin(self, data):
        """
        Send data to the RTMS process via stdin.

        Args:
            data (str): The string data to send to the process

        Returns:
            bool: True if data was sent successfully, False otherwise
        """
        if not hasattr(self, "rtms_process") or not self.rtms_process or self.rtms_process.stdin is None:
            logger.warning("No RTMS process available or stdin not accessible")
            return False

        try:
            # Ensure data ends with newline if needed
            if not data.endswith("\n"):
                data += "\n"

            # Write to stdin
            self.rtms_process.stdin.write(data)
            self.rtms_process.stdin.flush()  # Important to flush the buffer

            logger.info(f"Sent data to RTMS process stdin: {data.strip()}")
            return True
        except Exception as e:
            logger.error(f"Failed to send data to RTMS process stdin: {e}")
            return False

    def send_raw_image(self, png_image_bytes):
        return

    def send_raw_audio(self, bytes, sample_rate):
        return

    def disconnect(self):
        if self.left_meeting:
            return
        logger.info("disconnect called")
        
        self.send_to_rtms_stdin("leave")
        logger.info("sent leave command to RTMS process")
        self.rtms_process.wait(timeout=20)
        logger.info("RTMS process exited")
        self.left_meeting = True
        self.send_message_callback({"message": self.Messages.APP_SESSION_DISCONNECTED})
        return

    def leave(self):
        return self.disconnect()

    def get_first_buffer_timestamp_ms_offset(self):
        return 0

    def check_auto_leave_conditions(self):
        return

    def is_sent_video_still_playing(self):
        return False

    def send_video(self, video_url):
        logger.info(f"send_video called with video_url = {video_url}. This is not supported for zoom")
        return
