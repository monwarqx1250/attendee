import json
import logging
import subprocess

logger = logging.getLogger(__name__)


class ZoomRtmsMeetingAppSessionAdapter:
    def __init__(self, rtms_join_payload: dict, zoom_client_id: str, zoom_client_secret: str, recording_file_location: str):
        self.rtms_join_payload = rtms_join_payload
        self.recording_file_location = recording_file_location

        self.zoom_client_id = zoom_client_id
        self.zoom_client_secret = zoom_client_secret
        self.left_meeting = False

    def init(self):
        logger.info("init called")
        self.initialize_rtms_connection()

        return

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

    def leave(self):
        if self.left_meeting:
            return
        logger.info("leave called")
        self.send_to_rtms_stdin("leave")
        logger.info("sent leave command to RTMS process")
        self.rtms_process.wait(timeout=20)
        logger.info("RTMS process exited")
        self.left_meeting = True
        self.send_message_callback({"message": self.Messages.MEETING_ENDED})
        return

    def initialize_rtms_connection(self):
        logger.info("Initializing RTMS connection...")

        # Construct the command
        cmd_env = {
            "ZM_RTMS_CLIENT": self.zoom_client_id,
            "ZM_RTMS_SECRET": self.zoom_client_secret,
        }

        cmd = ["node", "/home/nduncan/Documents/attendee_stuff/rtms-developer-preview-js/index.js", "--", f"--recording_file_path={self.recording_file_location}", f"--join_payload={json.dumps(self.rtms_join_payload)}"]

        logger.info(f"Executing RTMS client with command: {' '.join(cmd)}")

        try:
            # Start the subprocess with stdin pipe opened
            process = subprocess.Popen(cmd, env=cmd_env, stdout=None, stderr=None, stdin=subprocess.PIPE, text=True)

            # You might want to store the process to interact with it later
            self.rtms_process = process

            logger.info("RTMS client started successfully")
            # self.send_message_callback({"message": self.Messages.BOT_JOINED_MEETING})
        except Exception as e:
            logger.error(f"Failed to start RTMS client: {e}")

        return
