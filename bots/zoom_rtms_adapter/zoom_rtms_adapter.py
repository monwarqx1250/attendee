import json
import subprocess
import time
import threading
import re
from pathlib import Path

import gi

from bots.bot_adapter import BotAdapter

gi.require_version("GLib", "2.0")
from gi.repository import GLib
import logging

from bots.automatic_leave_configuration import AutomaticLeaveConfiguration
from bots.models import ParticipantEventTypes

logger = logging.getLogger(__name__)

# Regex patterns for chunk files (same as simulate_rtms_stream.py)
VIDEO_RE = re.compile(r"video-(\d+)\.raw$")
AUDIO_RE = re.compile(r"audio-(\d+)\.raw$")

# Audio constants (same as simulate_rtms_stream.py)
AUDIO_RATE = 16000
AUDIO_CHANNELS = 1
AUDIO_BYTES_PER_SAMPLE = 2  # s16le


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
        upsert_caption_callback,
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
        self.upsert_caption_callback = upsert_caption_callback

        # Stdout IO watch
        self.stdout_watch_id = None

        self.first_buffer_timestamp_ms = None

        self.rtms_process = None

        # Simulation variables
        self.simulation_running = False
        self.simulation_threads = []
        self.simulation_start_time_ns = None

    def cleanup(self):
        logger.info("cleanup called")
        self.cleaned_up = True
        
        # Stop simulation
        self.simulation_running = False
        for thread in self.simulation_threads:
            if thread.is_alive():
                thread.join(timeout=1.0)
        
        # Remove stdout IO watch
        if self.stdout_watch_id:
            GLib.source_remove(self.stdout_watch_id)
            self.stdout_watch_id = None

    def init(self):
        logger.info("init called")
        self.initialize_rtms_connection()

        return

    def discover_chunks(self, indir: Path):
        """Discover video and audio chunk files in the directory"""
        vids, auds = [], []
        if not indir.is_dir():
            logger.warning(f"Simulation directory not found: {indir}")
            return vids, auds
            
        for p in indir.iterdir():
            m = VIDEO_RE.match(p.name)
            if m:
                ts = int(m.group(1))
                vids.append((ts, p))
                continue
            m = AUDIO_RE.match(p.name)
            if m:
                ts = int(m.group(1))
                auds.append((ts, p))
        vids.sort(key=lambda x: x[0])
        auds.sort(key=lambda x: x[0])
        return vids, auds

    def simulate_video_stream(self, video_chunks, pace=1.0):
        """Simulate video streaming by reading chunk files and calling the video callback"""
        logger.info(f"Starting video simulation with {len(video_chunks)} chunks")
        
        for idx, (ts_ms, path) in enumerate(video_chunks):
            if not self.simulation_running:
                break
                
            try:
                with path.open("rb") as f:
                    data = f.read()
                
                # Calculate timing for pacing
                if pace > 0 and self.simulation_start_time_ns:
                    pts_ns = ts_ms * 1_000_000  # ms to ns
                    target_wall_ns = self.simulation_start_time_ns + int(pts_ns * (1.0 / pace))
                    
                    while self.simulation_running:
                        now_ns = time.monotonic_ns()
                        if now_ns >= target_wall_ns:
                            break
                        time.sleep(0.001)
                        print(f"Waiting for video chunk {idx+1}/{len(video_chunks)} (ts={ts_ms}ms, size={len(data)} bytes)")
                
                if not self.simulation_running:
                    break
                    
                # Send video frame via callback
                if self.add_video_frame_callback and self.wants_any_video_frames_callback():
                    current_time_ns = time.time_ns()
                    print(f"Sending video chunk {idx+1}/{len(video_chunks)} (ts={ts_ms}ms, size={len(data)} bytes)")
                    self.add_video_frame_callback(data, current_time_ns)
                    
                print(f"Sent video chunk {idx+1}/{len(video_chunks)} (ts={ts_ms}ms, size={len(data)} bytes)")
                
            except Exception as e:
                logger.error(f"Error processing video chunk {path}: {e}")
                
        logger.info("Video simulation completed")

    def simulate_audio_stream(self, audio_chunks, pace=1.0):
        """Simulate audio streaming by reading chunk files and calling the audio callback"""
        logger.info(f"Starting audio simulation with {len(audio_chunks)} chunks")
        
        for idx, (ts_ms, path) in enumerate(audio_chunks):
            if not self.simulation_running:
                break
                
            try:
                with path.open("rb") as f:
                    data = f.read()
                
                # Calculate timing for pacing
                if pace > 0 and self.simulation_start_time_ns:
                    pts_ns = ts_ms * 1_000_000  # ms to ns
                    target_wall_ns = self.simulation_start_time_ns + int(pts_ns * (1.0 / pace))
                    
                    while self.simulation_running:
                        now_ns = time.monotonic_ns()
                        if now_ns >= target_wall_ns:
                            break
                        time.sleep(0.001)
                
                if not self.simulation_running:
                    break
                    
                # Send audio chunk via callback
                if self.add_mixed_audio_chunk_callback:
                    self.add_mixed_audio_chunk_callback(data)
                    
                logger.debug(f"Sent audio chunk {idx+1}/{len(audio_chunks)} (ts={ts_ms}ms, size={len(data)} bytes)")
                
            except Exception as e:
                logger.error(f"Error processing audio chunk {path}: {e}")
                
        logger.info("Audio simulation completed")

    def start_simulation(self, simulation_dir, pace=1.0):
        """Start the RTMS simulation using chunk files"""
        simulation_path = Path(simulation_dir)
        if not simulation_path.is_dir():
            logger.error(f"Simulation directory not found: {simulation_path}")
            return
            
        video_chunks, audio_chunks = self.discover_chunks(simulation_path)
        
        if not video_chunks and not audio_chunks:
            logger.warning("No simulation chunks found")
            return
            
        logger.info(f"Found {len(video_chunks)} video chunks and {len(audio_chunks)} audio chunks")
        
        self.simulation_running = True
        self.simulation_start_time_ns = time.monotonic_ns()
        
        # Start video simulation thread
        if video_chunks and self.use_video:
            video_thread = threading.Thread(
                target=self.simulate_video_stream,
                args=(video_chunks, pace),
                daemon=True,
                name="rtms-video-sim"
            )
            video_thread.start()
            self.simulation_threads.append(video_thread)
            
        # Start audio simulation thread  
        if audio_chunks and self.use_mixed_audio:
            audio_thread = threading.Thread(
                target=self.simulate_audio_stream,
                args=(audio_chunks, pace),
                daemon=True,
                name="rtms-audio-sim"
            )
            audio_thread.start()
            self.simulation_threads.append(audio_thread)

    def initialize_rtms_connection(self):
        logger.info("Initializing RTMS connection...")

        # Define recording file path - you may want to customize this
        recording_file_path = self.recording_file_path + ".temp"

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
            
            # Start simulation (temporary code for testing)
            simulation_dir = "temp_rtms_output_gold"  # Assume chunks are in a directory
            if Path(simulation_dir).is_dir():
                logger.info(f"Starting RTMS simulation from {simulation_dir}")
                self.start_simulation(simulation_dir, pace=1.0)
            else:
                logger.info(f"No simulation directory found at {simulation_dir}, skipping simulation")
                
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
        if data.startswith("rtmsdata."):
            self.handle_rtms_json_message(data[9:])

    def get_participant(self, participant_id):
        return self._participant_cache.get(participant_id)
    
    def handle_rtms_json_message(self, json_data):
        logger.info(f"handle_rtms_json_message called with json_data: {json_data}")
        json_data = json.loads(json_data)
        if json_data.get("type") == "userUpdate":
            logger.info(f"RTMS userUpdate: {json_data}")
            # {'op': 0, 'user': {'id': 16778240, 'name': 'Noah Duncan'}, 'type': 'userUpdate'}
            user_id = json_data.get("user").get("id")
            user_name = json_data.get("user").get("name")
            
            self._participant_cache[user_id] = {
                "participant_uuid": user_id,
                "participant_user_uuid": None,
                "participant_full_name": user_name,
                "participant_is_the_bot": False,
            }

            self.add_participant_event_callback(
                {
                    "participant_uuid": user_id, 
                    "event_type": ParticipantEventTypes.JOIN if json_data.get("op") == 0 else ParticipantEventTypes.LEAVE, 
                    "event_data": {}, 
                    "timestamp_ms": int(time.time() * 1000)
                }
            )
        elif json_data.get("type") == "transcriptUpdate":
            logger.info(f"RTMS transcriptUpdate: {json_data}")
            # {'user': {'userId': 16778240, 'name': 'Noah Duncan'}, 'text': 'Hello, how are you?', 'type': 'transcriptUpdate'}
            
            itemConverted = {
                "deviceId": json_data.get("user").get("userId"),
                "captionId": json_data.get("id"),
                "text": json_data.get("text"),
                "isFinal": True
            };

            self.upsert_caption_callback(itemConverted)
        
        elif json_data.get("type") == "firstVideoFrameReceived":
            self.first_buffer_timestamp_ms = time.time() * 1000
    
        

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

    def get_first_buffer_timestamp_ms(self):
        return self.first_buffer_timestamp_ms

    def check_auto_leave_conditions(self):
        return

    def is_sent_video_still_playing(self):
        return False

    def send_video(self, video_url):
        logger.info(f"send_video called with video_url = {video_url}. This is not supported for zoom")
        return

    def get_first_buffer_timestamp_ms_offset(self):
        return 0