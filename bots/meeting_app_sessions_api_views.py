import logging

from drf_spectacular.utils import (
    OpenApiExample,
    OpenApiParameter,
    OpenApiResponse,
    extend_schema,
)
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .authentication import ApiKeyAuthentication
from .launch_meeting_app_session_utils import launch_meeting_app_session
from .meeting_app_sessions_api_utils import create_meeting_app_session
from .models import MeetingAppSession
from .serializers import CreateMeetingAppSessionSerializer, MeetingAppSessionSerializer

logger = logging.getLogger(__name__)

TokenHeaderParameter = [
    OpenApiParameter(
        name="Authorization",
        type=str,
        location=OpenApiParameter.HEADER,
        description="API key for authentication",
        required=True,
        default="Token YOUR_API_KEY_HERE",
    ),
    OpenApiParameter(
        name="Content-Type",
        type=str,
        location=OpenApiParameter.HEADER,
        description="Should always be application/json",
        required=True,
        default="application/json",
    ),
]

NewMeetingAppSessionExample = OpenApiExample(
    "New Meeting App Session",
    value={
        "id": "mas_xxxxxxxxxxx",
        "state": "ready",
        "zoom_rtms": {"meeting_uuid": "abc123def456", "rtms_stream_id": "stream_789", "server_urls": ["wss://rtms1.zoom.us", "wss://rtms2.zoom.us"]},
        "transcription_settings": {"deepgram": {"language": "multi"}},
        "recording_settings": {"format": "mp4", "view": "speaker_view", "resolution": "1080p"},
        "created_at": "2024-01-18T12:34:56Z",
        "updated_at": "2024-01-18T12:34:56Z",
    },
    description="Example response when creating a new meeting app session",
)


@extend_schema(exclude=True)
class NotFoundView(APIView):
    def get(self, request, *args, **kwargs):
        return self.handle_request(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.handle_request(request, *args, **kwargs)

    def put(self, request, *args, **kwargs):
        return self.handle_request(request, *args, **kwargs)

    def patch(self, request, *args, **kwargs):
        return self.handle_request(request, *args, **kwargs)

    def delete(self, request, *args, **kwargs):
        return self.handle_request(request, *args, **kwargs)

    def handle_request(self, request, *args, **kwargs):
        return Response({"error": "Not found"}, status=status.HTTP_404_NOT_FOUND)


class MeetingAppSessionCreateView(APIView):
    authentication_classes = [ApiKeyAuthentication]

    @extend_schema(
        operation_id="Create Meeting App Session",
        summary="Create a new meeting app session",
        description="Creates a new meeting app session with the specified settings.",
        request=CreateMeetingAppSessionSerializer,
        responses={
            201: OpenApiResponse(
                response=MeetingAppSessionSerializer,
                description="Meeting app session created successfully",
                examples=[NewMeetingAppSessionExample],
            ),
            400: OpenApiResponse(description="Invalid input"),
        },
        parameters=TokenHeaderParameter,
        tags=["Meeting App Sessions"],
    )
    def post(self, request):
        try:
            project = request.auth.project

            meeting_app_session, error = create_meeting_app_session(request.data, project)

            if error:
                return Response(error, status=status.HTTP_400_BAD_REQUEST)

            launch_meeting_app_session(meeting_app_session)

            return Response(MeetingAppSessionSerializer(meeting_app_session).data, status=status.HTTP_201_CREATED)

        except Exception as e:
            logging.error(f"Error creating meeting app session: {str(e)}")
            return Response(
                {"error": "Failed to create meeting app session"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class MeetingAppSessionDetailView(APIView):
    authentication_classes = [ApiKeyAuthentication]

    @extend_schema(
        operation_id="Get Meeting App Session",
        summary="Get the details for a meeting app session",
        responses={
            200: OpenApiResponse(
                response=MeetingAppSessionSerializer,
                description="Meeting app session details",
                examples=[NewMeetingAppSessionExample],
            ),
            404: OpenApiResponse(description="Meeting app session not found"),
        },
        parameters=[
            *TokenHeaderParameter,
            OpenApiParameter(
                name="object_id",
                type=str,
                location=OpenApiParameter.PATH,
                description="Meeting App Session ID",
                examples=[OpenApiExample("Meeting App Session ID Example", value="mas_xxxxxxxxxxx")],
            ),
        ],
        tags=["Meeting App Sessions"],
    )
    def get(self, request, object_id):
        try:
            session = MeetingAppSession.objects.get(object_id=object_id)
            return Response(MeetingAppSessionSerializer(session).data)

        except MeetingAppSession.DoesNotExist:
            return Response({"error": "Meeting app session not found"}, status=status.HTTP_404_NOT_FOUND)
