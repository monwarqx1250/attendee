from django.urls import path

from . import meeting_app_sessions_api_views

urlpatterns = [
    path("meeting_app_sessions", meeting_app_sessions_api_views.MeetingAppSessionCreateView.as_view(), name="meeting_app_session-create"),
    path("meeting_app_sessions/end", meeting_app_sessions_api_views.MeetingAppSessionEndView.as_view(), name="meeting_app_session-end"),
    path(
        "meeting_app_sessions/<str:object_id>",
        meeting_app_sessions_api_views.MeetingAppSessionDetailView.as_view(),
        name="meeting-app-session-detail",
    ),
]

# catch any other paths and return a 404 json response - must be last
urlpatterns += [path("<path:any>", meeting_app_sessions_api_views.NotFoundView.as_view())]
