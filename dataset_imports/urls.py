from __future__ import annotations

from django.urls import path

from . import views

app_name = "dataset_imports"

urlpatterns = [
    path("", views.catalog_view, name="catalog"),
    path("explore/", views.explore_view, name="explore"),
    path("api/<slug:dataset_id>/status/", views.status_api, name="status"),
    path("api/<slug:dataset_id>/stream/", views.status_stream, name="status-stream"),
    path("api/<slug:dataset_id>/start/", views.start_import_api, name="start"),
    path("api/<slug:dataset_id>/reset/", views.reset_import_api, name="reset"),
    path("api/<slug:dataset_id>/analyze/", views.start_analysis_view, name="analyze"),
    path("<slug:dataset_id>/", views.dataset_detail, name="detail"),
    path("<slug:dataset_id>/status/", views.status_api, name="status-legacy"),
    path("<slug:dataset_id>/analyze/", views.start_analysis_view, name="analyze-legacy"),
]
