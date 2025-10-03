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
    path("api/<slug:dataset_id>/normalize/", views.start_normalization_api, name="normalize"),
    path("api/<slug:dataset_id>/normalize-cancel/", views.cancel_normalization_api, name="normalize-cancel"),
    path("api/<slug:dataset_id>/analyze/", views.start_analysis_view, name="analyze"),
    path("api/<slug:dataset_id>/analysis-reset/", views.reset_analysis_api, name="analysis-reset"),
    path("api/<slug:dataset_id>/analysis-cancel/", views.cancel_analysis_api, name="analysis-cancel"),
    path("<slug:dataset_id>/", views.dataset_detail, name="detail"),
    path("<slug:dataset_id>/status/", views.status_api, name="status-legacy"),
    path("<slug:dataset_id>/normalize/", views.start_normalization_api, name="normalize-legacy"),
    path("<slug:dataset_id>/analyze/", views.start_analysis_view, name="analyze-legacy"),
    path("<slug:dataset_id>/analysis-reset/", views.reset_analysis_api, name="analysis-reset-legacy"),
    path("<slug:dataset_id>/analysis-cancel/", views.cancel_analysis_api, name="analysis-cancel-legacy"),
]
