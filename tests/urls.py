""" Test URL router that mounts the dataset_imports app for browser specs. """

from django.urls import include, path

urlpatterns = [
    path("datasets/", include("dataset_imports.urls")),
]
