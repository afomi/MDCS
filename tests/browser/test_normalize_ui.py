from __future__ import annotations

import os
import time
import unittest
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.contrib.staticfiles.testing import StaticLiveServerTestCase
from django.urls import reverse

from dataset_imports.catalog import ArtifactDefinition, DatasetDefinition
from dataset_imports.models import DatasetImport, DatasetArtifact

from .driver import create_webdriver
try:
    from selenium import webdriver as _wd  # type: ignore
    SELENIUM_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    SELENIUM_AVAILABLE = False


@unittest.skipUnless(SELENIUM_AVAILABLE, "Selenium/WebDriver not available")
@override_settings(ALLOWED_HOSTS=["*"])
class NormalizeUiTests(StaticLiveServerTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.browser = create_webdriver()
        if cls.browser is None:
            raise unittest.SkipTest("No suitable WebDriver available")

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if cls.browser:
                cls.browser.quit()
        finally:
            super().tearDownClass()

    @patch("dataset_imports.views.get_dataset")
    def test_oasst1_page_shows_normalize_controls(self, mock_get_dataset):
        # Arrange a dataset def and an import with one downloaded artifact
        dataset_id = "oasst1"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="OpenAssistant Conversations (OASST1)",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="oasst1/messages.jsonl.gz",
                    download_url="https://example.com/oasst1/messages.jsonl.gz",
                )
            ],
            supported=True,
        )
        mock_get_dataset.return_value = dataset_def

        # Create a staff user for auth-protected controls (button enabled)
        User = get_user_model()
        user = User.objects.create_user(username="staff", password="pass", is_staff=True)
        self.client.force_login(user)

        import_obj = DatasetImport.objects.create(
            slug="oasst1-import",
            display_name=dataset_def.name,
            adapter=dataset_def.adapter,
            status=DatasetImport.Status.DOWNLOADED,
            notes={"dataset_id": dataset_id, "selected_artifacts": [dataset_def.artifacts[0].filename]},
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename=dataset_def.artifacts[0].filename,
            download_url=dataset_def.artifacts[0].download_url,
            status=DatasetArtifact.Status.DOWNLOADED,
            downloaded_bytes=1024,
            size_bytes=1024,
        )

        # Build URL and navigate with Selenium
        url = f"{self.live_server_url}{reverse('dataset_imports:detail', args=[dataset_id])}"
        # Carry over session cookies for authenticated view
        cookie = self.client.cookies.get("sessionid")
        if cookie and self.browser:
            self.browser.get(self.live_server_url + "/static/robots.txt")  # warm domain
            self.browser.add_cookie({"name": "sessionid", "value": cookie.value, "path": "/"})

        self.browser.get(url)

        # Assert the Normalize card is visible and button is enabled
        btn = self.browser.find_element("id", "start-normalization-btn")
        self.assertIsNotNone(btn)
        self.assertEqual(btn.get_attribute("disabled"), None)

        # Optional: assert the progress labels exist
        status_tag = self.browser.find_element("id", "normalize-status")
        self.assertIsNotNone(status_tag)
