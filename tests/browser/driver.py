import os
import contextlib

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
except Exception:  # pragma: no cover - optional dependency
    webdriver = None  # type: ignore
    ChromeOptions = None  # type: ignore
    FirefoxOptions = None  # type: ignore


def create_webdriver():
    """Create a headless WebDriver from environment variables.

    Env vars:
      - SELENIUM_BROWSER: one of 'chrome' or 'firefox' (default: chrome)
      - SELENIUM_DRIVER_PATH: path to chromedriver/geckodriver (optional if in PATH)
    """

    if webdriver is None:
        return None

    browser = (os.environ.get("SELENIUM_BROWSER") or "chrome").strip().lower()
    driver_path = (os.environ.get("SELENIUM_DRIVER_PATH") or "").strip() or None

    if browser == "firefox":
        if FirefoxOptions is None:
            return None
        options = FirefoxOptions()
        options.add_argument("-headless")
        with contextlib.suppress(Exception):
            options.set_preference("dom.webnotifications.enabled", False)
        if driver_path:
            return webdriver.Firefox(options=options, executable_path=driver_path)
        return webdriver.Firefox(options=options)

    # default to chrome
    if ChromeOptions is None:
        return None
    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,900")
    if driver_path:
        return webdriver.Chrome(options=options, executable_path=driver_path)
    return webdriver.Chrome(options=options)

