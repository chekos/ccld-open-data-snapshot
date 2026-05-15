"""
Re-discover the CCLD Transparency API endpoint surface.

Drives the public Care Facility Search SPA with Playwright, captures every JSON
request the Angular app makes, and prints the resulting endpoint list. Run this
when CCLD ships a new version of the SPA to confirm the documented endpoints in
docs/transparency-api.md still exist.

Usage:
    uv run --with playwright python scripts/discover_endpoints.py

Requires Playwright + Chromium:
    uv tool install playwright
    playwright install chromium
"""

import time
import urllib.parse
from collections import defaultdict

from playwright.sync_api import sync_playwright


HOME = "https://www.ccld.dss.ca.gov/carefacilitysearch/"


def main() -> None:
    api_calls: dict[str, list[str]] = defaultdict(list)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        def record(req):
            if "transparencyapi" in req.url:
                # Strip query string and record path + sample full URL
                parsed = urllib.parse.urlparse(req.url)
                api_calls[parsed.path].append(req.url)

        page.on("request", record)

        # 1. Load home — triggers Group/, CACounty, Announcement, FAQ
        page.goto(HOME, wait_until="networkidle", timeout=30000)
        time.sleep(2)

        # 2. License-number search — triggers a redirect to /FacDetail/{padded}
        page.locator("input[placeholder*='9 digits']").fill("013423996")
        for form in page.locator("form").all():
            txt = form.locator("input[type='text']").first
            if txt.count() and "9 digits" in (txt.get_attribute("placeholder") or ""):
                form.locator("input[type='submit']").click()
                break
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        time.sleep(3)

        # 3. Search via the facility-type advanced form — triggers FacilitySearch
        page.goto(HOME, wait_until="networkidle", timeout=30000)
        time.sleep(2)
        try:
            page.locator("text='Child Care'").first.click(timeout=5000)
            time.sleep(2)
            page.locator("input[type='submit'], button:has-text('Search')").first.click(timeout=5000)
            time.sleep(3)
        except Exception as exc:
            print(f"(advanced search probe failed: {exc})")

        # 4. Licensee-name search
        page.goto(HOME, wait_until="networkidle", timeout=30000)
        time.sleep(2)
        page.locator("input[placeholder*='Licensee Name']").fill("JOHNSON III, JOHNNY")
        for form in page.locator("form").all():
            txt = form.locator("input[type='text']").first
            if txt.count() and "Licensee Name" in (txt.get_attribute("placeholder") or ""):
                form.locator("input[type='submit']").click()
                break
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        time.sleep(3)

        browser.close()

    if not api_calls:
        print("No /transparencyapi/api/ calls captured. The endpoint base may have moved.")
        return

    print(f"Captured calls to {len(api_calls)} distinct endpoint paths:\n")
    for path in sorted(api_calls):
        sample = api_calls[path][0]
        print(f"  {path}")
        print(f"    sample: {sample}")


if __name__ == "__main__":
    main()
