"""
Scrape CCLD (Community Care Licensing Division) data from data.ca.gov
for Alameda County and save as CSV files with metadata.

Data sources:
  - Child Care Centers: resource ID 5bac6551-4d6c-45d6-93b8-e6ded428d98e
  - Family Child Care Homes: resource ID a8615948-c56f-4dba-90f5-5f802490a221

Uses only Python stdlib: urllib, json, csv, datetime, pathlib, time.
"""

import csv
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 5  # seconds; doubles on each retry

BASE_URL = "https://data.ca.gov/api/3/action/datastore_search"
COUNTY_FILTER = {"county_name": "ALAMEDA"}
LIMIT = 5000

RESOURCES = {
    "centers": "5bac6551-4d6c-45d6-93b8-e6ded428d98e",
    "homes": "a8615948-c56f-4dba-90f5-5f802490a221",
}

DATA_DIR = Path(__file__).parent / "data"


def fetch_url(req: urllib.request.Request) -> dict:
    """Fetch a URL with retry logic for transient failures."""
    delay = RETRY_BACKOFF
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError) as exc:
            if attempt == RETRY_ATTEMPTS:
                raise
            print(f"  Request failed (attempt {attempt}/{RETRY_ATTEMPTS}): {exc}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2


def fetch_all_rows(resource_id: str) -> list[dict]:
    """Fetch all rows for the given resource ID filtered to Alameda County."""
    rows = []
    offset = 0
    total = None

    while total is None or offset < total:
        params = {
            "resource_id": resource_id,
            "filters": json.dumps(COUNTY_FILTER),
            "limit": str(LIMIT),
            "offset": str(offset),
        }
        url = BASE_URL + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url, headers={"User-Agent": "ccld-open-data-tracker/1.0"}
        )
        data = fetch_url(req)

        if not data.get("success"):
            raise RuntimeError(f"API error for resource {resource_id}: {data}")

        result = data["result"]
        if total is None:
            total = result["total"]
            print(f"  Total rows: {total}")

        batch = result["records"]
        rows.extend(batch)
        offset += len(batch)
        print(f"  Fetched {offset}/{total} rows...")

        if not batch:
            break

    return rows


def parse_file_date(raw: object) -> str:
    """
    Parse file_date field encoded as integer MMDDYYYY (e.g. 5252025 -> 2025-05-25).
    Returns an ISO date string YYYY-MM-DD, or the raw string if parsing fails.
    """
    s = str(raw).strip()
    if not s or s == "None":
        return ""
    # Zero-pad to 8 characters: MMDDYYYY
    s = s.zfill(8)
    try:
        dt = datetime.strptime(s, "%m%d%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return s


def get_file_date(rows: list[dict]) -> str:
    """Extract and parse file_date from the first row that has it."""
    for row in rows:
        raw = row.get("file_date")
        if raw is not None:
            return parse_file_date(raw)
    return ""


def save_csv(rows: list[dict], path: Path, sort_field: str = "facility_number") -> None:
    """Save rows to CSV sorted by sort_field, columns alphabetical, excluding _id."""
    if not rows:
        path.write_text("")
        return

    # Determine columns: alphabetical, excluding _id
    all_fields = set()
    for row in rows:
        all_fields.update(row.keys())
    all_fields.discard("_id")
    columns = sorted(all_fields)

    # Sort rows by sort_field (as string for stability)
    rows_sorted = sorted(rows, key=lambda r: str(r.get(sort_field, "")))

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=columns,
            quoting=csv.QUOTE_ALL,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows_sorted)


def load_existing_counts(metadata_path: Path) -> dict:
    """Load existing counts from metadata.json if it exists."""
    if metadata_path.exists():
        try:
            return json.loads(metadata_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, KeyError):
            pass
    return {}


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    metadata_path = DATA_DIR / "metadata.json"

    # Load existing metadata for comparison
    existing = load_existing_counts(metadata_path)
    existing_centers = existing.get("centers_total")
    existing_homes = existing.get("homes_total")

    print("Fetching Child Care Centers...")
    center_rows = fetch_all_rows(RESOURCES["centers"])

    print("Fetching Family Child Care Homes...")
    home_rows = fetch_all_rows(RESOURCES["homes"])

    centers_total = len(center_rows)
    homes_total = len(home_rows)

    # Get file_date from either dataset
    file_date = get_file_date(center_rows) or get_file_date(home_rows)

    print(f"\nSaving data/centers.csv ({centers_total} rows)...")
    save_csv(center_rows, DATA_DIR / "centers.csv")

    print(f"Saving data/homes.csv ({homes_total} rows)...")
    save_csv(home_rows, DATA_DIR / "homes.csv")

    # Save metadata
    metadata = {
        "scrape_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "centers_total": centers_total,
        "homes_total": homes_total,
        "file_date": file_date,
    }
    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print("Saved data/metadata.json")

    # Print summary
    print("\n--- Summary ---")
    print(f"file_date : {file_date}")
    print(f"centers   : {centers_total}", end="")
    if existing_centers is not None:
        diff = centers_total - existing_centers
        print(f" (was {existing_centers}, change: {diff:+d})", end="")
    print()
    print(f"homes     : {homes_total}", end="")
    if existing_homes is not None:
        diff = homes_total - existing_homes
        print(f" (was {existing_homes}, change: {diff:+d})", end="")
    print()

    if existing_centers is None and existing_homes is None:
        print("(No previous data to compare against — first run)")
    elif centers_total == existing_centers and homes_total == existing_homes:
        print("Counts unchanged since last scrape.")
    else:
        print("Counts changed since last scrape!")


if __name__ == "__main__":
    main()
