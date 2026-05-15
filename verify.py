"""
Verify a CCLD facility license number against the live Transparency API.

Two modes:

  uv run verify.py 13423996                  # one license, pretty-printed
  uv run verify.py licenses.csv --col facnum # batch from CSV, prints TSV to stdout

Pads license numbers to 9 digits (leading zero) before calling the API. Prints
the facility name, type, status, address, and last visit date for matches, or
"NOT FOUND" for unknown license numbers. Stdlib only.

See docs/transparency-api.md for the endpoint surface.
"""

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

API_BASE = "https://www.ccld.dss.ca.gov/transparencyapi/api"
USER_AGENT = "ccld-verify/1.0 (+https://github.com/chekos/ccld-open-data-snapshot)"


def fetch_detail(facnum: str) -> dict | None:
    """Return the FacilityDetail dict, or None if the license isn't on file."""
    padded = facnum.strip().zfill(9)
    url = f"{API_BASE}/FacilityDetail/{padded}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    fd = data.get("FacilityDetail") or {}
    # The API returns 200 + empty record for unknown numbers — detect by empty STATUS.
    if not fd.get("STATUS"):
        return None
    return fd


def one(facnum: str) -> int:
    fd = fetch_detail(facnum)
    if not fd:
        print(f"NOT FOUND  {facnum.zfill(9)}")
        return 1
    print(f"FOUND      {fd['FACILITYNUMBER']}  {fd['FACILITYNAME']!r}")
    print(f"  type     {fd.get('FACILITYTYPE')}")
    print(f"  status   {fd.get('STATUS')}")
    print(f"  address  {fd.get('STREETADDRESS')}, {fd.get('CITY')} {fd.get('ZIPCODE')}  ({fd.get('COUNTY')} County)")
    print(f"  licensee {fd.get('LICENSEENAME')}")
    print(f"  capacity {fd.get('CAPACITY')}")
    print(f"  licensed {fd.get('LICENSEEFFECTIVEDATE')}  (first: {fd.get('LICENSEFIRSTDATE')})")
    if fd.get("DATECLOSED"):
        print(f"  closed   {fd['DATECLOSED']}")
    print(f"  visits   {fd.get('NBRALLVISITS')} total · last {fd.get('LASTVISITDATE')}")
    return 0


def batch(path: Path, col: str, delay: float) -> int:
    rows_in = list(csv.DictReader(path.open(newline="")))
    if not rows_in:
        print("(empty input)", file=sys.stderr)
        return 0
    if col not in rows_in[0]:
        print(f"column {col!r} not in {list(rows_in[0])}", file=sys.stderr)
        return 2

    fields = ["facnum", "found", "facility_name", "facility_type", "status", "city", "zip", "county", "last_visit"]
    writer = csv.DictWriter(sys.stdout, fieldnames=fields, delimiter="\t")
    writer.writeheader()
    for row in rows_in:
        raw = (row.get(col) or "").strip()
        if not raw:
            writer.writerow({"facnum": "", "found": ""})
            continue
        fd = fetch_detail(raw)
        if fd:
            writer.writerow({
                "facnum": raw.zfill(9),
                "found": "yes",
                "facility_name": fd.get("FACILITYNAME"),
                "facility_type": fd.get("FACILITYTYPE"),
                "status": fd.get("STATUS"),
                "city": fd.get("CITY"),
                "zip": fd.get("ZIPCODE"),
                "county": fd.get("COUNTY"),
                "last_visit": fd.get("LASTVISITDATE"),
            })
        else:
            writer.writerow({"facnum": raw.zfill(9), "found": "no"})
        sys.stdout.flush()
        time.sleep(delay)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Verify CCLD facility licenses.")
    p.add_argument("target", help="A single facility number, or a path to a CSV.")
    p.add_argument("--col", default="facnum", help="CSV column with license numbers (default: facnum)")
    p.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help=(
            "Seconds between batch requests (default: 0.5). CCLD publishes no rate "
            "limit; 0.5s is a politeness floor — see docs/transparency-api.md."
        ),
    )
    args = p.parse_args()

    path = Path(args.target)
    if path.exists() and path.is_file():
        return batch(path, args.col, args.delay)
    return one(args.target)


if __name__ == "__main__":
    sys.exit(main())
