# ccld-open-data-snapshot

Automated tracker for **CCLD (Community Care Licensing Division)** provider licensing
data for **Alameda County**, updated every 2 days via
[git scraping](https://simonwillison.net/2020/Oct/9/git-scraping/).

## What this tracks

This repository automatically downloads child care provider licensing records from
the [CHHS Open Data Portal](https://data.chhs.ca.gov/dataset/ccl-facilities) (CKAN dataset `46ffcbdf-4874-4cc1-92c2-fb715e3ad014`) and commits the results to
git. By diffing commits you can see exactly which providers were added, removed, or
changed their status over time.

Two datasets are tracked:

| File | Description | CKAN Resource ID |
|------|-------------|-----------------|
| `data/centers.csv` | Child Care Centers | `7aed8063-cea7-4367-8651-c81643164ae0` |
| `data/homes.csv` | Family Child Care Homes | `4b5cc48d-03b1-4f42-a7d1-b9816903eb2b` |

Both datasets are filtered to **Alameda County** and sourced from the California
Department of Social Services (CDSS).

## How to see what changed

Browse the git commit history to see every data update:

```bash
git log --oneline data/
```

To see the diff for a specific commit:

```bash
git show <commit-sha>
```

Or compare two dates:

```bash
git diff HEAD~1 HEAD -- data/centers.csv
```

## Data license

The data is published under a **CC-BY** license by the California Department of Social
Services. Attribution: CDSS / data.chhs.ca.gov.

## Running manually

```bash
python scrape.py
```

No dependencies beyond Python's standard library.

## Per-facility detail: the Transparency API

The open data CSVs are the bulk inventory. For richer per-facility data — capacity
overrides, license dates, visit counts, complaint outcomes, evaluation reports —
CCLD exposes an undocumented JSON API at
`https://www.ccld.dss.ca.gov/transparencyapi/api/`. The Care Facility Search SPA
uses it; nothing else has to.

See [`docs/transparency-api.md`](docs/transparency-api.md) for the full endpoint
surface, response shapes, and gotchas.

Quickest demonstration:

```bash
uv run verify.py 13423996
# FOUND      013423996  'JOHNSON III, JOHNNY'
#   type     FAMILY DAY CARE HOME
#   status   Licensed
#   ...
```

Batch mode reads license numbers from a CSV column and writes TSV to stdout:

```bash
uv run verify.py licenses.csv --col facnum > verified.tsv
```

The endpoint surface was reverse-engineered in May 2026 while reconciling First 5
R&R intake data. Re-run `scripts/discover_endpoints.py` after CCLD SPA upgrades to
confirm the documented endpoints still exist.

## Repo structure

```
.
├── .github/workflows/scrape.yml   # Scheduled GitHub Actions workflow
├── data/
│   ├── centers.csv                # Child Care Centers (Alameda County)
│   ├── homes.csv                  # Family Child Care Homes (Alameda County)
│   └── metadata.json              # Scrape timestamp, row counts, file_date
├── docs/
│   └── transparency-api.md        # Reverse-engineered per-facility JSON API
├── scripts/
│   └── discover_endpoints.py      # Playwright-based API rediscovery tool
├── scrape.py                      # Bulk CKAN scraper (stdlib only)
├── verify.py                      # Per-facility verifier using transparencyapi
└── README.md
```

## How it works

A [GitHub Actions](https://docs.github.com/en/actions) workflow runs `scrape.py` every
2 days at 06:00 UTC. The script:

1. Fetches all records from each CKAN datastore resource (paginating with `limit=5000`)
2. Normalizes the publisher's zero-padded integers and M/D/YYYY dates to unpadded
   integers and ISO timestamps, then saves sorted, deterministically ordered CSV files
   so diffs only show real changes
3. Writes a `data/metadata.json` with the scrape timestamp, row counts, and CDSS extract
   date (`file_date`)
4. Commits and pushes **only if the data changed**

This technique was popularized by Simon Willison —
see [Git scraping: track changes over time by scraping to a Git repository](https://simonwillison.net/2020/Oct/9/git-scraping/).
