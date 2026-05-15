# CCLD Transparency API — Undocumented endpoints

The CCLD Care Facility Search at <https://www.ccld.dss.ca.gov/carefacilitysearch/>
is an AngularJS SPA. It calls a JSON HTTP API at:

```
https://www.ccld.dss.ca.gov/transparencyapi/api/
```

The state of California has never published this API, but it returns JSON, ignores
referer/origin checks, and works with plain `curl` / `urllib`. This document captures
everything that was reverse-engineered while reconciling First 5 R&R intake data
against the canonical CCLD record (May 2026).

## Why this matters

The [data.ca.gov CKAN open data](https://data.ca.gov/) feed (used by `scrape.py` in this
repo) is the **bulk inventory**: one row per facility, ~2,000 facilities for Alameda
County, refreshed by CDSS roughly monthly.

The Transparency API is the **per-facility detail layer**:

- Capacity, age ranges served, hours of operation, district office contacts
- License effective date, first license date, closed date
- Visit counts broken down by type (complaint / inspection / other) and outcome
  (substantiated / unsubstantiated / inconclusive / unfounded)
- Type A and Type B citation counts
- Full evaluation reports (PDFs)
- Per-complaint records (`COMPLAINTARRAY`)
- Site-specific comments from the regional office (e.g., "MAX. CAP: 6 - NO MORE THAN
  3 INFANTS OR 4 INFANTS ONLY")

None of that is in the open data export.

## Authentication

None. No API key, no token, no cookie. Just a `User-Agent` header is enough.

## Rate limiting

CCLD doesn't publish a limit and doesn't return `X-RateLimit-*` headers. During the
May 2026 reverse-engineering session, sub-second back-to-back requests succeeded but
the underlying server is slow: the SPA-driven flow on `secure.dss.ca.gov/CareFacilitySearch/`
timed out at 60s under Playwright `domcontentloaded`, while plain
`urllib`/`curl` against `transparencyapi/api/` resolved in ~1–2s consistently. The
`verify.py` batch loop defaults to a `--delay 0.5` (one request every 500ms) on the
principle of *don't be the one who breaks this for everybody else*. Tune up for
small batches; leave it alone for batches over a few hundred.

## Facility number encoding

Every endpoint that takes a facility number expects it **left-padded to 9 digits with
a leading zero**:

| As-stored | API path |
|-----------|----------|
| `13423996` | `/FacilityDetail/013423996` |
| `15700561` | `/FacilityDetail/015700561` |

The first digit encodes facility type (e.g., `0` for child care). The CDSS R&R lists
typically store the 8-digit form; pad before querying.

## Endpoints

### `GET /FacilityDetail/{padded_facnum}`

Full record for one facility. Returns `{FacilityDetail: {…}, TSO: {…}}`.

`FacilityDetail` fields observed (Alameda child care):

| Field | Notes |
|-------|-------|
| `FACILITYNAME` | Display name (provider's name for FCCs) |
| `FACILITYNUMBER` | 9-digit padded license # |
| `FACILITYTYPE` | E.g. `FAMILY DAY CARE HOME`, `DAY CARE CENTER` |
| `STATUS` | `Licensed`, `Closed, Licensee Initiated`, `Probation`, etc. |
| `LICENSEENAME` | Legal licensee (may differ from FACILITYNAME) |
| `CONTACT` | Primary contact person |
| `STREETADDRESS`, `CITY`, `STATE`, `ZIPCODE`, `COUNTY` | Location |
| `TELEPHONE` | Facility phone |
| `CAPACITY` | Licensed capacity |
| `CLIENTSERVED1`..`CLIENTSERVED6` | Client-type codes (e.g. `960 - CHILDREN / INFANT`) |
| `COMMENTS`, `COMMENTS2` | District office free-text notes — capacity overrides, age splits, hours |
| `LICENSEEFFECTIVEDATE` | Current license start |
| `LICENSEFIRSTDATE` | First time this license number was issued |
| `DATECLOSED` | Empty if active |
| `LASTVISITDATE` | Most recent on-site visit |
| `NBRALLVISITS` / `NBRCMPLTVISITS` / `NBRINSPVISITS` / `NBROTHERVISITS` | Visit counts by type |
| `NBRCMPLTTYPA` / `NBRCMPLTTYPB` | Citation severity counts from complaint visits |
| `NBRINSPTYPA` / `NBRINSPTYPB` | Citation severity counts from inspection visits |
| `NBRCMPLTSUB` / `NBRCMPLTUNS` / `NBRCMPLTINC` / `NBRCMPLTUNF` | Complaint outcomes |
| `VSTDATEALL` / `VSTDATECMPLT` / `VSTDATEINSP` / `VSTDATEOTHER` | Most-recent date by visit type |
| `DISTRICTOFFICE`, `DOADDRESS`, `DOCITY`, `DOSTATE`, `DOZIPCODE`, `DOTELEPHONE` | Regional office contact |
| `CMPCOUNT`, `COMPLAINTARRAY` | Itemized complaint records (empty array if none) |
| `TOTCMPVISITS`, `TOTSUBALG`, `TOTINCALG`, `TOTUNSALG`, `TOTUNFALG`, `TOTTYPEA`, `TOTTYPEB` | Lifetime aggregates |

`TSO` is the Transparency Site Override block — meant to surface administrative
actions (revocation, civil penalty, etc.). In practice every TSO record sampled
during reverse-engineering returned the default empty shape, including facilities
flagged `ON PROBATION` in CKAN — see the "STATUS taxonomy" section below:

```json
{ "FacilityNumber": "", "CaseClosed": false, "PleadingDate": "1/1/0001", "ActionType": "" }
```

**404 behavior**: the endpoint returns 200 with `FacilityDetail.STATUS` empty and a
`No facilities match the search criteria` banner in the rendered HTML version. From
JSON, an unknown facility returns `FacilityDetail` with empty strings for most fields
and `STATUS == ""`. **Closed facilities are not returned via search** but *are*
returned via `FacilityDetail/{facnum}` (the API does not gate by status the way the
form does). Concretely: R&R 8290 (Bananas) listed `AHMADI, MARIAM` as active for
license `013423958`; `FacilityDetail/013423958` returns the full record with
`STATUS: "Closed, Licensee Initiated"` — the closure is invisible to `FacilitySearch`
but visible to `FacilityDetail`.

#### STATUS taxonomy (observed)

`FacilityDetail.STATUS` is more granular than the CKAN `facility_status` column.
Values sampled from Alameda data in May 2026:

| API `STATUS` | Meaning |
|--------------|---------|
| `Licensed` | Active license |
| `Licensed/Pending Increase` | Active, with a capacity-increase application open |
| `Provisional License` | Initial license, time-limited |
| `Pending` | Application in review |
| `Application Withdrawn` | Application abandoned before licensing |
| `Closed, Licensee Initiated` | Voluntary surrender |
| `Closed, Change of Ownership` | License retired during sale/transfer |
| `Closed, Non-payment` | Fee-related closure |
| `""` (empty) | Facility not exposed by the API — see below |

Other closure reasons surely exist (revocation, death, etc.) but weren't in the
sample.

##### Where CKAN and the API disagree

| CKAN `facility_status` | What the API returns |
|------------------------|---------------------|
| `LICENSED` | `Licensed` (~85% of sampled), occasionally `Licensed/Pending Increase`, rarely `Closed, Non-payment` (API is fresher) |
| `CLOSED` | Always a `Closed, …` reason — the API exposes the *why*, CKAN doesn't |
| `PENDING` | Splits across `Pending`, `Provisional License`, `Application Withdrawn`, or already-promoted-to `Licensed` |
| `INACTIVE` | Empty `STATUS` for 19 of 20 sampled — the API effectively does not surface inactive facilities. Treat CKAN as the only source for this status. |
| `ON PROBATION` | `Licensed` with an empty `TSO` block — **probation is hidden by the Transparency API**. CKAN is the only public source. |

Practical implication: when comparing CKAN and API state, never assume drift means
the API is wrong. The two are answering different questions — CKAN is the
administrative roster, the API is the enforcement-facing public view, and the
enforcement view deliberately omits probation and inactive states.

### `GET /FacilityReports/{padded_facnum}`

Evaluation reports linked to the facility. Returns `{COUNT, REPORTARRAY}`:

```json
{
  "COUNT": 1,
  "REPORTARRAY": [
    {
      "CONTROLNUMBER": "",
      "FACILITYNUMBER": "013423996",
      "REPORTDATE": "11/04/2024",
      "REPORTTITLE": "FACILITY EVALUATION REPORT",
      "REPORTTYPE": "Other",
      "REPORTPAGE": "http://www.fakeout.gov/WebReports/FASWebReportDisplay.nsf/..."
    }
  ]
}
```

`REPORTPAGE` URLs use a literal `fakeout.gov` domain — this is an internal CCLD proxy
target, not a real public URL. Treat the report list as a manifest; the PDFs
themselves aren't fetchable from outside CCLD.

### `GET /FacilitySearch?facType={id}&facility=&Street=&city=&zip=&county=&facnum=`

Catalog search. **`facType` is required**; everything else is an optional filter
(substring match). Returns `{COUNT, FACILITYARRAY}` with at most **250 facilities**:

```json
{
  "COUNTY": "Los Angeles",
  "FACILITYNAME": "TRACY INFANT CENTER",
  "FACILITYNUMBER": "191570967",
  "STATUS": "Licensed",
  "STREETADDRESS": "12222 CUESTA DRIVE, BLDG F",
  "TELEPHONE": "(562) 229-7762",
  "ZIPCODE": "90703"
}
```

Filter semantics (verified):

- `facility` — substring match on facility name (case-insensitive)
- `county` — match on county **name**, not the `CACounty.id` (e.g., `county=Alameda`)
- `zip` — exact 5-digit ZIP match
- `city` — substring match
- `facnum` — **observed to be ignored** in the search response. Use `FacilityDetail`
  for license-number lookups instead.

Gotchas:

- `facType=0` (Small FCC) returns 400 *Invalid Request parameter values*. The public
  form blocks unfiltered Small FCC searches entirely; the API enforces the same
  block. Use the open data CKAN feed for bulk Small FCC enumeration.
- `facType=All` returns 400.
- The 250-result cap is hard. There's no `offset` / `page` parameter. If you need
  more than 250 facilities of a type, filter by ZIP or city.

### `GET /FacilitySearch/GetByLicensee?licensee={exact_name}`

Exact-match search by licensee name. Returns `{COUNT, FACILITYARRAY}`.

Substring / fuzzy queries return `COUNT: 0` with a single `null` in the array
(`FACILITYARRAY: [null]`) — the API requires the exact stored licensee string,
formatted as `LASTNAME, FIRSTNAME` for individuals. Use this only when you already
know the canonical licensee text; otherwise prefer the open data CSVs.

### `GET /Group/`

Facility-type taxonomy. Returns an array of 7 groups, each with a `facility_type`
list. The relevant Child Care group (Alameda use cases):

| `id` | `display_name` |
|------|----------------|
| `0` | Family Child Care Home(Small) |
| `810` | Family Child Care Home(Large) |
| `830` | Child Care - Infant Center |
| `840` | School Age Child Care Center |
| `845` | Child Care Center |
| `850` | Child Care Center Preschool |
| `860` | Single Licensed Child Care Center |

These IDs map to `facType` in `FacilitySearch`. Other groups: `TwentyFourHourResChildren`,
`FosterFamilyAgencies`, residential adult care, etc.

### `GET /CACounty`

All 58 California counties with `{id, County}`. `id=1` is Alameda. The IDs are not
used by `FacilitySearch` (which matches by name), but are populated in `FacilityDetail`
in some shapes; preserve as a static lookup.

### `GET /Announcement`

Site-wide announcements posted by CCLD (e.g., glossary additions). Returns a list of
`{ID, Message, CreateDate, Active}`. Useful for telling "did anything change about
this dataset" — `Active=true` entries are what the UI shows.

### `GET /FAQ?mode=Public&id=context`

Returns **XML, not JSON** (`Content-Type: text/plain`). Parse with `xml.etree`.

### `GET /Error` (POST in practice)

Client-side error logger. Don't use.

## Web URL pattern (no API needed)

A standalone fact-check that doesn't even need to parse JSON: hit the HTML detail
URL directly:

```
https://www.ccld.dss.ca.gov/carefacilitysearch/FacDetail/{padded_facnum}
```

The page is server-rendered. If the body contains the phrase
`No facilities match the search criteria. Refine your search criteria and try your
search again.`, the license is unknown. Otherwise the facility name and `Status:`
field appear in the HTML. This is how the original R&R reconciliation verified
license numbers in May 2026 before the JSON API was discovered.

## Practical recipes

### Verify a single license number

```python
import urllib.request, json
def verify(facnum: str) -> dict | None:
    padded = facnum.zfill(9)
    url = f"https://www.ccld.dss.ca.gov/transparencyapi/api/FacilityDetail/{padded}"
    req = urllib.request.Request(url, headers={"User-Agent": "ccld-verify/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        d = json.loads(r.read())
    fd = d.get("FacilityDetail", {})
    return fd if fd.get("STATUS") else None  # None means unknown
```

### Pull the inspection history for a facility

`FacilityDetail` already includes counts. For the actual report manifest, hit
`FacilityReports/{padded}`. Reports' PDF bodies aren't fetchable.

### Walk Alameda Child Care Centers

```python
# Use the open data CSVs (data/centers.csv, data/homes.csv) — they have all of them.
# FacilitySearch is capped at 250 results.
```

### Detect a licensee that changed legal name

Compare `FacilityDetail.LICENSEENAME` against `data/centers.csv:licensee_name` over
two snapshots — drift on the API side without a CKAN refresh indicates a name change
not yet reflected in the open data extract.

## What this is good for

- **Sanity-checking R&R intakes** against the canonical CCLD record (verified for the
  May 2026 R&R reconciliation: 12 out of 13 licenses flagged as missing from a stale
  local CDSS snapshot were confirmed alive via the API in seconds).
- **Catching license-number errors** in vendor systems — license `013423996` was
  listed against "Little Sunflowers Center" by a Bananas R&R record, but the API
  shows it belongs to "JOHNSON III, JOHNNY" (FCC).
- **Reading evaluation report dates** without scraping the SPA.
- **Watching for newly-issued licenses** between CKAN refreshes (CKAN lags by weeks).

## What this is not good for

- **Bulk enumeration**: 250-result cap and no offset. Use the CKAN feed.
- **Anything for Small FCCs in aggregate**: `facType=0` is blocked.
- **Reading the actual PDF reports**: `REPORTPAGE` URLs are internal-only.
- **Real-time changes**: data appears to update on the same cadence as CKAN, not
  faster.

## How this was discovered

Reverse-engineered by capturing browser network traffic while submitting the public
Care Facility Search form, then inspecting the unminified Angular controllers under
`/javascript/Controller/`. See `scripts/discover_endpoints.py` in this repo for the
playwright script that did the capture. Replay it any time CCLD ships a new version
of the SPA to see if the endpoint surface changed.
