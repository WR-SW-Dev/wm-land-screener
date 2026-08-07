"""
In-commuters per county — how many jobs in a county are filled by people who
live somewhere else, from Census LEHD LODES origin-destination data.

Why this metric: an in-commuter is someone the local economy already supports
but the local housing stock doesn't. For BTR specifically that's demand that
exists today and would convert to a tenant if product existed — the Bowen HNAs
call it out as an explicit Opportunity in every county's SWOT ("Attract some of
the N commuters coming into the county for work to live in the county").

Why computed from LODES rather than transcribed from the HNA reports: the
reports do publish this figure (all six are available in OneDrive under Market
Research), but they're six separate studies with different vintages. Deriving
it from the one underlying source all of them used instead means every county is
measured identically, a county added to config.MARKET_COUNTIES picks it up with
no new transcription, and it refreshes when LODES publishes a new year — none of
which a hand-transcribed figure gives you. Doing it per-report would have
invited exactly the county-by-county inconsistency that got a
Grand-Traverse-only gap callout removed on 2026-08-07 (see the note in render.py
above _render_county_drilldown).

Validated against the reports' own published numbers using LODES 2020, the
vintage Bowen used — reproduces all three transcribable counties EXACTLY:
    Grand Traverse  19,329 in-commuters / 43.1% of jobs
    Antrim           1,982 / 44.5%
    Kalkaska         2,114 / 56.0%
so the aggregation below matches their methodology, not just their ballpark.

Definitions (deliberately matching the reports):
  • "jobs in the county" = every LODES job whose WORKPLACE block is in it.
  • "in-commuter"        = such a job whose worker's HOME block is outside it.
  • JT00 = All Jobs, the same job type the reports' tables use ("All Jobs").
  • The `main` file covers workers living in Michigan; `aux` covers workers
    living out of state, who are in-commuters by definition.

JT00 counts JOBS, not people — someone holding two jobs in a county is counted
twice. Checked against JT01 (Primary Jobs, one per person) for 2021: the COUNT
runs 3.9-6.7% lower under JT01, but the SHARE moves by at most 0.2 points in any
county (Ottawa 50.1% → 50.0%, Allegan 61.5% → 61.7%). So JT00 is kept — it's
what the reports used and what the validation above reproduces — and the UI
labels the count as jobs rather than people instead of switching job types.

Known limitations, worth re-reading before leaning hard on this:
  • Census applies noise infusion for confidentiality, so block-level pairs are
    unreliable by design; county aggregation like this is the intended use.
  • Jobs sit at the worksite the EMPLOYER reports, so multi-site employers that
    file everyone under a headquarters address over-concentrate jobs there, and
    remote workers still count against their office location.
  • UI wage records exclude the self-employed and independent contractors —
    relevant for the more agricultural counties (Allegan, Antrim).
  • Vintage lags ~5 years, and the newest years available (2020/2021) are the
    two most COVID-distorted labor years on record.

Source: https://lehd.ces.census.gov/data/lodes/LODES8/mi/od/ — plain gzipped
CSV, no auth, no API key. ~22 MB per year, streamed and aggregated to county
totals, so only the small result is cached.

Public API:
    load_lodes_data(refresh=False) -> dict
        {"year": int, "counties": {county_key: {"jobs": int,
                                                "in_commuters": int,
                                                "share_pct": float}}}
    commuter_metrics(county_key, lodes_data) -> dict | None
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import sys
from collections import defaultdict
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DATA_RAW, LODES_BASE_URL, LODES_JOB_TYPE, LODES_YEARS  # noqa: E402
from config import MARKET_COUNTIES  # noqa: E402

_CACHE = DATA_RAW / "market_lodes.json"


def _county_fips_map() -> dict:
    """{5-digit county FIPS: county_key} for the configured market counties."""
    return {c["geo"]["state"] + c["geo"]["county"]: c["key"] for c in MARKET_COUNTIES}


def _fetch_gz_csv(url: str) -> str:
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    return gzip.decompress(r.content).decode()


def _compute(year: int) -> dict | None:
    """Aggregate one LODES year to per-county job / in-commuter totals.

    Returns None if that year isn't published yet, so the caller can fall back
    to an older vintage rather than erroring.
    """
    want = _county_fips_map()
    jobs = defaultdict(int)
    in_commuters = defaultdict(int)

    for kind in ("main", "aux"):
        url = f"{LODES_BASE_URL}/mi_od_{kind}_{LODES_JOB_TYPE}_{year}.csv.gz"
        try:
            text = _fetch_gz_csv(url)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise
        for row in csv.DictReader(io.StringIO(text)):
            work_fips = row["w_geocode"][:5]
            key = want.get(work_fips)
            if key is None:
                continue
            n = int(row["S000"])
            jobs[key] += n
            # `aux` workers live outside Michigan → in-commuters by definition.
            if kind == "aux" or row["h_geocode"][:5] != work_fips:
                in_commuters[key] += n

    if not jobs:
        return None
    counties = {}
    for key in want.values():
        j, i = jobs.get(key, 0), in_commuters.get(key, 0)
        counties[key] = {"jobs": j, "in_commuters": i,
                         "share_pct": (i / j * 100) if j else None}
    return {"year": year, "counties": counties}


def _build() -> dict:
    """Newest published LODES year available, walking back if the newest 404s."""
    for year in LODES_YEARS:
        data = _compute(year)
        if data:
            return data
    raise RuntimeError(f"No LODES year available from {LODES_YEARS} at {LODES_BASE_URL}")


def load_lodes_data(refresh: bool = False) -> dict:
    """Return the per-county in-commuter dataset, using the on-disk cache by default."""
    if _CACHE.exists() and not refresh:
        return json.loads(_CACHE.read_text())
    data = _build()
    _CACHE.write_text(json.dumps(data))
    return data


def commuter_metrics(county_key: str, lodes_data: dict) -> dict | None:
    """In-commuter count, total jobs, and in-commuter share for one county."""
    if not lodes_data:
        return None
    rec = (lodes_data.get("counties") or {}).get(county_key)
    if not rec or not rec.get("jobs"):
        return None
    return {**rec, "year": lodes_data.get("year")}


if __name__ == "__main__":
    data = load_lodes_data(refresh="--refresh" in sys.argv)
    print(f"LODES {data['year']} ({LODES_JOB_TYPE}, all jobs) — in-commuters by county")
    rows = sorted(data["counties"].items(),
                  key=lambda kv: (kv[1]["share_pct"] or 0), reverse=True)
    for key, r in rows:
        share = f"{r['share_pct']:.1f}%" if r["share_pct"] is not None else "—"
        print(f"  {key:16s} jobs={r['jobs']:8,}  in-commuters={r['in_commuters']:8,}  share={share}")
