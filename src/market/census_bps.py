"""
Census Building Permits Survey (BPS) — county-level annual permits split by
structure type (1-unit / 2-4 unit / 5+ unit), fetched directly from Census.

FRED's own county permit series (BPPRIV0<FIPS>, used elsewhere in fred.py) is
a single blended total with no structure-type breakdown — confirmed live
against the FRED API 2026-07-30 (searched every phrasing; nothing exists for
any of the four counties beyond the one combined series). So this data has
to come straight from Census's own county files instead of through FRED.

Source: https://www2.census.gov/econ/bps/County/ — annual co{year}a.txt (one
row per county per year) and monthly co{yy}{mm}y.txt (year-to-date cumulative
through that month, same column layout). Plain CSV, no auth, no API key.

Public API:
    load_bps_data(refresh=False) -> dict
        {county_key: {
            "annual": [{"year": int, "sf": int, "mid": int, "mf": int}, ...],
            "ytd": {"as_of": "YYYY-MM", "sf": int, "mid": int, "mf": int,
                    "prior_sf": int, "prior_mid": int, "prior_mf": int} | None,
        }}
"""
from __future__ import annotations

import csv
import io
import json
import sys
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DATA_RAW, MARKET_COUNTIES  # noqa: E402

_CACHE = DATA_RAW / "market_census_bps.json"
_BASE = "https://www2.census.gov/econ/bps/County"
_YEARS_BACK = 10
# Census blocks the default python-requests user agent on this host.
_HEADERS = {"User-Agent": "Mozilla/5.0"}


def _cofips(geo: dict) -> str:
    return geo["state"] + geo["county"]


def _parse_row(row: list) -> dict:
    """Row -> {sf, mid, mf} unit counts from the fixed BPS column layout:
    ...,[1-unit Bldgs,Units,Value],[2-units B,U,V],[3-4units B,U,V],[5+units B,U,V],...
    "mid" (duplex-to-quad) combines the 2-unit and 3-4-unit Units columns —
    Census reports them as two separate size classes, not one "2-4 unit" one."""
    return {
        "sf": int(float(row[7])),
        "mid": int(float(row[10])) + int(float(row[13])),
        "mf": int(float(row[16])),
    }


def _fetch_csv_rows(url: str):
    r = requests.get(url, headers=_HEADERS, timeout=30)
    if r.status_code != 200:
        return None
    return list(csv.reader(io.StringIO(r.text)))


def _county_row(url: str, fips: str):
    rows = _fetch_csv_rows(url)
    if not rows:
        return None
    for row in rows[2:]:                       # first 2 rows are the split header
        if len(row) > 5 and row[1].strip() == fips[:2] and row[2].strip() == fips[2:]:
            return _parse_row(row)
    return None


def _latest_ytd(fips: str):
    """Most recent month with both a current-year and matching prior-year YTD
    file published. Probes backward from last month rather than hardcoding
    one — Census usually has last month's file up within a few weeks, and
    this keeps working correctly as the pipeline is re-run later in the year
    without needing an edit."""
    today = date.today()
    y, m = today.year, today.month
    for _ in range(4):
        m -= 1
        if m == 0:
            m, y = 12, y - 1
        yy, mm = f"{y % 100:02d}", f"{m:02d}"
        cur = _county_row(f"{_BASE}/co{yy}{mm}y.txt", fips)
        if cur is None:
            continue
        prior_yy = f"{(y - 1) % 100:02d}"
        prior = _county_row(f"{_BASE}/co{prior_yy}{mm}y.txt", fips)
        if prior is None:
            continue
        return {
            "as_of": f"{y}-{mm}",
            "sf": cur["sf"], "mid": cur["mid"], "mf": cur["mf"],
            "prior_sf": prior["sf"], "prior_mid": prior["mid"], "prior_mf": prior["mf"],
        }
    return None


def _build() -> dict:
    this_year = date.today().year
    out = {}
    for c in MARKET_COUNTIES:
        fips = _cofips(c["geo"])
        annual = []
        for year in range(this_year - _YEARS_BACK, this_year):
            try:
                row = _county_row(f"{_BASE}/co{year}a.txt", fips)
            except Exception as e:                    # noqa: BLE001
                print(f"  [warn] BPS annual fetch failed for {c['key']} {year}: {e}")
                row = None
            if row:
                annual.append({"year": year, **row})
        try:
            ytd = _latest_ytd(fips)
        except Exception as e:                        # noqa: BLE001
            print(f"  [warn] BPS YTD fetch failed for {c['key']}: {e}")
            ytd = None
        out[c["key"]] = {"annual": annual, "ytd": ytd}
    return out


def load_bps_data(refresh: bool = False) -> dict:
    """Return the combined Census BPS dataset, using the on-disk cache by default."""
    if _CACHE.exists() and not refresh:
        return json.loads(_CACHE.read_text())
    data = _build()
    _CACHE.write_text(json.dumps(data))
    return data


if __name__ == "__main__":
    data = load_bps_data(refresh="--refresh" in sys.argv)
    for c in MARKET_COUNTIES:
        d = data.get(c["key"], {})
        ytd = d.get("ytd")
        print(f"{c['label']:18s} {len(d.get('annual', [])):2d} years  "
              f"YTD: {ytd['as_of'] if ytd else 'none'}")
