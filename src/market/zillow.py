"""
Zillow Research data integration — two datasets, same free-CSV/no-API-key
pattern, same fetch/annualize machinery:

  - ZORI (Observed Rent Index): asking-rent trend. No state-level series
    (national/metro/county/city/zip only), so each county pairs against its
    own Zillow metro instead (see config.py's ZORI_METRO_FOR_COUNTY note).
  - ZHVI (Home Value Index): typical-home-value trend, replacing the
    FHFA-via-FRED HPI panel. DOES have a state-level series, so this one
    compares county vs Michigan directly.

Public API:
    load_zori_data(refresh=False) -> dict
        {"counties": {key: [{"date":.., "value":.., "months":..}]}, "metros": {name: [...]}}
    rent_metrics(county_key, zori_data) -> dict | None
    rent_yoy_frame(county_key, zori_data) -> list
    load_zhvi_data(refresh=False) -> dict
        {"counties": {key: [...]}, "state": [...]}
    value_metrics(county_key, zhvi_data) -> dict | None
    value_yoy_frame(county_key, zhvi_data) -> list
"""
from __future__ import annotations

import csv
import io
import json
import sys
from collections import defaultdict
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (  # noqa: E402
    DATA_RAW, MARKET_COUNTIES, ZORI_COUNTY_URL, ZORI_METRO_URL, ZORI_METRO_FOR_COUNTY,
    ZHVI_COUNTY_URL, ZHVI_STATE_URL,
)

_CACHE_ZORI = DATA_RAW / "market_zori.json"
_CACHE_ZHVI = DATA_RAW / "market_zhvi.json"


# ── Shared fetch/annualize machinery ────────────────────────────────────────────
def _fetch_csv(url: str) -> list:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return list(csv.DictReader(io.StringIO(r.text)))


def _date_cols(row: dict) -> list:
    return [k for k in row if k[:1].isdigit()]


def _annualize(row: dict, date_cols: list) -> list:
    """Collapse monthly observations to annual averages, matching the
    permits chart's cadence. A partial current year still becomes its own
    (lower-confidence) point rather than being dropped — same spirit as the
    permits chart's YTD bar. `months` records how many months went into that
    average, so a partial final year can be flagged downstream instead of
    silently compared to full prior years as if it were one."""
    by_year = defaultdict(list)
    for col in date_cols:
        v = row.get(col)
        if v:
            by_year[col[:4]].append(float(v))
    return [{"date": f"{y}-01-01", "value": sum(vals) / len(vals), "months": len(vals)}
            for y, vals in sorted(by_year.items())]


def _yoy_frame(obs: list) -> list:
    """Year-over-year growth, one row per year: {year, pct, provisional}.
    Shared by rent and home-value trends (and matches the permits chart's
    growth_rows shape in render.py) so all three growth charts can reuse the
    exact same zero-line / up-down-coloring / hollow-point rendering."""
    rows = []
    for i in range(1, len(obs)):
        y0, y1 = obs[i - 1], obs[i]
        if not y0["value"]:
            continue
        pct = (y1["value"] / y0["value"] - 1) * 100
        provisional = y1.get("months", 12) < 12
        rows.append({"year": y1["date"][:4], "pct": pct, "provisional": provisional})
    return rows


# ── ZORI (rent) ──────────────────────────────────────────────────────────────
def _build_zori() -> dict:
    county_rows = _fetch_csv(ZORI_COUNTY_URL)
    date_cols = _date_cols(county_rows[0]) if county_rows else []

    wanted_counties = {c["label"]: c["key"] for c in MARKET_COUNTIES}
    counties = {}
    for row in county_rows:
        if row.get("StateName") == "MI" and row.get("RegionName") in wanted_counties:
            key = wanted_counties[row["RegionName"]]
            counties[key] = _annualize(row, date_cols)

    metro_rows = _fetch_csv(ZORI_METRO_URL)
    metro_date_cols = _date_cols(metro_rows[0]) if metro_rows else []
    wanted_metros = set(ZORI_METRO_FOR_COUNTY.values())
    metros = {}
    for row in metro_rows:
        if row.get("RegionName") in wanted_metros:
            metros[row["RegionName"]] = _annualize(row, metro_date_cols)

    return {"counties": counties, "metros": metros}


def load_zori_data(refresh: bool = False) -> dict:
    """Return the combined ZORI dataset, using the on-disk cache by default."""
    if _CACHE_ZORI.exists() and not refresh:
        return json.loads(_CACHE_ZORI.read_text())
    data = _build_zori()
    _CACHE_ZORI.write_text(json.dumps(data))
    return data


def rent_metrics(county_key: str, zori_data: dict) -> dict | None:
    """Current typical rent, YoY rent growth, and delta vs the county's own
    Zillow metro — for the latest available annual ZORI observation."""
    obs = (zori_data.get("counties", {}) or {}).get(county_key) or []
    if len(obs) < 2:
        return None
    latest, prior_1y = obs[-1], obs[-2]
    yoy_pct = (latest["value"] / prior_1y["value"] - 1) * 100

    vs_metro_delta = None
    metro_name = ZORI_METRO_FOR_COUNTY.get(county_key)
    metro_obs = (zori_data.get("metros", {}) or {}).get(metro_name) or []
    metro_by_year = {o["date"][:4]: o["value"] for o in metro_obs}
    ly, py = latest["date"][:4], prior_1y["date"][:4]
    if ly in metro_by_year and py in metro_by_year and metro_by_year[py]:
        metro_yoy = (metro_by_year[ly] / metro_by_year[py] - 1) * 100
        vs_metro_delta = yoy_pct - metro_yoy

    return {
        "latest_year": ly, "latest_value": latest["value"],
        "yoy_pct": yoy_pct, "vs_metro_delta": vs_metro_delta,
        "metro_name": metro_name, "n_years": len(obs),
    }


def rent_yoy_frame(county_key: str, zori_data: dict) -> list:
    obs = (zori_data.get("counties", {}) or {}).get(county_key) or []
    return _yoy_frame(obs)


# ── ZHVI (home value) ────────────────────────────────────────────────────────
def _build_zhvi() -> dict:
    county_rows = _fetch_csv(ZHVI_COUNTY_URL)
    date_cols = _date_cols(county_rows[0]) if county_rows else []

    wanted_counties = {c["label"]: c["key"] for c in MARKET_COUNTIES}
    counties = {}
    for row in county_rows:
        if row.get("StateName") == "MI" and row.get("RegionName") in wanted_counties:
            key = wanted_counties[row["RegionName"]]
            counties[key] = _annualize(row, date_cols)

    state_rows = _fetch_csv(ZHVI_STATE_URL)
    state_date_cols = _date_cols(state_rows[0]) if state_rows else []
    state = []
    for row in state_rows:
        if row.get("RegionName") == "Michigan":
            state = _annualize(row, state_date_cols)
            break

    return {"counties": counties, "state": state}


def load_zhvi_data(refresh: bool = False) -> dict:
    """Return the combined ZHVI dataset, using the on-disk cache by default."""
    if _CACHE_ZHVI.exists() and not refresh:
        return json.loads(_CACHE_ZHVI.read_text())
    data = _build_zhvi()
    _CACHE_ZHVI.write_text(json.dumps(data))
    return data


def value_metrics(county_key: str, zhvi_data: dict) -> dict | None:
    """Current typical home value, YoY value growth, and delta vs Michigan —
    for the latest available annual ZHVI observation."""
    obs = (zhvi_data.get("counties", {}) or {}).get(county_key) or []
    if len(obs) < 2:
        return None
    latest, prior_1y = obs[-1], obs[-2]
    yoy_pct = (latest["value"] / prior_1y["value"] - 1) * 100

    vs_state_delta = None
    state_obs = zhvi_data.get("state") or []
    state_by_year = {o["date"][:4]: o["value"] for o in state_obs}
    ly, py = latest["date"][:4], prior_1y["date"][:4]
    if ly in state_by_year and py in state_by_year and state_by_year[py]:
        state_yoy = (state_by_year[ly] / state_by_year[py] - 1) * 100
        vs_state_delta = yoy_pct - state_yoy

    return {
        "latest_year": ly, "latest_value": latest["value"],
        "yoy_pct": yoy_pct, "vs_state_delta": vs_state_delta, "n_years": len(obs),
    }


def value_yoy_frame(county_key: str, zhvi_data: dict) -> list:
    obs = (zhvi_data.get("counties", {}) or {}).get(county_key) or []
    return _yoy_frame(obs)


if __name__ == "__main__":
    refresh = "--refresh" in sys.argv
    zori_data = load_zori_data(refresh=refresh)
    zhvi_data = load_zhvi_data(refresh=refresh)
    for c in MARKET_COUNTIES:
        r = rent_metrics(c["key"], zori_data)
        v = value_metrics(c["key"], zhvi_data)
        if r:
            vs_r = f"{r['vs_metro_delta']:+.1f} pts" if r["vs_metro_delta"] is not None else "n/a"
            print(f"{c['label']:18s} rent  ${r['latest_value']:>7,.0f}/mo  YoY {r['yoy_pct']:+.1f}%  "
                  f"vs metro ({r['metro_name']}) {vs_r}")
        else:
            print(f"{c['label']:18s} rent  no data")
        if v:
            vs_v = f"{v['vs_state_delta']:+.1f} pts" if v["vs_state_delta"] is not None else "n/a"
            print(f"{c['label']:18s} value ${v['latest_value']:>9,.0f}  YoY {v['yoy_pct']:+.1f}%  "
                  f"vs Michigan {vs_v}")
        else:
            print(f"{c['label']:18s} value no data")
