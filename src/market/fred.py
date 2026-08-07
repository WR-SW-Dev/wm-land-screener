"""
FRED (Federal Reserve Economic Data) integration — home-price appreciation,
building-permit activity, and labor-market health, layered onto the county
housing-need view as a "is the market already responding, and what's driving
the need" signal.

Four data types, one combined disk cache:
  - County HPI (FHFA All-Transactions Index, annual) — `ATNHPIUS<FIPS>A`
  - State HPI baseline (quarterly) — `MISTHPI`, annualized for comparison
  - County building permits (Census BPS, annual, residential-only) — `BPPRIV0<FIPS>`
  - County unemployment rate (BLS LAUS, monthly) — `MI<ABBR>URN`, where <ABBR>
    is a per-county code from config.FRED_COUNTY_ABBR (a different, non-FIPS
    naming scheme than HPI/permits, so it can't be built from FIPS alone).
    Chosen over a raw "Employed Persons" count deliberately: that count
    conflates population growth (already shown separately, see market/
    housing_needs.py) with actual labor-market health, and its most recent
    ~8-12 months are NOT seasonally adjusted preliminary LAUS estimates that
    showed a spurious synchronized ~5-7% "drop" across all 4 counties in
    2026-08 testing (confirmed artifact: the unemployment rate for the same
    counties/months stayed in a normal band, and October 2025 data was
    missing across the board — a benchmark-revision signature, not four
    independent economies declining in lockstep). Unemployment rate is also
    self-normalizing (already a %, no population-size effect to strip out).
  - National 30-yr mortgage rate (weekly, tool-wide) — `MORTGAGE30US`

County series IDs (except unemployment rate) are built from
config.MARKET_COUNTIES' FIPS codes, so a county added there is picked up
automatically — no new series ID to hardcode.

Public API:
    load_fred_data(refresh=False) -> dict
        {"mortgage_rate": [...], "state_hpi": [...],
         "counties": {key: {"hpi": [...], "permits": [...], "unemployment": [...]}}}
    mortgage_snapshot(fred_data) -> dict | None
    hpi_metrics(county_key, fred_data) -> dict | None
    unemployment_metrics(county_key, fred_data) -> dict | None
    unemployment_chart_frame(county_key, fred_data) -> list
    momentum_badge(county_key, fred_data, needs_row) -> dict | None
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

import requests
import yaml
from yaml.loader import SafeLoader

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (  # noqa: E402
    ROOT, DATA_RAW, MARKET_COUNTIES, FRED_BASE_URL, FRED_STATE_HPI_SERIES,
    FRED_MORTGAGE_SERIES, FRED_MOMENTUM_RED_MAX, FRED_MOMENTUM_YELLOW_MAX,
    FRED_COUNTY_ABBR,
)
from market import growth_utils  # noqa: E402

_CACHE = DATA_RAW / "market_fred.json"


# ── FRED API key ──────────────────────────────────────────────────────────────
def _fred_key() -> str:
    """Read the FRED API key from the gitignored credentials.yaml."""
    cred = ROOT / "credentials.yaml"
    if cred.exists():
        with open(cred) as f:
            cfg = yaml.load(f, Loader=SafeLoader) or {}
        key = (cfg.get("fred") or {}).get("api_key")
        if key:
            return key
    raise RuntimeError(
        "FRED API key not found. Add a `fred:\\n  api_key: <key>` block to "
        "credentials.yaml (free key: fredaccount.stlouisfed.org/apikeys)."
    )


def _cofips(geo: dict) -> str:
    return geo["state"] + geo["county"]


def _fetch_observations(series_id: str, api_key: str) -> list:
    """Fetch one FRED series' observations, dropping missing ('.') values."""
    r = requests.get(f"{FRED_BASE_URL}/series/observations", params={
        "series_id": series_id, "api_key": api_key, "file_type": "json",
        "sort_order": "asc",
    }, timeout=30)
    r.raise_for_status()
    obs = []
    for o in r.json().get("observations", []):
        if o["value"] in (".", "", None):
            continue
        obs.append({"date": o["date"], "value": float(o["value"])})
    return obs


def _annualize_quarterly(obs: list) -> list:
    """Collapse quarterly observations to annual by averaging each year's quarters."""
    by_year = defaultdict(list)
    for o in obs:
        by_year[o["date"][:4]].append(o["value"])
    return [{"date": f"{y}-01-01", "value": sum(v) / len(v)} for y, v in sorted(by_year.items())]


def _build() -> dict:
    api_key = _fred_key()

    mortgage_obs = _fetch_observations(FRED_MORTGAGE_SERIES, api_key)
    state_hpi_obs = _annualize_quarterly(_fetch_observations(FRED_STATE_HPI_SERIES, api_key))

    counties = {}
    for c in MARKET_COUNTIES:
        fips = _cofips(c["geo"])
        try:
            hpi_obs = _fetch_observations(f"ATNHPIUS{fips}A", api_key)
        except Exception as e:                       # noqa: BLE001
            print(f"  [warn] FRED HPI fetch failed for {c['key']}: {e}")
            hpi_obs = []
        try:
            permits_obs = _fetch_observations(f"BPPRIV0{fips}", api_key)
        except Exception as e:                       # noqa: BLE001
            print(f"  [warn] FRED permits fetch failed for {c['key']}: {e}")
            permits_obs = []
        try:
            abbr = FRED_COUNTY_ABBR[c["key"]]
            unemployment_monthly = _fetch_observations(f"MI{abbr}URN", api_key)
            unemployment_obs = growth_utils.annualize_observations(unemployment_monthly)
        except Exception as e:                       # noqa: BLE001
            print(f"  [warn] FRED unemployment fetch failed for {c['key']}: {e}")
            unemployment_monthly, unemployment_obs = [], []
        counties[c["key"]] = {
            "hpi": hpi_obs, "permits": permits_obs,
            "unemployment": unemployment_obs, "unemployment_monthly": unemployment_monthly,
        }

    return {"mortgage_rate": mortgage_obs, "state_hpi": state_hpi_obs, "counties": counties}


def load_fred_data(refresh: bool = False) -> dict:
    """Return the combined FRED dataset, using the on-disk cache by default."""
    if _CACHE.exists() and not refresh:
        return json.loads(_CACHE.read_text())
    data = _build()
    _CACHE.write_text(json.dumps(data))
    return data


def refresh_mortgage_rate() -> dict:
    """Re-pull just the weekly mortgage-rate series and patch it into the
    existing cache, leaving the annual HPI/permits data untouched — the rate
    updates far more often than those, so refreshing the whole bundle every
    time would just re-fetch annual series that haven't changed."""
    api_key = _fred_key()
    mortgage_obs = _fetch_observations(FRED_MORTGAGE_SERIES, api_key)
    data = json.loads(_CACHE.read_text()) if _CACHE.exists() else _build()
    data["mortgage_rate"] = mortgage_obs
    _CACHE.write_text(json.dumps(data))
    return data


# ── Derived metrics ────────────────────────────────────────────────────────────
def mortgage_snapshot(fred_data: dict) -> dict | None:
    """Latest 30-yr mortgage rate + delta vs ~1 quarter (13 weeks) ago."""
    obs = fred_data.get("mortgage_rate") or []
    if not obs:
        return None
    latest = obs[-1]
    prior = obs[max(0, len(obs) - 1 - 13)]
    return {
        "latest": latest["value"], "latest_date": latest["date"],
        "prior": prior["value"], "prior_date": prior["date"],
        "delta": latest["value"] - prior["value"],
    }


def hpi_metrics(county_key: str, fred_data: dict) -> dict | None:
    """YoY appreciation, 5-yr cumulative appreciation, and delta vs the MI
    state baseline's YoY — for the county's latest available HPI year."""
    hpi_obs = (fred_data.get("counties", {}).get(county_key) or {}).get("hpi") or []
    if len(hpi_obs) < 2:
        return None
    latest, prior_1y = hpi_obs[-1], hpi_obs[-2]
    yoy_pct = (latest["value"] / prior_1y["value"] - 1) * 100

    cum_5y_pct = None
    if len(hpi_obs) >= 6:
        base_5y = hpi_obs[-6]
        cum_5y_pct = (latest["value"] / base_5y["value"] - 1) * 100

    vs_state_delta = None
    state_by_year = {o["date"][:4]: o["value"] for o in fred_data.get("state_hpi") or []}
    ly, py = latest["date"][:4], prior_1y["date"][:4]
    if ly in state_by_year and py in state_by_year and state_by_year[py]:
        state_yoy = (state_by_year[ly] / state_by_year[py] - 1) * 100
        vs_state_delta = yoy_pct - state_yoy

    return {
        "latest_year": ly, "latest_value": latest["value"],
        "yoy_pct": yoy_pct, "cum_5y_pct": cum_5y_pct, "vs_state_delta": vs_state_delta,
    }


def hpi_chart_frame(county_key: str, fred_data: dict, years: int = 15):
    """Long-format rows {year, series, index} for a county-vs-state HPI trend
    chart, both rebased to 100 at the first year shown (source series use
    different base years, so only relative trend is comparable directly)."""
    hpi_obs = (fred_data.get("counties", {}).get(county_key) or {}).get("hpi") or []
    state_obs = fred_data.get("state_hpi") or []
    if not hpi_obs or not state_obs:
        return []
    county_recent = hpi_obs[-years:]
    start_year = county_recent[0]["date"][:4]
    state_by_year = {o["date"][:4]: o["value"] for o in state_obs}
    if start_year not in state_by_year:
        return []
    county_base = county_recent[0]["value"]
    state_base = state_by_year[start_year]

    rows = []
    for o in county_recent:
        rows.append({"year": o["date"][:4], "series": "County", "index": o["value"] / county_base * 100})
        y = o["date"][:4]
        if y in state_by_year:
            rows.append({"year": y, "series": "Michigan", "index": state_by_year[y] / state_base * 100})
    return rows


def unemployment_metrics(county_key: str, fred_data: dict) -> dict | None:
    """Latest monthly unemployment rate + delta vs the SAME month one year
    earlier (not vs an annual average — avoids any seasonal mismatch, and
    a rate is already normalized so there's no annualizing-then-comparing
    step needed the way there was for a raw employment count)."""
    monthly = (fred_data.get("counties", {}).get(county_key) or {}).get("unemployment_monthly") or []
    if not monthly:
        return None
    latest = monthly[-1]
    latest_year, month = latest["date"][:4], latest["date"][5:7]
    prior_year = str(int(latest_year) - 1)
    prior = next((o for o in monthly if o["date"][:4] == prior_year and o["date"][5:7] == month), None)
    return {
        "latest_date": latest["date"], "latest_value": latest["value"],
        "delta_pts": (latest["value"] - prior["value"]) if prior else None,
        "prior_date": prior["date"] if prior else None,
    }


def unemployment_chart_frame(county_key: str, fred_data: dict, years: int = 15) -> list:
    """Annual-average unemployment rate, most recent `years` — plotted
    directly as a plain %, no rebasing or YoY-growth-of-a-rate transform
    (unlike HPI/ZORI/ZHVI, a rate is already a directly comparable number,
    so a raw level line is the honest, intuitive chart here)."""
    obs = (fred_data.get("counties", {}).get(county_key) or {}).get("unemployment") or []
    return [{"year": o["date"][:4], "value": o["value"]} for o in obs[-years:]]


def permits_recent(county_key: str, fred_data: dict, years: int = 10) -> list:
    """Last N years of permit counts for the bar chart."""
    permits_obs = (fred_data.get("counties", {}).get(county_key) or {}).get("permits") or []
    return permits_obs[-years:]


def momentum_badge(county_key: str, fred_data: dict, needs_row) -> dict | None:
    """% of the county's 5-yr HNA unit gap already covered by permits issued
    so far within that study period, plus the red/yellow/green call."""
    permits_obs = (fred_data.get("counties", {}).get(county_key) or {}).get("permits") or []
    study_period = needs_row.get("study_period")
    total_need = needs_row.get("total_units")
    if not permits_obs or not study_period or not total_need:
        return None
    try:
        start_year, end_year = (int(y.strip()) for y in study_period.replace("–", "-").split("-"))
    except ValueError:
        return None

    cumulative = sum(o["value"] for o in permits_obs
                     if start_year <= int(o["date"][:4]) <= end_year)
    pct = (cumulative / total_need) * 100 if total_need else 0.0

    if pct < FRED_MOMENTUM_RED_MAX:
        color, label = "red", "Underserved"
    elif pct < FRED_MOMENTUM_YELLOW_MAX:
        color, label = "yellow", "Responding"
    else:
        color, label = "green", "Saturating"

    # Annual pace needed to close the gap by the end of the study period —
    # used as a reference line on the permits chart (see render.py).
    annual_pace = total_need / (end_year - start_year + 1)

    return {"pct": pct, "color": color, "label": label,
            "cumulative_permits": cumulative, "total_need": total_need,
            "study_period": study_period, "start_year": start_year,
            "end_year": end_year, "annual_pace": annual_pace}


if __name__ == "__main__":
    if "--mortgage-only" in sys.argv:
        data = refresh_mortgage_rate()
    else:
        data = load_fred_data(refresh="--refresh" in sys.argv)
    snap = mortgage_snapshot(data)
    print(f"Mortgage rate: {snap['latest']:.2f}% ({snap['latest_date']}), "
          f"{snap['delta']:+.2f} pts vs {snap['prior_date']}")
    for c in MARKET_COUNTIES:
        m = hpi_metrics(c["key"], data)
        if m:
            print(f"{c['label']:18s} HPI YoY {m['yoy_pct']:+.1f}%  "
                  f"5yr {m['cum_5y_pct']:+.1f}%  vs MI {m['vs_state_delta']:+.1f} pts")
        u = unemployment_metrics(c["key"], data)
        if u:
            delta = f"{u['delta_pts']:+.1f} pts vs {u['prior_date']}" if u["delta_pts"] is not None else "n/a"
            print(f"{c['label']:18s} unemployment {u['latest_value']:.1f}% ({u['latest_date']})  {delta}")
