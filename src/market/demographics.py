"""
ACS demographics + affordability for Section 1 market feasibility.

Pulls the per-submarket and per-county metrics defined in config.ACS_VARS from
the Census ACS 5-year API, derives affordability and demand signals, and caches
the assembled frame to data/raw/ so the Streamlit app loads instantly offline.

Public API:
    load_market_metrics(refresh=False) -> pandas.DataFrame
        One row per submarket + county. Columns include the raw ACS metrics,
        derived rates, max_affordable_rent, and pop_growth_pct.
"""
import json
import sys
from pathlib import Path

import pandas as pd
import requests
import yaml
from yaml.loader import SafeLoader

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (  # noqa: E402
    ROOT, DATA_RAW, CENSUS_BASE_URL, ACS_DATASET, ACS_YEAR, ACS_BASELINE_YEAR,
    ACS_VARS, ACS_PROFILE_VARS, AFFORDABILITY_INCOME_SHARE,
    MARKET_SUBMARKETS, MARKET_COUNTIES,
)

_CACHE     = DATA_RAW / "market_demographics.json"
_CACHE_MUNI = DATA_RAW / "market_municipal.json"


# ── Census API key ────────────────────────────────────────────────────────────
def _census_key() -> str:
    """Read the Census API key from the gitignored credentials.yaml."""
    cred = ROOT / "credentials.yaml"
    if cred.exists():
        with open(cred) as f:
            cfg = yaml.load(f, Loader=SafeLoader) or {}
        key = (cfg.get("census") or {}).get("api_key")
        if key:
            return key
    raise RuntimeError(
        "Census API key not found. Add a `census:\\n  api_key: <key>` block to "
        "credentials.yaml (free key: api.census.gov/data/key_signup.html)."
    )


# ── Geography → API query params ───────────────────────────────────────────────
def _geo_params(geo: dict) -> tuple[str, str]:
    """Return (for_clause, in_clause) for a submarket/county geography dict."""
    if geo["type"] == "county":
        return f"county:{geo['county']}", f"state:{geo['state']}"
    if geo["type"] == "cousub":
        return (f"county subdivision:{geo['cousub']}",
                f"state:{geo['state']} county:{geo['county']}")
    raise ValueError(f"Unknown geography type: {geo['type']}")


def _fetch_one(geo: dict, variables: list[str], year: int, key: str,
               profile: bool = False) -> dict:
    """Fetch one geography's variables for one ACS year. Returns {var: value}."""
    dataset = f"{ACS_DATASET}/profile" if profile else ACS_DATASET
    for_clause, in_clause = _geo_params(geo)
    params = {
        "get": "NAME," + ",".join(variables),
        "for": for_clause,
        "in": in_clause,
        "key": key,
    }
    url = f"{CENSUS_BASE_URL}/{year}/{dataset}"
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    rows = resp.json()                       # [header, values]
    header, values = rows[0], rows[1]
    return dict(zip(header, values))


def _num(val):
    """Census returns strings; negatives/nulls (-666666666 etc.) → None."""
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    # ACS jam values (annotations) are large negatives.
    return None if f <= -666666 else f


def _derive(raw: dict, baseline_pop) -> dict:
    """Compute rates, affordability, and growth from raw ACS values."""
    g = lambda k: _num(raw.get(ACS_VARS[k]))   # noqa: E731

    income      = g("median_hh_income")
    rent        = g("median_gross_rent")
    home_value  = g("median_home_value")
    pop         = g("population")
    ten_total   = g("tenure_total")
    ten_renter  = g("tenure_renter")
    occ_total   = g("occ_total")
    occ_occ     = g("occ_occupied")
    burden_tot  = g("burden_total")
    burdened    = sum(v for v in (g("burden_30_35"), g("burden_35_40"),
                                  g("burden_40_50"), g("burden_50_plus"))
                      if v is not None)

    max_afford = round(income / 12 * AFFORDABILITY_INCOME_SHARE) if income else None
    renter_share_pct = (ten_renter / ten_total * 100) if ten_total else None
    occupancy_pct    = (occ_occ / occ_total * 100) if occ_total else None
    cost_burden_pct  = (burdened / burden_tot * 100) if burden_tot else None
    rent_to_afford   = (rent / max_afford) if (rent and max_afford) else None
    pop_growth_pct   = (((pop - baseline_pop) / baseline_pop * 100)
                        if (pop and baseline_pop) else None)

    return {
        "median_hh_income":   income,
        "median_gross_rent":  rent,
        "median_home_value":  home_value,
        "median_age":         g("median_age"),
        "population":         pop,
        "households":         ten_total,   # occupied units ≈ households
        "population_baseline": baseline_pop,
        "pop_growth_pct":     pop_growth_pct,
        "renter_share_pct":   renter_share_pct,
        "occupancy_pct":      occupancy_pct,
        "cost_burden_pct":    cost_burden_pct,
        "max_affordable_rent": max_afford,
        "rent_to_afford":     rent_to_afford,
    }


def _build() -> pd.DataFrame:
    """Hit the Census API for every submarket + county and assemble a frame."""
    key = _census_key()
    detail_vars  = list(ACS_VARS.values())
    profile_vars = list(ACS_PROFILE_VARS.values())
    records = []

    areas = ([{**s, "tier": "submarket"} for s in MARKET_SUBMARKETS] +
             [{**c, "tier": "county"}    for c in MARKET_COUNTIES])

    for area in areas:
        geo = area["geo"]
        raw      = _fetch_one(geo, detail_vars, ACS_YEAR, key)
        profile  = _fetch_one(geo, profile_vars, ACS_YEAR, key, profile=True)
        baseline = _fetch_one(geo, [ACS_VARS["population"]], ACS_BASELINE_YEAR, key)
        baseline_pop = _num(baseline.get(ACS_VARS["population"]))

        derived = _derive(raw, baseline_pop)
        rv_rate = _num(profile.get(ACS_PROFILE_VARS["rental_vacancy_rate"]))
        rv_moe  = _num(profile.get(ACS_PROFILE_VARS["rental_vacancy_moe"]))
        derived["rental_vacancy_rate"] = rv_rate
        derived["rental_vacancy_moe"]  = rv_moe
        # Low reliability when the MOE is as large as the estimate (incl. a 0%
        # estimate with nonzero MOE) — a small-sample artifact, not a true zero.
        derived["rental_vacancy_unreliable"] = bool(
            rv_rate is not None and rv_moe is not None
            and (rv_rate == 0 or rv_moe >= rv_rate)
        )
        records.append({
            "key":          area["key"],
            "label":        area["label"],
            "tier":         area["tier"],
            "screener_key": area.get("screener_key"),
            "census_name":  raw.get("NAME"),
            "acs_year":     ACS_YEAR,
            "baseline_year": ACS_BASELINE_YEAR,
            **derived,
        })

    return pd.DataFrame(records)


def load_market_metrics(refresh: bool = False) -> pd.DataFrame:
    """
    Return the assembled market-metrics frame, using the on-disk cache unless
    `refresh=True`. One row per submarket + county.
    """
    if _CACHE.exists() and not refresh:
        return pd.read_json(_CACHE, orient="records")

    df = _build()
    _CACHE.write_text(df.to_json(orient="records"))
    return df


# ── Municipal (all county-subdivisions per county) ─────────────────────────────
def _fetch_group(county_geo: dict, variables: list[str], year: int, key: str,
                 profile: bool = False) -> dict:
    """
    Fetch `variables` for EVERY county subdivision in a county in one call.
    Returns {geoid: {var: value, ...}} where geoid = state+county+cousub.
    """
    dataset = f"{ACS_DATASET}/profile" if profile else ACS_DATASET
    params = {
        "get": "NAME," + ",".join(variables),
        "for": "county subdivision:*",
        "in": f"state:{county_geo['state']} county:{county_geo['county']}",
        "key": key,
    }
    resp = requests.get(f"{CENSUS_BASE_URL}/{year}/{dataset}", params=params, timeout=45)
    resp.raise_for_status()
    rows = resp.json()
    header, data = rows[0], rows[1:]
    out = {}
    for values in data:
        rec = dict(zip(header, values))
        geoid = rec["state"] + rec["county"] + rec["county subdivision"]
        out[geoid] = rec
    return out


def _short_name(census_name: str) -> str:
    """'Grand Haven city, Ottawa County, Michigan' → 'Grand Haven city'."""
    return (census_name or "").split(",")[0].strip()


def _build_municipal() -> pd.DataFrame:
    """ACS metrics for every city/township in the four market counties."""
    key = _census_key()
    detail_vars  = list(ACS_VARS.values())
    profile_vars = list(ACS_PROFILE_VARS.values())
    pop_var      = ACS_VARS["population"]
    records = []

    for county in MARKET_COUNTIES:
        geo = county["geo"]
        detail   = _fetch_group(geo, detail_vars, ACS_YEAR, key)
        profile  = _fetch_group(geo, profile_vars, ACS_YEAR, key, profile=True)
        baseline = _fetch_group(geo, [pop_var], ACS_BASELINE_YEAR, key)

        for geoid, raw in detail.items():
            # Skip the "County subdivisions not defined" placeholder (water; ...00000).
            if geoid.endswith("00000") or "not defined" in (raw.get("NAME") or ""):
                continue
            base_pop = _num(baseline.get(geoid, {}).get(pop_var))
            derived = _derive(raw, base_pop)
            prof = profile.get(geoid, {})
            rv_rate = _num(prof.get(ACS_PROFILE_VARS["rental_vacancy_rate"]))
            rv_moe  = _num(prof.get(ACS_PROFILE_VARS["rental_vacancy_moe"]))
            derived["rental_vacancy_rate"] = rv_rate
            derived["rental_vacancy_moe"]  = rv_moe
            derived["rental_vacancy_unreliable"] = bool(
                rv_rate is not None and rv_moe is not None
                and (rv_rate == 0 or rv_moe >= rv_rate))
            records.append({
                "key":           geoid,
                "label":         _short_name(raw.get("NAME")),
                "tier":          "municipal",
                "county_key":    county["key"],
                "county_label":  county["label"],
                "census_name":   raw.get("NAME"),
                "acs_year":      ACS_YEAR,
                "baseline_year": ACS_BASELINE_YEAR,
                **derived,
            })
    return pd.DataFrame(records)


def load_municipal_metrics(refresh: bool = False) -> pd.DataFrame:
    """
    ACS metrics for every county subdivision (city/township) in the four market
    counties — one row per municipality, tagged with its `county_key`. Cached.
    """
    if _CACHE_MUNI.exists() and not refresh:
        return pd.read_json(_CACHE_MUNI, orient="records", dtype={"key": str})

    df = _build_municipal()
    _CACHE_MUNI.write_text(df.to_json(orient="records"))
    return df


if __name__ == "__main__":
    refresh = "--refresh" in sys.argv
    frame = load_market_metrics(refresh=refresh)
    pd.set_option("display.max_columns", None, "display.width", 200)
    cols = ["label", "tier", "median_hh_income", "max_affordable_rent",
            "median_gross_rent", "median_home_value", "rental_vacancy_rate",
            "cost_burden_pct", "renter_share_pct", "occupancy_pct", "pop_growth_pct"]
    print(frame[cols].to_string(index=False))
