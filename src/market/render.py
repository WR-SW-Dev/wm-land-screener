"""
Section 1 — Market Feasibility UI (Streamlit).

Pure rendering — no auth, no page config — so it can be exercised by a
standalone harness as well as embedded in app_shell. The shell supplies the
Executive/Analyst `view` string and an `on_continue` callback, used only as
an escape hatch if county data fails to load — Market Feasibility and Land
Screener are otherwise independent sections (no carry-forward as of
2026-08-03 — Land Screener's pipeline only covers 3 Ottawa-area cities,
far short of Market Feasibility's 4-county/109-municipality reach).

Executive view is two tiers:
  1. PRIMARY — a county choropleth shaded by housing *units needed* (from the
     Bowen HNAs), toggleable Total↔Rental and Raw↔per-1,000-households.
  2. DRILL-DOWN — click a county for its ACS demographics/affordability + the
     rental gap by income band. Ottawa additionally drills into its 3 screener
     submarkets shaded by the demand score (the "secondary scoring").

Public API:
    render_market(view: str, on_continue) -> None
"""
import hashlib
import math

import pandas as pd
import streamlit as st
import folium
import branca.colormap as cm
from branca.element import MacroElement, Template
from streamlit_folium import st_folium

from market.demographics import load_market_metrics, load_municipal_metrics
from market.market_scoring import add_demand_score
from market.boundaries import load_boundaries, load_municipal_boundaries, load_opportunity_zones
from market.housing_needs import load_housing_needs
from market import econ_dev
from market import competition
from market import fred as fred_data_mod
from market import zillow as zillow_data_mod
from market import census_bps as census_bps_mod
import config
from config import DEMAND_WEIGHTS

# Green → red ramp; more need = red. Reused for both maps (rescaled per use).
_RAMP = ["#1a9850", "#fee08b", "#d73027"]

# Demand-score TIER for the municipal drill-down map — replaces a raw 0–100
# score (which has no meaning on its own; "67" only makes sense compared to
# other places) with a plain-language tier + rank. Same green/amber/red as the
# heat map and the FRED momentum badge, so it reads as one visual language.
_TIER_COLORS = {"low": "#1a9850", "mod": "#b45309", "high": "#d73027"}
_TIER_LABELS = {"low": "Low demand", "mod": "Moderate demand", "high": "High demand"}


def _demand_tiers(county_df):
    """Rank (1=highest) + tier for each municipality, by TERTILE within this
    county's own list — relative to that county, not a fixed cutoff, so it
    can't drift stale the way the old fixed 35–60 color scale did. Returns
    {key: {"rank": int, "n": int, "tier": "low"/"mod"/"high"}}."""
    n = len(county_df)
    ranked = county_df.sort_values("demand_score", ascending=False).reset_index(drop=True)
    out = {}
    for i, row in ranked.iterrows():
        pct = i / n if n else 0
        tier = "high" if pct < 1 / 3 else ("mod" if pct < 2 / 3 else "low")
        out[str(row["key"])] = {"rank": i + 1, "n": n, "tier": tier}
    return out

# Plain-English explanation of each demand-score factor (keys match DEMAND_WEIGHTS).
_DEMAND_FACTOR_HELP = {
    "tightness":     "How tight the rental market is — a low rental vacancy rate "
                     "means few empty units, i.e. unmet demand. (Inverted: lower "
                     "vacancy → higher points.)",
    "cost_burden":   "Share of renters paying more than 30% of income on rent. "
                     "More cost-burdened renters = stronger need for attainable housing.",
    "growth":        "Population growth since the prior ACS sample. Faster growth "
                     "= more new households needing homes.",
    "renter_share":  "Share of occupied homes that are renter-occupied — the size "
                     "of the existing rental market WR-Dev would serve.",
    "rent_pressure": "Median rent relative to what local incomes can afford "
                     "(rent ÷ max affordable rent). Higher = rents are stretching households.",
}

# Fixed color-scale bounds per metric — (floor, ceiling). Deliberately NOT
# recomputed from whichever counties happen to be loaded; a relative min/max
# made the map a rank (one county always pinned red, one always green) instead
# of a measurement.
#
# FLOOR (fully green) is a "too little need to develop here" threshold, not 0 —
# no real market has zero need, so anchoring at 0 wasted the bottom of the ramp.
# Floors are set just below a soft-market baseline (normal replacement + modest
# growth). The meaningful floors are on the INTENSITY metrics (units needed per
# 1,000 existing households over 5 yrs), which normalize for county size; the
# raw-count floors are lighter, since a small county can have a low absolute
# count yet still be proportionally starved. A county below the floor clamps to
# green — that's the intended "skip it" signal, not a bug.
#
# CEILING (fully red) is calibrated just above the current 4-county maximum, so
# genuinely similar counties render as similar colors rather than being stretched
# across the full ramp. Jul 2026 actuals: intensity_total 132–148/1k HH,
# intensity_rental 35–46/1k HH, total_units 6.2k–33.9k, rental_units 1.9k–11.8k.
# As Phase 4 adds counties, figures should land inside these ceilings; if one
# exceeds a ceiling, `_scale_bounds()` widens it rather than silently clipping —
# treat that as a signal to revisit these constants.
_NEED_SCALE_BOUNDS = {
    "total_units":      (3_000, 36_000),
    "rental_units":     (1_000, 13_000),
    "intensity_total":  (60, 160),
    "intensity_rental": (20, 55),
}


def _scale_bounds(value_col, present):
    """Fixed (vmin, vmax) for value_col, ceiling widened only if data exceeds it."""
    vmin, vmax = _NEED_SCALE_BOUNDS.get(value_col, (0, 1))
    if present and max(present) > vmax:
        vmax = max(present)
    return vmin, vmax

# Friendly labels + formatters for the ACS metric grid.
_FMT = {
    "median_hh_income":    ("Median HH income",          lambda v: f"${v:,.0f}"),
    "max_affordable_rent": ("Max affordable rent (30%)", lambda v: f"${v:,.0f}/mo"),
    "median_gross_rent":   ("Median gross rent",         lambda v: f"${v:,.0f}/mo"),
    "median_home_value":   ("Median home value",         lambda v: f"${v:,.0f}"),
    "rental_vacancy_rate": ("Rental vacancy rate",       lambda v: f"{v:.1f}%"),
    "cost_burden_pct":     ("Cost-burdened renters",     lambda v: f"{v:.0f}%"),
    "renter_share_pct":    ("Renter share",              lambda v: f"{v:.0f}%"),
    "occupancy_pct":       ("Occupancy",                 lambda v: f"{v:.1f}%"),
    "pop_growth_pct":      ("Population growth",          lambda v: f"{v:+.1f}%"),
    "population":          ("Population",                 lambda v: f"{v:,.0f}"),
    "median_age":          ("Median age",                lambda v: f"{v:.0f}"),
}

# Column set + number formatting for the Analyst ACS tables (county + municipal).
_ACS_TABLE_COLS = ["label", "demand_score", "median_hh_income", "max_affordable_rent",
                   "median_gross_rent", "median_home_value", "rental_vacancy_rate",
                   "cost_burden_pct", "renter_share_pct", "occupancy_pct", "pop_growth_pct",
                   "median_age", "population"]
_ACS_TABLE_FMT = {
    "median_hh_income": "${:,.0f}", "max_affordable_rent": "${:,.0f}",
    "median_gross_rent": "${:,.0f}", "median_home_value": "${:,.0f}",
    "population": "{:,.0f}", "demand_score": "{:.1f}",
    "rental_vacancy_rate": "{:.1f}%", "cost_burden_pct": "{:.1f}%",
    "renter_share_pct": "{:.1f}%", "occupancy_pct": "{:.1f}%",
    "pop_growth_pct": "{:+.1f}%", "median_age": "{:.0f}",
}


def _acs_table(frame, name_label):
    """Styled ACS demographics table; `name_label` renames the label column."""
    disp = frame[_ACS_TABLE_COLS].rename(columns={"label": name_label})
    return disp.style.format(_ACS_TABLE_FMT, na_rep="—")


@st.cache_data(show_spinner="Loading ACS + housing-needs data…")
def _market_data():
    """Cached: scored ACS frame, county housing-needs frame, boundary FCs, the
    scored municipal (all city/township) frame + boundaries, Opportunity Zone
    tract polygons, FRED pricing/momentum data, ZORI rent-trend data, and
    ZHVI home-value-trend data."""
    df = add_demand_score(load_market_metrics())
    needs = load_housing_needs(df)
    bounds = load_boundaries()
    muni = add_demand_score(load_municipal_metrics())
    muni_bounds = load_municipal_boundaries()
    oz = load_opportunity_zones()
    try:
        fred = fred_data_mod.load_fred_data()
    except Exception as e:                       # noqa: BLE001
        print(f"  [warn] FRED data unavailable: {e}")
        fred = {"mortgage_rate": [], "state_hpi": [], "counties": {}}
    try:
        zori = zillow_data_mod.load_zori_data()
    except Exception as e:                       # noqa: BLE001
        print(f"  [warn] ZORI data unavailable: {e}")
        zori = {"counties": {}, "metros": {}}
    try:
        zhvi = zillow_data_mod.load_zhvi_data()
    except Exception as e:                       # noqa: BLE001
        print(f"  [warn] ZHVI data unavailable: {e}")
        zhvi = {"counties": {}, "state": []}
    return df, needs, bounds, muni, muni_bounds, oz, fred, zori, zhvi


def _fval(row, col):
    v = row.get(col)
    if v is None or (isinstance(v, float) and v != v):
        return "—"
    return _FMT[col][1](v)


# ── ACS metric grid (shared by county + submarket drill-downs) ─────────────────
def _metric_grid(row):
    rv_unreliable = bool(row.get("rental_vacancy_unreliable"))
    rv_display = _fval(row, "rental_vacancy_rate") + ("*" if rv_unreliable else "")

    c1, c2, c3, c3b = st.columns(4)
    c1.metric(_FMT["median_hh_income"][0],    _fval(row, "median_hh_income"))
    c2.metric(_FMT["max_affordable_rent"][0], _fval(row, "max_affordable_rent"),
              help="Median HH income ÷ 12 × 30% — what this market can afford monthly.")
    c3.metric(_FMT["median_gross_rent"][0],   _fval(row, "median_gross_rent"))
    c3b.metric(_FMT["median_home_value"][0],  _fval(row, "median_home_value"),
               help="Median value of owner-occupied homes (ACS) — not the same as "
                    "the Zillow ZHVI figure shown in Market Pricing below.")

    c4, c5, c6 = st.columns(3)
    c4.metric(_FMT["rental_vacancy_rate"][0], rv_display,
              help="Lower = tighter rental market = stronger BTR demand.")
    c5.metric(_FMT["cost_burden_pct"][0],     _fval(row, "cost_burden_pct"),
              help="Renters paying >30% of income on rent.")
    c6.metric(_FMT["pop_growth_pct"][0],      _fval(row, "pop_growth_pct"))

    c7, c8, c9 = st.columns(3)
    c7.metric(_FMT["renter_share_pct"][0], _fval(row, "renter_share_pct"))
    c8.metric(_FMT["occupancy_pct"][0],    _fval(row, "occupancy_pct"))
    c9.metric(_FMT["population"][0],        _fval(row, "population"))

    if rv_unreliable:
        moe = row.get("rental_vacancy_moe")
        moe_txt = f" (±{moe:.1f} pts)" if moe is not None else ""
        st.caption(f"\\* Rental vacancy rate has a wide ACS margin of error{moe_txt} "
                   f"relative to the estimate — small-sample artifact. Read as "
                   f"*approximate / very tight*, not an exact figure.")


# ── Development-signal pins (economic-development overlay) ─────────────────────
def _feature_center(feat):
    """(lat, lon) center of a GeoJSON feature, from its bounding box."""
    (s, w), (n, e) = _bbox_of_features([feat])
    return (s + n) / 2, (w + e) / 2


def econ_pins(county_key, muni_bounds, county_bounds):
    """
    Locations for approved econ-dev / market-signal items. Geocoded internally
    by matching the analyst-entered city to a municipal polygon's center (no
    external geocoder); falls back to the county center. `county_key=None` =
    all counties.
    """
    muni_by_county = {}
    for f in muni_bounds["features"]:
        muni_by_county.setdefault(f["properties"].get("county_key"), []).append(f)
    county_center = {f["properties"]["key"]: _feature_center(f)
                     for f in county_bounds["features"]
                     if f["properties"]["tier"] == "county"}

    pins = []
    for v in econ_dev.load_queue().values():
        if v.get("status") != "approved":
            continue
        ck = v["county_key"]
        if county_key and ck != county_key:
            continue
        loc, city = None, (v.get("city") or "").strip().lower()
        if city:
            for f in muni_by_county.get(ck, []):
                lbl = (f["properties"].get("label") or "").lower()
                if city in lbl or lbl.split(" ")[0] in city:
                    loc = _feature_center(f)
                    break
        loc = loc or county_center.get(ck)
        if loc is None:
            continue
        pins.append({"lat": loc[0], "lon": loc[1],
                     "label": (v.get("employer") or v["title"][:40]),
                     "category": v.get("category", econ_dev.DEFAULT_CATEGORY),
                     "jobs": v.get("jobs"), "investment": v.get("investment_musd"),
                     "link": v["link"]})
    return pins


def _fmt_musd(m):
    """Format a $-millions value: $1.4B / $836M / $13.5M / — ."""
    if not m:
        return "—"
    if m >= 1000:
        return f"${m/1000:.1f}B"
    if m == int(m):
        return f"${int(m):,}M"
    return f"${m:.1f}M"


# Marker icon/color per category — a briefcase doesn't read as "new retail" or
# "water main expansion", so each category gets its own look on the map.
_PIN_STYLE = {
    "employer":    ("briefcase",     "cadetblue"),
    "retail":      ("shopping-cart", "orange"),
    "water_sewer": ("tint",          "blue"),
    "parks":       ("tree",          "green"),
}


def _add_pins(m, pins):
    """Drop standard teardrop map pins, styled by category; details show in
    the click popup (no always-on text on the map)."""
    for p in pins:
        icon_name, color = _PIN_STYLE.get(p["category"], _PIN_STYLE["employer"])
        j = p["jobs"]
        has_jobs = isinstance(j, (int, float)) and j == j
        popup = (f"<b>{p['label']}</b><br>"
                 + (f"+{int(j):,} projected jobs<br>" if has_jobs and j else "")
                 + (f"{_fmt_musd(p['investment'])} investment<br>" if p.get("investment") else "")
                 + f'<a href="{p["link"]}" target="_blank">Read article →</a>')
        folium.Marker(
            [p["lat"], p["lon"]],
            icon=folium.Icon(color=color, icon=icon_name, prefix="fa"),
            tooltip=p["label"],
            popup=folium.Popup(popup, max_width=260),
        ).add_to(m)


def _render_pins_summary(pins):
    """Summary box of total investment (+ projected jobs, where reported) from
    kept projects across all development-signal categories."""
    if not pins:
        st.caption("No development signals to pin yet — approve items in the "
                   "review inbox, then fill in city / investment in the Analyst view.")
        return
    jobs = sum(int(p["jobs"]) for p in pins
               if isinstance(p["jobs"], (int, float)) and p["jobs"] == p["jobs"])
    inv = sum(float(p["investment"]) for p in pins if p.get("investment"))
    inv_txt = _fmt_musd(inv)
    jobs_txt = f" &nbsp;·&nbsp; <b>+{jobs:,}</b> projected jobs" if jobs else ""
    st.markdown(
        f'<div style="background:#f5f6f4;border-left:5px solid #779FA1;'
        f'border-radius:8px;padding:10px 14px;margin:2px 0 10px 0;">'
        f'<span style="color:#2c3e3f;font-weight:700;">Pinned development signals</span>'
        f'&nbsp;—&nbsp; {len(pins)} project(s) &nbsp;·&nbsp; '
        f'<b>{inv_txt}</b> investment{jobs_txt}</div>',
        unsafe_allow_html=True)


# ── Competition-mapping pins (competing residential/BTR projects) ──────────────
# Marker color by stage — a light-to-dark progression mirrors "how far along"
# the project is (matches the teal intensity ramp used in data-review tooling).
# Available folium/AwesomeMarkers colors only, so this approximates the ramp.
_STAGE_PIN_COLOR = {
    "proposed": "lightblue",
    "planned": "cadetblue",
    "under_construction": "blue",
    "lease_up": "darkblue",
    "existing": "black",
}


def competition_pins(muni_bounds):
    """
    Locations for approved competition-mapping records. Geocodes the real
    street address via competition.geocode_address() when possible; falls
    back to the submarket's municipal-center point (same label-matching
    approach as econ_pins) when the address is missing or won't geocode.
    """
    ottawa_munis = [f for f in muni_bounds["features"]
                    if f["properties"].get("county_key") == "ottawa"]
    fallback_center = {}
    for f in ottawa_munis:
        lbl = (f["properties"].get("label") or "").lower()
        if lbl:
            fallback_center[lbl] = _feature_center(f)

    pins = []
    for v in competition.load_queue().values():
        if v.get("status") != "approved":
            continue
        addr = (v.get("address") or "").strip()
        sm_label = v.get("submarket_label") or ""
        loc = competition.geocode_address(addr, sm_label) if addr else None
        if loc is None:
            sm_lower = sm_label.lower()
            for lbl, center in fallback_center.items():
                if sm_lower and (sm_lower in lbl or lbl.split(" ")[0] in sm_lower):
                    loc = center
                    break
        if loc is None:
            continue
        pins.append({
            "lat": loc[0], "lon": loc[1],
            "label": v.get("project_name") or v.get("title", "")[:40] or "Untitled project",
            "stage": v.get("stage", competition.DEFAULT_STAGE),
            "is_direct_competitor": bool(v.get("is_direct_competitor")),
            "total_units": v.get("total_units"),
            "builder": v.get("builder") or "",
            "address": addr,
            "link": v.get("link") or "",
            "effective_rent": v.get("effective_rent"),
            "occupancy_pct": v.get("occupancy_pct"),
            "avg_sqft": v.get("avg_sqft"),
            "year_built": v.get("year_built"),
        })
    return pins


def _add_competition_pins(m, pins):
    """Drop pins for competing projects, colored by stage; a star marks the
    named direct competitor (Allen Edwin/CopperBay) instead of the generic
    home icon."""
    for p in pins:
        color = _STAGE_PIN_COLOR.get(p["stage"], _STAGE_PIN_COLOR["proposed"])
        icon_name = "star" if p["is_direct_competitor"] else "home"
        units = p.get("total_units")
        stage_label = competition.STAGES.get(p["stage"], p["stage"])
        rent = p.get("effective_rent")
        occ = p.get("occupancy_pct")
        sqft = p.get("avg_sqft")
        year = p.get("year_built")
        popup = (f"<b>{p['label']}</b><br>"
                 f"{stage_label}<br>"
                 + (f"{units} units<br>" if units not in (None, "") else "")
                 + (f"Effective rent: ${rent:,.0f}/mo<br>" if rent not in (None, "") else "")
                 + (f"Occupancy: {occ:.1f}%<br>" if occ not in (None, "") else "")
                 + (f"Avg {sqft:,.0f} sq ft<br>" if sqft not in (None, "") else "")
                 + (f"Built {int(year)}<br>" if year not in (None, "") else "")
                 + (f"Builder: {p['builder']}<br>" if p.get("builder") else "")
                 + (f"{p['address']}<br>" if p.get("address") else "")
                 + (f'<a href="{p["link"]}" target="_blank">Read article →</a>'
                    if p.get("link") else ""))
        folium.Marker(
            [p["lat"], p["lon"]],
            icon=folium.Icon(color=color, icon=icon_name, prefix="fa"),
            tooltip=p["label"],
            popup=folium.Popup(popup, max_width=260),
        ).add_to(m)


def _render_competition_summary(pins):
    """Summary box: total projects, stage breakdown, and a direct-competitor
    (Allen Edwin/CopperBay) callout — colored consistently with the heat map
    (this is informational, not a need/opportunity signal, so it stays neutral)."""
    if not pins:
        st.caption("No competition-mapping projects to pin yet — approve items "
                   "in the Analyst view, or check that addresses are filled in.")
        return
    by_stage = {k: 0 for k in competition.STAGES}
    units_by_stage = {k: 0 for k in competition.STAGES}
    for p in pins:
        by_stage[p["stage"]] = by_stage.get(p["stage"], 0) + 1
        units = p.get("total_units")
        if isinstance(units, (int, float)) and units == units:
            units_by_stage[p["stage"]] = units_by_stage.get(p["stage"], 0) + units
    direct = sum(1 for p in pins if p["is_direct_competitor"])

    def _stage_txt(k, label):
        n, u = by_stage[k], units_by_stage[k]
        unit_part = f" ({int(u):,} units)" if u else ""
        return f"{n} {label}{unit_part}"

    stage_txt = " &nbsp;·&nbsp; ".join(
        _stage_txt(k, v) for k, v in competition.STAGES.items() if by_stage[k])
    direct_txt = (f' &nbsp;·&nbsp; <b style="color:#8a4a17;">★ {direct} direct '
                  f'competitor project(s)</b>' if direct else "")
    st.markdown(
        f'<div style="background:#f5f6f4;border-left:5px solid #5a8a8c;'
        f'border-radius:8px;padding:10px 14px;margin:2px 0 10px 0;">'
        f'<span style="color:#2c3e3f;font-weight:700;">Competition pipeline</span>'
        f'&nbsp;—&nbsp; {len(pins)} project(s) &nbsp;·&nbsp; {stage_txt}{direct_txt}</div>',
        unsafe_allow_html=True)


# ── County heat map (PRIMARY) ──────────────────────────────────────────────────
_TIER_LEGEND_TEMPLATE = """
{% macro html(this, kwargs) %}
<div style="position: fixed; top: 80px; right: 10px; z-index: 9999;
            background: #ffffff; padding: 10px 14px; border-radius: 8px;
            box-shadow: 0 1px 4px rgba(0,0,0,0.25); font-family: sans-serif;
            font-size: 13px; color: #2c3e3f; line-height: 1.9;">
  <div style="font-weight:600; margin-bottom:4px;">Housing-need demand</div>
  <div><span style="display:inline-block;width:9px;height:9px;border-radius:50%;
       background:#d73027;margin-right:6px;"></span>High demand (top third)</div>
  <div><span style="display:inline-block;width:9px;height:9px;border-radius:50%;
       background:#b45309;margin-right:6px;"></span>Moderate demand (middle third)</div>
  <div><span style="display:inline-block;width:9px;height:9px;border-radius:50%;
       background:#1a9850;margin-right:6px;"></span>Low demand (bottom third)</div>
</div>
{% endmacro %}
"""


def _add_tier_legend(m):
    """Discrete Low/Moderate/High legend, replacing the old continuous
    colorbar — tiers are ranked within each county, not a fixed score cutoff."""
    legend = MacroElement()
    legend._template = Template(_TIER_LEGEND_TEMPLATE)
    m.get_root().add_child(legend)


def _add_opportunity_zones(m, oz_fc):
    """Outline-only overlay (no fill) so it doesn't compete with the choropleth
    shading underneath — reads as "this area is also an Opportunity Zone"."""
    if not oz_fc or not oz_fc.get("features"):
        return
    folium.GeoJson(
        oz_fc,
        name="Opportunity Zones",
        style_function=lambda _f: {
            "fillOpacity": 0, "color": "#3d2b00", "weight": 3, "dashArray": "5,4",
        },
        tooltip=folium.GeoJsonTooltip(fields=["tract"], aliases=["Opportunity Zone tract:"]),
    ).add_to(m)


def _build_county_map(bounds, needs, value_col, caption, pins=None, oz_fc=None):
    """Choropleth of the four counties shaded by the chosen units-needed metric."""
    counties = [f for f in bounds["features"] if f["properties"]["tier"] == "county"]
    vals = needs.set_index("key")
    present = [v for k, v in vals[value_col].items() if v is not None]
    vmin, vmax = _scale_bounds(value_col, present)
    cmap = cm.LinearColormap(_RAMP, vmin=vmin, vmax=vmax, caption=caption)

    for f in counties:
        k = f["properties"]["key"]
        rec = vals.loc[k] if k in vals.index else None
        f["properties"]["value"] = float(rec[value_col]) if rec is not None and rec[value_col] is not None else 0.0
        # Formatted strings (with thousands separators) for the hover tooltip.
        f["properties"]["total_units"]  = f"{int(rec['total_units']):,}"  if rec is not None else "—"
        f["properties"]["rental_units"] = f"{int(rec['rental_units']):,}" if rec is not None else "—"

    m = folium.Map(location=[43.05, -85.9], zoom_start=8,
                   tiles="cartodbpositron", control_scale=True)
    folium.GeoJson(
        {"type": "FeatureCollection", "features": counties},
        name="Counties",
        style_function=lambda f: {
            "fillColor": cmap(f["properties"]["value"]),
            "color": "#2c3e3f", "weight": 1.5, "fillOpacity": 0.72,
        },
        highlight_function=lambda _f: {"weight": 3, "color": "#779FA1",
                                       "fillOpacity": 0.85},
        # Reverted from a click-to-open popup back to a plain hover tooltip —
        # two attempts at a "sticky" on-map popup (GeoJsonPopup, then a
        # per-feature Popup with show=True) both failed in the live app
        # despite passing direct HTML-generation checks, matching the same
        # folium/streamlit-folium click-reliability fragility already
        # documented elsewhere in this app. Not worth continuing to fight —
        # the working "Go to municipal breakdown" button + county data panel
        # below the map (a separate piece, unaffected by this) already
        # covers the same need.
        tooltip=folium.GeoJsonTooltip(
            fields=["label", "total_units", "rental_units"],
            aliases=["County:", "Total units needed:", "Rental units needed:"],
        ),
    ).add_to(m)
    cmap.add_to(m)
    if pins:
        _add_pins(m, pins)
    _add_opportunity_zones(m, oz_fc)
    return m


def _render_rental_by_income(county_key, needs_raw):
    """Bar chart + table of the county's housing gap by AMI band — toggle
    between Rental, For-sale, and Total (both combined). Combining is exact,
    not approximate: each county's report bands rental and for-sale demand
    into the SAME AMI cutoffs (see housing_needs.py docstring), so summing
    matching bands is a legitimate "total units needed" breakdown, not a
    mismatched-bucket estimate."""
    import altair as alt
    from market.housing_needs import HOUSING_NEEDS
    record = HOUSING_NEEDS[county_key]
    rental_bands = record["rental_by_income"]
    forsale_bands = record.get("forsale_by_income")
    order = [b["ami"] for b in rental_bands]

    st.markdown("**Units needed by income band**")
    if forsale_bands:
        tenure = st.radio("View", ["Rental", "For-sale", "Total"], horizontal=True,
                          key=f"tenure_{county_key}", label_visibility="collapsed")
    else:
        tenure = "Rental"  # graceful fallback if a future county lacks for-sale-by-income data

    rdf = pd.DataFrame(rental_bands).rename(columns={"units": "rental_units"})
    if forsale_bands:
        fdf = pd.DataFrame(forsale_bands).rename(columns={"units": "forsale_units"})
        bdf = rdf.merge(fdf, on="ami", how="outer")
    else:
        bdf = rdf.assign(forsale_units=0, price=None)
    bdf["total_units"] = bdf["rental_units"] + bdf["forsale_units"]

    col, y_title = {
        "Rental":   ("rental_units", "Rental units needed"),
        "For-sale": ("forsale_units", "For-sale units needed"),
        "Total":    ("total_units", "Total units needed"),
    }[tenure]
    # Each band's share of the currently-selected view's total, for the
    # on-bar label. Worded "X% of need" (never a bare "%") so it can't be
    # confused with the x-axis's AMI-band percentages, which are unrelated.
    bdf["pct_label"] = (bdf[col] / bdf[col].sum() * 100).round(0).astype(int).astype(str) + "% of need"

    tooltip = [alt.Tooltip("ami", title="% of median income")]
    if tenure == "Rental":
        tooltip.append(alt.Tooltip("rent", title="Monthly rent"))
    elif tenure == "For-sale":
        tooltip.append(alt.Tooltip("price", title="Price point"))
    else:
        tooltip += [alt.Tooltip("rent", title="Monthly rent (rental)"),
                    alt.Tooltip("price", title="Price point (for-sale)")]
    tooltip.append(alt.Tooltip(f"{col}:Q", title="Units needed", format=","))

    bars = (
        alt.Chart(bdf)
        .mark_bar(color="#779FA1")
        .encode(
            x=alt.X("ami:N", sort=order, title="% of area median income",
                    axis=alt.Axis(labelAngle=0, labelLimit=1000, labelPadding=6)),
            y=alt.Y(f"{col}:Q", title=y_title,
                    scale=alt.Scale(domain=[0, bdf[col].max() * 1.15])),
            tooltip=tooltip,
        )
        .properties(height=240)
    )
    labels = bars.mark_text(dy=-8, color="#555", fontSize=11).encode(text="pct_label:N")
    chart = bars + labels

    if tenure == "Total":
        tbl = (bdf.rename(columns={"ami": "% of median income", "rental_units": "Rental units",
                                   "forsale_units": "For-sale units", "total_units": "Total units"})
                  [["% of median income", "Rental units", "For-sale units", "Total units"]]
                  .style.format({"Rental units": "{:,.0f}", "For-sale units": "{:,.0f}",
                                 "Total units": "{:,.0f}"}))
    else:
        range_col, range_label = ("rent", "Monthly rent") if tenure == "Rental" else ("price", "Price point")
        tbl = (bdf.rename(columns={"ami": "% of median income", range_col: range_label,
                                   col: "Units needed"})
                  [["% of median income", range_label, "Units needed"]]
                  .style.format({"Units needed": "{:,.0f}"}))

    # Side by side, centered: equal spacer columns keep the pair off the edges
    # and each element ~40% wide (readable, not stretched across the screen).
    _, c_chart, c_table, _ = st.columns([1, 4, 4, 1])
    c_chart.altair_chart(chart, use_container_width=True,
                         key=f"income_band_chart_{county_key}")
    c_table.dataframe(tbl, use_container_width=True, hide_index=True)


_BPS_CATEGORIES = [
    ("Single-family", "sf", 0, "#2a78d6"),
    ("Duplex–quad", "mid", 1, "#eb6834"),
    ("Multifamily", "mf", 2, "#1baf7a"),
]


def _pct_axis_bounds(values):
    """(domain, tick_values) for a signed-percentage axis — rounded out to nice
    round steps, always including 0, targeting ~4 ticks so the labels stay
    readable in a short panel. Returned explicitly rather than left to Vega's
    inference: see the y-scale note in _render_permits_split_chart."""
    vals = [float(v) for v in values]
    lo, hi = min(vals + [0.0]), max(vals + [0.0])
    step = 5
    for step in (5, 10, 20, 25, 50, 100, 200, 500):
        lo_b = -(math.ceil(abs(lo) / step) * step) if lo < 0 else 0
        hi_b = (math.ceil(hi / step) * step) if hi > 0 else 0
        if hi_b == lo_b:                     # all-zero data — give it real height
            hi_b = lo_b + step
        if (hi_b - lo_b) / step <= 5:
            break
    ticks, v = [], lo_b
    while v <= hi_b + 1e-9:
        ticks.append(int(round(v)))
        v += step
    return [lo_b, hi_b], ticks


def _render_yoy_growth_chart(rows, chart_key, hollow_caption=None):
    """Shared year-over-year growth line — zero-line reference, blue/red
    up-down point coloring, and a hollow marker + dashed connector for a
    provisional (partial-year) final point. `rows` is a list of
    {year, pct, provisional} dicts. Used by the permits, rent, and
    home-value trend panels so all three read as one consistent visual
    language instead of three near-identical hand-copied charts.

    Every layer shares ONE explicit y encoding (same field, same pinned
    domain, same tick values) — this is load-bearing, not cosmetic: with the
    domain left to Vega to infer, this layered chart's shared y-scale could
    resolve to the zero-reference layer's degenerate [0, 0] domain instead
    of the union across layers, collapsing every point onto the zero line
    with a single "0" axis label. Pinning the domain makes that state
    unreachable."""
    import altair as alt

    gdf = pd.DataFrame(rows)
    gdf["sign"] = gdf["pct"].apply(lambda v: "up" if v >= 0 else "down")
    hist = gdf[~gdf["provisional"]]
    prov = gdf[gdf["provisional"]]
    color_scale = alt.Scale(domain=["up", "down"], range=["#2a78d6", "#e34948"])
    year_order = list(gdf["year"])

    y_domain, y_ticks = _pct_axis_bounds(gdf["pct"])
    y_enc = alt.Y(
        "pct:Q", title="YoY change",
        scale=alt.Scale(domain=y_domain, nice=False, zero=False, clamp=True),
        axis=alt.Axis(values=y_ticks, grid=True,
                      labelExpr="(datum.value > 0 ? '+' : '') + datum.value + '%'"),
    )
    x_enc = alt.X("year:N", title=None, sort=year_order)
    pct_tip = alt.Tooltip("pct:Q", title="YoY", format="+.1f")

    layers = [alt.Chart(pd.DataFrame({"pct": [0.0]}))
              .mark_rule(strokeDash=[4, 4], color="#c3c2b7").encode(y=y_enc)]
    if not hist.empty:
        layers.append(
            alt.Chart(hist).mark_line(color="#9ca3af", strokeWidth=2)
            .encode(x=x_enc, y=y_enc))
    if not prov.empty and not hist.empty:
        connect_df = pd.concat([hist.iloc[[-1]], prov])
        layers.append(
            alt.Chart(connect_df).mark_line(color="#9ca3af", strokeWidth=2, strokeDash=[4, 4])
            .encode(x=x_enc, y=y_enc))
    if not hist.empty:
        layers.append(
            alt.Chart(hist).mark_point(filled=True, size=90)
            .encode(x=x_enc, y=y_enc,
                   color=alt.Color("sign:N", scale=color_scale, legend=None),
                   tooltip=["year", pct_tip]))
    if not prov.empty:
        layers.append(
            alt.Chart(prov).mark_point(filled=False, size=90, strokeWidth=2)
            .encode(x=x_enc, y=y_enc,
                   color=alt.Color("sign:N", scale=color_scale, legend=None),
                   tooltip=["year", pct_tip]))
    # 150: the pinned axis renders real tick labels, and the rotated year
    # labels below need the extra room or Vega's fit-autosize squeezes the
    # plot area itself.
    chart = alt.layer(*layers).properties(height=150)
    st.altair_chart(chart, use_container_width=True, key=chart_key)
    if not prov.empty and hollow_caption:
        st.caption(hollow_caption)


# Census files each permit-issuing place under a SINGLE county even when the
# place straddles a county line, so a split municipality's units all land on
# one side. Holland is the case that matters for these four counties, and the
# skew is material rather than a rounding note — verified 2026-07-30 against
# both this project's municipal boundaries (Census place 38640 appears under
# Allegan AND Ottawa, ~48% of its land area on the Allegan side) and the raw
# BPS place files (exactly one Holland record exists, filed under Ottawa).
# Holland supplied 46-52% of Ottawa's 5+ unit units in 2022-2024.
_SPLIT_PLACE_NOTES = {
    "ottawa":
        "Holland is filed entirely under **Ottawa** by Census even though ~48% of "
        "the city's land area sits in Allegan County, and it supplied 46–52% of "
        "Ottawa's multifamily units in 2022–2024 — so multifamily here runs high "
        "relative to where the units physically are.",
    "allegan":
        "Holland is filed entirely under **Ottawa** by Census even though ~48% of "
        "the city's land area sits in Allegan County — apartment construction in "
        "southern Holland is credited to Ottawa, not here.",
}


def _render_permits_data_notes(county_key, annual):
    """Attribution caveats that stop this chart being misread — an empty
    multifamily band as missing data, and a county-line split as real geography.
    Both were checked against the raw Census place-level files rather than
    assumed; see the _SPLIT_PLACE_NOTES comment."""
    if annual and not any(a["mf"] for a in annual):
        st.caption(
            f"**No 5+ unit permits recorded anywhere in this county since "
            f"{min(a['year'] for a in annual)}** — the multifamily band is "
            f"genuinely empty, not missing data. Every township, city and village "
            f"here is in the Census survey and reporting, and smaller 2–4 unit "
            f"permits do come through, so this reads as an all-single-family "
            f"construction market."
        )
    note = _SPLIT_PLACE_NOTES.get(county_key)
    if note:
        st.caption(note)


def _render_permits_split_chart(county_key, annual, ytd, badge):
    """Stacked bar (single-family / duplex-quad / multifamily units permitted
    per year, Census BPS) + a year-over-year total-growth line underneath —
    two panels sharing one x-axis, not one dual-axis chart (a % line and a
    raw-unit-count bar chart are different scales; overlaying them on one
    y-axis invites false visual correlations). A provisional year-to-date
    bar/point (lighter fill / hollow marker) is appended when `ytd` is
    available, comparing the same partial period in both years rather than
    a partial year against a full one."""
    import altair as alt

    st.markdown("###### Permits trend (Census Building Permits Survey)")

    bar_rows = []
    for a in annual:
        for label, key, order, _ in _BPS_CATEGORIES:
            bar_rows.append({"year": str(a["year"]), "category": label,
                             "units": a[key], "order": order, "provisional": False})
    ytd_label = None
    if ytd:
        ytd_label = f"{ytd['as_of'][:4]} YTD"
        for label, key, order, _ in _BPS_CATEGORIES:
            bar_rows.append({"year": ytd_label, "category": label,
                             "units": ytd[key], "order": order, "provisional": True})
    bdf = pd.DataFrame(bar_rows)

    bar_chart = (
        alt.Chart(bdf)
        .mark_bar()
        .encode(
            x=alt.X("year:N", title=None, sort=None),
            y=alt.Y("units:Q", title="Units permitted"),
            order=alt.Order("order:Q"),
            color=alt.Color("category:N", title=None,
                            scale=alt.Scale(domain=[c[0] for c in _BPS_CATEGORIES],
                                            range=[c[3] for c in _BPS_CATEGORIES])),
            opacity=alt.condition("datum.provisional", alt.value(0.5), alt.value(1.0)),
            tooltip=["year", "category", alt.Tooltip("units:Q", title="Units", format=",.0f")],
        )
        .properties(height=220)
    )

    # Same pace-needed reference line as before, now drawn over the stacked
    # total (a rule's y-position doesn't care whether the bar below it is
    # flat or stacked) — clipped to the study-period years actually on this
    # chart. The line's right end is pinned to the last category actually on
    # the x-axis (the provisional YTD bar when present) rather than to the
    # last *full* study year, so it always reaches the edge of the plot
    # instead of stopping one bar short once a YTD bar is appended.
    years_present = sorted({r["year"] for r in bar_rows if not r["provisional"]}, key=int)
    study_years = [str(y) for y in range(badge["start_year"], badge["end_year"] + 1)] if badge else []
    chart_years = [y for y in years_present if y in study_years]
    year_order = years_present + ([ytd_label] if ytd_label else [])
    year_totals = {str(a["year"]): a["sf"] + a["mid"] + a["mf"] for a in annual}

    if badge and chart_years:
        line_end = ytd_label if ytd_label else chart_years[-1]
        rule = (
            alt.Chart(pd.DataFrame({
                "x_start": [chart_years[0]], "x_end": [line_end],
                "pace": [badge["annual_pace"]],
            }))
            .mark_rule(color="#9ca3af", strokeDash=[6, 4], size=2)
            .encode(x=alt.X("x_start:N", sort=year_order, bandPosition=0),
                    x2="x_end:N", y=alt.Y("pace:Q"))
        )
        bar_chart = bar_chart + rule

        # Gap-to-pace bracket: offset to the right of each year's bar (not
        # drawn through its fill) — two end-caps at the bar's actual total
        # and at the pace line, joined by a stem, with the numeric gap
        # printed beside it. One neutral gray throughout (matching the pace
        # line) — the +/- sign on the number itself already says whether
        # that year cleared pace, so the bracket doesn't need to repeat it
        # in color.
        gap_df = pd.DataFrame([
            {"year": y, "total": year_totals[y], "pace": badge["annual_pace"],
             "mid_y": (year_totals[y] + badge["annual_pace"]) / 2,
             "gap": year_totals[y] - badge["annual_pace"], "row": i % 2}
            for i, y in enumerate(chart_years)
        ])
        _BRACKET_GRAY = "#9ca3af"
        _LABEL_GRAY = "#6b7280"
        gap_tooltip = ["year",
                       alt.Tooltip("total:Q", title="Permitted", format=",.0f"),
                       alt.Tooltip("pace:Q", title="Pace needed", format=",.0f"),
                       alt.Tooltip("gap:Q", title="Gap", format="+,.0f")]
        # Positioned via bandPosition (a fraction of each category's own
        # band width) rather than a fixed pixel xOffset — a fixed-pixel
        # offset doesn't shrink with the chart on a narrower screen, so at
        # some point the label runs into the bracket (or the next bar).
        # bandPosition scales with the band itself, so the gap stays
        # proportional at any container width.
        bracket_x = alt.X("year:N", sort=year_order, bandPosition=0.82)
        label_x = alt.X("year:N", sort=year_order, bandPosition=0.82)
        stem = (
            alt.Chart(gap_df).mark_rule(strokeWidth=2, color=_BRACKET_GRAY)
            .encode(x=bracket_x, y="total:Q", y2="pace:Q", tooltip=gap_tooltip)
        )
        cap_total = (
            alt.Chart(gap_df).mark_tick(thickness=2, size=12, color=_BRACKET_GRAY)
            .encode(x=bracket_x, y="total:Q", tooltip=gap_tooltip)
        )
        cap_pace = (
            alt.Chart(gap_df).mark_tick(thickness=2, size=12, color=_BRACKET_GRAY)
            .encode(x=bracket_x, y="pace:Q", tooltip=gap_tooltip)
        )
        # Neighboring years' brackets sit only one band-width apart, and a
        # 6-character number can be wider than that band once the chart is
        # narrow enough — no horizontal offset fixes that, since the text's
        # pixel width doesn't shrink with the container. Staggering
        # consecutive labels to alternating heights keeps them apart
        # vertically instead, which holds at any width.
        bar_chart = bar_chart + stem + cap_total + cap_pace
        for row, dy in {0: -6, 1: 20}.items():
            row_df = gap_df[gap_df["row"] == row]
            if row_df.empty:
                continue
            bar_chart = bar_chart + (
                alt.Chart(row_df)
                .mark_text(align="left", dx=4, dy=dy, fontSize=11,
                          fontWeight="bold", color=_LABEL_GRAY)
                .encode(x=label_x, y="mid_y:Q", text=alt.Text("gap:Q", format="+,.0f"))
            )
    st.altair_chart(bar_chart, use_container_width=True,
                    key=f"permits_bar_{county_key}")
    if badge and chart_years:
        st.caption(
            f"Dashed line = **{badge['annual_pace']:,.0f}/yr** pace needed to "
            f"close the gap within the {badge['study_period']} study period. "
            f"Brackets show each year's gap to that pace, in units."
        )
    if ytd_label:
        st.caption(f"Lighter bar = {ytd_label} (through {ytd['as_of'][5:]}/{ytd['as_of'][:4]}) — "
                   f"partial year, not directly comparable to the full-year bars beside it.")

    _render_permits_data_notes(county_key, annual)

    # ── Year-over-year growth of the total, as its own panel below ─────────
    totals = year_totals
    years_sorted = sorted(totals, key=int)
    growth_rows = []
    for i in range(1, len(years_sorted)):
        y0, y1 = years_sorted[i - 1], years_sorted[i]
        pct = (totals[y1] - totals[y0]) / totals[y0] * 100 if totals[y0] else None
        if pct is not None:
            growth_rows.append({"year": y1, "pct": pct, "provisional": False})
    if ytd and years_sorted:
        ytd_total = ytd["sf"] + ytd["mid"] + ytd["mf"]
        prior_total = ytd["prior_sf"] + ytd["prior_mid"] + ytd["prior_mf"]
        if prior_total:
            pct = (ytd_total - prior_total) / prior_total * 100
            growth_rows.append({"year": ytd_label, "pct": pct, "provisional": True})

    if growth_rows:
        st.caption("Year-over-year change, total units permitted")
        hollow_caption = (f"Hollow point = {ytd_label} vs the same months in the prior year "
                          f"(not a full-year comparison).") if ytd_label else None
        _render_yoy_growth_chart(growth_rows, f"permits_growth_{county_key}", hollow_caption)


def _render_market_pricing(county_key, needs_row, fred, zori=None, zhvi=None):
    """Market Pricing & Momentum (Zillow ZHVI/ZORI + Census BPS + FRED): home
    value trend, rent trend, and permits vs the HNA need. FRED still supplies
    the momentum badge's permits total and the mortgage-rate banner above
    this section — only the HPI panel itself was swapped for ZHVI (see
    config.py's ZHVI note for why)."""
    permit_rows = fred_data_mod.permits_recent(county_key, fred)
    badge = fred_data_mod.momentum_badge(county_key, fred, needs_row)
    value_metrics = zillow_data_mod.value_metrics(county_key, zhvi) if zhvi else None
    value_growth_rows = zillow_data_mod.value_yoy_frame(county_key, zhvi) if zhvi else []
    # Structure-type split (single-family / duplex-quad / multifamily) comes
    # straight from Census's own county BPS files, not FRED — confirmed live
    # 2026-07-30 that FRED's county permit series has no such breakdown for
    # any of these counties, only the single blended total used above for
    # the momentum badge (which stays total-based; the HNA need figure it's
    # compared against isn't split by structure type either).
    bps_annual = census_bps_mod.load_bps_data().get(county_key, {})
    annual = bps_annual.get("annual", [])
    ytd = bps_annual.get("ytd")

    if not value_metrics and not permit_rows and not annual:
        return  # no Zillow/FRED/Census data for this county — skip the section quietly

    st.markdown("##### Market pricing & momentum")
    st.caption("Is the market already responding to this need? Home-value "
               "appreciation and building-permit activity (Zillow / Census).")
    left, right = st.columns(2)

    with left:
        st.markdown("###### Home value trend (Zillow Home Value Index)")
        if value_metrics:
            v1, v2, v3 = st.columns(3)
            v1.metric("Typical home value", f"${value_metrics['latest_value']:,.0f}",
                      help=f"ZHVI, {value_metrics['latest_year']}. Zillow's own smoothed, "
                           "seasonally adjusted estimate for the middle tier of homes — "
                           "a valuation model, not a recorded sale price.")
            v2.metric("Value YoY", f"{value_metrics['yoy_pct']:+.1f}%",
                      help=f"ZHVI, {value_metrics['latest_year']} vs prior year.")
            v3.metric("vs Michigan", f"{value_metrics['vs_state_delta']:+.1f} pts"
                      if value_metrics["vs_state_delta"] is not None else "—",
                      help="This county's YoY value growth minus Michigan's YoY value growth.")

        if len(value_growth_rows) >= 2:
            hollow_caption = (f"Hollow point = {value_growth_rows[-1]['year']}, based on a "
                              f"partial year of data (not directly comparable to the "
                              f"full-year points beside it).") if value_growth_rows[-1]["provisional"] else None
            _render_yoy_growth_chart(value_growth_rows, f"value_growth_{county_key}", hollow_caption)
        elif value_metrics and value_metrics["n_years"] < 3:
            yr_word = "year" if value_metrics["n_years"] == 1 else "years"
            st.caption(f"{needs_row['label']}'s home-value index only has "
                       f"{value_metrics['n_years']} {yr_word} of annual data so far — "
                       f"not enough yet for a year-over-year growth chart.")
        if value_metrics or value_growth_rows:
            st.caption("Data provided by Zillow Group.")

        rent_metrics = zillow_data_mod.rent_metrics(county_key, zori) if zori else None
        rent_growth_rows = zillow_data_mod.rent_yoy_frame(county_key, zori) if zori else []
        if rent_metrics or rent_growth_rows:
            st.write("")
            st.markdown("###### Rent trend (Zillow Observed Rent Index)")
            if rent_metrics:
                r1, r2 = st.columns(2)
                r1.metric("Typical rent", f"${rent_metrics['latest_value']:,.0f}/mo",
                          help=f"ZORI asking rent, {rent_metrics['latest_year']}. This is "
                               "what a new resident would pay today.")
                r2.metric("Rent YoY", f"{rent_metrics['yoy_pct']:+.1f}%",
                          help=f"ZORI, {rent_metrics['latest_year']} vs prior year.")

            if len(rent_growth_rows) >= 2:
                hollow_caption = (f"Hollow point = {rent_growth_rows[-1]['year']}, based on a "
                                  f"partial year of data (not directly comparable to the "
                                  f"full-year points beside it).") if rent_growth_rows[-1]["provisional"] else None
                _render_yoy_growth_chart(rent_growth_rows, f"rent_growth_{county_key}", hollow_caption)
            elif rent_metrics and rent_metrics["n_years"] < 3:
                yr_word = "year" if rent_metrics["n_years"] == 1 else "years"
                st.caption(f"{needs_row['label']}'s rent index only has "
                           f"{rent_metrics['n_years']} {yr_word} of annual data so far — "
                           f"not enough yet for a year-over-year growth chart.")
            st.caption("Data provided by Zillow Group.")

    with right:
        if annual:
            _render_permits_split_chart(county_key, annual, ytd, badge)
        elif permit_rows:
            # Census BPS unavailable for this county but FRED's blended total
            # still is — fall back to the old single-series bar rather than
            # showing nothing.
            pdf = pd.DataFrame(permit_rows)
            pdf["year"] = pdf["date"].str[:4]
            pchart = (
                alt.Chart(pdf)
                .mark_bar(color="#779FA1")
                .encode(
                    x=alt.X("year:N", title=None),
                    y=alt.Y("value:Q", title="New housing units permitted"),
                    tooltip=["year", alt.Tooltip("value:Q", title="Units", format=",.0f")],
                )
                .properties(height=220)
            )
            st.altair_chart(pchart, use_container_width=True,
                            key=f"permits_fallback_{county_key}")
        if badge:
            dot = {"red": "🔴", "yellow": "🟡", "green": "🟢"}[badge["color"]]
            st.markdown(f"{dot} **{badge['label']}** — permits issued in "
                        f"{badge['study_period']} so far cover "
                        f"**{badge['pct']:.0f}%** of the {badge['total_need']:,.0f}-unit gap "
                        f"({badge['cumulative_permits']:,.0f} units permitted).")
            st.caption("Red = still well below need (opportunity). Yellow = "
                       "responding. Green = permits have largely caught up to "
                       "the need — matches the heat map's red=more-need convention.")


def _render_county_drilldown(county_key, needs, acs_df, fred=None, zori=None, zhvi=None):
    needs_row = needs.set_index("key").loc[county_key]
    st.markdown(f"#### {needs_row['label']} — housing need")
    st.caption(f"Source: {needs_row['report']}. Gap = new units needed over "
               f"{needs_row['study_period']}.")

    c1, c2, c3 = st.columns(3)
    c1.metric("Total units needed",   f"{needs_row['total_units']:,.0f}")
    c2.metric("Rental units needed",  f"{needs_row['rental_units']:,.0f}",
              help="WR-Dev builds build-to-rent — this is the directly relevant gap.")
    c3.metric("For-sale units needed", f"{needs_row['forsale_units']:,.0f}")
    if needs_row.get("households"):
        st.caption(f"Intensity: {needs_row['intensity_total']:.0f} total / "
                   f"{needs_row['intensity_rental']:.0f} rental units needed per "
                   f"1,000 existing households ({needs_row['households']:,.0f} households).")

    if fred:
        st.write("")
        _render_market_pricing(county_key, needs_row, fred, zori, zhvi)

    st.write("")
    _render_rental_by_income(county_key, needs)

    st.divider()
    st.markdown("##### Economic development")
    econ = econ_dev.summary_by_county().get(county_key)
    if econ and econ["projects"]:
        e1, e2, e3 = st.columns(3)
        e1.metric("Projected new jobs",
                  f"+{econ['jobs']:,}" if econ["jobs"] else "—",
                  help="Summed from the announcements you've kept for this county.")
        e2.metric("Announced projects", econ["projects"])
        e3.metric("Total investment", _fmt_musd(econ["investment_musd"]))
        tail = (f" from {econ['employers']} employer(s)" if econ["employers"] else "")
        st.caption(f"From your kept economic-development announcements{tail}. "
                   f"Add or edit the underlying items in the Analyst view.")
    else:
        st.caption("No kept economic-development announcements for this county yet — "
                   "run **Scan now** and review them in the Analyst view.")

    st.divider()
    st.markdown("##### Demographics & affordability")
    acs_match = acs_df[(acs_df["tier"] == "county") & (acs_df["key"] == county_key)]
    if not acs_match.empty:
        row = acs_match.iloc[0]
        st.caption(f"ACS 5-year {int(row['acs_year'])} ({row['census_name']}).")
        _metric_grid(row)
    else:
        st.info("County-level ACS metrics not available.")


# ── Municipal (city/township) sub-map — SECONDARY demand scoring ───────────────
def _bbox_of_features(features):
    """[[south, west], [north, east]] over a list of GeoJSON features, for zoom."""
    xs, ys = [], []
    def walk(coords):
        if isinstance(coords[0], (int, float)):
            xs.append(coords[0]); ys.append(coords[1])
        else:
            for c in coords:
                walk(c)
    for f in features:
        geom = f.get("geometry")
        if geom:
            walk(geom["coordinates"])
    return [[min(ys), min(xs)], [max(ys), max(xs)]]


def _build_municipal_map(muni_bounds, muni_df, county_key, pins=None,
                         competition_pins_list=None, oz_fc=None):
    """Choropleth of one county's municipalities, shaded by demand TIER (low/
    moderate/high, by tertile within this county), zoomed in."""
    feats = [f for f in muni_bounds["features"]
             if f["properties"].get("county_key") == county_key]
    keyed = muni_df.set_index(muni_df["key"].astype(str))
    label_by_key = keyed["label"].to_dict()
    tiers = _demand_tiers(muni_df)
    for f in feats:
        k = str(f["properties"]["key"])
        info = tiers.get(k, {"rank": None, "n": len(muni_df), "tier": "low"})
        # NOTE: named "demand_tier", not "tier" — "tier" is already used on this
        # same feature to mean the GEOGRAPHIC level ("municipal"/"county"/
        # "submarket", set in boundaries.py) and is what the map-click handler
        # below keys off of. Reusing "tier" here silently broke click-to-select
        # (the geographic marker got overwritten, so clicks never matched).
        f["properties"]["demand_tier"] = info["tier"]
        f["properties"]["tier_label"] = _TIER_LABELS[info["tier"]]
        # Boundary NAME is just the base name ("Grand Haven") and can't tell a
        # city from its township; use the ACS-derived label ("Grand Haven
        # city" vs "…charter township") so the tooltip matches the dropdown/detail.
        f["properties"]["label"] = label_by_key.get(k, f["properties"].get("label"))
        f["properties"]["rank_text"] = (f"{info['rank']} of {info['n']}"
                                        if info["rank"] else "—")

    m = folium.Map(tiles="cartodbpositron", control_scale=True)
    if feats:
        m.fit_bounds(_bbox_of_features(feats))
    folium.GeoJson(
        {"type": "FeatureCollection", "features": feats},
        name="Municipalities",
        style_function=lambda f: {
            "fillColor": _TIER_COLORS[f["properties"]["demand_tier"]],
            "color": "#2c3e3f", "weight": 1.0, "fillOpacity": 0.72,
        },
        highlight_function=lambda _f: {"weight": 3, "color": "#779FA1",
                                       "fillOpacity": 0.85},
        tooltip=folium.GeoJsonTooltip(
            fields=["label", "tier_label", "rank_text"],
            aliases=["Municipality:", "Demand:", "Rank in county:"]),
    ).add_to(m)
    _add_tier_legend(m)
    if pins:
        _add_pins(m, pins)
    if competition_pins_list:
        _add_competition_pins(m, competition_pins_list)
    _add_opportunity_zones(m, oz_fc)
    return m


def _render_place_detail(row):
    st.markdown(f"#### {row['label']} &nbsp;·&nbsp; demand score "
                f"**{row['demand_score']:.0f}**/100", unsafe_allow_html=True)
    st.caption(f"Source: ACS 5-year {int(row['acs_year'])} "
               f"({row['census_name']}). Growth vs ACS {int(row['baseline_year'])}.")
    _metric_grid(row)
    with st.expander("Demand-score breakdown"):
        st.caption("The 0–100 demand score is a weighted blend of five signals "
                   "(max points shown per factor). Higher = stronger housing demand.")
        for comp, weight in DEMAND_WEIGHTS.items():
            pts = row.get(f"pts_{comp}", 0) or 0
            st.write(f"**{comp.replace('_', ' ').title()}** — {pts:.1f} / {weight}")
            st.progress(pts / weight if weight else 0.0)
            help_txt = _DEMAND_FACTOR_HELP.get(comp)
            if help_txt:
                st.caption(help_txt)


def _render_municipalities(county_key, muni_df, muni_bounds, pins=None,
                           competition_pins_list=None, oz_fc=None):
    """Municipal demand-score heat map for one county + selected-place detail."""
    muni = muni_df[muni_df["county_key"] == county_key].reset_index(drop=True)
    if muni.empty:
        st.info("No municipal data for this county.")
        return

    map_out = st_folium(_build_municipal_map(muni_bounds, muni, county_key,
                                             pins=pins,
                                             competition_pins_list=competition_pins_list,
                                             oz_fc=oz_fc),
                        height=650, use_container_width=True,
                        key=f"muni_map_{county_key}",
                        returned_objects=["last_active_drawing"])
    clicked = (map_out or {}).get("last_active_drawing")
    clicked_key = None
    if clicked and clicked.get("properties", {}).get("tier") == "municipal":
        clicked_key = str(clicked["properties"]["key"])

    labels = muni.sort_values("demand_score", ascending=False)["label"].tolist()
    key_to_label = dict(zip(muni["key"].astype(str), muni["label"]))

    # Clicking a municipality on the map selects it. Streamlit ignores a
    # selectbox's `index` once its keyed state exists, so we instead write the
    # clicked place into the selectbox's session state — but only on a *new*
    # click (tracked via muni_lastclick), so the dropdown stays freely usable.
    sel_key  = f"muni_sel_{county_key}"
    last_key = f"muni_lastclick_{county_key}"
    if clicked_key and clicked_key != st.session_state.get(last_key):
        st.session_state[last_key] = clicked_key
        clicked_label = key_to_label.get(clicked_key)
        if clicked_label in labels:
            st.session_state[sel_key] = clicked_label
    st.session_state.setdefault(sel_key, labels[0])   # default = highest score

    sel_label = st.selectbox("Municipality (ranked by demand score)", labels,
                             key=sel_key)
    _render_place_detail(muni[muni["label"] == sel_label].iloc[0])
    return sel_label


# ── Compare view — any county or municipality against any other ────────────────
# Direction is fixed per metric (not a per-row UI toggle) — decided with the
# user 2026-07-29. "higher"/"lower" = which direction is favorable for the
# BASE entity; None = shown plainly, no color (home value and cost burden
# were deliberately left uncolored — each requires a two-hop inference to
# call "favorable" that isn't obvious from the number alone, unlike e.g.
# vacancy where lower→tighter→more BTR demand is a direct read). Population
# itself was dropped entirely — raw population variance isn't a meaningful
# comparison on its own.
_COMPARE_SECTIONS = [
    ("Affordability", [
        ("median_hh_income",    "Median HH income",          "dollar",    "higher"),
        ("max_affordable_rent", "Max affordable rent (30%)", "dollar_mo", "higher"),
        ("median_gross_rent",   "Median gross rent",         "dollar_mo", "higher"),
        ("median_home_value",   "Median home value",         "dollar",    None),
    ]),
    ("Rental market", [
        ("rental_vacancy_rate", "Rental vacancy rate",   "pct1", "lower"),
        ("cost_burden_pct",     "Cost-burdened renters", "pct0", None),
        ("renter_share_pct",    "Renter share",          "pct0", "higher"),
    ]),
    ("Growth", [
        ("pop_growth_pct", "Population growth", "pct1", "higher"),
    ]),
]

_CMP_FAVORABLE_COLOR   = "#1a9850"   # same green as the "low demand" tier
_CMP_UNFAVORABLE_COLOR = "#d73027"   # same red as the "high demand" tier
_CMP_NEUTRAL_COLOR     = "#6b7280"


def _cmp_fmt_value(kind, v):
    if v is None or (isinstance(v, float) and v != v):
        return "—"
    if kind == "dollar":    return f"${v:,.0f}"
    if kind == "dollar_mo": return f"${v:,.0f}/mo"
    if kind == "pct1":      return f"{v:.1f}%"
    if kind == "pct0":      return f"{v:.0f}%"
    if kind == "int":       return f"{v:,.0f}"
    return str(v)


def _cmp_fmt_delta(kind, d):
    if d is None or (isinstance(d, float) and d != d):
        return "—"
    sign, ad = ("+" if d >= 0 else "-"), abs(d)
    if kind in ("dollar", "dollar_mo"): return f"{sign}${ad:,.0f}"
    if kind == "pct1":                  return f"{sign}{ad:.1f} pts"
    if kind == "pct0":                  return f"{sign}{ad:.0f} pts"
    if kind == "int":                   return f"{sign}{ad:,.0f}"
    return f"{sign}{ad}"


def _cmp_delta_color(direction, d):
    if direction is None or d is None or (isinstance(d, float) and d != d) or d == 0:
        return _CMP_NEUTRAL_COLOR
    favorable = (d > 0) if direction == "higher" else (d < 0)
    return _CMP_FAVORABLE_COLOR if favorable else _CMP_UNFAVORABLE_COLOR


def _cmp_entity_header_html(info):
    """Name + tier badge (municipality) or 'Baseline' caption (county)."""
    html = f"<div style='font-weight:600;'>{info['label']}</div>"
    if info["kind"] == "muni" and info.get("tier"):
        color = _TIER_COLORS[info["tier"]]
        county_label = info.get("county_label", "")
        rank_txt = f"rank {info['rank']} of {info['n']}"
        rank_txt += f" in {county_label}" if county_label else ""
        html += (f"<span style='display:inline-block;margin-top:4px;padding:2px 8px;"
                 f"border-radius:6px;font-size:12px;background:{color}22;color:{color};'>"
                 f"{_TIER_LABELS[info['tier']]} · {rank_txt}</span>")
    else:
        html += "<div style='font-size:12px;color:#6b7280;'>Baseline</div>"
    return html


def _render_compare(df, muni):
    """Standalone comparison of any county against any county or municipality —
    intentionally its own top-level view (not nested in the municipal
    breakdown) since it needs to compare across counties too, not just within
    one county's municipalities."""
    st.markdown("##### Compare any county or municipality")
    st.caption("Pick a base and something to compare it against. The variance "
               "is the base minus the comparison — green when that's the "
               "favorable direction for the base, red when it isn't. A few "
               "metrics (home value, cost burden) are shown plainly with no "
               "color — the favorable direction for those isn't a direct "
               "read, so we didn't force a judgment.")

    counties = df[df["tier"] == "county"]
    county_label_by_key = dict(zip(counties["key"].astype(str), counties["label"]))

    tiers = {}
    for _, grp in muni.groupby("county_key"):
        tiers.update(_demand_tiers(grp))

    entities = {}
    for _, row in counties.iterrows():
        entities[f"county:{row['key']}"] = {"label": row["label"], "kind": "county", "row": row}
    for _, row in muni.iterrows():
        info = tiers.get(str(row["key"]), {})
        county_label = county_label_by_key.get(str(row["county_key"]), "")
        label = f"{row['label']} ({county_label})" if county_label else row["label"]
        entities[f"muni:{row['key']}"] = {"label": label, "kind": "muni", "row": row,
                                          "tier": info.get("tier"), "rank": info.get("rank"),
                                          "n": info.get("n"), "county_label": county_label}

    options = sorted(entities, key=lambda k: entities[k]["label"])
    if len(options) < 2:
        st.info("Not enough counties/municipalities loaded to compare.")
        return

    c1, c2 = st.columns(2)
    base_key = c1.selectbox("Base", options, index=0,
                            format_func=lambda k: entities[k]["label"], key="compare_base")
    cmp_key  = c2.selectbox("Compare against", options, index=1,
                            format_func=lambda k: entities[k]["label"], key="compare_cmp")

    base, cmp = entities[base_key], entities[cmp_key]
    hcol1, hcol2 = st.columns(2)
    hcol1.markdown(_cmp_entity_header_html(base), unsafe_allow_html=True)
    hcol2.markdown(_cmp_entity_header_html(cmp), unsafe_allow_html=True)

    rows_html = ""
    for section_title, rows in _COMPARE_SECTIONS:
        rows_html += (f"<tr><td colspan='3' style='padding:10px 6px 4px;font-size:12px;"
                      f"color:#6b7280;font-weight:600;'>{section_title}</td></tr>")
        for col, label, kind, direction in rows:
            base_val = base["row"].get(col)
            cmp_val = cmp["row"].get(col)
            delta = (base_val - cmp_val
                     if pd.notna(base_val) and pd.notna(cmp_val) else None)
            color = _cmp_delta_color(direction, delta)
            rows_html += (
                "<tr>"
                f"<td style='padding:6px;color:#374151;'>{label}</td>"
                f"<td style='padding:6px;background:#f9fafb;'>{_cmp_fmt_value(kind, base_val)}"
                f"<div style='font-size:11px;color:{color};'>{_cmp_fmt_delta(kind, delta)}</div></td>"
                f"<td style='padding:6px;'>{_cmp_fmt_value(kind, cmp_val)}</td>"
                "</tr>"
            )

    st.markdown(
        f"""<table style='width:100%;border-collapse:collapse;font-size:13px;'>
        <tbody>{rows_html}</tbody></table>""",
        unsafe_allow_html=True)


# ── Economic development / employer news — on-demand scan + review inbox ───────
def _render_econ_dev(county_keys, county_labels):
    st.markdown("##### Economic development & market news")
    last = econ_dev.last_scan_ts()
    when = f"last scan {last[:10]}" if last else "no scan yet — first run pulls history"
    cat_labels = [lbl for _, _, lbl in econ_dev.CATEGORIES.values()]
    st.caption(f"Scan for {', '.join(cat_labels).lower()} announcements across the "
               "market counties. The first scan pulls available history; each later "
               "scan adds only what's new since the last one. Nothing is kept until "
               f"you approve it — several outlets may cover one project, so keep one "
               f"and skip the duplicates.  _({when})_")

    scan_col, add_col = st.columns([1, 1])
    if scan_col.button("Scan now", key="econ_scan"):
        with st.spinner("Scanning West Michigan economic-development & market news…"):
            try:
                new, pending, catchup = econ_dev.run_scan()
                kind = "History catch-up" if catchup else "Scan"
                st.success(f"{kind} complete — {new} new item(s); {pending} pending review.")
            except Exception as e:                   # noqa: BLE001
                st.error(f"Scan failed: {e}")

    with add_col.popover("➕ Add a link manually"):
        st.caption("For announcements the scanner missed. Added straight to your "
                   "kept items to fill in below.")
        m_url = st.text_input("Article URL", key="manual_url")
        m_title = st.text_input("Headline (optional)", key="manual_title")
        m_county = st.selectbox("County", county_labels, key="manual_county")
        cat_by_label = {lbl: key for key, (_, _, lbl) in econ_dev.CATEGORIES.items()}
        m_category = st.selectbox("Category", list(cat_by_label), key="manual_category")
        if st.button("Add to kept items", key="manual_add"):
            if m_url.strip():
                ck = county_keys[county_labels.index(m_county)]
                _, added = econ_dev.add_manual(m_url.strip(), ck, m_county,
                                               title=(m_title.strip() or None),
                                               category=cat_by_label[m_category])
                st.success("Added — fill in its details below." if added
                           else "That link is already in the list.")
            else:
                st.warning("Enter a URL first.")

    queue = econ_dev.load_queue()
    if not queue:
        st.info("No scans yet — click **Scan now** to pull recent announcements.")
        return

    label_by_key = dict(zip(county_keys, county_labels))
    pending = [v for v in queue.values() if v.get("status") == "pending"]
    approved = [v for v in queue.values() if v.get("status") == "approved"]

    def _by_county(records, ck):
        return sorted([r for r in records if r["county_key"] == ck],
                      key=lambda r: r.get("published_ts", ""), reverse=True)

    st.markdown(f"**Review inbox — {len(pending)} pending**")
    if not pending:
        st.caption("Nothing pending — all caught up. ✅")
    for ck in county_keys:
        items = _by_county(pending, ck)
        if not items:
            continue
        st.markdown(f"**{label_by_key.get(ck, ck)}** ({len(items)})")
        for r in items:
            col, keep, skip = st.columns([7, 1, 1])
            date = (r.get("published", "") or "")[:16]
            cat = econ_dev.CATEGORIES.get(
                r.get("category", econ_dev.DEFAULT_CATEGORY),
                (None, None, "Employer expansion"))[2]
            col.markdown(f"[{r['title']}]({r['link']})  \n"
                         f"<small>{r.get('source','')} · {date} · {cat}</small>",
                         unsafe_allow_html=True)
            keep.button("✓ Keep", key=f"ekeep_{r['id']}",
                        on_click=econ_dev.set_status, args=(r["id"], "approved"))
            skip.button("✕ Skip", key=f"eskip_{r['id']}",
                        on_click=econ_dev.set_status, args=(r["id"], "rejected"))

    approved_employer = [r for r in approved
                         if r.get("category", econ_dev.DEFAULT_CATEGORY) == "employer"]
    approved_market = [r for r in approved
                       if r.get("category", econ_dev.DEFAULT_CATEGORY) != "employer"]

    if approved_employer:
        st.markdown(f"**Kept items — add job / investment details ({len(approved_employer)})**")
        st.caption("Click **Read →** to open the article, then fill in employer, "
                   "projected jobs, and investment ($M). These feed the Executive "
                   "summary. Tick **Send back** to return an item to the review inbox.")
        rows = []
        for r in sorted(approved_employer, key=lambda x: (x["county_label"],
                                                           x.get("published_ts", ""))):
            rows.append({
                "id": r["id"], "County": r["county_label"],
                "Employer": r.get("employer", "") or "",
                "Projected jobs": r.get("jobs"),
                "Investment ($M)": r.get("investment_musd"),
                "City": r.get("city", "") or "",
                "Article": r["link"], "Headline": r["title"],
                "Notes": r.get("notes", "") or "", "Send back": False,
            })
        edited = st.data_editor(
            pd.DataFrame(rows), key=_editor_key("econ_editor", approved_employer),
            hide_index=True, use_container_width=True,
            column_config={
                "id": None,
                "County": st.column_config.TextColumn(disabled=True, width="small"),
                "Article": st.column_config.LinkColumn("Article", display_text="Read →",
                                                       disabled=True, width="small"),
                "Headline": st.column_config.TextColumn(disabled=True, width="medium"),
                "Employer": st.column_config.TextColumn(width="small"),
                "Projected jobs": st.column_config.NumberColumn(format="%d", min_value=0),
                "Investment ($M)": st.column_config.NumberColumn(format="%.0f", min_value=0),
                "City": st.column_config.TextColumn(width="small"),
                "Notes": st.column_config.TextColumn(width="small"),
                "Send back": st.column_config.CheckboxColumn(width="small"),
            },
        )
        for _, row in edited.iterrows():
            if row["Send back"]:
                econ_dev.set_status(row["id"], "pending")
                continue
            econ_dev.update_record(
                row["id"], employer=(row["Employer"] or ""),
                jobs=(int(row["Projected jobs"]) if pd.notna(row["Projected jobs"]) else None),
                investment_musd=(float(row["Investment ($M)"]) if pd.notna(row["Investment ($M)"]) else None),
                city=(row["City"] or ""), notes=(row["Notes"] or ""))

    if approved_market:
        st.markdown(f"**Kept items — market-attractiveness projects ({len(approved_market)})**")
        st.caption("New retail, water/sewer, and parks projects aren't employers — "
                   "fill in a short project description, investment value, and city. "
                   "Wrong category from the scan? Fix it in the **Category** column — "
                   "the map pin updates to match. Tick **Send back** to return an item "
                   "to the review inbox.")
        market_cat_keys = [k for k in econ_dev.CATEGORIES if k != "employer"]
        market_cat_labels = [econ_dev.CATEGORIES[k][2] for k in market_cat_keys]
        label_to_cat = {econ_dev.CATEGORIES[k][2]: k for k in market_cat_keys}
        rows = []
        for r in sorted(approved_market, key=lambda x: (x["county_label"],
                                                         x.get("published_ts", ""))):
            cat = econ_dev.CATEGORIES.get(
                r.get("category", econ_dev.DEFAULT_CATEGORY),
                (None, None, market_cat_labels[0]))[2]
            rows.append({
                "id": r["id"], "County": r["county_label"], "Category": cat,
                "Project": r.get("employer", "") or "",
                "Investment ($M)": r.get("investment_musd"),
                "City": r.get("city", "") or "",
                "Article": r["link"], "Headline": r["title"],
                "Notes": r.get("notes", "") or "", "Send back": False,
            })
        edited_market = st.data_editor(
            pd.DataFrame(rows), key=_editor_key("econ_editor_market", approved_market),
            hide_index=True, use_container_width=True,
            column_config={
                "id": None,
                "County": st.column_config.TextColumn(disabled=True, width="small"),
                "Category": st.column_config.SelectboxColumn(
                    options=market_cat_labels, width="small",
                    help="Recategorize if the scan matched the wrong keyword set."),
                "Article": st.column_config.LinkColumn("Article", display_text="Read →",
                                                       disabled=True, width="small"),
                "Headline": st.column_config.TextColumn(disabled=True, width="medium"),
                "Project": st.column_config.TextColumn(width="medium",
                    help="Short description, e.g. \"New Costco warehouse store\" or "
                         "\"Water main extension to Sec. 14\"."),
                "Investment ($M)": st.column_config.NumberColumn(format="%.1f", min_value=0),
                "City": st.column_config.TextColumn(width="small"),
                "Notes": st.column_config.TextColumn(width="small"),
                "Send back": st.column_config.CheckboxColumn(width="small"),
            },
        )
        for _, row in edited_market.iterrows():
            if row["Send back"]:
                econ_dev.set_status(row["id"], "pending")
                continue
            econ_dev.update_record(
                row["id"], employer=(row["Project"] or ""),
                category=label_to_cat.get(row["Category"], "retail"),
                investment_musd=(float(row["Investment ($M)"]) if pd.notna(row["Investment ($M)"]) else None),
                city=(row["City"] or ""), notes=(row["Notes"] or ""))


def _safe_num(v):
    """Best-effort numeric conversion for fields that are USUALLY numbers but
    sometimes free text in the historical data ("6+", "TBD", "-" for acres/
    units) — keeps the original text rather than crashing the save on rerun."""
    if pd.isna(v) or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return v


def _editor_key(prefix, records):
    """Stable st.data_editor key derived from the exact SET of row ids being
    shown. A fixed key makes Streamlit reconcile edits by row POSITION across
    reruns — if a row gets added/removed from outside the editor (e.g. the
    "Add existing property" popover), the stale position-based state can get
    misaligned with the new rows, and the save loop then writes one record's
    edited values onto a DIFFERENT record's id. Changing the key whenever the
    id set changes forces a fresh widget instead, avoiding that corruption."""
    sig = hashlib.md5("|".join(sorted(r["id"] for r in records)).encode()).hexdigest()[:10]
    return f"{prefix}_{len(records)}_{sig}"


# ── Competition mapping — on-demand scan + review inbox ─────────────────────────
def _render_competition():
    st.markdown("##### Competition mapping")
    submarket_labels = [sm["label"] for sm in config.MARKET_SUBMARKETS]
    submarket_keys = [sm["key"] for sm in config.MARKET_SUBMARKETS]
    last = competition.last_scan_ts()
    when = f"last scan {last[:10]}" if last else "no scan yet — first run pulls history"
    st.caption("Scan for competing residential/BTR development projects in Grand "
               "Haven, Grand Haven Twp, and Spring Lake Twp. Allen Edwin (builder of "
               "CopperBay, WR-Dev's only direct BTR competitor in Ottawa County) is "
               "searched by name — bare \"CopperBay\" isn't, since it collides with "
               "an unrelated drink brand; add CopperBay-specific coverage manually "
               f"below.  _({when})_")

    scan_col, add_col, existing_col = st.columns([1, 1, 1])
    if scan_col.button("Scan now", key="competition_scan"):
        with st.spinner("Scanning for competing development projects…"):
            try:
                new, pending, catchup = competition.run_scan()
                kind = "History catch-up" if catchup else "Scan"
                st.success(f"{kind} complete — {new} new item(s); {pending} pending review.")
            except Exception as e:                   # noqa: BLE001
                st.error(f"Scan failed: {e}")

    with add_col.popover("➕ Add a link manually"):
        st.caption("For CopperBay/Allen Edwin coverage or anything else the "
                   "scanner missed. Added straight to your kept items to fill in.")
        m_url = st.text_input("Article URL", key="comp_manual_url")
        m_title = st.text_input("Headline (optional)", key="comp_manual_title")
        m_submarket = st.selectbox("Submarket", submarket_labels, key="comp_manual_submarket")
        stage_labels = list(competition.STAGES.values())
        stage_by_label = {v: k for k, v in competition.STAGES.items()}
        m_stage = st.selectbox("Stage", stage_labels, key="comp_manual_stage")
        if st.button("Add to kept items", key="comp_manual_add"):
            if m_url.strip():
                sk = submarket_keys[submarket_labels.index(m_submarket)]
                _, added = competition.add_manual(
                    m_url.strip(), sk, m_submarket, title=(m_title.strip() or None),
                    stage=stage_by_label[m_stage])
                st.success("Added — fill in its details below." if added
                           else "That link is already in the list.")
            else:
                st.warning("Enter a URL first.")

    with existing_col.popover("➕ Add existing property"):
        st.caption("For stabilized/lease-up comps from RealPage Explore or "
                   "similar — no source link needed, just the property data.")
        p_name = st.text_input("Property name", key="comp_prop_name")
        p_address = st.text_input("Address", key="comp_prop_address")
        p_submarket = st.selectbox("Submarket", submarket_labels, key="comp_prop_submarket")
        p_stage_options = ["Existing", "Lease-up"]
        p_stage_by_label = {"Existing": "existing", "Lease-up": "lease_up"}
        p_stage = st.selectbox("Status", p_stage_options, key="comp_prop_stage")
        p_units = st.number_input("Total units", min_value=0, step=1, key="comp_prop_units")
        p_rent = st.number_input("Effective rent ($/mo)", min_value=0, step=1,
                                 key="comp_prop_rent")
        p_occ = st.number_input("Occupancy (%)", min_value=0.0, max_value=100.0,
                                step=0.1, key="comp_prop_occ")
        p_sqft = st.number_input("Avg sq ft", min_value=0, step=1, key="comp_prop_sqft")
        p_year = st.number_input("Year built", min_value=1900, max_value=2100, step=1,
                                 value=2000, key="comp_prop_year")
        if st.button("Add to kept items", key="comp_prop_add"):
            if p_name.strip():
                sk = submarket_keys[submarket_labels.index(p_submarket)]
                _, added = competition.add_existing_property(
                    p_name.strip(), sk, p_submarket, address=p_address.strip(),
                    stage=p_stage_by_label[p_stage],
                    total_units=(int(p_units) or None), effective_rent=(p_rent or None),
                    occupancy_pct=(p_occ or None), avg_sqft=(int(p_sqft) or None),
                    year_built=(int(p_year) or None))
                st.success("Added — fill in any remaining details below." if added
                           else "That property is already in the list.")
            else:
                st.warning("Enter a property name first.")

    queue = competition.load_queue()
    if not queue:
        st.info("No scans yet — click **Scan now** to pull recent announcements.")
        return

    label_by_key = dict(zip(submarket_keys, submarket_labels))
    pending = [v for v in queue.values() if v.get("status") == "pending"]
    approved = [v for v in queue.values() if v.get("status") == "approved"]

    def _by_submarket(records, key):
        return sorted([r for r in records if r["submarket_key"] == key],
                      key=lambda r: r.get("published_ts", ""), reverse=True)

    st.markdown(f"**Review inbox — {len(pending)} pending**")
    if not pending:
        st.caption("Nothing pending — all caught up. ✅")
    for sk in submarket_keys:
        items = _by_submarket(pending, sk)
        if not items:
            continue
        st.markdown(f"**{label_by_key.get(sk, sk)}** ({len(items)})")
        for r in items:
            col, keep, skip = st.columns([7, 1, 1])
            date = (r.get("published", "") or "")[:16]
            stage_label = competition.STAGES.get(r.get("stage", competition.DEFAULT_STAGE),
                                                 "Proposed")
            flag = " · ★ direct competitor" if r.get("is_direct_competitor") else ""
            col.markdown(f"[{r['title']}]({r['link']})  \n"
                         f"<small>{r.get('source','')} · {date} · {stage_label}{flag}</small>",
                         unsafe_allow_html=True)
            keep.button("✓ Keep", key=f"ckeep_{r['id']}",
                        on_click=competition.set_status, args=(r["id"], "approved"))
            skip.button("✕ Skip", key=f"cskip_{r['id']}",
                        on_click=competition.set_status, args=(r["id"], "rejected"))

    if approved:
        st.markdown(f"**Kept items — competing projects ({len(approved)})**")
        st.caption("Wrong stage from the scan? Fix it in the **Stage** column — "
                   "the map pin updates to match. Tick **Send back** to return an "
                   "item to the review inbox.")
        stage_labels = list(competition.STAGES.values())
        stage_by_label = {v: k for k, v in competition.STAGES.items()}
        rows = []
        for r in sorted(approved, key=lambda x: (x["submarket_label"],
                                                 x.get("published_ts", ""))):
            stage_label = competition.STAGES.get(r.get("stage", competition.DEFAULT_STAGE),
                                                 stage_labels[0])
            rows.append({
                "id": r["id"], "Submarket": r["submarket_label"],
                "Direct competitor": bool(r.get("is_direct_competitor")),
                "Stage": stage_label,
                "Project": r.get("project_name", "") or "",
                "Address": r.get("address", "") or "",
                "Type": r.get("type", "") or "",
                "Units": r.get("total_units"),
                "Builder": r.get("builder", "") or "",
                "Acres": r.get("acres"),
                "Effective rent": r.get("effective_rent"),
                "Occupancy %": r.get("occupancy_pct"),
                "Avg sq ft": r.get("avg_sqft"),
                "Year built": r.get("year_built"),
                "Approved on": r.get("approved_on", "") or "",
                "Constr. start": r.get("construction_start", "") or "",
                "Constr. end": r.get("construction_end", "") or "",
                "Article": r["link"], "Headline": r["title"],
                "Notes": r.get("notes", "") or "", "Send back": False,
            })
        edited = st.data_editor(
            pd.DataFrame(rows), key=_editor_key("competition_editor", approved),
            hide_index=True, use_container_width=True,
            column_config={
                "id": None,
                "Submarket": st.column_config.TextColumn(disabled=True, width="small"),
                "Direct competitor": st.column_config.CheckboxColumn(
                    width="small", help="Allen Edwin / CopperBay"),
                "Stage": st.column_config.SelectboxColumn(options=stage_labels, width="small"),
                "Project": st.column_config.TextColumn(width="medium"),
                "Address": st.column_config.TextColumn(width="medium"),
                "Type": st.column_config.TextColumn(width="small"),
                "Units": st.column_config.NumberColumn(format="%d", min_value=0),
                "Builder": st.column_config.TextColumn(width="small"),
                "Acres": st.column_config.NumberColumn(format="%.1f", min_value=0),
                "Effective rent": st.column_config.NumberColumn(
                    format="$%d", min_value=0, help="Effective rent per month"),
                "Occupancy %": st.column_config.NumberColumn(
                    format="%.1f%%", min_value=0, max_value=100),
                "Avg sq ft": st.column_config.NumberColumn(format="%d", min_value=0),
                "Year built": st.column_config.NumberColumn(format="%d", min_value=1900),
                "Approved on": st.column_config.TextColumn(width="small"),
                "Constr. start": st.column_config.TextColumn(width="small"),
                "Constr. end": st.column_config.TextColumn(width="small"),
                "Article": st.column_config.LinkColumn("Article", display_text="Read →",
                                                       disabled=True, width="small"),
                "Headline": st.column_config.TextColumn(disabled=True, width="medium"),
                "Notes": st.column_config.TextColumn(width="medium"),
                "Send back": st.column_config.CheckboxColumn(width="small"),
            },
        )
        for _, row in edited.iterrows():
            if row["Send back"]:
                competition.set_status(row["id"], "pending")
                continue
            competition.update_record(
                row["id"],
                stage=stage_by_label.get(row["Stage"], competition.DEFAULT_STAGE),
                is_direct_competitor=bool(row["Direct competitor"]),
                project_name=(row["Project"] or ""), address=(row["Address"] or ""),
                type=(row["Type"] or ""),
                total_units=_safe_num(row["Units"]),
                builder=(row["Builder"] or ""),
                acres=_safe_num(row["Acres"]),
                effective_rent=_safe_num(row["Effective rent"]),
                occupancy_pct=_safe_num(row["Occupancy %"]),
                avg_sqft=_safe_num(row["Avg sq ft"]),
                year_built=_safe_num(row["Year built"]),
                approved_on=(row["Approved on"] or ""),
                construction_start=(row["Constr. start"] or ""),
                construction_end=(row["Constr. end"] or ""),
                notes=(row["Notes"] or ""))


# ── Main entry ─────────────────────────────────────────────────────────────────
def render_market(view: str, on_continue):
    st.subheader("1. Market Feasibility")

    try:
        df, needs, bounds, muni, muni_bounds, oz, fred, zori, zhvi = _market_data()
    except Exception as e:                       # noqa: BLE001
        st.error(f"Couldn't load market data: {e}")
        st.button("Continue to Land Screener →", on_click=on_continue, type="primary")
        return

    st.caption("Where should we build? County housing-need (units needed) heat "
              "map, then drill into demographics, affordability & submarkets.")

    mort = fred_data_mod.mortgage_snapshot(fred)
    if mort:
        arrow = "▲" if mort["delta"] > 0 else ("▼" if mort["delta"] < 0 else "→")
        st.markdown(
            f"""<div style="background-color:#dcebec; border-left:4px solid #779FA1;
                        border-radius:6px; padding:12px 16px; margin-bottom:8px;">
            <span style="color:#2c3e3f; font-size:14px;">
            <strong style="color:#3f6b6d;">30-yr mortgage rate (national, FRED): {mort['latest']:.2f}%</strong>
            {arrow} {mort['delta']:+.2f} pts vs ~1 quarter ago
            </span></div>""",
            unsafe_allow_html=True)

    county_labels = needs["label"].tolist()
    county_keys   = needs["key"].tolist()
    label_by_key  = dict(zip(county_keys, county_labels))

    # Zoom state machine: "counties" overview, or a county key (zoomed to municipalities).
    # selected_county_key tracks which county's detail panel shows beneath the
    # county map — separate from market_level, which only changes when the
    # user explicitly clicks "Go to municipal breakdown". This is what makes
    # a county click "stick" (show detail in place) instead of navigating away.
    level = st.session_state.setdefault("market_level", "counties")
    st.session_state.setdefault("selected_county_key", None)
    st.session_state.setdefault("county_map_nonce", 0)

    def _select_county(county_key):
        st.session_state.selected_county_key = county_key
        st.session_state.county_map_nonce += 1   # fresh map/selectbox widgets → no stale click/pick replay

    def _zoom_to_municipal(county_key):
        st.session_state.market_level = county_key

    def _back_to_counties():
        st.session_state.market_level = "counties"
        st.session_state.county_map_nonce += 1
        # selected_county_key intentionally left as-is — "Back" returns to
        # that same county's detail panel, not a blank map.

    if view == "Executive":
        if level == "counties":
            st.markdown("##### County housing-need heat map")
            st.caption("🟩 less need → 🟥 more need · shading = **total units "
                       "needed per 1,000 households** (size-normalized so a big "
                       "county isn't red just for being big). Hover for the "
                       "figures; **click a county to see its details below**.")

            t1, t2, _ = st.columns([1, 1, 2])
            show_pins = t1.checkbox(
                "Show development pins", key="econ_pins_counties",
                help="Overlay pins for your kept economic-development / market "
                     "signals — employer expansions, new retail, water/sewer, "
                     "and parks projects, each styled by category.")
            show_oz = t2.checkbox(
                "Show opportunity zones", key="oz_counties",
                help="Overlay IRS-approved Opportunity Zone census tracts "
                     "(MSHDA/state GIS) across all four counties.")

            pins = econ_pins(None, muni_bounds, bounds) if show_pins else None
            if show_pins:
                _render_pins_summary(pins)

            nonce = st.session_state.county_map_nonce
            map_out = st_folium(
                _build_county_map(bounds, needs, "intensity_total",
                                  "Total units needed per 1,000 households", pins=pins,
                                  oz_fc=oz if show_oz else None),
                height=650, use_container_width=True,
                key=f"county_map_{nonce}", returned_objects=["last_active_drawing"])
            clicked = (map_out or {}).get("last_active_drawing")
            if clicked and clicked.get("properties", {}).get("tier") == "county":
                _select_county(clicked["properties"]["key"])
                st.rerun()

            # Selectbox fallback (accessibility / no-click select) — same
            # click-to-select behavior as the map (sticks, doesn't navigate
            # away), sharing the same reset nonce so neither widget can hand
            # back a stale pick and fight with the other on rerun.
            pick = st.selectbox("…or choose a county to view details",
                                ["—"] + county_labels, key=f"county_pick_{nonce}")
            if pick != "—":
                _select_county(county_keys[county_labels.index(pick)])
                st.rerun()

            # County detail panel, beneath the map — shows once a county is
            # selected (via click or the dropdown) and stays put. This is
            # ALL county-level data now (moved out of the municipal
            # breakdown below, to avoid showing it in two places).
            selected_key = st.session_state.selected_county_key
            if selected_key:
                st.divider()
                sel_county_label = label_by_key.get(selected_key, selected_key)
                st.markdown(f"##### {sel_county_label} — county detail")
                st.button("Go to municipal breakdown →", on_click=_zoom_to_municipal,
                          args=(selected_key,))
                _render_county_drilldown(selected_key, needs, df, fred=fred, zori=zori, zhvi=zhvi)

        else:  # zoomed into a county → municipal view
            county_key = level
            sel_county_label = label_by_key.get(county_key, county_key)
            st.button("⬅ Back to counties", on_click=_back_to_counties)

            st.markdown(f"##### {sel_county_label} municipalities")
            st.caption("🟩 low → 🟥 high demand, ranked by tertile within this county's own "
                       "municipalities (not a fixed score cutoff). Hover for the tier and rank; "
                       "click a city/township to drill in. Small rural townships have noisier "
                       "ACS estimates — read their tier as approximate.")

            t1, t2, t3, _ = st.columns([1, 1, 1, 1])
            show_pins = t1.checkbox(
                "Show development pins", key=f"econ_pins_{county_key}",
                help="Overlay pins for kept economic-development / market signals "
                     "in this county, each styled by category.")
            show_competition = t2.checkbox(
                "Show competition pins", key=f"competition_pins_{county_key}",
                help="Overlay kept competing-development projects for Grand Haven / "
                     "Grand Haven Twp / Spring Lake Twp, colored by stage — a star "
                     "marks Allen Edwin/CopperBay, WR-Dev's direct BTR competitor.")
            show_oz = t3.checkbox(
                "Show opportunity zones", key=f"oz_{county_key}",
                help="Overlay IRS-approved Opportunity Zone census tracts "
                     "(MSHDA/state GIS) in this county.")

            pins = econ_pins(county_key, muni_bounds, bounds) if show_pins else None
            if show_pins:
                _render_pins_summary(pins)

            if show_competition:
                with st.spinner("Geocoding project addresses…"):
                    comp_pins = competition_pins(muni_bounds)
                _render_competition_summary(comp_pins)
            else:
                comp_pins = None

            county_oz = None
            if show_oz:
                county_oz = {"type": "FeatureCollection",
                            "features": [f for f in oz["features"]
                                        if f["properties"].get("county_key") == county_key]}

            # Municipal heat map occupies the same spot the county map did,
            # so zooming in feels continuous rather than making the map
            # "vanish". County-level data (need/pricing/demographics) no
            # longer repeats here — it lives in the county detail panel,
            # one "Back to counties" click away (see the "counties" branch
            # above), so it isn't shown in two places.
            _render_municipalities(county_key,
                                   muni, muni_bounds, pins=pins,
                                   competition_pins_list=comp_pins,
                                   oz_fc=county_oz)

    elif view == "Compare":
        _render_compare(df, muni)

    else:  # Analyst — full tables
        st.markdown("##### County housing need — units needed (5-year gap)")
        need_cols = {"label": "County", "study_period": "Study period",
                     "total_units": "Total units", "rental_units": "Rental units",
                     "forsale_units": "For-sale units", "households": "Households",
                     "intensity_total": "Total / 1k HH", "intensity_rental": "Rental / 1k HH"}
        nd = needs[list(need_cols)].rename(columns=need_cols)
        st.dataframe(nd.style.format({
            "Total units": "{:,.0f}", "Rental units": "{:,.0f}",
            "For-sale units": "{:,.0f}", "Households": "{:,.0f}",
            "Total / 1k HH": "{:.0f}", "Rental / 1k HH": "{:.0f}"}, na_rep="—"),
            use_container_width=True, hide_index=True)
        st.caption("Source: county Housing Needs Assessments (Bowen National "
                   "Research). Ottawa/Kent 2024–2029; Allegan/Muskegon 2022–2027.")

        st.markdown("##### Market pricing & momentum — by county (FRED)")
        st.caption("Plain historical series behind the Executive view's charts — "
                   "FHFA All-Transactions HPI (annual) and Census building permits (annual).")
        for c_key, c_label in zip(county_keys, county_labels):
            hpi_hist = (fred.get("counties", {}).get(c_key) or {}).get("hpi") or []
            permits_hist = (fred.get("counties", {}).get(c_key) or {}).get("permits") or []
            if not hpi_hist and not permits_hist:
                continue
            with st.expander(f"{c_label} — HPI & permits history"):
                hcol, pcol = st.columns(2)
                if hpi_hist:
                    hdf = pd.DataFrame(hpi_hist).rename(
                        columns={"date": "Year", "value": "HPI (2000=100)"})
                    hdf["Year"] = hdf["Year"].str[:4]
                    hcol.dataframe(hdf.iloc[::-1], use_container_width=True, hide_index=True)
                if permits_hist:
                    pdf_hist = pd.DataFrame(permits_hist).rename(
                        columns={"date": "Year", "value": "Units permitted"})
                    pdf_hist["Year"] = pdf_hist["Year"].str[:4]
                    pcol.dataframe(pdf_hist.iloc[::-1], use_container_width=True, hide_index=True)

        st.markdown("##### ACS demographics & affordability — by county")
        st.dataframe(_acs_table(df[df["tier"] == "county"], "County"),
                     use_container_width=True, hide_index=True)
        st.caption("Expand a county below to break it out into its cities & townships.")

        for c_key, c_label in zip(county_keys, county_labels):
            sub = (muni[muni["county_key"] == c_key]
                   .sort_values("demand_score", ascending=False))
            with st.expander(f"{c_label} — {len(sub)} municipalities"):
                st.dataframe(_acs_table(sub, "Municipality"),
                             use_container_width=True, hide_index=True)
                st.caption("Ranked by demand score. Small rural townships have "
                           "noisier ACS estimates (esp. rental vacancy) — read "
                           "those as approximate.")

        if config.IS_LOCAL:
            st.divider()
            _render_econ_dev(county_keys, county_labels)
            st.divider()
            _render_competition()
        else:
            st.divider()
            st.caption("Economic-development and competition-mapping scanning & "
                       "curation is done locally by the analyst — the Executive "
                       "view's pins and summaries already reflect the latest "
                       "curated data.")
