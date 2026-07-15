"""
Section 1 — Market Feasibility UI (Streamlit).

Pure rendering — no auth, no page config — so it can be exercised by a
standalone harness as well as embedded in app_shell. The shell supplies the
Executive/Analyst `view` string and an `on_continue` callback (Market → Land).

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

import pandas as pd
import streamlit as st
import folium
import branca.colormap as cm
from streamlit_folium import st_folium

from market.demographics import load_market_metrics, load_municipal_metrics
from market.market_scoring import add_demand_score
from market.boundaries import load_boundaries, load_municipal_boundaries
from market.housing_needs import load_housing_needs
from market import econ_dev
from market import competition
import config
from config import DEMAND_WEIGHTS

# Green → red ramp; more need = red. Reused for both maps (rescaled per use).
_RAMP = ["#1a9850", "#fee08b", "#d73027"]

# Demand-score ramp for the Ottawa submarket sub-map.
_DEMAND_CMAP = cm.LinearColormap(_RAMP, vmin=35, vmax=60,
                                 caption="Housing-need / demand score (0–100)")

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
                   "median_gross_rent", "rental_vacancy_rate", "cost_burden_pct",
                   "renter_share_pct", "occupancy_pct", "pop_growth_pct",
                   "median_age", "population"]
_ACS_TABLE_FMT = {
    "median_hh_income": "${:,.0f}", "max_affordable_rent": "${:,.0f}",
    "median_gross_rent": "${:,.0f}", "population": "{:,.0f}", "demand_score": "{:.1f}",
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
    """Cached: scored ACS frame, county housing-needs frame, boundary FCs, and
    the scored municipal (all city/township) frame + boundaries."""
    df = add_demand_score(load_market_metrics())
    needs = load_housing_needs(df)
    bounds = load_boundaries()
    muni = add_demand_score(load_municipal_metrics())
    muni_bounds = load_municipal_boundaries()
    return df, needs, bounds, muni, muni_bounds


def _fval(row, col):
    v = row.get(col)
    if v is None or (isinstance(v, float) and v != v):
        return "—"
    return _FMT[col][1](v)


# ── ACS metric grid (shared by county + submarket drill-downs) ─────────────────
def _metric_grid(row):
    rv_unreliable = bool(row.get("rental_vacancy_unreliable"))
    rv_display = _fval(row, "rental_vacancy_rate") + ("*" if rv_unreliable else "")

    c1, c2, c3 = st.columns(3)
    c1.metric(_FMT["median_hh_income"][0],    _fval(row, "median_hh_income"))
    c2.metric(_FMT["max_affordable_rent"][0], _fval(row, "max_affordable_rent"),
              help="Median HH income ÷ 12 × 30% — what this market can afford monthly.")
    c3.metric(_FMT["median_gross_rent"][0],   _fval(row, "median_gross_rent"))

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
        f'<span style="color:#2c3e3f;font-weight:700;">📍 Pinned development signals</span>'
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
        f'<span style="color:#2c3e3f;font-weight:700;">🏘️ Competition pipeline</span>'
        f'&nbsp;—&nbsp; {len(pins)} project(s) &nbsp;·&nbsp; {stage_txt}{direct_txt}</div>',
        unsafe_allow_html=True)


# ── County heat map (PRIMARY) ──────────────────────────────────────────────────
def _build_county_map(bounds, needs, value_col, caption, pins=None):
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
        tooltip=folium.GeoJsonTooltip(
            fields=["label", "total_units", "rental_units"],
            aliases=["County:", "Total units needed:", "Rental units needed:"],
        ),
    ).add_to(m)
    cmap.add_to(m)
    if pins:
        _add_pins(m, pins)
    return m


def _render_rental_by_income(county_key, needs_raw):
    """Bar chart + table of the county's rental gap by AMI/rent band."""
    import altair as alt
    from market.housing_needs import HOUSING_NEEDS
    bands = HOUSING_NEEDS[county_key]["rental_by_income"]
    bdf = pd.DataFrame(bands)
    # Short AMI-band labels (rent range stays in tooltip + table) so the x-axis
    # reads horizontally with no rotation or truncation.
    order = bdf["ami"].tolist()
    st.markdown("**Rental units needed by income / rent band**")
    chart = (
        alt.Chart(bdf)
        .mark_bar(color="#779FA1")
        .encode(
            x=alt.X("ami:N", sort=order, title="% of area median income",
                    axis=alt.Axis(labelAngle=0, labelLimit=1000, labelPadding=6)),
            y=alt.Y("units:Q", title="Units needed"),
            tooltip=[alt.Tooltip("ami", title="% of median income"),
                     alt.Tooltip("rent", title="Monthly rent"),
                     alt.Tooltip("units", title="Units needed", format=",")],
        )
        .properties(height=240)
    )
    tbl = (bdf.rename(columns={"ami": "% of median income", "rent": "Monthly rent",
                               "units": "Units needed"})
              [["% of median income", "Monthly rent", "Units needed"]]
              .style.format({"Units needed": "{:,.0f}"}))

    # Side by side, centered: equal spacer columns keep the pair off the edges
    # and each element ~40% wide (readable, not stretched across the screen).
    _, c_chart, c_table, _ = st.columns([1, 4, 4, 1])
    c_chart.altair_chart(chart, use_container_width=True)
    c_table.dataframe(tbl, use_container_width=True, hide_index=True)


def _render_county_drilldown(county_key, needs, acs_df):
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


def _build_municipal_map(muni_bounds, muni_df, county_key, pins=None, competition_pins_list=None):
    """Choropleth of one county's municipalities, shaded by demand score, zoomed in."""
    feats = [f for f in muni_bounds["features"]
             if f["properties"].get("county_key") == county_key]
    keyed = muni_df.set_index(muni_df["key"].astype(str))
    score_by_key = keyed["demand_score"].to_dict()
    # Boundary NAME is just the base name ("Grand Haven") and can't tell a city
    # from its township; use the ACS-derived label ("Grand Haven city" vs
    # "…charter township") so the map tooltip matches the dropdown/detail.
    label_by_key = keyed["label"].to_dict()
    for f in feats:
        k = str(f["properties"]["key"])
        f["properties"]["score"] = round(float(score_by_key.get(k, 0) or 0), 1)
        f["properties"]["label"] = label_by_key.get(k, f["properties"].get("label"))

    m = folium.Map(tiles="cartodbpositron", control_scale=True)
    if feats:
        m.fit_bounds(_bbox_of_features(feats))
    folium.GeoJson(
        {"type": "FeatureCollection", "features": feats},
        name="Municipalities",
        style_function=lambda f: {
            "fillColor": _DEMAND_CMAP(f["properties"]["score"]),
            "color": "#2c3e3f", "weight": 1.0, "fillOpacity": 0.72,
        },
        highlight_function=lambda _f: {"weight": 3, "color": "#779FA1",
                                       "fillOpacity": 0.85},
        tooltip=folium.GeoJsonTooltip(fields=["label", "score"],
                                      aliases=["Municipality:", "Demand score:"]),
    ).add_to(m)
    _DEMAND_CMAP.add_to(m)
    if pins:
        _add_pins(m, pins)
    if competition_pins_list:
        _add_competition_pins(m, competition_pins_list)
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


def _render_municipalities(county_key, county_label, muni_df, muni_bounds, pins=None,
                           competition_pins_list=None):
    """Municipal demand-score heat map for one county + selected-place detail."""
    muni = muni_df[muni_df["county_key"] == county_key].reset_index(drop=True)
    if muni.empty:
        st.info("No municipal data for this county.")
        return
    st.markdown(f"##### {county_label} municipalities — demand score (secondary scoring)")
    st.caption("🟩 lower → 🟥 higher demand. Hover for the score; click a "
               "city/township to drill in. Small rural townships have noisier ACS "
               "estimates — read their scores as approximate.")

    map_out = st_folium(_build_municipal_map(muni_bounds, muni, county_key, pins=pins,
                                             competition_pins_list=competition_pins_list),
                        height=420, use_container_width=True,
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
    if scan_col.button("🔎 Scan now", key="econ_scan"):
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
    if scan_col.button("🔎 Scan now", key="competition_scan"):
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
    st.caption("Where should we build? County housing-need (units needed) heat "
               "map, then drill into demographics, affordability & submarkets.")

    try:
        df, needs, bounds, muni, muni_bounds = _market_data()
    except Exception as e:                       # noqa: BLE001
        st.error(f"Couldn't load market data: {e}")
        st.button("Continue to Land Screener →", on_click=on_continue, type="primary")
        return

    county_labels = needs["label"].tolist()
    county_keys   = needs["key"].tolist()
    label_by_key  = dict(zip(county_keys, county_labels))
    sel_label = st.session_state.get("submarket") or "Grand Haven"

    # Zoom state machine: "counties" overview, or a county key (zoomed to municipalities).
    level = st.session_state.setdefault("market_level", "counties")
    st.session_state.setdefault("county_map_nonce", 0)

    def _zoom_to(county_key):
        st.session_state.market_level = county_key

    def _back_to_counties():
        st.session_state.market_level = "counties"
        st.session_state.county_map_nonce += 1   # fresh county map → no stale click

    if view == "Executive":
        if level == "counties":
            st.markdown("##### County housing-need heat map")
            st.caption("🟩 less need → 🟥 more need · shading = **total units "
                       "needed per 1,000 households** (size-normalized so a big "
                       "county isn't red just for being big). Hover for the "
                       "figures; **click a county to zoom into its municipalities**.")

            show_pins = st.checkbox(
                "📍 Show development pins", key="econ_pins_counties",
                help="Overlay pins for your kept economic-development / market "
                     "signals — employer expansions, new retail, water/sewer, "
                     "and parks projects, each styled by category.")
            pins = econ_pins(None, muni_bounds, bounds) if show_pins else None
            if show_pins:
                _render_pins_summary(pins)

            nonce = st.session_state.county_map_nonce
            map_out = st_folium(
                _build_county_map(bounds, needs, "intensity_total",
                                  "Total units needed per 1,000 households", pins=pins),
                height=460, use_container_width=True,
                key=f"county_map_{nonce}", returned_objects=["last_active_drawing"])
            clicked = (map_out or {}).get("last_active_drawing")
            if clicked and clicked.get("properties", {}).get("tier") == "county":
                _zoom_to(clicked["properties"]["key"])
                st.rerun()

            # Selectbox fallback (accessibility / no-click drill-in).
            pick = st.selectbox("…or choose a county to zoom in",
                                ["—"] + county_labels, key="county_pick")
            if pick != "—":
                _zoom_to(county_keys[county_labels.index(pick)])
                st.rerun()

        else:  # zoomed into a county → municipal view
            county_key = level
            sel_county_label = label_by_key.get(county_key, county_key)
            st.button("⬅ Back to counties", on_click=_back_to_counties)

            show_pins = st.checkbox(
                "📍 Show development pins", key=f"econ_pins_{county_key}",
                help="Overlay pins for kept economic-development / market signals "
                     "in this county, each styled by category.")
            pins = econ_pins(county_key, muni_bounds, bounds) if show_pins else None
            if show_pins:
                _render_pins_summary(pins)

            show_competition = st.checkbox(
                "Show competition pins", key=f"competition_pins_{county_key}",
                help="Overlay kept competing-development projects for Grand Haven / "
                     "Grand Haven Twp / Spring Lake Twp, colored by stage — a star "
                     "marks Allen Edwin/CopperBay, WR-Dev's direct BTR competitor.")
            if show_competition:
                with st.spinner("Geocoding project addresses…"):
                    comp_pins = competition_pins(muni_bounds)
                _render_competition_summary(comp_pins)
            else:
                comp_pins = None

            # Municipal heat map goes FIRST — same spot the county map occupied,
            # so zooming in feels continuous rather than making the map "vanish".
            picked = _render_municipalities(county_key, sel_county_label,
                                            muni, muni_bounds, pins=pins,
                                            competition_pins_list=comp_pins)
            if picked:
                sel_label = picked

            # County housing-need + demographics below, as supporting context.
            st.divider()
            _render_county_drilldown(county_key, needs, df)

    else:  # Analyst — full tables
        sel_label = st.selectbox("Carry submarket into Land Screener",
                                 df[df["tier"] == "submarket"]["label"].tolist(),
                                 key="market_submarket")

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

    st.session_state.submarket = sel_label
    st.success(f"Selected submarket **{sel_label}** will carry into the Land "
               f"Screener.", icon="🔗")
    st.button("Continue to Land Screener →", on_click=on_continue, type="primary")
