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


# ── County heat map (PRIMARY) ──────────────────────────────────────────────────
def _build_county_map(bounds, needs, value_col, caption):
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
        inv = econ["investment_musd"]
        inv_txt = ("—" if not inv else
                   f"${inv/1000:.1f}B" if inv >= 1000 else f"${inv:,.0f}M")
        e3.metric("Total investment", inv_txt)
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


def _build_municipal_map(muni_bounds, muni_df, county_key):
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


def _render_municipalities(county_key, county_label, muni_df, muni_bounds):
    """Municipal demand-score heat map for one county + selected-place detail."""
    muni = muni_df[muni_df["county_key"] == county_key].reset_index(drop=True)
    if muni.empty:
        st.info("No municipal data for this county.")
        return
    st.markdown(f"##### {county_label} municipalities — demand score (secondary scoring)")
    st.caption("🟩 lower → 🟥 higher demand. Hover for the score; click a "
               "city/township to drill in. Small rural townships have noisier ACS "
               "estimates — read their scores as approximate.")

    map_out = st_folium(_build_municipal_map(muni_bounds, muni, county_key),
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
    st.markdown("##### Economic development & employer news")
    st.caption("On-demand scan for expansion / new-jobs / investment announcements "
               "across the market counties (last ~60 days). Nothing is kept until "
               "you approve it. Several outlets may cover the same project — keep "
               "one, skip the duplicates.")

    if st.button("🔎 Scan now", key="econ_scan"):
        with st.spinner("Scanning West Michigan economic-development news…"):
            try:
                new, pending = econ_dev.run_scan()
                st.success(f"Scan complete — {new} new item(s); {pending} pending review.")
            except Exception as e:                   # noqa: BLE001
                st.error(f"Scan failed: {e}")

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
            col.markdown(f"[{r['title']}]({r['link']})  \n"
                         f"<small>{r.get('source','')} · {date}</small>",
                         unsafe_allow_html=True)
            keep.button("✓ Keep", key=f"ekeep_{r['id']}",
                        on_click=econ_dev.set_status, args=(r["id"], "approved"))
            skip.button("✕ Skip", key=f"eskip_{r['id']}",
                        on_click=econ_dev.set_status, args=(r["id"], "rejected"))

    if approved:
        st.markdown(f"**Kept items — add job / investment details ({len(approved)})**")
        st.caption("Click **Read →** to open the article, then fill in employer, "
                   "projected jobs, and investment ($M). These feed the Executive "
                   "summary. Tick **Send back** to return an item to the review inbox.")
        rows = []
        for r in sorted(approved, key=lambda x: (x["county_label"],
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
            pd.DataFrame(rows), key="econ_editor", hide_index=True,
            use_container_width=True,
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

            nonce = st.session_state.county_map_nonce
            map_out = st_folium(
                _build_county_map(bounds, needs, "intensity_total",
                                  "Total units needed per 1,000 households"),
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

            # Municipal heat map goes FIRST — same spot the county map occupied,
            # so zooming in feels continuous rather than making the map "vanish".
            picked = _render_municipalities(county_key, sel_county_label,
                                            muni, muni_bounds)
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

        st.divider()
        _render_econ_dev(county_keys, county_labels)

    st.session_state.submarket = sel_label
    st.success(f"Selected submarket **{sel_label}** will carry into the Land "
               f"Screener.", icon="🔗")
    st.button("Continue to Land Screener →", on_click=on_continue, type="primary")
