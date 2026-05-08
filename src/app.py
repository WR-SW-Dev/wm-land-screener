"""
West Michigan Vacant Land Screener — Streamlit UI
Run with:  streamlit run src/app.py
"""

import json
import sys
import subprocess
from datetime import datetime
from pathlib import Path

import folium
import geopandas as gpd
import pandas as pd
import streamlit as st
import yaml
from yaml.loader import SafeLoader
import streamlit_authenticator as stauth
from streamlit_folium import st_folium

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).parent))
from config import CITIES, OUTPUT_DIR, MIN_ACRES, MAX_FLOOD_PCT  # noqa: E402
from scoring import SCORE_COMPONENTS                              # noqa: E402

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="WM Land Screener",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Authentication ─────────────────────────────────────────────────────────────
_CRED_FILE = ROOT / "credentials.yaml"
if not _CRED_FILE.exists():
    st.error(
        "credentials.yaml not found. "
        "Run `python manage_users.py` from the project root to create user accounts."
    )
    st.stop()

with open(_CRED_FILE) as _f:
    _auth_config = yaml.load(_f, Loader=SafeLoader)

_authenticator = stauth.Authenticate(
    _auth_config["credentials"],
    _auth_config["cookie"]["name"],
    _auth_config["cookie"]["key"],
    _auth_config["cookie"]["expiry_days"],
    auto_hash=False,   # passwords are pre-hashed by manage_users.py
)

_authenticator.login()

if st.session_state["authentication_status"] is False:
    st.error("Incorrect username or password.")
    st.stop()
elif st.session_state["authentication_status"] is None:
    st.info("Please log in to access the WM Land Screener.")
    st.stop()

# ── Logged in — determine role ─────────────────────────────────────────────────
_username  = st.session_state["username"]
_user_data = _auth_config["credentials"]["usernames"].get(_username, {})
IS_ADMIN   = _user_data.get("role") == "admin"

# ── Constants ─────────────────────────────────────────────────────────────────
SCORE_HIGH   = 70
SCORE_MED    = 50
COLOR_HIGH   = "#22c55e"   # green
COLOR_MED    = "#f59e0b"   # amber
COLOR_LOW    = "#ef4444"   # red
COLOR_STROKE = "#1e293b"   # dark border

# Parcel tracker
TRACKER_FILE   = ROOT / "data" / "tracker.json"
STATUS_OPTIONS = ["Not contacted", "Pursuing", "Backburner", "No"]
STATUS_COLORS  = {
    "Not contacted": "#9ca3af",
    "Pursuing":      "#22c55e",
    "Backburner":    "#f59e0b",
    "No":            "#ef4444",
}

# Development pathway badge colours
PATHWAY_COLORS = {
    "By right":           "#22c55e",   # green  — no approval needed
    "PRD special use":    "#3b82f6",   # blue   — Grand Haven PRD (Sec. 40-552)
    "PUD special use":    "#3b82f6",   # blue   — Spring Lake Twp PUD (Article 14)
    "Master plan upzone": "#8b5cf6",   # purple — rezoning with master plan support
    "PD rezoning":        "#f59e0b",   # amber  — legislative rezoning, case-by-case
    "Not viable":         "#9ca3af",   # gray   — no identified path to 3+ u/ac
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_tracker() -> dict:
    """Load parcel tracker data from JSON file."""
    try:
        if TRACKER_FILE.exists():
            return json.loads(TRACKER_FILE.read_text())
    except Exception:
        pass
    return {}


def save_tracker(updates: dict):
    """Merge updates into tracker JSON and write back."""
    TRACKER_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = json.loads(TRACKER_FILE.read_text()) if TRACKER_FILE.exists() else {}
    except Exception:
        existing = {}
    existing.update(updates)
    TRACKER_FILE.write_text(json.dumps(existing, indent=2))


def score_color(score: float) -> str:
    if score >= SCORE_HIGH:
        return COLOR_HIGH
    if score >= SCORE_MED:
        return COLOR_MED
    return COLOR_LOW


@st.cache_data(ttl=300)
def load_data(city_key: str):
    """Load all-parcels CSV and qualified GeoJSON for a city."""
    csv_path  = OUTPUT_DIR / f"{city_key}_all_parcels.csv"
    geoj_path = OUTPUT_DIR / f"{city_key}_qualified_parcels.geojson"

    if not csv_path.exists():
        return None, None

    df  = pd.read_csv(csv_path)
    gdf = gpd.read_file(geoj_path) if geoj_path.exists() else gpd.GeoDataFrame()
    return df, gdf


def run_pipeline(city_key: str, force: bool = False):
    """Run the pipeline as a subprocess and stream output into the UI."""
    cmd = [sys.executable, str(ROOT / "src" / "pipeline.py"), "--city", city_key]
    if force:
        cmd.append("--refresh")

    with st.status("Running pipeline…", expanded=True) as status:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(ROOT / "src"),
        )
        output_area = st.empty()
        lines: list[str] = []
        for line in proc.stdout:
            lines.append(line.rstrip())
            output_area.code("\n".join(lines[-30:]))
        proc.wait()

        if proc.returncode == 0:
            status.update(label="Pipeline complete ✓", state="complete")
            st.cache_data.clear()
        else:
            status.update(label="Pipeline failed — see output above", state="error")


def make_map(gdf: gpd.GeoDataFrame, bbox: tuple,
             mode_label: str = "Single-Family") -> folium.Map:
    """Build a Folium map of qualified parcels, coloured by score."""
    min_lon, min_lat, max_lon, max_lat = bbox
    center = [(min_lat + max_lat) / 2, (min_lon + max_lon) / 2]

    m = folium.Map(location=center, zoom_start=13, tiles=None)

    # ── Base tile layers (toggled via top-right control) ──────────────────────
    folium.TileLayer(
        tiles="CartoDB positron",
        name="🗺️ Street Map",
        control=True,
        show=True,
    ).add_to(m)
    folium.TileLayer(
        tiles=(
            "https://server.arcgisonline.com/ArcGIS/rest/services/"
            "World_Imagery/MapServer/tile/{z}/{y}/{x}"
        ),
        attr=(
            "Esri, DigitalGlobe, GeoEye, Earthstar Geographics, "
            "CNES/Airbus DS, USDA, USGS, AeroGRID, IGN, GIS User Community"
        ),
        name="🛰️ Satellite",
        overlay=False,
        control=True,
        show=False,
    ).add_to(m)

    if gdf is None or gdf.empty:
        folium.LayerControl(position="topright", collapsed=False).add_to(m)
        m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])
        return m

    gdf = gdf.to_crs("EPSG:4326")

    # Group all parcel polygons into one toggleable layer
    parcel_group = folium.FeatureGroup(name="📍 Parcels", show=True)

    for _, row in gdf.iterrows():
        if row.geometry is None or row.geometry.is_empty:
            continue

        score  = float(row.get("score", 0) or 0)
        color  = score_color(score)
        addr   = row.get("address") or "(no address)"
        owner  = row.get("owner")   or ""
        acres  = float(row.get("calc_acres", 0)    or 0)
        net    = float(row.get("net_dev_acres", 0) or 0)
        zone_c = row.get("zone_code", "")  or ""
        zone_l = row.get("zone_label", "") or ""
        u_con  = int(row.get("units_conservative", 0) or 0)
        u_opt  = int(row.get("units_optimistic",   0) or 0)
        density_val = float(row.get("max_units_per_acre", 0) or 0)
        flood  = float(row.get("flood_pct",   0) or 0) * 100
        wet    = float(row.get("wetland_pct", 0) or 0) * 100
        mf     = row.get("mf_permitted",  "") or ""
        adu    = row.get("adu_permitted", "") or ""
        bldgs     = int(row.get("building_count", 0) or 0)
        bldg_pct  = float(row.get("building_pct", 0) or 0) * 100
        pathway   = str(row.get("dev_pathway", "") or "")
        # Future Land Use
        flu_code  = str(row.get("future_lu_code",  "") or "")
        flu_label = str(row.get("future_lu_label", "") or "")
        flu_max   = int(row.get("future_max_units", 0) or 0)
        rezone_up = bool(row.get("rezoning_upside", False))
        rezone_delta = int(row.get("rezoning_delta", 0) or 0)
        # Soil data
        soil_1 = str(row.get("soil_1", "") or "")
        soil_2 = str(row.get("soil_2", "") or "")
        soil_3 = str(row.get("soil_3", "") or "")

        # Tracker status badge
        _pid = str(row.get("parcel_id", "") or "")
        _trk = tracker.get(_pid, {})
        _status = _trk.get("status", "Not contacted")
        _notes  = _trk.get("notes", "")
        _status_color = STATUS_COLORS.get(_status, "#9ca3af")
        status_badge = (
            f"<span style='background:{_status_color};color:#fff;font-size:11px;"
            f"font-weight:600;padding:2px 8px;border-radius:10px;'>{_status}</span>"
        )

        # Dev pathway badge
        p_color = PATHWAY_COLORS.get(pathway, "#9ca3af")
        pathway_badge = (
            f"<span style='background:{p_color};color:#fff;font-size:11px;"
            f"font-weight:600;padding:2px 8px;border-radius:10px;'>{pathway}</span>"
            if pathway else ""
        )

        # Build score bar rows for each component
        score_bars = ""
        for comp in SCORE_COMPONENTS:
            pts     = float(row.get(comp["key"], 0) or 0)
            max_pts = comp["max"]
            pct     = int(pts / max_pts * 100) if max_pts else 0
            bar_color = COLOR_HIGH if pct >= 80 else (COLOR_MED if pct >= 40 else COLOR_LOW)
            score_bars += f"""
    <tr>
      <td style="color:#888;white-space:nowrap;padding-right:6px;">{comp['label']}</td>
      <td style="width:100%;">
        <div style="background:#e5e7eb;border-radius:3px;height:8px;width:100%;">
          <div style="background:{bar_color};border-radius:3px;height:8px;width:{pct}%;"></div>
        </div>
      </td>
      <td style="color:#333;padding-left:6px;white-space:nowrap;">{pts:.0f}/{max_pts}</td>
    </tr>"""

        # FLU row — only shown when data is available
        if flu_code:
            rezone_badge = (
                f"<span style='color:{COLOR_HIGH};font-weight:700;'>"
                f" ↑ +{rezone_delta} u/ac upside</span>"
                if rezone_up else ""
            )
            flu_row = (
                f"<tr><td style='color:#888;'>Future LU</td>"
                f"<td colspan='2'>{flu_label} ({flu_max} u/ac){rezone_badge}</td></tr>"
            )
        else:
            flu_row = ""

        popup_html = f"""
<div style="font-family:sans-serif;min-width:270px;font-size:13px;">
  <b style="font-size:15px;">{addr}</b><br>
  <span style="color:#555;">{owner}</span>
  <div style="margin-top:5px;">{status_badge}</div>
  {f"<div style='font-size:11px;color:#555;margin-top:3px;font-style:italic;'>{_notes}</div>" if _notes else ""}
  <hr style="margin:6px 0;">
  <table style="width:100%;border-collapse:collapse;line-height:1.7;">
    <tr><td style="color:#888;">Acres</td>
        <td colspan="2">{acres:.2f} gross / {net:.2f} net dev</td></tr>
    <tr><td style="color:#888;">Structures</td>
        <td colspan="2">{
            "<span style='color:#22c55e;font-weight:600;'>✅ Vacant (0 structures)</span>"
            if bldgs == 0 else
            f"<span style='color:#22c55e;font-weight:600;'>✅ Minor structure only ({bldgs} detected, {'< 0.1' if bldg_pct < 0.1 else f'{bldg_pct:.1f}'}% coverage)</span>"
            if bldg_pct < 0.5 else
            f"<span style='color:{'#f59e0b' if bldg_pct < 2 else '#ef4444'};font-weight:600;'>⚠️ {bldgs} structure{'s' if bldgs != 1 else ''} ({'< 0.1' if bldg_pct < 0.1 else f'{bldg_pct:.1f}'}% coverage)</span>"
        }</td></tr>
    <tr><td style="color:#888;">Zone</td>
        <td colspan="2">{zone_c} — {zone_l}</td></tr>
    <tr><td style="color:#888;">Density ({"MF" if mode_label == "Multifamily" else "SF"})</td>
        <td colspan="2">{density_val:.0f} u/ac</td></tr>
    <tr><td style="color:#888;">Pathway</td>
        <td colspan="2">{pathway_badge}</td></tr>
    <tr><td style="color:#888;">Units</td>
        <td colspan="2">{u_con}–{u_opt} (cons–opt)</td></tr>
    <tr><td style="color:#888;">Flood</td>
        <td colspan="2">{flood:.1f}%</td></tr>
    <tr><td style="color:#888;">Wetland</td>
        <td colspan="2">{wet:.1f}%</td></tr>
    <tr><td style="color:#888;">MF / ADU</td>
        <td colspan="2">{mf} / {adu}</td></tr>
    {flu_row}
    {f"<tr><td style='color:#888;'>Soil</td><td colspan='2'>{soil_1}</td></tr>" if soil_1 else ""}
    {f"<tr><td style='color:#888;'></td><td colspan='2' style='color:#666;font-size:12px;'>{soil_2}</td></tr>" if soil_2 else ""}
    {f"<tr><td style='color:#888;'></td><td colspan='2' style='color:#666;font-size:12px;'>{soil_3}</td></tr>" if soil_3 else ""}
  </table>
  <hr style="margin:6px 0;">
  <div style="font-weight:600;margin-bottom:4px;">
    Score: <span style="color:{color};">{score:.1f} / 100</span>
  </div>
  <table style="width:100%;border-collapse:collapse;line-height:2;">
    {score_bars}
  </table>
</div>"""

        folium.GeoJson(
            row.geometry.__geo_interface__,
            style_function=lambda _x, c=color: {
                "fillColor":   c,
                "color":       COLOR_STROKE,
                "weight":      1.5,
                "fillOpacity": 0.55,
            },
            highlight_function=lambda _x: {
                "weight":      3,
                "fillOpacity": 0.8,
            },
            popup=folium.Popup(popup_html, max_width=290),
            tooltip=f"{addr}  |  Score {score:.0f}  |  {u_con}–{u_opt} units",
        ).add_to(parcel_group)

    parcel_group.add_to(m)
    folium.LayerControl(position="topright", collapsed=False).add_to(m)
    m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])
    return m


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🏗️ WM Land Screener")
    st.caption("West Michigan Vacant Land Feasibility Tool — Phase 1")

    # User info + logout
    _display_name = _user_data.get("first_name") or _username
    _role_badge   = " · Admin" if IS_ADMIN else ""
    st.caption(f"Logged in as **{_display_name}**{_role_badge}")
    _authenticator.logout(button_name="Log out", location="sidebar")
    st.divider()

    # City selector
    city_options = {v["label"]: k for k, v in CITIES.items()}
    selected_label = st.selectbox("City / Area", list(city_options.keys()))
    city_key = city_options[selected_label]
    city_cfg = CITIES[city_key]
    city_min  = city_cfg.get("min_acres", MIN_ACRES)

    st.divider()
    dev_type = st.radio(
        "Development type",
        ["Single-Family", "Multifamily"],
        horizontal=True,
        help=(
            "**Single-Family** — densities based on SF minimum lot standards. "
            "Full score credit at 7 u/ac.\n\n"
            "**Multifamily** — densities based on MF zoning caps. "
            "Full score credit at 30 u/ac. Unlocks additional zones (MFR, NMU, OS, etc.)."
        ),
    )

    st.divider()
    st.subheader("Display filters")
    st.caption("These filter the map view only — re-run the pipeline to change hard filters.")

    min_score_ui = st.slider("Min score", 0, 100, 0, step=5)
    min_acres_ui = st.slider(
        "Min acres (display)",
        0.0, 50.0, float(city_min), step=0.5,
        help=f"Pipeline hard filter is {city_min} ac for this city",
    )
    max_flood_ui = st.slider("Max flood % (display)", 0, 100, 25, step=5)

    max_building_pct_ui = st.slider(
        "Max building coverage %",
        min_value=0.0,
        max_value=5.0,
        value=5.0,
        step=0.1,
        help=(
            "Filters by how much of the parcel is covered by building footprints "
            "(Microsoft satellite-derived data).\n\n"
            "The pipeline hard filter already removes parcels above **5%** "
            "(dense housing communities). This slider lets you tighten further:\n\n"
            "• **5.0% (default)** — show all qualifying parcels\n"
            "• **1.0%** — likely includes at most 1–2 structures "
            "(e.g. a house on a large lot)\n"
            "• **0.0%** — truly vacant: no structures detected at all\n\n"
            "A single 2,000 sq ft home on 5 acres ≈ 0.9% coverage."
        ),
    )

    # Zone filter — placeholder filled after data loads
    zone_placeholder = st.empty()

    # Development pathway filter
    pathway_placeholder = st.empty()

    # Parcel tracker status filter
    status_placeholder = st.empty()

    st.divider()
    st.subheader("🔮 Master plan data")
    flu_file = (
        Path(__file__).parent.parent / "data" / "raw"
        / f"{city_key}_future_lu.geojson"
    )
    flu_svc = city_cfg.get("flu_service")
    if flu_file.exists():
        st.success("Future Land Use layer loaded ✓", icon="✅")
    elif flu_svc:
        st.info("FLU service configured — will download on next run.", icon="🌐")
    else:
        st.warning(
            f"No Future Land Use data loaded for **{city_cfg['label']}**.\n\n"
            "To enable rezoning scoring, set `flu_service` in "
            f"`CITIES['{city_key}']` in `config.py`, or place a GeoJSON at "
            f"`data/raw/{city_key}_future_lu.geojson`.",
            icon="📋",
        )

    # Parcel service status — highlight when not configured
    parcel_svc = city_cfg.get("parcel_service")
    parcel_cache = (
        Path(__file__).parent.parent / "data" / "raw"
        / f"{city_key}_parcels.geojson"
    )
    if parcel_svc is None and not parcel_cache.exists():
        st.warning(
            f"No parcel service configured for **{city_cfg['label']}**.\n\n"
            "Set `parcel_service` in `config.py` or place a GeoJSON at "
            f"`data/raw/{city_key}_parcels.geojson`. See config.py TODO comment.",
            icon="⚠️",
        )

    st.divider()
    st.subheader("Pipeline")
    if IS_ADMIN:
        st.caption("Cached data loads in seconds. Refresh re-downloads all layers (~2 min).")
        col_a, col_b = st.columns(2)
        with col_a:
            run_btn = st.button("▶ Run", use_container_width=True,
                                help="Run pipeline using cached layer data")
        with col_b:
            refresh_btn = st.button("⟳ Refresh", use_container_width=True,
                                    help="Re-download all layers then run pipeline")
    else:
        run_btn = False
        refresh_btn = False
        st.caption("Data is refreshed by the admin. Results update automatically.")

# ── Trigger pipeline if requested ─────────────────────────────────────────────
if run_btn:
    run_pipeline(city_key, force=False)
if refresh_btn:
    run_pipeline(city_key, force=True)

# ── Load data ─────────────────────────────────────────────────────────────────
df_all, gdf_qual = load_data(city_key)
tracker = load_tracker()

# ── Main content ──────────────────────────────────────────────────────────────
st.title(f"🏗️ {city_cfg['label']} — Vacant Land Screener")

if df_all is None:
    st.warning(
        f"No data found for **{city_cfg['label']}**. "
        "Click **▶ Run** in the sidebar to run the pipeline first."
    )
    st.stop()

# ── Zone multiselect (needs data to populate) ─────────────────────────────────
all_zones = sorted(df_all["zone_code"].dropna().unique().tolist())
selected_zones = zone_placeholder.multiselect(
    "Zone codes", all_zones, default=all_zones
)

# ── Pathway filter (needs data to populate) ───────────────────────────────────
if "dev_pathway" in df_all.columns:
    qual_pathways = sorted(
        df_all.loc[df_all["pass_filter"] == True, "dev_pathway"].dropna().unique().tolist()
    )
    selected_pathways = pathway_placeholder.multiselect(
        "Dev pathway (3+ u/ac)",
        options=qual_pathways,
        default=qual_pathways,
        help=(
            "Filter by how the parcel reaches ≥3 units/acre:\n\n"
            "🟢 **By right** — current zoning already allows it\n"
            "🔵 **PRD special use** — Grand Haven: ≥5 ac in LDR/MDR/MFR via Sec. 40-552\n"
            "🔵 **PUD special use** — Spring Lake Twp: ≥5 ac with density bonus via Article 14\n"
            "🟣 **Master plan upzone** — future land use supports higher density\n"
            "🟡 **PD rezoning** — legislative rezoning (case-by-case)\n"
            "⚫ **Not viable** — no clear path to 3+ u/ac (eliminated from results)"
        ),
    )
else:
    selected_pathways = None

# ── Tracker status filter (filled into sidebar placeholder) ───────────────────
selected_statuses = status_placeholder.multiselect(
    "Tracker status",
    options=STATUS_OPTIONS,
    default=STATUS_OPTIONS,
    help="Filter qualifying parcels by their tracker status.",
)

# ── Apply development type mode ───────────────────────────────────────────────
MF_DENSITY_COL = "mf_max_units_per_acre"
USE_MF = (dev_type == "Multifamily") and (MF_DENSITY_COL in df_all.columns)

if USE_MF:
    # Refilter from all parcels using MF density — bypass the SF pass_filter
    _exempt_col = next((c for c in ["class", "propclass", "prop_class", "propertyclass"]
                        if c in df_all.columns), None)
    _exempt_mask = (
        df_all[_exempt_col].astype(str).str.strip().isin({"701"})
        if _exempt_col else pd.Series(False, index=df_all.index)
    )
    _flood_mask   = df_all["flood_pct"].fillna(0) > MAX_FLOOD_PCT
    _bldg_mask    = df_all["building_pct"].fillna(0) > 0.01 if "building_pct" in df_all.columns \
                    else pd.Series(False, index=df_all.index)
    _density_mask = df_all[MF_DENSITY_COL].fillna(0) < 3

    _fail = _exempt_mask | _flood_mask | _bldg_mask | _density_mask
    qual_all = df_all[~_fail].copy()

    # Recompute density score and total score using MF density + MF ceiling (30 u/ac)
    qual_all["max_units_per_acre"] = qual_all[MF_DENSITY_COL]
    qual_all["pts_density"] = (qual_all[MF_DENSITY_COL] / 30).clip(upper=1.0).mul(40).round(1)
    score_cols = ["pts_density", "pts_rezoning", "pts_wetland", "pts_flood", "pts_shape"]
    existing   = [c for c in score_cols if c in qual_all.columns]
    qual_all["score"] = qual_all[existing].fillna(0).sum(axis=1).clip(upper=100).round(1)

    # Recompute unit estimates using MF density
    net = qual_all["net_dev_acres"] if "net_dev_acres" in qual_all.columns \
          else qual_all["calc_acres"]
    qual_all["units_conservative"] = (net * qual_all[MF_DENSITY_COL] * 0.70).round(0).astype(int)
    qual_all["units_optimistic"]   = (net * qual_all[MF_DENSITY_COL]).round(0).astype(int)
else:
    qual_all = df_all[df_all["pass_filter"] == True].copy()

# ── Apply display filters to qualifying parcels ───────────────────────────────

_building_mask = (
    qual_all["building_pct"].fillna(0) <= (max_building_pct_ui / 100)
    if "building_pct" in qual_all.columns
    else pd.Series(True, index=qual_all.index)
)

_pathway_mask = (
    qual_all["dev_pathway"].isin(selected_pathways)
    if selected_pathways is not None and "dev_pathway" in qual_all.columns
    else pd.Series(True, index=qual_all.index)
)

_tracker_status_mask = qual_all["parcel_id"].astype(str).map(
    lambda pid: tracker.get(pid, {}).get("status", "Not contacted")
).isin(selected_statuses) if "parcel_id" in qual_all.columns else pd.Series(True, index=qual_all.index)

qual_filtered = qual_all[
    (qual_all["score"]      >= min_score_ui) &
    (qual_all["calc_acres"] >= min_acres_ui) &
    (qual_all["flood_pct"]  <= max_flood_ui / 100) &
    (qual_all["zone_code"].isin(selected_zones)) &
    _building_mask &
    _pathway_mask &
    _tracker_status_mask
]

# Filter GeoJSON to match
if gdf_qual is not None and not gdf_qual.empty and "parcel_id" in gdf_qual.columns:
    shown_ids   = set(qual_filtered["parcel_id"].dropna().astype(str))
    gdf_shown   = gdf_qual[gdf_qual["parcel_id"].astype(str).isin(shown_ids)]
else:
    gdf_shown = gdf_qual  # fall back to all if no parcel_id

# ── Top metrics ───────────────────────────────────────────────────────────────
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Total parcels scanned", f"{len(df_all):,}")
m2.metric("Passed hard filters", f"{len(qual_all):,}")
m3.metric("Shown on map", f"{len(qual_filtered):,}")
m4.metric("Units — conservative", f"{int(qual_filtered['units_conservative'].sum()):,}")
m5.metric("Units — optimistic",   f"{int(qual_filtered['units_optimistic'].sum()):,}")

# ── Legend ────────────────────────────────────────────────────────────────────
leg1, leg2, leg3, _rest = st.columns([1, 1, 1, 5])
leg1.markdown(f"<span style='color:{COLOR_HIGH}'>⬛</span> Score ≥ {SCORE_HIGH}",
              unsafe_allow_html=True)
leg2.markdown(f"<span style='color:{COLOR_MED}'>⬛</span> Score {SCORE_MED}–{SCORE_HIGH-1}",
              unsafe_allow_html=True)
leg3.markdown(f"<span style='color:{COLOR_LOW}'>⬛</span> Score < {SCORE_MED}",
              unsafe_allow_html=True)

# ── Merge MF-recomputed values into gdf_shown so popup reflects active mode ───
if USE_MF and gdf_shown is not None and not gdf_shown.empty \
        and "parcel_id" in qual_filtered.columns and "parcel_id" in gdf_shown.columns:
    mf_merge_cols = ["parcel_id", "score", "max_units_per_acre",
                     "units_conservative", "units_optimistic", "pts_density"]
    mf_merge_cols = [c for c in mf_merge_cols if c in qual_filtered.columns]
    drop_cols = [c for c in mf_merge_cols if c != "parcel_id" and c in gdf_shown.columns]
    gdf_shown = gdf_shown.drop(columns=drop_cols)
    gdf_shown = gdf_shown.merge(qual_filtered[mf_merge_cols], on="parcel_id", how="left")

# ── Map ───────────────────────────────────────────────────────────────────────
_mode_label_map = "Multifamily" if USE_MF else "Single-Family"
m = make_map(gdf_shown, city_cfg["bbox"], mode_label=_mode_label_map)
st_folium(m, use_container_width=True, height=530, returned_objects=[])

# ── Qualifying parcels table ──────────────────────────────────────────────────
with st.expander(f"📋 Qualifying parcels  ({len(qual_filtered)} shown)", expanded=True):
    display_cols = [
        "parcel_id", "address", "owner",
        "calc_acres", "net_dev_acres",
        "building_count", "building_pct",   # vacancy indicators — shown early
        "zone_code", "zone_label",
        "dev_pathway",
        "max_units_per_acre", "units_conservative", "units_optimistic",
        "flood_pct", "wetland_pct",
        "shape_score",
        "soil_1", "soil_2", "soil_3",
        "mf_permitted", "adu_permitted",
        # FLU columns (only shown when data is loaded — filtered below)
        "future_lu_label", "future_max_units", "rezoning_delta",
        "score", "review_flag",
    ]
    display_cols = [c for c in display_cols if c in qual_filtered.columns]
    fmt = qual_filtered[display_cols].copy()

    # Format shape_score as a % (0–100%) and rename for clarity
    if "shape_score" in fmt.columns:
        fmt["shape_score"] = (fmt["shape_score"] * 100).round(0).astype(int).astype(str) + "%"
        fmt = fmt.rename(columns={"shape_score": "Shape %"})

    # Rename soil columns for readability
    soil_rename = {"soil_1": "Dominant Soil", "soil_2": "Soil 2", "soil_3": "Soil 3"}
    fmt = fmt.rename(columns={k: v for k, v in soil_rename.items() if k in fmt.columns})
    # Drop Soil 2 / Soil 3 columns if entirely empty (keeps table clean when parcels have one soil)
    for col in ["Soil 2", "Soil 3"]:
        if col in fmt.columns and fmt[col].fillna("").eq("").all():
            fmt = fmt.drop(columns=[col])

    # Rename review_flag column for readability
    if "review_flag" in fmt.columns:
        fmt = fmt.rename(columns={"review_flag": "Needs Review"})

    for col in ("calc_acres", "net_dev_acres"):
        if col in fmt.columns:
            fmt[col] = fmt[col].round(2)
    for col in ("flood_pct", "wetland_pct", "building_pct"):
        if col in fmt.columns:
            fmt[col] = (fmt[col] * 100).round(1).astype(str) + "%"
    if "building_pct" in fmt.columns:
        fmt = fmt.rename(columns={"building_pct": "Bldg Coverage %",
                                   "building_count": "Structures"})

    # Add tracker columns
    _pid_col = "parcel_id" if "parcel_id" in fmt.columns else None
    if _pid_col:
        fmt["Status"] = fmt[_pid_col].astype(str).map(
            lambda pid: tracker.get(pid, {}).get("status", "Not contacted")
        )
        fmt["Notes"] = fmt[_pid_col].astype(str).map(
            lambda pid: tracker.get(pid, {}).get("notes", "")
        )
        fmt["Reviewed ✓"] = fmt[_pid_col].astype(str).map(
            lambda pid: bool(tracker.get(pid, {}).get("reviewed", False))
        )

    fmt_sorted = fmt.sort_values("score", ascending=False).reset_index(drop=True)

    # Determine which columns are editable
    _editable = {"Status", "Notes", "Reviewed ✓"} if _pid_col else set()
    _disabled = [c for c in fmt_sorted.columns if c not in _editable]

    edited = st.data_editor(
        fmt_sorted,
        column_config={
            "Status": st.column_config.SelectboxColumn(
                "Status",
                options=STATUS_OPTIONS,
                required=True,
            ),
            "Notes": st.column_config.TextColumn("Notes", width="large"),
            "Reviewed ✓": st.column_config.CheckboxColumn(
                "Reviewed ✓",
                help="Check once you've manually reviewed this parcel.",
                default=False,
            ),
        },
        disabled=_disabled,
        hide_index=True,
        use_container_width=True,
        key=f"tracker_editor_{city_key}",
    )

    # Detect changes and persist
    if _pid_col and edited is not None:
        updates = {}
        for _, erow in edited.iterrows():
            pid      = str(erow[_pid_col])
            status   = erow.get("Status", "Not contacted")
            notes    = erow.get("Notes", "") or ""
            reviewed = bool(erow.get("Reviewed ✓", False))
            old      = tracker.get(pid, {})
            if (old.get("status", "Not contacted") != status
                    or old.get("notes", "") != notes
                    or bool(old.get("reviewed", False)) != reviewed):
                updates[pid] = {
                    "status":     status,
                    "notes":      notes,
                    "reviewed":   reviewed,
                    "updated":    datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "updated_by": _username,
                }
        if updates:
            save_tracker(updates)
            tracker.update(updates)

    csv_bytes = qual_filtered.to_csv(index=False).encode()
    st.download_button(
        "⬇ Download filtered CSV",
        data=csv_bytes,
        file_name=f"{city_key}_qualified_filtered.csv",
        mime="text/csv",
    )

# ── Tracker summary ───────────────────────────────────────────────────────────
if tracker:
    with st.expander("📊 Parcel tracker summary", expanded=False):
        st.caption("Counts across all parcels ever tracked (not filtered by current display).")
        counts = {s: 0 for s in STATUS_OPTIONS}
        for v in tracker.values():
            s = v.get("status", "Not contacted")
            if s in counts:
                counts[s] += 1
        summary_cols = st.columns(len(STATUS_OPTIONS))
        for col_ui, status_name in zip(summary_cols, STATUS_OPTIONS):
            sc = STATUS_COLORS[status_name]
            col_ui.markdown(
                f"<div style='background:{sc}18;border-left:4px solid {sc};"
                f"border-radius:6px;padding:10px 12px;'>"
                f"<div style='font-size:11px;color:{sc};font-weight:700;"
                f"text-transform:uppercase;letter-spacing:.04em;'>{status_name}</div>"
                f"<div style='font-size:26px;font-weight:800;color:#1e293b;'>{counts[status_name]}</div>"
                f"<div style='font-size:11px;color:#64748b;'>parcels</div></div>",
                unsafe_allow_html=True,
            )

# ── Per-parcel score breakdown ────────────────────────────────────────────────
comp_keys = [c["key"] for c in SCORE_COMPONENTS]
if not qual_filtered.empty and all(k in qual_filtered.columns for k in comp_keys):
    with st.expander("🔢 Score breakdown — per parcel", expanded=True):
        st.caption(
            "Shows how each component contributed to a parcel's total score. "
            "Bar = % of that component's maximum earned."
        )

        for _, row in qual_filtered.sort_values("score", ascending=False).iterrows():
            addr       = row.get("address") or row.get("parcel_id") or "Parcel"
            score      = float(row.get("score", 0) or 0)
            color      = score_color(score)
            rezone_up  = bool(row.get("rezoning_upside", False))
            rezone_d   = int(row.get("rezoning_delta", 0) or 0)
            flu_label  = str(row.get("future_lu_label", "") or "")

            # Rezoning upside badge
            rezone_badge = ""
            if rezone_up and flu_label:
                rezone_badge = (
                    f" &nbsp;<span style='background:{COLOR_HIGH};color:#fff;"
                    f"font-size:11px;font-weight:700;padding:2px 7px;"
                    f"border-radius:10px;'>🔮 REZONING +{rezone_d} u/ac → {flu_label}</span>"
                )
            elif flu_label:
                rezone_badge = (
                    f" &nbsp;<span style='background:#e5e7eb;color:#555;"
                    f"font-size:11px;padding:2px 7px;border-radius:10px;'>"
                    f"FLU: {flu_label}</span>"
                )

            st.markdown(
                f"**{addr}** &nbsp; "
                f"<span style='color:{color};font-size:1.1em;font-weight:700;'>"
                f"Total: {score:.1f} / 100</span>"
                f"{rezone_badge}",
                unsafe_allow_html=True,
            )

            cols = st.columns(len(SCORE_COMPONENTS))
            for col_ui, comp in zip(cols, SCORE_COMPONENTS):
                pts     = float(row.get(comp["key"], 0) or 0)
                max_pts = comp["max"]
                pct     = pts / max_pts if max_pts else 0
                bar_col = COLOR_HIGH if pct >= 0.8 else (COLOR_MED if pct >= 0.4 else COLOR_LOW)
                col_ui.markdown(
                    f"<div style='font-size:11px;color:#888;margin-bottom:2px;'>"
                    f"{comp['label']}</div>"
                    f"<div style='background:#e5e7eb;border-radius:4px;height:10px;'>"
                    f"<div style='background:{bar_col};border-radius:4px;height:10px;"
                    f"width:{int(pct*100)}%;'></div></div>"
                    f"<div style='font-size:12px;margin-top:2px;'>"
                    f"<b>{pts:.0f}</b> / {max_pts}</div>",
                    unsafe_allow_html=True,
                )
            st.divider()

# ── Rezoning watch list ───────────────────────────────────────────────────────
# Show qualifying parcels that have a positive rezoning delta (FLU > current zoning)
flu_available = "future_lu_code" in qual_filtered.columns and \
                qual_filtered["future_lu_code"].astype(str).str.strip().any()

if flu_available:
    rezone_parcels = qual_filtered[qual_filtered["rezoning_upside"] == True].copy()
    if not rezone_parcels.empty:
        with st.expander(
            f"🔮 Rezoning watch list  ({len(rezone_parcels)} parcels with upside)",
            expanded=True,
        ):
            st.caption(
                "These qualifying parcels are **master-planned for higher density** than "
                "their current zoning allows. They are strong rezoning candidates — "
                "the current zoning may be a temporary constraint rather than a ceiling."
            )
            rezone_display_cols = [c for c in [
                "address", "owner", "calc_acres",
                "zone_code", "zone_label", "max_units_per_acre",
                "future_lu_label", "future_max_units", "rezoning_delta",
                "units_conservative", "units_optimistic", "score",
            ] if c in rezone_parcels.columns]
            rz_fmt = rezone_parcels[rezone_display_cols].copy()
            if "calc_acres" in rz_fmt.columns:
                rz_fmt["calc_acres"] = rz_fmt["calc_acres"].round(2)
            st.dataframe(
                rz_fmt.sort_values("rezoning_delta", ascending=False),
                width="stretch",
                hide_index=True,
            )

# ── Development pathway breakdown ────────────────────────────────────────────
if "dev_pathway" in qual_filtered.columns:
    with st.expander(
        "🛣️ Development pathway breakdown  (how each parcel reaches 3+ u/ac)",
        expanded=True,
    ):
        st.caption(
            "Every qualifying parcel is classified by the **simplest available route** to "
            "≥ 3 units/acre. Approval burden increases left → right. "
            "Use the sidebar filter to isolate a specific pathway."
        )
        # Summary counts
        pathway_order = [
            "By right", "PRD special use", "PUD special use",
            "Master plan upzone", "PD rezoning", "Not viable",
        ]
        counts = qual_filtered["dev_pathway"].value_counts().reindex(
            pathway_order, fill_value=0
        ).reset_index()
        counts.columns = ["Pathway", "Parcels"]

        # Color-coded metric tiles
        metric_cols = st.columns(len(pathway_order))
        for col_ui, (_, row_p) in zip(metric_cols, counts.iterrows()):
            pname  = row_p["Pathway"]
            pcount = int(row_p["Parcels"])
            pcolor = PATHWAY_COLORS.get(pname, "#9ca3af")
            col_ui.markdown(
                f"<div style='background:{pcolor}18;border-left:4px solid {pcolor};"
                f"border-radius:6px;padding:10px 12px;'>"
                f"<div style='font-size:11px;color:{pcolor};font-weight:700;"
                f"text-transform:uppercase;letter-spacing:.04em;'>{pname}</div>"
                f"<div style='font-size:26px;font-weight:800;color:#1e293b;'>{pcount}</div>"
                f"<div style='font-size:11px;color:#64748b;'>parcels</div></div>",
                unsafe_allow_html=True,
            )

        # Detailed pathway table (excluding 'Not viable' — they won't show in map anyway)
        viable = qual_filtered[qual_filtered["dev_pathway"] != "Not viable"].copy()
        if not viable.empty:
            st.markdown("---")
            pathway_detail_cols = [c for c in [
                "address", "owner", "calc_acres",
                "zone_code", "zone_label", "dev_pathway",
                "max_units_per_acre", "future_lu_label", "future_max_units",
                "units_conservative", "units_optimistic", "score",
            ] if c in viable.columns]
            pwy_fmt = viable[pathway_detail_cols].copy()
            if "calc_acres" in pwy_fmt.columns:
                pwy_fmt["calc_acres"] = pwy_fmt["calc_acres"].round(2)
            st.dataframe(
                pwy_fmt.sort_values(["dev_pathway", "score"], ascending=[True, False]),
                width="stretch",
                hide_index=True,
            )

# ── Ordinance review list ─────────────────────────────────────────────────────
review_available = "review_flag" in qual_filtered.columns and \
                   qual_filtered["review_flag"].any()

if review_available:
    review_parcels = qual_filtered[qual_filtered["review_flag"] == True].copy()
    if not review_parcels.empty:
        with st.expander(
            f"⚠️ Needs manual review  ({len(review_parcels)} parcels)",
            expanded=False,
        ):
            st.caption(
                "These qualifying parcels have ordinance conditions that cannot be resolved "
                "automatically — density is unconfirmed, multifamily requires special use "
                "approval, a per-structure unit cap applies, or another provision needs "
                "manual verification. Review reasons and ordinance links are shown below."
            )
            review_display_cols = [c for c in [
                "address", "owner", "calc_acres",
                "zone_code", "zone_label", "max_units_per_acre",
                "units_conservative", "units_optimistic",
                "review_reasons", "ordinance_url", "score",
            ] if c in review_parcels.columns]
            rv_fmt = review_parcels[review_display_cols].copy()
            if "calc_acres" in rv_fmt.columns:
                rv_fmt["calc_acres"] = rv_fmt["calc_acres"].round(2)
            # Make ordinance_url a clickable link
            if "ordinance_url" in rv_fmt.columns:
                rv_fmt["ordinance_url"] = rv_fmt["ordinance_url"].apply(
                    lambda u: f'<a href="{u}" target="_blank">Ordinance ↗</a>'
                    if u else ""
                )
                st.write(
                    rv_fmt.sort_values("score", ascending=False).to_html(
                        escape=False, index=False
                    ),
                    unsafe_allow_html=True,
                )
            else:
                st.dataframe(
                    rv_fmt.sort_values("score", ascending=False),
                    width="stretch",
                    hide_index=True,
                )

# ── Scoring methodology ───────────────────────────────────────────────────────
with st.expander("📐 How scores are calculated"):
    _mode_label = "Multifamily" if USE_MF else "Single-Family"
    st.markdown(
        f"Each qualifying parcel is scored **0–100** across five components "
        f"in **{_mode_label}** mode. "
        "Hard filters (flood, buildings, zoning) must pass first — "
        "parcels that fail any hard filter are excluded entirely and not scored."
    )
    # Density description varies by mode
    _density_desc = (
        "Multifamily units/acre allowed by zoning. Full credit at 30 u/ac "
        "(e.g. MFR, NMU, OS zones). Based on MF zoning caps per ordinance."
        if USE_MF else
        "Single-family units/acre allowed by zoning. Full credit at 7 u/ac "
        "(e.g. MDR, OT, S zones). Based on SF minimum lot area per ordinance."
    )
    methodology_rows = []
    for comp in SCORE_COMPONENTS:
        desc = _density_desc if comp["key"] == "pts_density" else comp["description"]
        methodology_rows.append({
            "Component":    comp["label"],
            "Max points":   comp["max"],
            "How it works": desc,
        })
    st.dataframe(
        pd.DataFrame(methodology_rows),
        width="stretch",
        hide_index=True,
    )
    st.caption(
        "**Phase 2 additions planned:** tax delinquency flag (+pts), MLS listing status, "
        "water/sewer availability."
    )

# ── Filter breakdown ─────────────────────────────────────────────────────────
with st.expander("📊 Why parcels were eliminated"):
    reason_counts = df_all["filter_reason"].value_counts().reset_index()
    reason_counts.columns = ["Reason", "Count"]
    reason_counts["% of total"] = (
        reason_counts["Count"] / len(df_all) * 100
    ).round(1).astype(str) + "%"
    st.dataframe(reason_counts, width="stretch", hide_index=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Data sources: Ottawa County ArcGIS parcels/zoning · FEMA NFHL flood zones · "
    "EGLE Part 303 State Wetland Inventory (gisagoegle.state.mi.us) · OSM building footprints · "
    "Ottawa County MasterPlanZoning service (Future Land Use — gis.miottawa.org).  "
    "Grand Haven density values verified against Chapter 40 (Municode, Jan 2026). "
    "Parcels flagged ⚠️ have ordinance conditions requiring manual review — see the Needs Review section."
)
