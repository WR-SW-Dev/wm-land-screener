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
from config import (CITIES, OUTPUT_DIR, MIN_ACRES, MAX_FLOOD_PCT,  # noqa: E402
                    DRAIN_MAINTYPE_COLORS, DRAIN_DEFAULT_COLOR)
from utility_pdf import (WATER_SIZE_HEX, SEWER_SPEC_HEX,           # noqa: E402
                         sewer_spec_label)
from ordinance import load_ordinance, get_district, ordinance_url  # noqa: E402
from scoring import SCORE_COMPONENTS                              # noqa: E402

# ── Brand CSS (owned here; injected by the shell) ────────────────────────────
def inject_brand_css():
    """Inject the WR-Dev brand stylesheet. Called once by app_shell.py."""
    st.markdown("""
<style>
/* ── Fonts ── */
html, body, [class*="css"], .stMarkdown, .stDataFrame,
button, input, select, textarea {
    font-family: Arial, sans-serif !important;
}

/* ── Brand colors ── */
:root {
    --wr-teal:      #779FA1;
    --wr-gray:      #A1ABAC;
    --wr-warm:      #C5C5B9;
    --wr-dark:      #2c3e3f;
    --wr-light-bg:  #f5f6f4;
}

/* ── Page background ── */
.stApp { background-color: #ffffff; }

/* ── Main title ── */
h1 { color: var(--wr-dark) !important; font-family: Arial, sans-serif !important; }
h2, h3 { color: var(--wr-teal) !important; font-family: Arial, sans-serif !important; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background-color: var(--wr-light-bg) !important;
    border-right: 1px solid var(--wr-warm);
}
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stRadio label {
    color: var(--wr-dark) !important;
    font-family: Arial, sans-serif !important;
}
/* Sidebar section dividers */
[data-testid="stSidebar"] hr { border-color: var(--wr-warm); }

/* ── Radio button — remove label highlight ── */
[data-testid="stRadio"] label {
    background: transparent !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    border-bottom: 2px solid var(--wr-warm);
}
.stTabs [data-baseweb="tab"] {
    font-family: Arial, sans-serif !important;
    color: var(--wr-gray) !important;
    font-weight: 600;
}
.stTabs [aria-selected="true"] {
    color: var(--wr-teal) !important;
    border-bottom: 2px solid var(--wr-teal) !important;
}

/* ── Metric cards ── */
[data-testid="stMetric"] {
    background: var(--wr-light-bg);
    border-left: 3px solid var(--wr-teal);
    border-radius: 6px;
    padding: 10px 14px !important;
}
[data-testid="stMetricValue"] {
    color: var(--wr-dark) !important;
    font-family: Arial, sans-serif !important;
}
[data-testid="stMetricLabel"] {
    color: var(--wr-gray) !important;
    font-family: Arial, sans-serif !important;
    font-size: 12px !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}

/* ── Expander headers ── */
[data-testid="stExpander"] summary {
    font-family: Arial, sans-serif !important;
    font-weight: 600;
    color: var(--wr-dark) !important;
    border-left: 3px solid var(--wr-teal);
    padding-left: 8px;
}

/* ── Buttons ── */
.stButton button {
    background-color: var(--wr-teal) !important;
    color: white !important;
    border: none !important;
    font-family: Arial, sans-serif !important;
    font-weight: 600;
    border-radius: 6px !important;
}
.stButton button:hover {
    background-color: #5a8a8c !important;
}

/* ── Download button ── */
.stDownloadButton button {
    background-color: white !important;
    color: var(--wr-teal) !important;
    border: 1.5px solid var(--wr-teal) !important;
    font-family: Arial, sans-serif !important;
    font-weight: 600;
    border-radius: 6px !important;
}

/* ── Slider — replace pink with teal ── */
[data-testid="stSlider"] [data-baseweb="slider"] [role="slider"] {
    background-color: var(--wr-teal) !important;
    border-color: var(--wr-teal) !important;
}
[data-testid="stSlider"] [data-baseweb="slider"] div[data-testid="stThumbValue"],
[data-testid="stSlider"] [data-baseweb="slider"] [class*="Track"] > div:first-child {
    background-color: var(--wr-teal) !important;
}

/* ── Radio buttons (SF / MF toggle) — replace pink with teal ── */
[data-testid="stRadio"] [data-baseweb="radio"] [data-checked="true"] div,
[data-testid="stRadio"] input[type="radio"]:checked + div {
    background-color: var(--wr-teal) !important;
    border-color: var(--wr-teal) !important;
}
[data-baseweb="radio"] [data-checked="true"] > div:first-child {
    border-color: var(--wr-teal) !important;
    background-color: var(--wr-teal) !important;
}

/* ── Selectbox / number input focus ring — replace pink with teal ── */
[data-baseweb="select"] [aria-selected="true"],
[data-baseweb="select"]:focus-within [data-baseweb="select-control"],
input:focus, select:focus, textarea:focus {
    border-color: var(--wr-teal) !important;
    box-shadow: 0 0 0 1px var(--wr-teal) !important;
}

/* ── Checkbox — replace pink with teal ── */
[data-baseweb="checkbox"] [data-checked="true"] div {
    background-color: var(--wr-teal) !important;
    border-color: var(--wr-teal) !important;
}

/* ── Progress / number display on slider — replace pink ── */
[data-testid="stSliderThumbValue"] { color: var(--wr-teal) !important; }
[class*="sliderThumb"] { background-color: var(--wr-teal) !important; }

/* ── Any remaining Streamlit primary pink overrides ── */
a, a:visited { color: var(--wr-teal) !important; }
[class*="primary"] { color: var(--wr-teal) !important; }

/* ── Divider ── */
hr { border-color: var(--wr-warm) !important; }

/* ── Body text — ensure readable dark color, not light gray ── */
p, li, td, th, label, .stMarkdown {
    color: var(--wr-dark) !important;
}

/* ── Caption / footer text ── */
.stCaption, [data-testid="stCaptionContainer"] {
    color: #5a6a6b !important;
    font-family: Arial, sans-serif !important;
}

/* ── Hide running-person spinner ── */
[data-testid="stStatusWidget"] { display: none !important; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
SCORE_HIGH   = 70
SCORE_MED    = 50
COLOR_HIGH   = "#22c55e"   # green
COLOR_MED    = "#f59e0b"   # amber
COLOR_LOW    = "#ef4444"   # red
COLOR_STROKE = "#2c3e3f"   # WR-Dev dark

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


@st.cache_data(ttl=300)
def load_wetlands_overlay(city_key: str) -> gpd.GeoDataFrame:
    """Load cached wetland polygons for the map overlay."""
    path = ROOT / "data" / "raw" / f"{city_key}_wetlands.geojson"
    if not path.exists():
        return gpd.GeoDataFrame()
    return gpd.read_file(path)


@st.cache_data(ttl=300)
def load_drains_overlay(city_key: str) -> gpd.GeoDataFrame:
    """Load cached Ottawa County drain lines for the map overlay (Ottawa only)."""
    path = ROOT / "data" / "raw" / f"{city_key}_drains.geojson"
    if not path.exists():
        return gpd.GeoDataFrame()
    return gpd.read_file(path)


@st.cache_data(ttl=300)
def load_water_overlay(city_key: str) -> gpd.GeoDataFrame:
    """Load cached water-main lines extracted from the township PDF (if any)."""
    path = ROOT / "data" / "utility" / f"{city_key}_water.geojson"
    if not path.exists():
        return gpd.GeoDataFrame()
    return gpd.read_file(path)


@st.cache_data(ttl=300)
def load_sewer_overlay(city_key: str) -> gpd.GeoDataFrame:
    """Load cached sewer (gravity + force main) lines from the township PDF (if any)."""
    path = ROOT / "data" / "utility" / f"{city_key}_sewer.geojson"
    if not path.exists():
        return gpd.GeoDataFrame()
    return gpd.read_file(path)


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


def _zoning_reqs_html(district: dict, url: str = "") -> str:
    """Compact zoning-requirements block for the parcel popup ('' if no data)."""
    if not district:
        return ""
    rows = []
    ml = district.get("min_lot_area_sqft")
    mw = district.get("min_lot_width_ft")
    if ml:
        s = f"Min lot: {int(ml):,} sqft"
        if mw:
            s += f" · {int(mw)} ft wide"
        rows.append(s)
    note = district.get("setbacks_note")
    sb = district.get("setbacks") or {}
    if note:
        rows.append(f"Setbacks: {note}")
    elif any(sb.get(k) is not None for k in ("front_ft", "side_ft", "rear_ft")):
        rows.append(f"Setbacks F/S/R: {sb.get('front_ft', '–')}/"
                    f"{sb.get('side_ft', '–')}/{sb.get('rear_ft', '–')} ft")
    extra = []
    cov = district.get("max_lot_coverage_pct")
    h = district.get("max_height_ft")
    fa = district.get("min_floor_area_sqft")
    if cov is not None:
        extra.append(f"Max cover: {cov * 100:.0f}%")
    if h:
        extra.append(f"Max height: {int(h)} ft")
    if fa:
        extra.append(f"Min floor: {int(fa):,} sqft")
    if extra:
        rows.append(" · ".join(extra))
    if not rows:
        return ""
    link = (f' · <a href="{url}" target="_blank" style="color:#779FA1;">full ordinance →</a>'
            if url else "")
    use_note = district.get("use_note")
    note_html = (f'<div style="color:#888;font-size:11px;margin-top:3px;font-style:italic;">{use_note}</div>'
                 if use_note else "")
    return ('<div style="margin-top:5px;font-size:12px;background:#f5f6f4;'
            'border-radius:5px;padding:6px 8px;line-height:1.5;">'
            f'<b>Zoning requirements</b>{link}<br>{"<br>".join(rows)}{note_html}</div>')


def make_map(gdf: gpd.GeoDataFrame, bbox: tuple,
             mode_label: str = "Single-Family",
             wetlands_gdf: gpd.GeoDataFrame = None,
             tracker: dict = None,
             drains_gdf: gpd.GeoDataFrame = None,
             water_gdf: gpd.GeoDataFrame = None,
             sewer_gdf: gpd.GeoDataFrame = None,
             ordinance: dict = None) -> folium.Map:
    """Build a Folium map of qualified parcels, coloured by score."""
    tracker = tracker or {}
    min_lon, min_lat, max_lon, max_lat = bbox
    center = [(min_lat + max_lat) / 2, (min_lon + max_lon) / 2]

    m = folium.Map(location=center, zoom_start=13, tiles=None)

    # ── Base tile layers (toggled via top-right control) ──────────────────────
    folium.TileLayer(
        tiles="CartoDB positron",
        name="Street Map",
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
        name="Satellite",
        overlay=False,
        control=True,
        show=False,
    ).add_to(m)

    # ── Wetland overlay (off by default — toggle in layer control) ───────────────
    if wetlands_gdf is not None and not wetlands_gdf.empty:
        wetland_group = folium.FeatureGroup(name="Wetlands", show=False)
        folium.GeoJson(
            wetlands_gdf.to_crs("EPSG:4326").__geo_interface__,
            style_function=lambda _x: {
                "fillColor":   "#38bdf8",   # sky blue
                "color":       "#0369a1",   # darker blue border
                "weight":      1,
                "fillOpacity": 0.45,
            },
            tooltip="Wetland",
        ).add_to(wetland_group)
        wetland_group.add_to(m)

    # ── County drains overlay (off by default — toggle in layer control) ─────────
    if drains_gdf is not None and not drains_gdf.empty:
        drains_4326 = drains_gdf.to_crs("EPSG:4326")

        def _drain_style(feat):
            mt = (feat["properties"].get("maintype") or "").strip()
            return {
                "color":   DRAIN_MAINTYPE_COLORS.get(mt, DRAIN_DEFAULT_COLOR),
                "weight":  3,
                "opacity": 0.85,
            }

        drain_group = folium.FeatureGroup(name="Drains (county)", show=False)
        folium.GeoJson(
            drains_4326.__geo_interface__,
            style_function=_drain_style,
            highlight_function=lambda _x: {"weight": 6, "opacity": 1.0},
            tooltip=folium.GeoJsonTooltip(
                fields=["maintype", "drainclassification", "ownedby", "dfacilityid"],
                aliases=["Type:", "Drain Code:", "Owned by:", "Facility ID:"],
                sticky=True,
            ),
        ).add_to(drain_group)
        drain_group.add_to(m)
        # (legend rendered below the map in Streamlit, not as an in-map overlay)

    # ── Water mains overlay (added when toggled; legend rendered below map) ───────
    if water_gdf is not None and not water_gdf.empty:
        water_4326 = water_gdf.to_crs("EPSG:4326")

        def _water_style(feat):
            sz = str(feat["properties"].get("spec") or "")
            return {"color": WATER_SIZE_HEX.get(sz, "#0070ff"), "weight": 2.5, "opacity": 0.85}

        water_group = folium.FeatureGroup(name="Water mains", show=False)
        folium.GeoJson(
            water_4326.__geo_interface__,
            style_function=_water_style,
            highlight_function=lambda _x: {"weight": 5, "opacity": 1.0},
            tooltip=folium.GeoJsonTooltip(fields=["spec"], aliases=["Water main (in):"]),
        ).add_to(water_group)
        water_group.add_to(m)
        # (legend rendered below the map in Streamlit, not as an in-map overlay)

    # ── Sewer mains overlay (added when present; legend rendered below map) ───────
    if sewer_gdf is not None and not sewer_gdf.empty:
        sewer_4326 = sewer_gdf.to_crs("EPSG:4326")

        def _sewer_style(feat):
            sp = str(feat["properties"].get("spec") or "")
            dash = "4,4" if sp == "FM" else None   # force main = dashed
            return {"color": SEWER_SPEC_HEX.get(sp, "#6fb300"), "weight": 2.5,
                    "opacity": 0.85, "dashArray": dash}

        sewer_group = folium.FeatureGroup(name="Sewer mains", show=False)
        folium.GeoJson(
            sewer_4326.__geo_interface__,
            style_function=_sewer_style,
            highlight_function=lambda _x: {"weight": 5, "opacity": 1.0},
            tooltip=folium.GeoJsonTooltip(fields=["spec"], aliases=["Sewer main:"]),
        ).add_to(sewer_group)
        sewer_group.add_to(m)
        # (legend rendered below the map in Streamlit, not as an in-map overlay)

    if gdf is None or gdf.empty:
        folium.LayerControl(position="topright", collapsed=False).add_to(m)
        m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])
        return m

    gdf = gdf.to_crs("EPSG:4326")

    # Deduplicate by geometry — condo/PUD developments often map many parcel
    # records to the exact same polygon. Stacking 40+ layers at 0.35 opacity
    # each produces an effectively opaque result. Keep the highest-scoring row
    # per unique geometry so only one polygon is drawn per footprint.
    geom_wkt = gdf.geometry.apply(lambda g: g.wkt)
    gdf = gdf.loc[gdf.assign(_geom_wkt=geom_wkt)
                       .sort_values("score", ascending=False)
                       .drop_duplicates("_geom_wkt")
                       .index]

    # Group all parcel polygons into one toggleable layer
    parcel_group = folium.FeatureGroup(name="Parcels", show=True)

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
        # Zoning ordinance link + requirements (from data/ordinance/<city>_zoning.json)
        _zurl = ordinance_url(zone_c, ordinance or {})
        zone_disp = (f'<a href="{_zurl}" target="_blank" '
                     f'style="color:#779FA1;font-weight:600;">{zone_c}</a>'
                     if _zurl and zone_c else zone_c)
        reqs_block = _zoning_reqs_html(get_district(zone_c, ordinance or {}), _zurl)
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

        # Build score bar rows — base components first, rezoning bonus separately
        score_bars = ""
        pts_rezoning_earned = 0.0
        for comp in SCORE_COMPONENTS:
            pts     = min(float(row.get(comp["key"], 0) or 0), comp["max"])
            max_pts = comp["max"]
            is_bonus = comp.get("bonus", False)
            if is_bonus:
                pts_rezoning_earned = pts
                continue   # rendered separately below
            pct       = min(int(pts / max_pts * 100), 100) if max_pts else 0
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

        # Rezoning bonus row — only shown when there's actual upside
        if pts_rezoning_earned > 0:
            rezone_pct = min(int(pts_rezoning_earned / 10 * 100), 100)
            score_bars += f"""
    <tr>
      <td style="color:#888;white-space:nowrap;padding-right:6px;">Rezoning bonus</td>
      <td style="width:100%;">
        <div style="background:#e5e7eb;border-radius:3px;height:8px;width:100%;">
          <div style="background:#779FA1;border-radius:3px;height:8px;width:{rezone_pct}%;"></div>
        </div>
      </td>
      <td style="color:#333;padding-left:6px;white-space:nowrap;">+{pts_rezoning_earned:.0f}</td>
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

        # Review flag banner — only shown for assessor-improved flag, not zoning flags
        _review_reasons = str(row.get("review_reasons", "") or "")
        _assessor_flag  = "Assessor says improved" in _review_reasons
        review_banner = (
            f"<div style='background:#fef3c7;border-left:3px solid #f59e0b;"
            f"padding:5px 8px;margin-bottom:6px;border-radius:3px;"
            f"font-size:11px;color:#92400e;'>"
            f"⚠️ <b>Needs Review:</b> Assessor classifies as improved but no building detected — confirm vacant or parking/storage only</div>"
        ) if _assessor_flag else ""

        popup_html = f"""
<div style="font-family:Arial,sans-serif;min-width:270px;font-size:13px;max-height:440px;overflow-y:auto;overflow-x:hidden;padding-right:6px;">
  {review_banner}
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
        <td colspan="2">{zone_disp} — {zone_l}</td></tr>
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
  </table>
  {reqs_block}
  <hr style="margin:6px 0;">
  <div style="font-weight:600;margin-bottom:4px;">
    Score: <span style="color:{color};">{score:.1f}</span>
    {f"<span style='font-weight:400;color:#888;font-size:11px;'> (base {score - pts_rezoning_earned:.0f} + {pts_rezoning_earned:.0f} rezoning bonus)</span>" if pts_rezoning_earned > 0 else "<span style='color:#888;font-size:11px;font-weight:400;'> / 100</span>"}
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
                "fillOpacity": 0.35,
            },
            highlight_function=lambda _x: {
                "weight":      3,
                "fillOpacity": 0.55,
            },
            popup=folium.Popup(popup_html, max_width=290),
            tooltip=f"{addr}  |  Score {score:.0f}  |  {u_con}–{u_opt} units",
        ).add_to(parcel_group)

    parcel_group.add_to(m)
    folium.LayerControl(position="topright", collapsed=False).add_to(m)
    m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])
    return m


# ── Land Screener view (called by the navigation shell) ──────────────────────
def render_land(_username, _user_data, IS_ADMIN, _authenticator):
    """Render the full Land Screener UI (sidebar + map + tabs).

    Auth, page config, CSS, and logo are owned by the shell (app_shell.py);
    this function assumes the user is already logged in and receives the
    authenticated user's identity/role.
    """
    # ── Sidebar ───────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.title("WM Land Screener")
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
        dev_type = st.segmented_control(
            "Development type",
            ["Single-Family", "Multifamily"],
            default="Single-Family",
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
    _logo_path = ROOT / "assets" / "wr_dev_logo.png"
    if _logo_path.exists():
        st.logo(str(_logo_path), size="large")
    st.title(f"{city_cfg['label']} — Vacant Land Screener")

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
        _bldg_mask    = df_all["building_pct"].fillna(0) > 0.05 if "building_pct" in df_all.columns \
                        else pd.Series(False, index=df_all.index)
        _density_mask = df_all[MF_DENSITY_COL].fillna(0) < 3

        _fail = _exempt_mask | _flood_mask | _bldg_mask | _density_mask
        qual_all = df_all[~_fail].copy()

        # Recompute density score and total score using MF density + MF ceiling (30 u/ac)
        qual_all["max_units_per_acre"] = qual_all[MF_DENSITY_COL]
        qual_all["pts_density"] = (qual_all[MF_DENSITY_COL] / 30).clip(upper=1.0).mul(40).round(1)
        # Base score (4 components, max 100) + rezoning bonus on top (no clip at 100)
        base_cols = ["pts_density", "pts_wetland", "pts_flood", "pts_shape"]
        existing_base = [c for c in base_cols if c in qual_all.columns]
        base_score = qual_all[existing_base].fillna(0).sum(axis=1).round(1)
        rezone_bonus = qual_all["pts_rezoning"].fillna(0) if "pts_rezoning" in qual_all.columns \
                       else pd.Series(0, index=qual_all.index)
        qual_all["score"] = (base_score + rezone_bonus).round(1)

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

    # ── Page tabs ─────────────────────────────────────────────────────────────────
    tab1, tab2 = st.tabs(["Screened Parcels", "Manual Listings"])

    with tab1:

        # ── Top metrics ───────────────────────────────────────────────────────────────
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total parcels scanned", f"{len(df_all):,}")
        m2.metric("Passed hard filters", f"{len(qual_all):,}")
        m3.metric("Shown on map", f"{len(qual_filtered):,}")
        m4.metric("Units — conservative", f"{int(qual_filtered['units_conservative'].sum()):,}")
        m5.metric("Units — optimistic",   f"{int(qual_filtered['units_optimistic'].sum()):,}")

        # ── Legend ────────────────────────────────────────────────────────────────────
        leg1, leg2, leg3, _rest = st.columns([1, 1, 1, 5])
        leg1.markdown(f"<span style='display:inline-block;width:14px;height:14px;background:{COLOR_HIGH};border-radius:2px;vertical-align:middle;margin-right:4px;'></span> Score ≥ {SCORE_HIGH}",
                      unsafe_allow_html=True)
        leg2.markdown(f"<span style='display:inline-block;width:14px;height:14px;background:{COLOR_MED};border-radius:2px;vertical-align:middle;margin-right:4px;'></span> Score {SCORE_MED}–{SCORE_HIGH-1}",
                      unsafe_allow_html=True)
        leg3.markdown(f"<span style='display:inline-block;width:14px;height:14px;background:{COLOR_LOW};border-radius:2px;vertical-align:middle;margin-right:4px;'></span> Score < {SCORE_MED}",
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
        _wetlands_overlay = load_wetlands_overlay(city_key)
        _drains_overlay = load_drains_overlay(city_key)
        _water_overlay = load_water_overlay(city_key)
        _sewer_overlay = load_sewer_overlay(city_key)
        _ordinance = load_ordinance(city_key)

        # Utility overlays are toggled in the map's top-right control (instant,
        # no reload). Always pass them; their labeled keys render below the map.
        _has_drains = not _drains_overlay.empty
        _has_water = not _water_overlay.empty
        _has_sewer = not _sewer_overlay.empty

        m = make_map(gdf_shown, city_cfg["bbox"], mode_label=_mode_label_map,
                     wetlands_gdf=_wetlands_overlay, tracker=tracker,
                     drains_gdf=_drains_overlay, water_gdf=_water_overlay,
                     sewer_gdf=_sewer_overlay, ordinance=_ordinance)
        st_folium(m, use_container_width=True, height=700, returned_objects=[])

        # Overlay keys below the map (labeled), for the in-map toggles.
        def _legend_row(color, label):
            return (f'<span style="display:inline-block;margin-right:14px;white-space:nowrap;">'
                    f'<span style="display:inline-block;width:18px;height:3px;background:{color};'
                    f'vertical-align:middle;margin-right:5px;"></span>{label}</span>')
        _legends = []
        if _has_drains:
            _types = [t for t in DRAIN_MAINTYPE_COLORS
                      if t in set(_drains_overlay.get("maintype", []).dropna())]
            _legends.append("**County Drains**<br>"
                            + "".join(_legend_row(DRAIN_MAINTYPE_COLORS[t], t) for t in _types))
        if _has_water:
            _sizes = [s for s in WATER_SIZE_HEX
                      if s in set(_water_overlay.get("spec", []).dropna().astype(str))]
            _legends.append(
                '**Water Mains** <span style="color:#999;font-size:11px;">(~40 ft · PDF snapshot)</span><br>'
                + "".join(_legend_row(WATER_SIZE_HEX[s], f'{s}"') for s in _sizes))
        if _has_sewer:
            _specs = [s for s in SEWER_SPEC_HEX
                      if s in set(_sewer_overlay.get("spec", []).dropna().astype(str))]
            _legends.append(
                '**Sewer Mains** <span style="color:#999;font-size:11px;">(~30 ft · PDF snapshot)</span><br>'
                + "".join(_legend_row(SEWER_SPEC_HEX[s], sewer_spec_label(s)) for s in _specs))
        if _legends:
            st.caption("Overlay keys — toggle the layers in the map's top-right control.")
            for _col, _md in zip(st.columns(len(_legends)), _legends):
                _col.markdown(_md, unsafe_allow_html=True)

        # ── Qualifying parcels table ──────────────────────────────────────────────────
        with st.expander(f"Qualifying parcels  ({len(qual_filtered)} shown)", expanded=True):
            display_cols = [
                "parcel_id", "address", "owner",
                "calc_acres", "net_dev_acres",
                "building_count", "building_pct",   # vacancy indicators — shown early
                "zone_code", "zone_label",
                "dev_pathway",
                "max_units_per_acre", "units_conservative", "units_optimistic",
                "flood_pct", "wetland_pct",
                "shape_score",
                "soil_1",
                "mf_permitted", "adu_permitted",
                # FLU columns (only shown when data is loaded — filtered below)
                "future_lu_label", "future_max_units", "rezoning_delta",
                "score",
            ]
            display_cols = [c for c in display_cols if c in qual_filtered.columns]
            fmt = qual_filtered[display_cols].copy()

            # Format shape_score as a % (0–100%) and rename for clarity
            if "shape_score" in fmt.columns:
                fmt["shape_score"] = (fmt["shape_score"] * 100).round(0).astype(int).astype(str) + "%"
                fmt = fmt.rename(columns={"shape_score": "Shape %"})

            # Rename soil_1 column for readability
            if "soil_1" in fmt.columns:
                fmt = fmt.rename(columns={"soil_1": "Dominant Soil"})

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
            with st.expander("Parcel tracker summary", expanded=False):
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
            with st.expander("Score breakdown — per parcel", expanded=True):
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
                            f"border-radius:10px;'>REZONING +{rezone_d} u/ac → {flu_label}</span>"
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
                        pts     = min(float(row.get(comp["key"], 0) or 0), comp["max"])
                        max_pts = comp["max"]
                        pct     = min(pts / max_pts, 1.0) if max_pts else 0
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
        flu_available = "future_lu_code" in qual_filtered.columns and \
                        qual_filtered["future_lu_code"].astype(str).str.strip().any()

        if flu_available:
            rezone_parcels = qual_filtered[qual_filtered["rezoning_upside"] == True].copy()
            if not rezone_parcels.empty:
                with st.expander(
                    f"Rezoning watch list  ({len(rezone_parcels)} parcels with upside)",
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

        # ── Development pathway breakdown ─────────────────────────────────────────────
        if "dev_pathway" in qual_filtered.columns:
            with st.expander(
                "Development pathway breakdown  (how each parcel reaches 3+ u/ac)",
                expanded=True,
            ):
                st.caption(
                    "Every qualifying parcel is classified by the **simplest available route** to "
                    "≥ 3 units/acre. Approval burden increases left → right. "
                    "Use the sidebar filter to isolate a specific pathway."
                )
                pathway_order = [
                    "By right", "PRD special use", "PUD special use",
                    "Master plan upzone", "PD rezoning", "Not viable",
                ]
                counts = qual_filtered["dev_pathway"].value_counts().reindex(
                    pathway_order, fill_value=0
                ).reset_index()
                counts.columns = ["Pathway", "Parcels"]

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

        # ── Ordinance review list ──────────────────────────────────────────────────────
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

        # ── Scoring methodology ────────────────────────────────────────────────────────
        with st.expander("How scores are calculated"):
            _mode_label = "Multifamily" if USE_MF else "Single-Family"
            st.markdown(
                f"Each qualifying parcel is scored **0–100** across five components "
                f"in **{_mode_label}** mode. "
                "Hard filters (flood, buildings, zoning) must pass first — "
                "parcels that fail any hard filter are excluded entirely and not scored."
            )
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

        # ── Filter breakdown ──────────────────────────────────────────────────────────
        with st.expander("Why parcels were eliminated"):
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
            "EGLE Part 303 State Wetland Inventory (gisagoegle.state.mi.us) · "
            "Microsoft Global ML Building Footprints · "
            "Ottawa County MasterPlanZoning service (Future Land Use — gis.miottawa.org).  "
            "Grand Haven density values verified against Chapter 40 (Municode, Jan 2026). "
            "Parcels flagged ⚠️ have ordinance conditions requiring manual review — see the Needs Review section."
        )


    # ── Manual Listings tab ───────────────────────────────────────────────────────
    with tab2:
        _ML_FILE = ROOT / "Potential Land New Process for AI Tool.xlsx"

        @st.cache_data(ttl=300)
        def load_manual_listings(path: str):
            """Read CARWM and Facebook listings from Excel, preserving hyperlinks."""
            import openpyxl as _oxl
            # ── CARWM sheet ───────────────────────────────────────────────────────
            carwm = pd.read_excel(path, sheet_name="CARWM", header=3)
            carwm.columns = [
                "Address", "Listing_Date", "Days_on_Market", "List_Price",
                "City", "County", "Zoning", "Allowable_Density",
                "Master_Plan", "Master_Plan_Density", "Lot_Acres", "List_Price_per_Acre",
                "Road_Frontage", "Utilities_Available", "EGLE_Wetland",
                "Min_Lot_Area", "Max_Lot_Coverage", "Min_Unit_Size",
                "Min_Ground_Floor", "Notes",
            ]
            carwm = carwm[carwm["Address"].notna() & (carwm["Address"].astype(str).str.strip() != "")].reset_index(drop=True)
            carwm["List_Price"] = pd.to_numeric(carwm["List_Price"], errors="coerce")
            carwm["Lot_Acres"]  = pd.to_numeric(carwm["Lot_Acres"],  errors="coerce")
            carwm["Notes"]      = carwm["Notes"].astype(str).str.replace("\n", " ").replace("nan", "")
            carwm["Utilities_Available"] = carwm["Utilities_Available"].astype(str).replace("nan", "")

            # ── FB Marketplace sheet ──────────────────────────────────────────────
            facebook = pd.read_excel(path, sheet_name="FB Marketplace", header=4)
            facebook.columns = [
                "ID", "Listing_URL", "City", "County", "Lot_Acres", "List_Price",
                "Price_per_Acre", "Zoning", "Master_Plan", "Utilities_Available",
                "Wetlands", "Allowable_Density", "Min_Lot_Area", "Max_Lot_Coverage",
                "Min_Unit_Size", "Min_Ground_Floor", "Notes",
            ]
            facebook = facebook[facebook["ID"].notna() & (facebook["ID"].astype(str).str.strip() != "")].reset_index(drop=True)
            facebook["List_Price"] = pd.to_numeric(facebook["List_Price"], errors="coerce")
            facebook["Lot_Acres"]  = pd.to_numeric(facebook["Lot_Acres"],  errors="coerce")
            facebook["Notes"]      = facebook["Notes"].astype(str).str.replace("\n", " ").replace("nan", "")
            facebook["Listing_URL"] = facebook["Listing_URL"].astype(str).replace("nan", "")

            return carwm, facebook

        if not _ML_FILE.exists():
            st.warning(
                "Manual listings file not found. Place **Potential Land New Process for AI Tool.xlsx** "
                "in the project root folder.",
            
            )
        else:
            ml_carwm, ml_facebook = load_manual_listings(str(_ML_FILE))

            st.caption(f"Reading from **Potential Land New Process for AI Tool.xlsx** · {len(ml_carwm)} CARWM listings · {len(ml_facebook)} Facebook Marketplace listings")

            # ── Listings table ────────────────────────────────────────────────────────
            st.subheader(f"Listings  ({len(ml_carwm)})")

            carwm_display = ml_carwm[[
                "Address", "City", "County", "Lot_Acres",
                "List_Price", "Zoning", "Utilities_Available", "Notes",
            ]].copy()
            carwm_display = carwm_display.rename(columns={
                "Lot_Acres":           "Acres",
                "List_Price":          "List Price ($)",
                "Utilities_Available": "Utilities",
            })
            carwm_display["List Price ($)"] = carwm_display["List Price ($)"].apply(
                lambda x: f"${x:,.0f}" if pd.notna(x) else ""
            )
            carwm_display["Acres"] = carwm_display["Acres"].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else ""
            )

            # Add tracker columns — key off "CARWM_{address}"
            carwm_display["_tracker_key"] = ml_carwm["Address"].astype(str).apply(lambda a: f"CARWM_{a}")
            carwm_display["Status"]     = carwm_display["_tracker_key"].map(lambda k: tracker.get(k, {}).get("status", "Not contacted"))
            carwm_display["Notes"]      = carwm_display["_tracker_key"].map(lambda k: tracker.get(k, {}).get("notes", ""))
            carwm_display["Reviewed ✓"] = carwm_display["_tracker_key"].map(lambda k: bool(tracker.get(k, {}).get("reviewed", False)))
            _carwm_keys = carwm_display.pop("_tracker_key")
            _carwm_disabled = [c for c in carwm_display.columns if c not in {"Status", "Notes", "Reviewed ✓"}]

            carwm_edited = st.data_editor(
                carwm_display, hide_index=True, use_container_width=True,
                key="tracker_carwm",
                disabled=_carwm_disabled,
                column_config={
                    "Status":     st.column_config.SelectboxColumn("Status", options=STATUS_OPTIONS, required=True),
                    "Notes":      st.column_config.TextColumn("Notes", width="large"),
                    "Reviewed ✓": st.column_config.CheckboxColumn("Reviewed ✓", default=False),
                },
            )
            # Save CARWM tracker changes
            _carwm_updates = {}
            for i, erow in carwm_edited.iterrows():
                k = _carwm_keys.iloc[i]
                status = erow.get("Status", "Not contacted")
                notes  = erow.get("Notes", "") or ""
                reviewed = bool(erow.get("Reviewed ✓", False))
                old = tracker.get(k, {})
                if old.get("status", "Not contacted") != status or old.get("notes", "") != notes or bool(old.get("reviewed", False)) != reviewed:
                    _carwm_updates[k] = {"status": status, "notes": notes, "reviewed": reviewed,
                                          "updated": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                                          "updated_by": _username}
            if _carwm_updates:
                save_tracker(_carwm_updates)
                tracker.update(_carwm_updates)

            # ── Facebook Marketplace listings ──────────────────────────────────────────
            if not ml_facebook.empty:
                st.divider()
                st.subheader(f"Facebook Marketplace Listings  ({len(ml_facebook)})")
                st.caption(
                    "Sourced from Facebook Marketplace. Locations are approximate — "
                    "click the listing link to view the original post."
                )

                fb_display = ml_facebook[[
                    "City", "County", "Lot_Acres", "List_Price", "Notes", "Listing_URL",
                ]].copy()
                fb_display = fb_display.rename(columns={
                    "Lot_Acres":  "Acres",
                    "List_Price": "List Price ($)",
                })
                fb_display["List Price ($)"] = fb_display["List Price ($)"].apply(
                    lambda x: f"${x:,.0f}" if pd.notna(x) else ""
                )
                fb_display["Acres"] = fb_display["Acres"].apply(
                    lambda x: f"{x:.2f}" if pd.notna(x) else ""
                )
                fb_display["Listing"] = fb_display["Listing_URL"]
                fb_display = fb_display.drop(columns=["Listing_URL"])

                # Add tracker columns — key off "FB_{id}"
                fb_display["_tracker_key"] = ml_facebook["ID"].astype(str).apply(lambda i: f"FB_{i}")
                fb_display["Status"]     = fb_display["_tracker_key"].map(lambda k: tracker.get(k, {}).get("status", "Not contacted"))
                fb_display["Notes"]      = fb_display["_tracker_key"].map(lambda k: tracker.get(k, {}).get("notes", ""))
                fb_display["Reviewed ✓"] = fb_display["_tracker_key"].map(lambda k: bool(tracker.get(k, {}).get("reviewed", False)))
                _fb_keys = fb_display.pop("_tracker_key")
                _fb_disabled = [c for c in fb_display.columns if c not in {"Status", "Notes", "Reviewed ✓"}]

                fb_edited = st.data_editor(
                    fb_display, hide_index=True, use_container_width=True,
                    key="tracker_fb",
                    disabled=_fb_disabled,
                    column_config={
                        "Listing":    st.column_config.LinkColumn("Listing", display_text="View ↗"),
                        "Status":     st.column_config.SelectboxColumn("Status", options=STATUS_OPTIONS, required=True),
                        "Notes":      st.column_config.TextColumn("Notes", width="large"),
                        "Reviewed ✓": st.column_config.CheckboxColumn("Reviewed ✓", default=False),
                    },
                )
                # Save FB tracker changes
                _fb_updates = {}
                for i, erow in fb_edited.iterrows():
                    k = _fb_keys.iloc[i]
                    status = erow.get("Status", "Not contacted")
                    notes  = erow.get("Notes", "") or ""
                    reviewed = bool(erow.get("Reviewed ✓", False))
                    old = tracker.get(k, {})
                    if old.get("status", "Not contacted") != status or old.get("notes", "") != notes or bool(old.get("reviewed", False)) != reviewed:
                        _fb_updates[k] = {"status": status, "notes": notes, "reviewed": reviewed,
                                           "updated": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                                           "updated_by": _username}
                if _fb_updates:
                    save_tracker(_fb_updates)
                    tracker.update(_fb_updates)
