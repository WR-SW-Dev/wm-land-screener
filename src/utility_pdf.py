"""
Water/sewer utility overlay extraction from township PDF maps.

West Michigan township water/sewer systems have no public GIS service — but the
township PDF maps are TRUE VECTOR PDFs. So we extract the utility lines directly:

  1. Georeference via the PLSS section grid: the maps print section numbers at
     each section's center; we match those to Ottawa County section centroids
     and fit an affine (PDF -> Web Mercator) with RANSAC. ~40 ft accuracy.
  2. Extract utility lines by stroke color and classify by spec (pipe size),
     filtering out water-body outlines (filled / very long smooth curves).
  3. Write a cached GeoJSON the app overlays like the drains/wetlands layers.

BUILD (one-off per map; needs pdfplumber, pypdfium2, pyproj, numpy):
    python src/utility_pdf.py --map slt_water
    python src/utility_pdf.py --all

The running app only READS the cached GeoJSON (data/utility/) — no PDF libs needed.
Accuracy is screening-grade ("is there a main on/near this parcel?"), not
survey-grade; the maps are static snapshots of their publication date.
"""

import argparse
import json
import math
import random
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PDF_DIR = ROOT / "data" / "utility_pdfs"
OUT_DIR = ROOT / "data" / "utility"          # tracked (not gitignored) — app reads here

PLSS_SECTIONS = ("https://gis.miottawa.org/arcgis/rest/services/HostedServices"
                 "/PLSS/FeatureServer/3/query")

# ── Spec legends: stroke RGB (0-1) -> spec label ─────────────────────────────────
WATER_MAIN_COLORS = {
    (0.45, 0.87, 1.0): "6",  (0.0, 0.44, 1.0): "8",  (0.33, 1.0, 0.0): "10",
    (1.0, 0.67, 0.0): "12",  (0.48, 0.96, 0.79): "16", (0.77, 0.0, 1.0): "20",
    (0.0, 0.0, 1.0): "24",
}
# Display colors (size -> hex) used by the app legend + line styling.
WATER_SIZE_HEX = {
    "6": "#87cefa", "8": "#0070ff", "10": "#50c800", "12": "#ff9600",
    "16": "#5ad2aa", "20": "#be00ff", "24": "#0000c8",
}

# Sewer: gravity main by size + force main. Stroke RGB (0-1) -> spec.
SEWER_MAIN_COLORS = {
    (0.44, 0.66, 0.0): "8",  (1.0, 0.67, 0.0): "10", (0.0, 0.4, 1.0): "12",
    (0.5, 0.5, 0.0): "15",   (0.67, 0.4, 0.8): "18", (0.5, 0.3, 0.0): "21",
    (1.0, 0.0, 0.7): "24",   (1.0, 0.0, 0.0): "FM",
}
SEWER_SPEC_HEX = {
    "8": "#6fb300", "10": "#ffab00", "12": "#0066ff", "15": "#808000",
    "18": "#b366cc", "21": "#804d00", "24": "#ff00b3", "FM": "#e00000",
}
# Legend label per spec: sizes show as e.g. 8"; FM = Force main.
def sewer_spec_label(spec):
    return "Force main" if spec == "FM" else f'{spec}"'

# ── Per-map registry ─────────────────────────────────────────────────────────────
UTILITY_MAPS = {
    "slt_water": {
        "pdf": "Water-System-Map-Overall.pdf",
        "city_key": "spring_lake_twp",
        "system": "water",
        "township": {"tn": "08", "td": "N", "rn": "16", "rd": "W"},
        "legend": WATER_MAIN_COLORS,
        "out": "spring_lake_twp_water.geojson",
    },
    # NOTE: SLT sewer + both Grand Haven Twp maps are intentionally NOT registered.
    # Only the SLT water map (Ottawa County template) prints extractable SECTION
    # NUMBERS, which the automatic georeference() relies on. The sewer map's text
    # is outlined (not extractable) and the GH Twp maps are aerial with no section
    # numbers — so section-grid georeferencing can't lock onto them, and a build
    # here produced a degenerate (collapsed) layer. These await the underlying GIS
    # shapefiles from the townships / Prein & Newhof (the PDFs were generated from
    # GIS), which would give exact georeferenced data. The sewer color/spec maps
    # and the app's "Sewer mains" rendering are kept ready for that source data.
}


# ── Georeferencing ───────────────────────────────────────────────────────────────
def _section_centroids(township):
    """Area-centroid (lon,lat) per section number for one PLSS township."""
    import requests
    import shapely.geometry as sg
    where = (f"TownshipNumber='{township['tn']}' AND TownshipDirection='{township['td']}' "
             f"AND RangeNumber='{township['rn']}' AND RangeDirection='{township['rd']}'")
    r = requests.get(PLSS_SECTIONS, params={
        "where": where, "outFields": "Name", "returnGeometry": "true",
        "outSR": "4326", "f": "geojson"}, headers={"User-Agent": "wm-land-screener/1.0"},
        timeout=40).json()
    cent = {}
    for ft in r.get("features", []):
        m = re.search(r"(\d{1,2})$", ft["properties"].get("Name") or "")
        if not m:
            continue
        cent[int(m.group(1))] = sg.shape(ft["geometry"]).centroid
    return cent


def _section_tokens(page):
    """Printed section-number labels (value, x, y) in the map body."""
    out = []
    for w in page.extract_words():
        if re.fullmatch(r"\d{1,2}", w["text"]) and 1 <= int(w["text"]) <= 36:
            cx = (w["x0"] + w["x1"]) / 2
            cy = (w["top"] + w["bottom"]) / 2
            if 250 < cx < 2350 and 250 < cy < 2350:   # exclude margins/legend/title
                out.append((int(w["text"]), cx, cy))
    return out


def _fit_affine(src, dst):
    import numpy as np
    A = np.array([[x, y, 1] for x, y in src])
    cx = np.linalg.lstsq(A, np.array([d[0] for d in dst]), rcond=None)[0]
    cy = np.linalg.lstsq(A, np.array([d[1] for d in dst]), rcond=None)[0]
    return cx, cy


def _apply(aff, x, y):
    cx, cy = aff
    return (cx[0] * x + cx[1] * y + cx[2], cy[0] * x + cy[1] * y + cy[2])


def georeference(page, township):
    """Return (affine PDF->mercator, transformer merc->lonlat, residual_ft).

    RANSAC matches printed section numbers to section centroids (in Web Mercator,
    the map's near-native projection). Degenerate/collinear samples are rejected.
    """
    import numpy as np
    from pyproj import Transformer
    to3857 = Transformer.from_crs(4326, 3857, always_xy=True)
    to4326 = Transformer.from_crs(3857, 4326, always_xy=True)

    cent = {n: to3857.transform(c.x, c.y) for n, c in _section_centroids(township).items()}
    toks = _section_tokens(page)
    pairs = [(tx, ty, cent[v]) for v, tx, ty in toks if v in cent]
    if len(pairs) < 4:
        raise RuntimeError(f"too few section labels matched ({len(pairs)}) — "
                           "georeferencing needs the printed section grid")

    def det(a):
        return a[0][0] * a[1][1] - a[0][1] * a[1][0]

    rng = random.Random(11)
    best = None
    for _ in range(30000):
        s = rng.sample(pairs, 3)
        (x1, y1), (x2, y2), (x3, y3) = [(a, b) for a, b, c in s]
        if abs((x2 - x1) * (y3 - y1) - (x3 - x1) * (y2 - y1)) < 5000:
            continue  # near-collinear sample
        a = _fit_affine([(p, q) for p, q, c in s], [c for _, _, c in s])
        if abs(det(a)) < 1e-6:
            continue
        inl = [(tx, ty, c) for tx, ty, c in pairs
               if math.dist(_apply(a, tx, ty), c) < 200]
        if best is None or len(inl) > len(best[1]):
            best = (a, inl)
    a, inl = best
    for tol in (120, 60, 40, 30):       # iteratively tighten the inlier set
        a = _fit_affine([(x, y) for x, y, c in inl], [c for _, _, c in inl])
        inl = [(tx, ty, c) for tx, ty, c in pairs if math.dist(_apply(a, tx, ty), c) < tol]
    a = _fit_affine([(x, y) for x, y, c in inl], [c for _, _, c in inl])

    res = []
    for tx, ty, c in inl:
        lon, lat = to4326.transform(*_apply(a, tx, ty))
        lon0, lat0 = to4326.transform(*c)
        res.append(math.hypot((lon - lon0) * math.cos(math.radians(43)) * 364000,
                              (lat - lat0) * 364000))
    return a, to4326, (sum(res) / len(res) if res else float("nan")), len(inl)


# ── Line extraction ──────────────────────────────────────────────────────────────
def _nearest_spec(color, legend, tol=0.12):
    if not isinstance(color, (list, tuple)) or len(color) < 3:
        return None
    best, bd = None, tol
    for col, spec in legend.items():
        d = sum((a - b) ** 2 for a, b in zip(color, col)) ** 0.5
        if d < bd:
            bd, best = d, spec
    return best


def _pathlen(pts):
    return sum(math.dist(pts[i], pts[i + 1]) for i in range(len(pts) - 1)) if pts and len(pts) > 1 else 0


def extract_lines(page, legend):
    """[(spec, [(x,y) pdf-pts])] — matched lines + short unfilled curves (real pipe
    runs). Drops filled / very long curves (water-body outlines)."""
    out = []
    for o in page.lines:
        spec = _nearest_spec(o.get("stroking_color"), legend)
        pts = o.get("pts")
        if spec and pts and len(pts) >= 2:
            out.append((spec, pts))
    for o in page.curves:
        spec = _nearest_spec(o.get("stroking_color"), legend)
        pts = o.get("pts")
        filled = o.get("non_stroking_color") is not None and o.get("fill")
        if spec and pts and len(pts) >= 2 and not filled and len(pts) < 50 and _pathlen(pts) < 300:
            out.append((spec, pts))
    return out


# ── Build one map -> cached GeoJSON ──────────────────────────────────────────────
def build(map_key, verbose=True):
    import pdfplumber
    cfg = UTILITY_MAPS[map_key]
    pdf_path = PDF_DIR / cfg["pdf"]
    if not pdf_path.exists():
        raise FileNotFoundError(f"missing PDF: {pdf_path} (place it in data/utility_pdfs/)")

    with pdfplumber.open(str(pdf_path)) as pdf:
        page = pdf.pages[0]
        aff, to4326, resid_ft, ninl = georeference(page, cfg["township"])
        segs = extract_lines(page, cfg["legend"])

    features = []
    for spec, pts in segs:
        coords = [list(to4326.transform(*_apply(aff, x, y))) for x, y in pts]  # [lon,lat]
        features.append({
            "type": "Feature",
            "properties": {"system": cfg["system"], "spec": spec},
            "geometry": {"type": "LineString", "coordinates": coords},
        })
    fc = {"type": "FeatureCollection",
          "metadata": {"map": map_key, "system": cfg["system"], "source_pdf": cfg["pdf"],
                       "georef_residual_ft": round(resid_ft, 1), "georef_inliers": ninl,
                       "note": "Screening-grade overlay extracted from a static PDF map; "
                               "not survey-grade. Verify against utility records before design."},
          "features": features}

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / cfg["out"]
    out_path.write_text(json.dumps(fc))
    if verbose:
        print(f"[{map_key}] {len(features)} segments | georef ~{resid_ft:.0f} ft "
              f"({ninl} control pts) -> {out_path.relative_to(ROOT)}")
    return out_path


def main():
    ap = argparse.ArgumentParser(description="Extract water/sewer overlays from township PDFs")
    ap.add_argument("--map", choices=list(UTILITY_MAPS), help="single map key to build")
    ap.add_argument("--all", action="store_true", help="build every registered map")
    args = ap.parse_args()
    keys = list(UTILITY_MAPS) if args.all else ([args.map] if args.map else [])
    if not keys:
        ap.error("specify --map <key> or --all")
    for k in keys:
        build(k)


if __name__ == "__main__":
    main()
