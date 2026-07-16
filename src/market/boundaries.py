"""
Boundary polygons for the market-feasibility choropleth heat map.

Uses Census **cartographic boundary** geometries (the "500k" generalized set),
which are clipped to the shoreline — so county/township shapes follow the Lake
Michigan coast instead of extending legal boundaries out over the water. One
Michigan county-subdivision file supplies both layers:
  • submarkets → the three screener cousubs, taken directly
  • counties   → all cousubs dissolved by county FIPS (shoreline-clipped)

Caches the assembled FeatureCollection to data/raw/.

Public API:
    load_boundaries(refresh=False) -> dict   # GeoJSON FeatureCollection
        Each feature's properties carry {"key", "label", "tier"} matching the
        demographics frame's `key`, so the app can join score/need → polygon.
"""
import json
import sys
import time
from pathlib import Path

import geopandas as gpd
import requests
from shapely.geometry import mapping

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (DATA_RAW, MARKET_SUBMARKETS, MARKET_COUNTIES,  # noqa: E402
                     MSHDA_OPPORTUNITY_ZONES_SERVICE)
from data_loader import _arcgis_query  # noqa: E402

_CACHE = DATA_RAW / "market_boundaries.geojson"
# Michigan county-subdivision cartographic boundaries (shoreline-clipped).
_CB_YEAR = 2023
_CB_ZIP  = DATA_RAW / f"cb_{_CB_YEAR}_26_cousub_500k.zip"
_CB_URL  = (f"https://www2.census.gov/geo/tiger/GENZ{_CB_YEAR}/shp/"
            f"cb_{_CB_YEAR}_26_cousub_500k.zip")


def _ensure_cb_file():
    """Download the cartographic-boundary shapefile if not already cached."""
    if _CB_ZIP.exists() and _CB_ZIP.stat().st_size > 1000:
        return
    hdr = {"User-Agent": "Mozilla/5.0 (research; wrdev-tool)"}
    last = None
    for _ in range(5):
        try:
            r = requests.get(_CB_URL, headers=hdr, timeout=90)
            if r.status_code == 200 and len(r.content) > 1000:
                _CB_ZIP.write_bytes(r.content)
                return
            last = f"HTTP {r.status_code}"
        except Exception as e:                       # noqa: BLE001
            last = type(e).__name__
        time.sleep(3)
    raise RuntimeError(f"Could not download Census cartographic boundaries "
                       f"({_CB_URL}): {last}")


def _cousub_geoid(geo: dict) -> str:
    return geo["state"] + geo["county"] + geo["cousub"]


def _cofips(geo: dict) -> str:
    return geo["state"] + geo["county"]


def _build() -> dict:
    _ensure_cb_file()
    gdf = gpd.read_file(_CB_ZIP).to_crs(4326)
    gdf["cofips"] = gdf["STATEFP"] + gdf["COUNTYFP"]

    features = []

    # Submarkets — individual cousubs, taken directly.
    by_geoid = gdf.set_index("GEOID")
    for s in MARKET_SUBMARKETS:
        gid = _cousub_geoid(s["geo"])
        if gid in by_geoid.index:
            geom = by_geoid.loc[gid, "geometry"]
            features.append({
                "type": "Feature",
                "geometry": mapping(geom),
                "properties": {"key": s["key"], "label": s["label"], "tier": "submarket"},
            })

    # Counties — dissolve all cousubs by county FIPS (keeps shoreline clip).
    dissolved = gdf.dissolve(by="cofips")
    for c in MARKET_COUNTIES:
        fips = _cofips(c["geo"])
        if fips in dissolved.index:
            geom = dissolved.loc[fips, "geometry"]
            features.append({
                "type": "Feature",
                "geometry": mapping(geom),
                "properties": {"key": c["key"], "label": c["label"], "tier": "county"},
            })

    return {"type": "FeatureCollection", "features": features}


def load_boundaries(refresh: bool = False) -> dict:
    """Return the boundary FeatureCollection, using the on-disk cache by default."""
    if _CACHE.exists() and not refresh:
        return json.loads(_CACHE.read_text())
    fc = _build()
    _CACHE.write_text(json.dumps(fc))
    return fc


_CACHE_MUNI = DATA_RAW / "market_municipal_boundaries.geojson"


def _build_municipal() -> dict:
    """Every county-subdivision polygon in the four market counties (shoreline-clipped)."""
    _ensure_cb_file()
    gdf = gpd.read_file(_CB_ZIP).to_crs(4326)
    gdf["cofips"] = gdf["STATEFP"] + gdf["COUNTYFP"]
    want = {_cofips(c["geo"]): c["key"] for c in MARKET_COUNTIES}

    features = []
    for _, r in gdf[gdf["cofips"].isin(want)].iterrows():
        features.append({
            "type": "Feature",
            "geometry": mapping(r["geometry"]),
            "properties": {
                "key": str(r["GEOID"]),
                "label": r["NAME"],
                "tier": "municipal",
                "county_key": want[r["cofips"]],
            },
        })
    return {"type": "FeatureCollection", "features": features}


def load_municipal_boundaries(refresh: bool = False) -> dict:
    """Return the municipal (county-subdivision) boundary FC, cached to disk."""
    if _CACHE_MUNI.exists() and not refresh:
        return json.loads(_CACHE_MUNI.read_text())
    fc = _build_municipal()
    _CACHE_MUNI.write_text(json.dumps(fc))
    return fc


_CACHE_OZ = DATA_RAW / "market_opportunity_zones.geojson"
# Envelope loose enough to contain all of Michigan — the real filter is the
# FULL_TRACT/county-FIPS `where` clause below, not this bbox.
_MI_BBOX = (-90.5, 41.5, -82.0, 48.5)


def _build_opportunity_zones() -> dict:
    """Opportunity Zone census tracts for every county in MARKET_COUNTIES.

    Filtered by county FIPS prefix on FULL_TRACT (not by bbox), so adding a
    new county to config.MARKET_COUNTIES picks up its zones automatically —
    no code change needed here.
    """
    where = " OR ".join(f"FULL_TRACT LIKE '{_cofips(c['geo'])}%'" for c in MARKET_COUNTIES)
    key_by_fips = {_cofips(c["geo"]): c["key"] for c in MARKET_COUNTIES}
    gdf = _arcgis_query(MSHDA_OPPORTUNITY_ZONES_SERVICE, _MI_BBOX,
                        extra_params={"where": where})
    if gdf.empty:
        return {"type": "FeatureCollection", "features": []}

    features = []
    for _, r in gdf.iterrows():
        tract = str(r["FULL_TRACT"])
        features.append({
            "type": "Feature",
            "geometry": mapping(r["geometry"]),
            "properties": {"tract": tract, "county_key": key_by_fips.get(tract[:5])},
        })
    return {"type": "FeatureCollection", "features": features}


def load_opportunity_zones(refresh: bool = False) -> dict:
    """Return the Opportunity Zone tract FeatureCollection, cached to disk."""
    if _CACHE_OZ.exists() and not refresh:
        return json.loads(_CACHE_OZ.read_text())
    fc = _build_opportunity_zones()
    _CACHE_OZ.write_text(json.dumps(fc))
    return fc


if __name__ == "__main__":
    fc = load_boundaries(refresh="--refresh" in sys.argv)
    for f in fc["features"]:
        p = f["properties"]
        gtype = f["geometry"]["type"] if f.get("geometry") else "NONE"
        print(f"{p['key']:<18} {p['tier']:<10} {gtype}")
