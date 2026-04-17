"""
Download parcel, zoning, floodplain, wetland, and building footprint data.
All functions return a GeoDataFrame in EPSG:4326 (WGS84).
"""
import json
import time
import requests
import geopandas as gpd
import pandas as pd
from pathlib import Path
import io
import shapely.ops
from shapely.geometry import box, shape, Polygon

from config import (
    DATA_RAW,
    GH_PARCEL_SERVICE,
    GH_ZONING_SERVICE,
    GH_MASTERPLAN_SERVICE,
    GH_OC_ZONING_SERVICE,
    FEMA_FLOOD_SERVICE,
    EGLE_WETLAND_SERVICE,
)

# ── ArcGIS REST helper ────────────────────────────────────────────────────────

def _arcgis_query(endpoint: str, bbox: tuple, extra_params: dict = None,
                  max_records: int = 2000, timeout: int = 60) -> gpd.GeoDataFrame:
    """
    Page through an ArcGIS Feature Service REST endpoint using a bounding box.
    bbox = (min_lon, min_lat, max_lon, max_lat) in WGS84.
    Returns a GeoDataFrame in EPSG:4326, or an empty GeoDataFrame on failure.
    timeout: seconds to wait per request (default 60).
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    base_params = {
        "where":          "1=1",
        "geometry":       f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "geometryType":   "esriGeometryEnvelope",
        "inSR":           "4326",
        "spatialRel":     "esriSpatialRelIntersects",
        "outFields":      "*",
        "returnGeometry": "true",
        "f":              "geojson",
        "resultOffset":   0,
        "resultRecordCount": max_records,
    }
    if extra_params:
        base_params.update(extra_params)

    all_features = []
    offset = 0

    while True:
        base_params["resultOffset"] = offset
        try:
            r = requests.get(endpoint, params=base_params, timeout=timeout)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  [warn] Request failed ({endpoint}): {e}")
            break

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        # ArcGIS signals more pages with exceededTransferLimit
        if not data.get("exceededTransferLimit", False):
            break
        offset += max_records
        time.sleep(0.3)  # be polite

    if not all_features:
        return gpd.GeoDataFrame()

    geojson = {"type": "FeatureCollection", "features": all_features}
    gdf = gpd.GeoDataFrame.from_features(geojson["features"], crs="EPSG:4326")
    return gdf


# ── Parcel data ───────────────────────────────────────────────────────────────

def load_parcels(bbox: tuple, city_key: str, force_download: bool = False,
                 service_url: str = None) -> gpd.GeoDataFrame:
    """
    Fetch parcel data for the given bounding box from the specified ArcGIS service.
    Caches to data/raw/<city_key>_parcels.geojson.

    service_url: ArcGIS REST query endpoint for parcels.
      - None: no service configured — use cache if available, otherwise return empty GDF.
      - Explicit URL: download from that endpoint.
      The pipeline resolves the correct URL from city_cfg before calling this function.
    """
    cache = DATA_RAW / f"{city_key}_parcels.geojson"

    if cache.exists() and not force_download:
        print(f"  Loading parcels from cache: {cache.name}")
        return gpd.read_file(cache)

    if service_url is None:
        # No service configured and no cache — tell the user how to fix it
        print(
            f"  [info] No parcel service configured for {city_key}.\n"
            f"         To add: set 'parcel_service' in CITIES['{city_key}'] in config.py\n"
            f"         or place a GeoJSON at data/raw/{city_key}_parcels.geojson."
        )
        return gpd.GeoDataFrame()

    print(f"  Downloading parcel data for {city_key} ...")
    gdf = _arcgis_query(service_url, bbox)

    if gdf.empty:
        print("  [warn] No parcel data returned — check service URL in config.py")
        return gdf

    # Normalise column names to lowercase
    gdf.columns = [c.lower() for c in gdf.columns]
    gdf.to_file(cache, driver="GeoJSON")
    print(f"  Saved {len(gdf)} parcels to {cache.name}")
    return gdf


# ── Zoning data ───────────────────────────────────────────────────────────────

def load_zoning(bbox: tuple, city_key: str, force_download: bool = False,
                service_url: str = None) -> gpd.GeoDataFrame:
    """
    Fetch zoning district polygons for the given bounding box.
    Caches to data/raw/<city_key>_zoning.geojson.

    service_url: ArcGIS REST query endpoint for zoning.
      - None: falls back to GH_ZONING_SERVICE (Grand Haven city default).
      - Ottawa County Layer 0 (GH_OC_ZONING_SERVICE) covers Grand Haven Township,
        Spring Lake Township, Spring Lake Village, and Ferrysburg.
    """
    cache = DATA_RAW / f"{city_key}_zoning.geojson"

    if cache.exists() and not force_download:
        print(f"  Loading zoning from cache: {cache.name}")
        return gpd.read_file(cache)

    if service_url is None:
        # GH_ZONING_SERVICE is the legacy default (pipeline resolves this before calling)
        service_url = GH_ZONING_SERVICE

    print(f"  Downloading zoning data for {city_key} ...")
    gdf = _arcgis_query(service_url, bbox)

    if gdf.empty:
        print("  [warn] No zoning data returned — check service URL in config.py")
        return gdf

    gdf.columns = [c.lower() for c in gdf.columns]
    gdf.to_file(cache, driver="GeoJSON")
    print(f"  Saved {len(gdf)} zoning districts to {cache.name}")
    return gdf


# ── FEMA flood zones ──────────────────────────────────────────────────────────

def load_flood_zones(bbox: tuple, city_key: str, force_download: bool = False) -> gpd.GeoDataFrame:
    """
    Fetch FEMA Special Flood Hazard Area polygons (NFHL layer 28).
    Filters to zones starting with 'A' or 'V' (regulatory flood zones).
    Caches to data/raw/<city_key>_flood.geojson.
    """
    cache = DATA_RAW / f"{city_key}_flood.geojson"

    if cache.exists() and not force_download:
        print(f"  Loading flood zones from cache: {cache.name}")
        return gpd.read_file(cache)

    print(f"  Downloading FEMA flood zones for {city_key} ...")
    # FEMA uses ESRI service; request flood zone A/V polygons only
    gdf = _arcgis_query(
        FEMA_FLOOD_SERVICE,
        bbox,
        extra_params={"where": "FLD_ZONE LIKE 'A%' OR FLD_ZONE LIKE 'V%'"},
    )

    if gdf.empty:
        print("  [info] No FEMA flood zones found in area (may be unmapped)")
        return gdf

    gdf.columns = [c.lower() for c in gdf.columns]
    gdf.to_file(cache, driver="GeoJSON")
    print(f"  Saved {len(gdf)} flood zone polygons to {cache.name}")
    return gdf


# ── EGLE wetlands ─────────────────────────────────────────────────────────────

def load_wetlands(bbox: tuple, city_key: str, force_download: bool = False) -> gpd.GeoDataFrame:
    """
    Fetch EGLE Part 303 State Wetland Inventory polygons.
    Michigan Wetlands Protection Act (Part 303) — the regulatory layer for MI permitting.
    Caches to data/raw/<city_key>_wetlands.geojson.
    """
    cache = DATA_RAW / f"{city_key}_wetlands.geojson"

    if cache.exists() and not force_download:
        print(f"  Loading wetlands from cache: {cache.name}")
        return gpd.read_file(cache)

    print(f"  Downloading EGLE Part 303 wetland data for {city_key} ...")
    # EGLE caps at 1,000 records per page — _arcgis_query handles pagination automatically
    gdf = _arcgis_query(EGLE_WETLAND_SERVICE, bbox, max_records=1000)

    if gdf.empty:
        print("  [info] No wetland polygons found in area")
        return gdf

    gdf.columns = [c.lower() for c in gdf.columns]
    gdf.to_file(cache, driver="GeoJSON")
    print(f"  Saved {len(gdf)} wetland polygons to {cache.name}")
    return gdf


# ── Building footprints (OSM Overpass) ────────────────────────────────────────

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

def load_buildings(bbox: tuple, city_key: str, force_download: bool = False) -> gpd.GeoDataFrame:
    """
    Fetch building footprint polygons from OpenStreetMap via Overpass API.
    Returns a GeoDataFrame of building polygons in EPSG:4326.
    Caches to data/raw/<city_key>_buildings.geojson.
    """
    cache = DATA_RAW / f"{city_key}_buildings.geojson"

    if cache.exists() and not force_download:
        print(f"  Loading buildings from cache: {cache.name}")
        return gpd.read_file(cache)

    print(f"  Downloading OSM building footprints for {city_key} ...")
    min_lon, min_lat, max_lon, max_lat = bbox
    # Overpass bbox format: (south, west, north, east)
    query = f"""
[out:json][timeout:120];
(
  way["building"]({min_lat},{min_lon},{max_lat},{max_lon});
  relation["building"]({min_lat},{min_lon},{max_lat},{max_lon});
);
out body;
>;
out skel qt;
"""
    try:
        r = requests.post(OVERPASS_URL, data={"data": query}, timeout=120)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  [warn] Overpass request failed: {e}")
        return gpd.GeoDataFrame()

    elements = data.get("elements", [])
    if not elements:
        print("  [info] No building footprints found in area")
        return gpd.GeoDataFrame()

    # Build a node-id → (lon, lat) lookup
    nodes = {el["id"]: (el["lon"], el["lat"])
             for el in elements if el["type"] == "node"}

    features = []
    for el in elements:
        if el["type"] != "way":
            continue
        coords = [nodes[nid] for nid in el.get("nodes", []) if nid in nodes]
        if len(coords) < 4:   # need at least 3 unique + closing node
            continue
        try:
            poly = Polygon(coords)
            if not poly.is_valid or poly.is_empty:
                continue
        except Exception:
            continue
        tags = el.get("tags", {})
        features.append({
            "geometry": poly,
            "osm_id":   el["id"],
            "building": tags.get("building", "yes"),
            "name":     tags.get("name", ""),
        })

    if not features:
        print("  [info] No valid building polygons after parsing")
        return gpd.GeoDataFrame()

    gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
    gdf.to_file(cache, driver="GeoJSON")
    print(f"  Saved {len(gdf)} building footprints to {cache.name}")
    return gdf


# ── USDA NRCS Soil Data (SSURGO via Soil Data Access) ────────────────────────

SDA_URL = "https://sdmdataaccess.sc.egov.usda.gov/Tabular/post.rest"

_NRCS_WFS_URL = "https://sdmdataaccess.sc.egov.usda.gov/Spatial/SDMNAD83Geographic.wfs"

def load_soils(bbox: tuple, city_key: str, force_download: bool = False) -> gpd.GeoDataFrame:
    """
    Fetch SSURGO soil map unit polygons for the bbox.

    Two-step process:
      1. NRCS WFS endpoint  → soil polygon geometry (mukey + geometry)
      2. SDA tabular query  → soil attributes (muname, drainagecl, hydricrating)
    Merged on mukey and cached to data/raw/<city_key>_soils.geojson.

    Returns a GeoDataFrame in EPSG:4326 with columns:
      mukey, muname, drainagecl, hydricrating, geometry
    Each row is one polygon fragment (a mukey may appear in multiple rows
    if the soil unit is non-contiguous across the landscape).
    """
    cache = DATA_RAW / f"{city_key}_soils.geojson"

    if cache.exists() and not force_download:
        print(f"  Loading soils from cache: {cache.name}")
        return gpd.read_file(cache)

    print(f"  Downloading USDA NRCS soil data for {city_key} ...")
    min_lon, min_lat, max_lon, max_lat = bbox

    # ── Step 1: Polygon geometry via NRCS WFS ─────────────────────────────────
    # The WFS returns GML with coordinates in (lat, lon) order — we flip below.
    wfs_params = {
        "SERVICE":  "WFS",
        "VERSION":  "1.1.0",
        "REQUEST":  "GetFeature",
        "TYPENAME": "MapunitPoly",
        "BBOX":     f"{min_lon},{min_lat},{max_lon},{max_lat}",
    }
    try:
        wfs_resp = requests.get(_NRCS_WFS_URL, params=wfs_params, timeout=60)
        wfs_resp.raise_for_status()
        geo_gdf = gpd.read_file(io.BytesIO(wfs_resp.content))
    except Exception as e:
        print(f"  [warn] NRCS WFS soil geometry request failed: {e}")
        return gpd.GeoDataFrame()

    if geo_gdf.empty:
        print("  [info] No soil polygons returned from NRCS WFS for this area")
        return gpd.GeoDataFrame()

    # GML WFS returns coordinates in (lat, lon) order; flip to (lon, lat) / EPSG:4326
    geo_gdf["geometry"] = geo_gdf["geometry"].map(
        lambda g: shapely.ops.transform(lambda x, y, *args: (y, x), g)
    )
    geo_gdf = geo_gdf.set_crs("EPSG:4326")
    geo_gdf = geo_gdf[["mukey", "geometry"]].copy()
    geo_gdf["mukey"] = geo_gdf["mukey"].astype(str)

    # ── Step 2: Attributes via SDA tabular query ──────────────────────────────
    mukeys = ",".join(geo_gdf["mukey"].unique().tolist())
    attr_query = f"""
SELECT mu.mukey, mu.muname, co.drainagecl, co.hydricrating
FROM mapunit mu
LEFT JOIN component co ON co.mukey = mu.mukey AND co.majcompflag = 'Yes'
WHERE mu.mukey IN ({mukeys})
"""
    try:
        attr_resp = requests.post(
            SDA_URL,
            data={"query": attr_query, "format": "json+columnname+metadata"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        attr_resp.raise_for_status()
        attr_data = attr_resp.json()
    except Exception as e:
        print(f"  [warn] SDA soil attributes query failed: {e}")
        return gpd.GeoDataFrame()

    table = attr_data.get("Table", [])
    # table[0] = column names, table[1] = type metadata, table[2:] = data rows
    if len(table) < 3:
        print("  [info] No soil attributes returned from SDA")
        return gpd.GeoDataFrame()

    attr_df = pd.DataFrame(
        table[2:], columns=["mukey", "muname", "drainagecl", "hydricrating"]
    )
    attr_df["mukey"] = attr_df["mukey"].astype(str)
    # Rare edge case: multiple major components per mukey — keep first
    attr_df = attr_df.drop_duplicates(subset=["mukey"])

    # ── Step 3: Merge geometry + attributes ───────────────────────────────────
    merged = geo_gdf.merge(attr_df, on="mukey", how="left")
    merged["muname"]      = merged["muname"].fillna("Unknown")
    merged["drainagecl"]  = merged["drainagecl"].fillna("Unknown")
    merged["hydricrating"] = merged["hydricrating"].fillna("Unknown")

    merged.to_file(cache, driver="GeoJSON")
    print(
        f"  Saved {len(merged)} soil polygons "
        f"({merged['mukey'].nunique()} map units) to {cache.name}"
    )
    return merged


# ── Future Land Use (master plan) ─────────────────────────────────────────────

def load_future_landuse(bbox: tuple, city_key: str, city_cfg: dict,
                        force_download: bool = False) -> gpd.GeoDataFrame:
    """
    Load Future Land Use (master plan) polygons for a city.
    Tries sources in priority order:
      1. Cached file (data/raw/<city_key>_future_lu.geojson) — always checked first
         unless force_download=True and a service URL is configured.
      2. ArcGIS REST service (city_cfg["flu_service"] URL) — when provided.
      3. Manually placed file (same cache path) — user-digitized from PDF.
      4. Empty GeoDataFrame — overlay step skipped gracefully.

    Returns a GeoDataFrame in EPSG:4326, or an empty GeoDataFrame when
    no data is available.  Emits an [info] log so the pipeline console
    shows the status clearly.
    """
    cache = DATA_RAW / f"{city_key}_future_lu.geojson"
    service_url = city_cfg.get("flu_service")

    # 1. Use cache if it exists (skip on force_download only when a service is available)
    if cache.exists() and not (force_download and service_url):
        print(f"  Loading future land use from cache: {cache.name}")
        return gpd.read_file(cache)

    # 2. Download from REST service if configured
    if service_url:
        print(f"  Downloading Future Land Use from service: {service_url[:60]}...")
        gdf = _arcgis_query(service_url, bbox)
        if not gdf.empty:
            gdf.columns = [c.lower() for c in gdf.columns]
            gdf.to_file(cache, driver="GeoJSON")
            print(f"  Saved {len(gdf)} FLU polygons to {cache.name}")
            return gdf
        print("  [warn] FLU service returned no data — check service URL in config.py")

    # 3. Check for manually placed file (user digitized from PDF / obtained from city)
    if cache.exists():
        print(f"  Loading manually-placed future land use file: {cache.name}")
        return gpd.read_file(cache)

    # 4. No data available
    print(
        f"  [info] No Future Land Use data for {city_key}.\n"
        f"         To add: place a GeoJSON at data/raw/{city_key}_future_lu.geojson\n"
        f"         or set 'flu_service' in config.py CITIES['{city_key}'].\n"
        f"         Source: contact planning@grandhavencity.org for the shapefile."
    )
    return gpd.GeoDataFrame()
