"""
Spatial overlay operations: attach zoning, floodplain, and wetland info to parcels.
All input GeoDataFrames must be in the same CRS; this module projects to
EPSG:26917 (UTM Zone 17N) for accurate area calculations in Michigan.
"""
import geopandas as gpd
import pandas as pd

# Michigan / West Michigan uses UTM Zone 17N for metre-based area calcs
AREA_CRS = "EPSG:26917"


def _to_area_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.empty:
        return gdf
    return gdf.to_crs(AREA_CRS)


# ── Parcel area ───────────────────────────────────────────────────────────────

def add_parcel_area(parcels: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add calc_acres column (computed from geometry, not from assessor data)."""
    p = _to_area_crs(parcels.copy())
    p["calc_acres"] = p.geometry.area / 4046.86   # sq metres → acres
    # Keep original CRS for downstream joins
    parcels = parcels.copy()
    parcels["calc_acres"] = p["calc_acres"].values
    return parcels


# ── Zoning join ───────────────────────────────────────────────────────────────

def add_zoning(parcels: gpd.GeoDataFrame, zoning: gpd.GeoDataFrame,
               zone_col: str = "zone_code") -> gpd.GeoDataFrame:
    """
    Spatial join: assign the zoning district with the largest overlap to each parcel.
    Adds columns: zone_code, zone_label (if present).
    """
    if zoning.empty:
        parcels = parcels.copy()
        parcels["zone_code"] = "UNKNOWN"
        return parcels

    # Ensure matching CRS
    zoning = zoning.to_crs(parcels.crs)

    # Zoning field names for Grand Haven layer (after lowercasing)
    # zone1 = zone code (e.g. "R-1"), zone_ = description
    # Also carry through permission + development standard fields if present
    EXTRA_ZONE_FIELDS = [
        "zone1",   # full description (Grand Haven)
        "mf_permitted", "adu_permitted", "sf_permitted",
        "mixedusedev_permitted", "livework_permitted",
        "update_lc", "update_setbacks", "update_width", "update_uses",
    ]

    # Primary zone code field (auto-detected from common naming patterns)
    # Grand Haven city (zZoning2020):    zone_  = short code (MDR, MFR…), zone1 = description
    # Ottawa County Layer 0 zoning:      zon_code = local unit code (R-1, R-2, A-1 …)
    #                                    zon_class = local unit description
    # Prefer city-specific short codes first; Ottawa County zon_code is a reliable fallback
    candidate_cols = ["zone_", "zone_code", "zonecode", "zone1", "zoning",
                      "zon_code", "zon_class",
                      "zone_dist", "zoning_code", "type", "zone_type"]
    zone_field = next((c for c in candidate_cols if c in zoning.columns), None)

    if zone_field is None:
        print(f"  [warn] Could not find zone code column in zoning data. "
              f"Available: {list(zoning.columns)}")
        parcels = parcels.copy()
        parcels["zone_code"] = "UNKNOWN"
        return parcels

    # Collect whichever extra fields actually exist in this layer
    carry_fields = [f for f in EXTRA_ZONE_FIELDS if f in zoning.columns]
    join_fields  = [zone_field] + carry_fields + ["geometry"]

    # Use largest-overlap join: project to area CRS, intersect, pick max
    p_proj = _to_area_crs(parcels[["geometry"]].copy()).reset_index(drop=True)
    p_proj["_pidx"] = p_proj.index          # stable parcel row index
    z_proj = _to_area_crs(zoning[join_fields].copy())

    intersected = gpd.overlay(p_proj[["_pidx", "geometry"]], z_proj,
                               how="intersection", keep_geom_type=False)
    intersected["overlap_area"] = intersected.geometry.area

    if intersected.empty:
        parcels = parcels.copy()
        parcels["zone_code"] = "UNKNOWN"
        return parcels

    # For each original parcel, keep the zone with the largest overlap
    keep = [zone_field] + carry_fields
    best = (
        intersected.sort_values("overlap_area", ascending=False)
                   .groupby("_pidx", sort=False)[keep]
                   .first()
    )
    best = best.rename(columns={zone_field: "zone_code"})

    result = parcels.copy().reset_index(drop=True)
    result["zone_code"] = result.index.map(best["zone_code"]).fillna("UNKNOWN")

    for f in carry_fields:
        result[f] = result.index.map(best[f]).fillna("")

    return result


# ── Environmental overlays ────────────────────────────────────────────────────

def _overlay_coverage(parcels: gpd.GeoDataFrame,
                       constraint_layer: gpd.GeoDataFrame,
                       col_name: str) -> gpd.GeoDataFrame:
    """
    For each parcel, calculate what fraction of its area is covered by
    the constraint layer. Adds <col_name>_pct and <col_name>_acres columns.
    """
    result = parcels.copy()
    result[f"{col_name}_pct"]   = 0.0
    result[f"{col_name}_acres"] = 0.0

    if constraint_layer.empty:
        return result

    p_proj = _to_area_crs(parcels[["geometry"]].copy())
    c_proj = _to_area_crs(constraint_layer[["geometry"]].copy())

    # Dissolve constraint into a single geometry for speed
    dissolved = c_proj.dissolve()

    intersected = gpd.overlay(
        p_proj.reset_index(),
        dissolved.reset_index(drop=True),
        how="intersection",
    )
    if intersected.empty:
        return result

    intersected["overlap_area_m2"] = intersected.geometry.area
    overlap_by_parcel = intersected.groupby("index")["overlap_area_m2"].sum()

    parcel_areas_m2 = p_proj.geometry.area

    pct   = (overlap_by_parcel / parcel_areas_m2).clip(0, 1)
    acres = overlap_by_parcel / 4046.86

    result[f"{col_name}_pct"]   = pct.reindex(result.index).fillna(0).values
    result[f"{col_name}_acres"] = acres.reindex(result.index).fillna(0).values
    return result


def add_flood_coverage(parcels: gpd.GeoDataFrame,
                        flood: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return _overlay_coverage(parcels, flood, "flood")


def add_wetland_coverage(parcels: gpd.GeoDataFrame,
                          wetlands: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return _overlay_coverage(parcels, wetlands, "wetland")


# ── Net developable area ──────────────────────────────────────────────────────

def add_building_coverage(parcels: gpd.GeoDataFrame,
                          buildings: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    For each parcel, count overlapping building footprints and compute total
    building footprint area as a fraction of parcel area.
    Adds: building_count (int), building_pct (float 0-1).
    A parcel with building_count > 0 is considered improved/occupied.
    """
    result = parcels.copy()
    result["building_count"] = 0
    result["building_pct"]   = 0.0

    if buildings.empty:
        return result

    buildings = buildings.to_crs(parcels.crs)

    p_proj = _to_area_crs(parcels[["geometry"]].copy()).reset_index(drop=True)
    p_proj["_pidx"] = p_proj.index
    b_proj = _to_area_crs(buildings[["geometry"]].copy())

    joined = gpd.sjoin(p_proj[["_pidx", "geometry"]], b_proj,
                       how="left", predicate="intersects")
    count_per_parcel = joined.groupby("_pidx").size()

    # Footprint area fraction via intersection
    intersected = gpd.overlay(
        p_proj[["_pidx", "geometry"]],
        b_proj.reset_index(drop=True),
        how="intersection",
        keep_geom_type=False,
    )
    if not intersected.empty:
        intersected["overlap_m2"] = intersected.geometry.area
        footprint_by_parcel = intersected.groupby("_pidx")["overlap_m2"].sum()
        parcel_areas_m2 = p_proj.geometry.area
        pct = (footprint_by_parcel / parcel_areas_m2).clip(0, 1)
        result["building_pct"] = pct.reindex(result.index).fillna(0).values

    result["building_count"] = count_per_parcel.reindex(result.index).fillna(0).astype(int).values
    return result


# ── Future Land Use overlay ───────────────────────────────────────────────────

def add_future_landuse(parcels: gpd.GeoDataFrame,
                       flu_gdf: gpd.GeoDataFrame,
                       flu_lookup: dict,
                       flu_code_field: str = None) -> gpd.GeoDataFrame:
    """
    Spatial join: assign the Future Land Use category with the largest overlap
    to each parcel.

    Adds columns:
      future_lu_code   — FLU category code/label from the master plan layer
      future_lu_label  — Human-readable FLU description (from flu_lookup)
      future_max_units — Max units/acre implied by FLU category
      rezoning_upside  — True when future_max_units > current max_units_per_acre
      rezoning_delta   — Difference (future - current) in units/acre

    When flu_gdf is empty (no data loaded), all columns are added as defaults
    (empty string / 0 / False) so downstream code runs without changes.
    """
    result = parcels.copy()
    # Initialise with neutral defaults
    result["future_lu_code"]   = ""
    result["future_lu_label"]  = ""
    result["future_max_units"] = 0
    result["rezoning_upside"]  = False
    result["rezoning_delta"]   = 0

    if flu_gdf is None or flu_gdf.empty or not flu_lookup:
        return result

    flu = flu_gdf.to_crs(parcels.crs)

    # ── Detect code field ──────────────────────────────────────────────────────
    if flu_code_field and flu_code_field in flu.columns:
        code_col = flu_code_field
    else:
        # Common field names for FLU categories
        candidates = [
            # Ottawa County MasterPlanZoning service (gis.miottawa.org)
            "stan_class",   # county-standardized class (preferred — consistent across cities)
            "mast_class",   # local municipality class (city-specific labels)
            # Generic alternatives
            "flu_code", "futureuse", "future_lu", "future_land_use",
            "landuse", "land_use", "lu_code", "category",
            "flucategory", "flu_cat", "type", "class", "code", "zone",
        ]
        code_col = next((c for c in candidates if c in flu.columns), None)

    if code_col is None:
        print(
            f"  [warn] Could not detect FLU code field in future land use layer.\n"
            f"         Available columns: {list(flu.columns)}\n"
            f"         Set 'flu_code_field' in CITIES config to fix this."
        )
        return result

    print(f"  FLU code field detected: '{code_col}'")

    # ── Largest-overlap spatial join ───────────────────────────────────────────
    p_proj = _to_area_crs(parcels[["geometry"]].copy()).reset_index(drop=True)
    p_proj["_pidx"] = p_proj.index
    f_proj = _to_area_crs(flu[[code_col, "geometry"]].copy())

    intersected = gpd.overlay(
        p_proj[["_pidx", "geometry"]],
        f_proj.reset_index(drop=True),
        how="intersection",
        keep_geom_type=False,
    )

    if intersected.empty:
        print("  [info] FLU overlay: no intersections found — check CRS or bbox alignment")
        return result

    intersected["overlap_area"] = intersected.geometry.area
    best = (
        intersected.sort_values("overlap_area", ascending=False)
                   .groupby("_pidx", sort=False)[code_col]
                   .first()
    )

    result["future_lu_code"] = result.index.map(best).fillna("").astype(str)

    # ── Look up density from flu_lookup ───────────────────────────────────────
    def _flu_info(code: str):
        code = code.strip()
        if code in flu_lookup:
            entry = flu_lookup[code]
            return entry.get("label", code), int(entry.get("max_units_per_acre", 0))
        # Fuzzy: try case-insensitive match
        code_lower = code.lower()
        for key, entry in flu_lookup.items():
            if key.lower() == code_lower:
                return entry.get("label", code), int(entry.get("max_units_per_acre", 0))
        return code if code else "", 0

    info = result["future_lu_code"].apply(_flu_info)
    result["future_lu_label"]  = [x[0] for x in info]
    result["future_max_units"] = [x[1] for x in info]

    # ── Rezoning metrics ──────────────────────────────────────────────────────
    current = result.get("max_units_per_acre", pd.Series(0, index=result.index)).fillna(0)
    result["rezoning_delta"]  = (result["future_max_units"] - current).astype(int)
    result["rezoning_upside"] = (result["future_lu_code"] != "") & (result["rezoning_delta"] > 0)

    upside_count = result["rezoning_upside"].sum()
    print(f"  FLU overlay complete — {upside_count} parcel(s) show rezoning upside")
    return result


def add_net_developable(parcels: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    net_dev_acres = gross acres minus flood and wetland overlap.
    Conservative estimate deducts both; optimistic deducts the larger of the two
    (assuming some overlap between flood and wetland areas).
    """
    p = parcels.copy()
    flood_a   = p.get("flood_acres",   pd.Series(0, index=p.index))
    wetland_a = p.get("wetland_acres", pd.Series(0, index=p.index))

    # Conservative: subtract both (may double-count if they overlap, which errs safe)
    p["net_dev_acres"] = (p["calc_acres"] - flood_a - wetland_a).clip(lower=0)
    return p
