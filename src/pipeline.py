"""
Main pipeline: loads data, runs overlays, scores parcels, writes output CSV.

Usage:
    cd "MLS Tool Experiment"
    python src/pipeline.py

To force a fresh download (ignoring cache):
    python src/pipeline.py --refresh
"""
import sys
import argparse
import geopandas as gpd
import pandas as pd
from pathlib import Path

# Allow running from repo root or from src/
sys.path.insert(0, str(Path(__file__).parent))

from config import (
    CITIES, OUTPUT_DIR, MIN_ACRES, GRAND_HAVEN_ZONING,
    GH_PARCEL_SERVICE, GH_ZONING_SERVICE,
)
from data_loader import (
    load_parcels, load_zoning, load_flood_zones, load_wetlands,
    load_buildings, load_future_landuse,
)
from overlay import (
    add_parcel_area, add_zoning, add_flood_coverage, add_wetland_coverage,
    add_building_coverage, add_net_developable, add_future_landuse,
)
from scoring import add_scores, SCORE_COMPONENTS

# Columns to include in the output CSV (order matters for readability)
OUTPUT_COLS = [
    "parcel_id", "address", "owner",
    "calc_acres", "net_dev_acres",
    "zone_code", "zone_label", "zone_description",
    "mf_permitted", "adu_permitted", "sf_permitted",
    "max_units_per_acre", "units_conservative", "units_optimistic",
    "flood_pct", "wetland_pct",
    "building_count", "building_pct",
    "sevvalue", "taxableval", "taxablevalue",   # taxableval = GH city; taxablevalue = Ottawa County
    # Future Land Use / master plan
    "future_lu_code", "future_lu_label", "future_max_units",
    "rezoning_upside", "rezoning_delta",
    # Scoring
    "pass_filter", "filter_reason", "score",
    "pts_density", "pts_size", "pts_wetland", "pts_flood", "pts_permitted",
    "pts_rezoning",
    # Ordinance review
    "review_flag", "review_reasons", "ordinance_url",
    # Development pathway (how the parcel reaches ≥3 u/ac)
    "dev_pathway",
]


def run_city(city_key: str, city_cfg: dict, force_download: bool = False):
    label        = city_cfg["label"]
    bbox         = city_cfg["bbox"]
    min_acres    = city_cfg.get("min_acres", MIN_ACRES)
    # Per-city service URLs and zoning table.
    # .get(key, default) means: use default only when key is ABSENT from config.
    # When key is present but set to None, that signals "no service configured".
    parcel_svc   = city_cfg.get("parcel_service",  GH_PARCEL_SERVICE)
    zoning_svc   = city_cfg.get("zoning_service",  GH_ZONING_SERVICE)
    zoning_table = city_cfg.get("zoning_table",    GRAND_HAVEN_ZONING)

    print(f"\n{'='*60}")
    print(f"  Running pipeline: {label}  (min {min_acres} acres)")
    print(f"{'='*60}")

    # ── 1. Load raw layers ────────────────────────────────────────────────────
    print("\n[1/7] Loading parcels...")
    parcels = load_parcels(bbox, city_key, force_download, service_url=parcel_svc)
    if parcels.empty:
        print(f"  [error] No parcel data for {label}. Skipping.")
        return None

    print("\n[2/7] Loading zoning...")
    zoning = load_zoning(bbox, city_key, force_download, service_url=zoning_svc)

    print("\n[3/7] Loading flood zones...")
    flood = load_flood_zones(bbox, city_key, force_download)

    print("\n[4/7] Loading wetlands...")
    wetlands = load_wetlands(bbox, city_key, force_download)

    print("\n[5/7] Loading building footprints...")
    buildings = load_buildings(bbox, city_key, force_download)

    print("\n[6/7] Loading future land use (master plan)...")
    flu_gdf = load_future_landuse(bbox, city_key, city_cfg, force_download)

    # ── 2. Overlays ───────────────────────────────────────────────────────────
    print("\n[7/7] Running overlays and scoring...")
    parcels = add_parcel_area(parcels)
    parcels = add_zoning(parcels, zoning)
    parcels = add_flood_coverage(parcels, flood)
    parcels = add_wetland_coverage(parcels, wetlands)
    parcels = add_building_coverage(parcels, buildings)
    parcels = add_net_developable(parcels)

    # FLU overlay — runs after zoning so max_units_per_acre is populated for delta calc
    flu_lookup     = city_cfg.get("flu_lu_table") or {}
    flu_code_field = city_cfg.get("flu_code_field")
    parcels = add_future_landuse(parcels, flu_gdf, flu_lookup, flu_code_field)

    # ── 3. Score ──────────────────────────────────────────────────────────────
    parcels = add_scores(parcels, min_acres=min_acres, zoning_table=zoning_table,
                         city_key=city_key)

    # ── 4. Identify useful ID/address columns ─────────────────────────────────
    # Grand Haven city fields (after lowercasing): PARCELNUMB / PIN, ADDRESS / PROPSTREET, OWNERNAME
    # Ottawa County ParcelsPublic fields (after lowercasing): finalpin, propertyaddress, ownername
    parcel_id_col = _find_col(parcels, ["parcelnumb", "parcel_pin", "pin", "parcelid", "apn",
                                         "finalpin", "finalpackedpin"])
    address_col   = _find_col(parcels, ["address", "propstreet", "propstre_1",
                                         "siteaddress", "prop_address", "propertyaddress"])
    owner_col     = _find_col(parcels, ["ownername", "owner", "owner_name", "taxpayer"])
    # Zoning description from zone_ field (GH city); zon_class = Ottawa County local class
    zone_desc_col = _find_col(parcels, ["zone1", "zon_class"])

    rename = {}
    if parcel_id_col: rename[parcel_id_col] = "parcel_id"
    if address_col:   rename[address_col]   = "address"
    if owner_col:     rename[owner_col]     = "owner"
    if zone_desc_col: rename[zone_desc_col] = "zone_description"
    parcels = parcels.rename(columns=rename)

    for col in ["parcel_id", "address", "owner"]:
        if col not in parcels.columns:
            parcels[col] = ""

    # ── 5. Output ─────────────────────────────────────────────────────────────
    out_cols = [c for c in OUTPUT_COLS if c in parcels.columns]
    result = parcels[out_cols].copy()

    # Summary
    passing = result[result["pass_filter"] == True].sort_values("score", ascending=False)
    print(f"\n  Total parcels loaded:    {len(result)}")
    print(f"  Passed hard filters:     {len(passing)}")
    if not passing.empty:
        print(f"  Score range:             {passing['score'].min()} – {passing['score'].max()}")
        print(f"  Max potential units:     {passing['units_optimistic'].sum()} (optimistic)")

    # Save all parcels (with filter reason for transparency)
    all_out = OUTPUT_DIR / f"{city_key}_all_parcels.csv"
    result.to_csv(all_out, index=False)
    print(f"\n  All parcels saved to:    {all_out.name}")

    # Save only passing parcels
    passing_out = OUTPUT_DIR / f"{city_key}_qualified_parcels.csv"
    passing.to_csv(passing_out, index=False)
    print(f"  Qualified parcels saved: {passing_out.name}  ({len(passing)} parcels)")

    # Save as GeoJSON for map viewer
    geo_out = OUTPUT_DIR / f"{city_key}_qualified_parcels.geojson"
    parcels[parcels["pass_filter"] == True][out_cols + ["geometry"]].to_file(
        geo_out, driver="GeoJSON"
    )
    print(f"  GeoJSON saved:           {geo_out.name}")

    return passing


def _find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def main():
    parser = argparse.ArgumentParser(description="West Michigan Land Screener — Phase 1")
    parser.add_argument("--city", default=None, help="Run for a single city key (e.g. grand_haven)")
    parser.add_argument("--refresh", action="store_true", help="Force re-download of all data")
    args = parser.parse_args()

    cities = {args.city: CITIES[args.city]} if args.city else CITIES

    for city_key, city_cfg in cities.items():
        run_city(city_key, city_cfg, force_download=args.refresh)

    print("\nDone.")


if __name__ == "__main__":
    main()
