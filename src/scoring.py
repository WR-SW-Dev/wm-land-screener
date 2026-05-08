"""
Feasibility filtering and 0-100 scoring for each parcel.
"""
import pandas as pd
import geopandas as gpd

from config import (
    MAX_FLOOD_PCT,
    WETLAND_PENALTY_PCT,
    VACANT_USE_CODES,
    GRAND_HAVEN_ZONING,
)
from ordinance import load_ordinance, get_review_flags, ordinance_url


# ── Zoning lookup ─────────────────────────────────────────────────────────────

def get_max_density(zone_code: str, zoning_table: dict = None,
                    density_field: str = "max_units_per_acre") -> int:
    """
    Return density for a zone code using the provided zoning table.
    density_field: "max_units_per_acre" (SF, default) or "mf_units_per_acre" (MF).
    Falls back to GRAND_HAVEN_ZONING when no table is supplied.
    Returns 0 for unknown or non-residential zones.
    """
    table = zoning_table if zoning_table is not None else GRAND_HAVEN_ZONING
    if not zone_code or zone_code == "UNKNOWN":
        return 0
    # Exact match first, then prefix match (e.g. "R-2A" → "R-2")
    if zone_code in table:
        return table[zone_code].get(density_field, table[zone_code].get("max_units_per_acre", 0))
    for key in table:
        if zone_code.startswith(key):
            return table[key].get(density_field, table[key].get("max_units_per_acre", 0))
    return 0


def get_zone_label(zone_code: str, zoning_table: dict = None) -> str:
    """Return human-readable zone label. Uses GRAND_HAVEN_ZONING when no table supplied."""
    table = zoning_table if zoning_table is not None else GRAND_HAVEN_ZONING
    if zone_code in table:
        return table[zone_code]["label"]
    # Prefix match — handles variants like "R-2A" → "R-2" (same as get_max_density)
    for key in table:
        if zone_code.startswith(key):
            return table[key]["label"]
    return "Unknown"


# ── Hard filters ──────────────────────────────────────────────────────────────

def apply_hard_filters(parcels: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Mark each parcel as feasible (pass=True) or not.
    Adds a 'filter_reason' column explaining the first failure.
    """
    p = parcels.copy()
    p["pass_filter"] = True
    p["filter_reason"] = ""

    # 1. Floodplain coverage
    if "flood_pct" in p.columns:
        flooded = p["flood_pct"] > MAX_FLOOD_PCT
        p.loc[flooded & p["pass_filter"], "filter_reason"] = (
            f"Too much floodplain (>{MAX_FLOOD_PCT*100:.0f}%)"
        )
        p.loc[flooded, "pass_filter"] = False

    # 3. Improved parcel detection
    # Primary method: building footprint count from OSM overlay.
    # Fallback: SEV/acre proxy when building data is unavailable.
    exempt_classes = {"701"}  # public/exempt land — exclude

    # Grand Haven city field: "class" | Ottawa County ParcelsPublic field: "propertyclass"
    use_col = _find_col(p, ["class", "propclass", "prop_class", "propertyclass"])
    if use_col:
        is_exempt = p[use_col].astype(str).str.strip().isin(exempt_classes)
        p.loc[is_exempt & p["pass_filter"], "filter_reason"] = "Exempt parcel (class 701)"
        p.loc[is_exempt, "pass_filter"] = False

    # 5% building coverage threshold — calibrated to allow a single home on a
    # large parcel (typically 0.3–2% coverage) while eliminating housing
    # communities and dense subdivisions (typically 8–30% coverage).
    # Example: 1 house (2,000 sq ft) on 5 acres ≈ 0.9% → passes.
    #          10 homes on 5 acres ≈ 9% → fails.
    BUILDING_PCT_THRESHOLD = 0.05
    if "building_pct" in p.columns:
        has_buildings = p["building_pct"] > BUILDING_PCT_THRESHOLD
        p.loc[has_buildings & p["pass_filter"], "filter_reason"] = (
            f"Substantially improved (>{BUILDING_PCT_THRESHOLD*100:.0f}% building coverage)"
        )
        p.loc[has_buildings, "pass_filter"] = False
    else:
        # Fallback: SEV/acre proxy ($150k/acre → likely has structures)
        SEV_PER_ACRE_THRESHOLD = 150_000
        sev_col = _find_col(p, ["sevvalue", "sev_value", "sev"])
        if sev_col and "calc_acres" in p.columns:
            safe_acres = p["calc_acres"].clip(lower=0.01)
            sev_per_acre = p[sev_col].fillna(0) / safe_acres
            likely_improved = (sev_per_acre > SEV_PER_ACRE_THRESHOLD) & (p[sev_col] > 0)
            p.loc[likely_improved & p["pass_filter"], "filter_reason"] = (
                "Likely improved (high SEV/acre — verify manually)"
            )
            p.loc[likely_improved, "pass_filter"] = False

    # 4. Zoning allows no residential density
    if "max_units_per_acre" in p.columns:
        no_density = p["max_units_per_acre"] == 0
        p.loc[no_density & p["pass_filter"], "filter_reason"] = "Zoning allows 0 residential units"
        p.loc[no_density, "pass_filter"] = False

    # 5. No path to ≥3 u/ac via any route (by right, PUD/PRD, master plan, or PD rezoning)
    #    dev_pathway must be computed before apply_hard_filters() is called — see add_scores().
    if "dev_pathway" in p.columns:
        not_viable = p["dev_pathway"] == "Not viable"
        p.loc[not_viable & p["pass_filter"], "filter_reason"] = (
            f"No path to ≥{DEV_DENSITY_THRESHOLD:.0f} u/ac "
            "(current zoning, master plan, PUD/PRD, and PD rezoning all below threshold)"
        )
        p.loc[not_viable, "pass_filter"] = False

    return p


def _find_col(df: pd.DataFrame, candidates: list):
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ── Soft scoring ──────────────────────────────────────────────────────────────

# Scoring component definitions — single source of truth used by both the
# scoring logic and the UI methodology table.
SCORE_COMPONENTS = [
    {"key": "pts_density",   "label": "Zoning density",     "max": 40,
     "description": "Single-family units/acre allowed by zoning. Full credit at 7 u/ac (realistic SF ceiling). Anything above 7 is capped at 40 pts."},
    {"key": "pts_rezoning",  "label": "Rezoning potential",  "max": 20,
     "description": (
         "Master plan future land use shows higher density than current zoning. "
         "Scales 0–20 pts based on the density gap (full credit at +15 u/ac upside). "
         "Only awarded when FLU data is available; 0 pts when not loaded."
     )},
    {"key": "pts_wetland",   "label": "Wetland coverage",    "max": 15,
     "description": "Full credit ≤10% wetland. Scales to 0 at 50% coverage."},
    {"key": "pts_flood",     "label": "Floodplain coverage", "max": 15,
     "description": "Full credit at 0% flood. Scales to 0 at the 25% hard-filter ceiling."},
    {"key": "pts_shape",     "label": "Parcel shape",        "max": 10,
     "description": "How square/compact the parcel is. Higher score = more rectangular and development-friendly. Lower score = skinny, irregular, or oddly shaped."},
]


def score_components(row: pd.Series) -> dict:
    """
    Return each scoring component's earned points as a dict keyed by pts_* name.
    Called for parcels that pass hard filters.
    """
    # 1. Zoning density (0-40 pts) — SF mode: full credit at 7 u/ac (realistic SF ceiling)
    max_density = row.get("max_units_per_acre", 0)
    pts_density = round(min(max_density / 7, 1.0) * 40, 1)

    # 2. Rezoning potential (0-20 pts) — only when FLU data is present
    flu_code       = str(row.get("future_lu_code", "")).strip()
    rezoning_delta = float(row.get("rezoning_delta", 0) or 0)
    if flu_code and rezoning_delta > 0:
        pts_rezoning = round(min(rezoning_delta / 15.0, 1.0) * 20, 1)
    else:
        pts_rezoning = 0.0

    # 3. Wetland coverage (0-15 pts)
    wetland_pct = row.get("wetland_pct", 0)
    if wetland_pct <= WETLAND_PENALTY_PCT:
        wet_frac = 1.0
    else:
        wet_frac = max(0.0, 1 - (wetland_pct - WETLAND_PENALTY_PCT) / 0.4)
    pts_wetland = round(wet_frac * 15, 1)

    # 4. Floodplain coverage (0-15 pts)
    flood_pct = row.get("flood_pct", 0)
    pts_flood = round(max(0.0, 1 - flood_pct / MAX_FLOOD_PCT) * 15, 1)

    # 5. Parcel shape compactness (0-10 pts)
    # Uses the isoperimetric quotient: 4π × area / perimeter²
    # Perfect square ≈ 0.785, circle = 1.0. We normalize so a square scores ~10.
    shape_score = float(row.get("shape_score", 0) or 0)
    pts_shape = round(min(shape_score, 1.0) * 10, 1)

    return {
        "pts_density":   pts_density,
        "pts_rezoning":  pts_rezoning,
        "pts_wetland":   pts_wetland,
        "pts_flood":     pts_flood,
        "pts_shape":     pts_shape,
    }


def score_parcel(row: pd.Series) -> float:
    """
    Compute total 0-100 feasibility score.
    Only called for parcels that pass hard filters.
    """
    return round(min(sum(score_components(row).values()), 100), 1)


# ── Development pathway classification ────────────────────────────────────────

DEV_DENSITY_THRESHOLD = 3.0  # units/acre — minimum viable residential density


def _classify_dev_pathway(row: pd.Series, ordinance: dict,
                           threshold: float = DEV_DENSITY_THRESHOLD) -> str:
    """
    Return the simplest available path for a parcel to reach ≥ threshold u/ac.

    Priority order (easiest → most approval burden):
      1. 'By right'           — current zoning already allows ≥ threshold u/ac
      2. 'PRD special use'    — ≥5 ac in LDR/MDR/MFR; PRD overall density ≥ threshold
      3. 'Master plan upzone' — FLU designation ≥ threshold (current zoning < threshold)
      4. 'PD rezoning'        — pd_eligible + ≥2 ac parcel (legislative rezoning)
      5. 'Not viable'         — no identified path reaches threshold u/ac
    """
    current_den = float(row.get("max_units_per_acre", 0) or 0)
    calc_acres  = float(row.get("calc_acres", 0) or 0)
    future_den  = float(row.get("future_max_units", 0) or 0)
    zone_code   = str(row.get("zone_code", "") or "").strip()

    # 1. By right — current zoning already reaches the threshold
    if current_den >= threshold:
        return "By right"

    # 2. PRD/PUD special use — available for ≥ min_acres in eligible districts
    #    Label is taken from the JSON ("PRD special use", "PUD special use", etc.)
    if ordinance:
        prd             = ordinance.get("prd_special_use", {})
        prd_zones       = prd.get("eligible_districts", [])
        prd_min_acres   = float(prd.get("min_acres", 5.0))
        pathway_label   = prd.get("pathway_label", "PRD special use")
        if zone_code in prd_zones and calc_acres >= prd_min_acres:
            dist_den = prd.get("density_by_district", {}).get(zone_code, {})
            prd_den  = float(dist_den.get("max_units_per_acre_overall", 0) or 0)
            if prd_den >= threshold:
                return pathway_label

    # 3. Master plan upzone — FLU shows ≥ threshold even though current zoning doesn't
    flu_code = str(row.get("future_lu_code", "") or "").strip()
    if flu_code and future_den >= threshold:
        return "Master plan upzone"

    # 4. PD rezoning — legislative, case-by-case but available for pd_eligible parcels
    if ordinance:
        district  = ordinance.get("districts", {}).get(zone_code, {})
        pd_data   = ordinance.get("pd_process", {})
        pd_thresh = float(pd_data.get("flag_parcels_above_acres", 2.0))
        if district.get("pd_eligible", False) and calc_acres >= pd_thresh:
            return "PD rezoning"

    return "Not viable"


def add_scores(parcels: gpd.GeoDataFrame, zoning_table: dict = None,
               city_key: str = None) -> gpd.GeoDataFrame:
    """
    Add max_units_per_acre, zone_label, density estimates, score, score components,
    review flags, and ordinance URLs.

    zoning_table: city-specific dict mapping zone codes → {label, max_units_per_acre}.
      Defaults to GRAND_HAVEN_ZONING for backwards compatibility.
    city_key: used to load the ordinance JSON for review flag logic.
      When None or no ordinance file exists, review columns are empty.
    """
    p = parcels.copy()

    p["max_units_per_acre"] = p["zone_code"].apply(
        lambda z: get_max_density(z, zoning_table, "max_units_per_acre")
    )
    p["mf_max_units_per_acre"] = p["zone_code"].apply(
        lambda z: get_max_density(z, zoning_table, "mf_units_per_acre")
    )
    p["zone_label"] = p["zone_code"].apply(
        lambda z: get_zone_label(z, zoning_table)
    )

    # Recompute rezoning_delta / rezoning_upside now that max_units_per_acre is known.
    # add_future_landuse() runs before this function so it uses 0 as the current-density
    # baseline (max_units_per_acre didn't exist yet).  Correct here once zoning is set.
    if "future_max_units" in p.columns and "future_lu_code" in p.columns:
        p["rezoning_delta"] = (
            p["future_max_units"].fillna(0).astype(int)
            - p["max_units_per_acre"].fillna(0).astype(int)
        )
        p["rezoning_upside"] = (
            p["future_lu_code"].astype(str).str.strip().ne("")
            & (p["rezoning_delta"] > 0)
        )

    # Density estimates
    net = p["net_dev_acres"] if "net_dev_acres" in p.columns else p["calc_acres"]
    p["units_conservative"] = (net * p["max_units_per_acre"] * 0.70).round(0).astype(int)
    p["units_optimistic"]   = (net * p["max_units_per_acre"] * 1.00).round(0).astype(int)

    # ── Load ordinance early — needed for pathway classification before hard filter ──
    ordinance = load_ordinance(city_key) if city_key else {}

    # ── Development pathway classification (BEFORE hard filter) ────────────────
    # Must run first so apply_hard_filters() can eliminate "Not viable" parcels.
    p["dev_pathway"] = p.apply(
        lambda row: _classify_dev_pathway(row, ordinance), axis=1
    )

    # ── Hard filters (includes "Not viable" pathway check) ────────────────────
    p = apply_hard_filters(p)

    # Score + per-component breakdown for passing parcels only
    mask = p["pass_filter"]
    if mask.any():
        breakdown = p.loc[mask].apply(score_components, axis=1, result_type="expand")
        for col in breakdown.columns:
            p[col] = 0.0
            p.loc[mask, col] = breakdown[col]
        p.loc[mask, "score"] = breakdown.sum(axis=1).clip(upper=100).round(1)
    else:
        for comp in SCORE_COMPONENTS:
            p[comp["key"]] = 0.0

    p.loc[~mask, "score"] = 0.0

    # ── Ordinance review flags ─────────────────────────────────────────────────
    if ordinance:
        flag_lists = p.apply(lambda row: get_review_flags(row, ordinance), axis=1)
        p["review_flag"]    = flag_lists.apply(lambda f: len(f) > 0)
        p["review_reasons"] = flag_lists.apply(lambda f: " | ".join(f))
        p["ordinance_url"]  = p["zone_code"].apply(
            lambda z: ordinance_url(z, ordinance)
        )
        n_flagged = p["review_flag"].sum()
        if n_flagged:
            print(f"  Ordinance review flags: {n_flagged} parcels flagged for manual review")
    else:
        p["review_flag"]    = False
        p["review_reasons"] = ""
        p["ordinance_url"]  = ""

    # ── Pathway count summary ─────────────────────────────────────────────────
    pathway_counts = p.loc[p["pass_filter"], "dev_pathway"].value_counts()
    if not pathway_counts.empty:
        for pathway, count in pathway_counts.items():
            print(f"  Dev pathway '{pathway}': {count} qualifying parcels")

    return p
