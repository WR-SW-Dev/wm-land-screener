"""
Ordinance-aware density analysis and parcel review flagging.

Loads per-city zoning ordinance JSON files from data/ordinance/.
When no ordinance file exists for a city, all functions return safe defaults
so the pipeline runs unchanged for un-researched municipalities.

JSON files live at:  data/ordinance/<city_key>_zoning.json
Currently available: grand_haven, gh_township, spring_lake_twp
"""
import json
from pathlib import Path

# Resolved at import time — works whether called from src/ or repo root
_ORDINANCE_DIR = Path(__file__).parent.parent / "data" / "ordinance"

# Human-readable explanations for each review trigger code
_TRIGGER_MESSAGES = {
    "density_unconfirmed": (
        "Density estimated from min lot area — no explicit cap in ordinance"
    ),
    "density_cap_unconfirmed": (
        "Explicit density cap not found in ordinance — verify against district section"
    ),
    "mf_special_use_required": (
        "Multifamily requires Special Land Use approval (Planning Commission)"
    ),
    "affordable_housing_inclusionary": None,   # built dynamically — depends on unit count
    "critical_dune_setback": (
        "Rear/waterfront setback subject to EGLE Critical Dune Area requirements (Sec. 40-422)"
    ),
    # Spring Lake Township-specific triggers
    "mf_public_sewer_required": (
        "All multi-family and two-family dwellings must connect to public water and sewer (Section 355) — confirm service availability"
    ),
    "large_scale_pud_required": (
        "Developments of 8+ lots or site condo units MUST proceed as PUD — no lots may be sold or permits issued without PUD approval (Section 332)"
    ),
}


def load_ordinance(city_key: str) -> dict:
    """
    Load ordinance JSON for a city key (e.g. 'grand_haven').
    Returns an empty dict when the file doesn't exist — callers treat this as
    'no ordinance data available' and skip review flag logic gracefully.
    """
    path = _ORDINANCE_DIR / f"{city_key}_zoning.json"
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"  [warn] Could not load ordinance file {path.name}: {e}")
        return {}


def get_district(zone_code: str, ordinance: dict) -> dict:
    """
    Return district data for a zone code.  Tries exact match, then prefix match
    (so 'R-2A' matches 'R-2' if 'R-2A' isn't an explicit key).
    Returns an empty dict when not found.
    """
    if not ordinance or not zone_code:
        return {}
    districts = ordinance.get("districts", {})
    if zone_code in districts:
        return districts[zone_code]
    for key in districts:
        if zone_code.startswith(key):
            return districts[key]
    return {}


def ordinance_url(zone_code: str, ordinance: dict) -> str:
    """Return the Municode URL for a zone district, or the chapter base URL."""
    district = get_district(zone_code, ordinance)
    if district.get("url"):
        return district["url"]
    return ordinance.get("_meta", {}).get("base_url", "")


def get_review_flags(row, ordinance: dict) -> list:
    """
    Return a list of human-readable review flag strings for a parcel row.
    Empty list means no flags — parcel can be evaluated with confidence.

    Triggered by:
      - Density not confirmed in ordinance (implied from lot size)
      - Explicit density cap not found (CB, C, B, WF-2)
      - Multifamily requires special land use approval
      - Affordable housing inclusionary requirement (OT/NMU, ≥10 units)
      - Per-structure unit cap may limit achievable density (MFR, NMU, OS)
      - PD rezoning could unlock higher density on larger parcels
      - EGLE Critical Dune setback constraints (DR, WF)
    """
    if not ordinance:
        return []

    zone_code      = str(row.get("zone_code", "") or "").strip()
    district       = get_district(zone_code, ordinance)
    if not district:
        return []

    flags          = []
    units_opt      = float(row.get("units_optimistic", 0) or 0)
    calc_acres     = float(row.get("calc_acres", 0) or 0)

    # ── Static triggers defined on the district ────────────────────────────────
    for trigger in district.get("review_triggers", []):
        if trigger == "affordable_housing_inclusionary":
            # Only relevant when the parcel could actually hit the threshold
            threshold = ordinance.get("affordable_housing", {}).get("threshold_units", 10)
            pct       = ordinance.get("affordable_housing", {}).get("required_pct", 0.10)
            section   = ordinance.get("affordable_housing", {}).get("section", "")
            if units_opt >= threshold:
                flags.append(
                    f"{int(pct * 100)}% affordable units required for "
                    f"≥{threshold}-unit developments ({section})"
                )
        else:
            msg = _TRIGGER_MESSAGES.get(trigger)
            if msg:
                flags.append(f"{msg} ({district.get('section', '')})")

    # ── Per-structure unit cap ─────────────────────────────────────────────────
    cap = district.get("units_per_structure_cap")
    if cap and units_opt > cap:
        flags.append(
            f"Per-structure unit cap ({cap} max) applies — "
            f"{int(units_opt)} projected units require multiple structures "
            f"({district.get('section', '')})"
        )

    # ── PD upside: flag larger parcels in lower-density zones ──────────────────
    pd_data          = ordinance.get("pd_process", {})
    pd_threshold     = pd_data.get("flag_parcels_above_acres", 2.0)
    pd_max_height    = pd_data.get("max_height_ft", 96)
    pd_section       = pd_data.get("section", "Sec. 40-421")
    if (
        district.get("pd_eligible", False)
        and calc_acres >= pd_threshold
        and district.get("max_units_per_acre", 0) < 20
    ):
        flags.append(
            f"PD (Planned Development) rezoning may unlock higher density or height "
            f"up to {pd_max_height} ft — no minimum acreage required ({pd_section})"
        )

    # ── PRD/PUD special use: flag eligible parcels meeting the minimum acreage ───
    # For Grand Haven City: PRD (Sec. 40-552) — unit type flexibility, clustering,
    #   up to 50% MF; density stays at base zoning.
    # For GH Township / Spring Lake Township: PUD (Chapter 14 / Article 14) —
    #   density bonus of up to 25-35% for open space; mandatory for 8-9+ unit projects.
    # The JSON's "pathway_label" and "label" fields distinguish the two.
    prd_data             = ordinance.get("prd_special_use", {})
    prd_eligible_zones   = prd_data.get("eligible_districts", [])
    prd_min_acres        = float(prd_data.get("min_acres", 5.0))
    prd_section          = prd_data.get("section", "Sec. 40-552")
    prd_label            = prd_data.get("label", "PRD special use")
    prd_max_mf           = prd_data.get("max_mf_fraction")       # None for townships
    prd_max_single_acre  = prd_data.get("max_units_per_single_acre")  # None for townships
    if zone_code in prd_eligible_zones and calc_acres >= prd_min_acres:
        dist_density = prd_data.get("density_by_district", {}).get(zone_code, {})
        overall_den  = dist_density.get("max_units_per_acre_overall", "")
        density_note = f" ({overall_den} u/ac with bonus)" if overall_den else ""
        if prd_max_mf is not None and prd_max_single_acre is not None:
            # Grand Haven City PRD format
            flags.append(
                f"{prd_label} eligible (≥{prd_min_acres:.0f} ac) — up to "
                f"{int(prd_max_mf * 100)}% multifamily, clustering up to "
                f"{prd_max_single_acre} u/ac on any single acre{density_note} — "
                f"Planning Commission + City Council approval ({prd_section})"
            )
        else:
            # Township PUD format
            flags.append(
                f"{prd_label} eligible (≥{prd_min_acres:.0f} ac){density_note} — "
                f"Planning Commission + Township Board approval ({prd_section})"
            )

    return flags
