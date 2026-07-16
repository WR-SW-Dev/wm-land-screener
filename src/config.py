"""
Central config: paths, bounding boxes, API endpoints, and zoning constants.
"""
import os
from pathlib import Path

# ── Project paths ─────────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent.parent
DATA_RAW   = ROOT / "data" / "raw"
DATA_PROC  = ROOT / "data" / "processed"
OUTPUT_DIR = ROOT / "output"

ORDINANCE_DIR = ROOT / "data" / "ordinance"

for d in (DATA_RAW, DATA_PROC, OUTPUT_DIR, ORDINANCE_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ── Deploy environment ─────────────────────────────────────────────────────────
# Set WR_DEPLOY_ENV=production in the server's systemd unit (never locally) to
# gate curation UI (econ-dev scan/review/edit) to local-only — see DEPLOY.md.
# The curated data (econ_dev_queue.json) is still git-tracked and deploys with
# the code; this only hides the controls that would let the live site's own
# copy of that file drift out of sync with git.
IS_LOCAL = os.environ.get("WR_DEPLOY_ENV", "local") != "production"

# ── Ottawa County Master Plan / Zoning Service (gis.miottawa.org) ─────────────
# Covers: Grand Haven city, Grand Haven Township, Spring Lake Township,
#         Spring Lake Village, Ferrysburg — use Gov_Unit_Name to filter if needed.
# Layer 0 = current Zoning  |  Layer 2 = Master Plan (Future Land Use)
GH_MASTERPLAN_SERVICE = (
    "https://gis.miottawa.org/arcgis/rest/services/HostedServices"
    "/MasterPlanZoning/FeatureServer/2/query"
)

# Ottawa County current zoning layer (all municipalities — same FeatureServer, Layer 0)
# Used for Grand Haven Township, Spring Lake Township, and any future Ottawa County cities.
# Zone code field auto-detected by overlay.py (candidates: zone_, zoning_code, zone_dist, etc.)
GH_OC_ZONING_SERVICE = (
    "https://gis.miottawa.org/arcgis/rest/services/HostedServices"
    "/MasterPlanZoning/FeatureServer/0/query"
)

# ── Ottawa County parcel service (confirmed live) ─────────────────────────────
# "Ottawa County Parcels (Public)" — county-wide, most updated parcel database.
# Layer 0 of ParcelsPublic FeatureServer on gis.miottawa.org.
# Key fields (after lowercasing by load_parcels):
#   finalpin         — parcel PIN (unique ID)
#   ownername        — owner name
#   propertyaddress  — street address
#   propertyclass    — Michigan assessor class code (e.g. "401" = residential)
#   sevvalue         — State Equalized Value (used as improved-parcel proxy)
#   taxablevalue     — taxable value
#   acreage          — assessor acreage (pipeline computes calc_acres from geometry)
#   governmentunit   — municipality name (filter by this if bbox overlaps city)
GH_OC_PARCEL_SERVICE = (
    "https://gis.miottawa.org/arcgis/rest/services/HostedServices"
    "/ParcelsPublic/FeatureServer/0/query"
)

# ── Grand Haven ArcGIS Feature Service (org: nPodhnBI3xr2aJGu / wwplanning) ──
# Discovered from: arcgis.com/home/item.html?id=d8baaa612f5043ffbafcfb20cb45252a
# FeatureServer base: GH_Zoning_Map_WFL1
GH_BASE = "https://services2.arcgis.com/nPodhnBI3xr2aJGu/arcgis/rest/services/GH_Zoning_Map_WFL1/FeatureServer"

# Layer 8  — Grand Haven Parcels
# Fields: PARCELNUMB, PARCEL_PIN, PIN, OWNERNAME, ADDRESS, PROPSTREET,
#         PROPCITY, CLASS, SEVVALUE, TAXABLEVAL, ASSESSEDVA
GH_PARCEL_SERVICE = f"{GH_BASE}/8/query"

# Layer 12 — zZoning2020 (zoning districts)
# Fields: ZONE1 (code), ZONE_ (description), MF_Permitted, ADU_Permitted,
#         SF_Permitted, MixedUseDev_Permitted, Update_LC, Update_Setbacks
GH_ZONING_SERVICE = f"{GH_BASE}/12/query"

# Layer 11 — Sensitive Areas Overlay (bonus environmental layer)
GH_SENSITIVE_SERVICE = f"{GH_BASE}/11/query"

# ── FEMA National Flood Hazard Layer REST ─────────────────────────────────────
# Layer 28 = S_FLD_HAZ_AR (Special Flood Hazard Area polygons)
FEMA_FLOOD_SERVICE = (
    "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query"
)

# ── EGLE Part 303 State Wetland Inventory ────────────────────────────────────
# Michigan Wetlands Protection Act (Part 303) regulated wetlands.
# EGLE Water Resources Division Open Data — FeatureServer Layer 11.
# Field: SymbologyDescription (e.g. "NWI only", "Part 303") — not used by pipeline
#   (we only need the geometry for spatial area calculations).
# Max records per request: 1,000 — auto-paginated by _arcgis_query().
# More current and granular than NWI; the actual regulatory layer for Michigan permitting.
EGLE_WETLAND_SERVICE = (
    "https://gisagoegle.state.mi.us/arcgis/rest/services/EGLE"
    "/WrdOpenData/FeatureServer/11/query"
)

# ── MSHDA Opportunity Zones (state-wide) ─────────────────────────────────────
# IRS-approved Opportunity Zone census tracts (2018 designations, static).
# Source web app: michigan.maps.arcgis.com/apps/webappviewer/index.html?id=8b1413d59b8d420faaf5217a5ab52851
# Underlying feature service (public, no auth) — layer 3 "OpportunityZone_Tracts".
# Fields used: FULL_TRACT (state+county+tract FIPS), CNTY_NAME.
MSHDA_OPPORTUNITY_ZONES_SERVICE = (
    "https://utility.arcgis.com/usrsvcs/servers"
    "/478f5a4e75a7469a94ab4478e75878c4/rest/services/CSS/CSS_MSHDA/MapServer/3/query"
)

# ── Ottawa County Drains (OCWRC) ─────────────────────────────────────────────
# Ottawa County Water Resources Commissioner drain infrastructure.
# Layer 16 "All Gravity Mains" filtered to ESTABLISHED COUNTY DRAINS only
# (DrainClassification set = MI Drain Code Ch 4/6/20/21 or Sec 433). These are
# the development-relevant drains — they carry easements/setbacks and drain-
# district assessments. Private storm sewers (null classification) are excluded.
# Ottawa County only. Visual map overlay only (not scored). Keyed by MainType.
# Caches to data/raw/<city_key>_drains.geojson.
OTTAWA_DRAINS_SERVICE = (
    "https://gis.miottawa.org/arcgis/rest/services/HostedServices"
    "/DrainInfrastructure/FeatureServer/16/query"
)
OTTAWA_DRAINS_WHERE = "DrainClassification IS NOT NULL AND DrainClassification <> ''"

# MainType → map color (this is the on-map legend / key for the Drains layer).
DRAIN_MAINTYPE_COLORS = {
    "Open Channel": "#b45309",  # amber-brown — open ditch (most visible/constraining)
    "Culvert":      "#ea580c",  # orange       — culverted crossing
    "Collector":    "#7c3aed",  # purple       — piped collector main
    "Storm Lead":   "#a78bfa",  # light purple — piped lead
    "Underdrain":   "#0891b2",  # cyan         — subsurface underdrain
    "Bioswale":     "#16a34a",  # green        — vegetated swale
    "Siphon":       "#db2777",  # pink         — siphon
}
DRAIN_DEFAULT_COLOR = "#6b7280"  # gray — other / unspecified type

# ── Parcel filtering thresholds ───────────────────────────────────────────────
MIN_ACRES            = 2.0     # hard filter: below this is too small
MAX_FLOOD_PCT        = 0.25    # hard filter: >25% floodplain = disqualify
WETLAND_PENALTY_PCT  = 0.10    # soft: penalize wetland coverage above 10%

# Owner name fragments to hard-filter (case-insensitive, partial match).
# Add any owner names here that should never appear as qualified parcels —
# golf courses, nature conservancies, utilities, etc.
EXCLUDED_OWNER_PATTERNS = [
    "PUBLIC SCHOOLS",    # public school sports fields, parking lots, etc.
    "AMERICAN DUNES",    # American Dunes golf course (Grand Haven Township)
]

# Michigan assessor "property class" codes that represent vacant/developable land
# https://www.michigan.gov/documents/treasury/L4023_Property_Classification_Codes_527469_7.pdf
VACANT_USE_CODES = {
    "100", "101", "102",        # Agricultural
    "200", "201",               # Commercial vacant
    "300", "301",               # Industrial vacant
    "400", "401", "402",        # Residential vacant lots
    "VACANT", "VAC",            # Some counties use text labels
}

# ── Grand Haven zoning density lookup ────────────────────────────────────────
# Keys match ZONE_ field values from GH_Zoning_Map_WFL1/FeatureServer/12
# max_units_per_acre: estimated from Grand Haven ordinance (Chapter 40).
# TODO: verify exact density/FAR limits against current ordinance text.
GRAND_HAVEN_ZONING = {
    # Residential — densities verified against Chapter 40 (Municode, Jan 2026)
    # density_confirmed=True: explicitly stated in ordinance
    # density_confirmed=False: implied from min lot area; see data/ordinance/grand_haven_zoning.json
    "LDR":  {"label": "Low Density Residential",       "max_units_per_acre": 5,  "mf_units_per_acre": 5},    # SF: 8,700 sqft min lot; MF: same
    "MDR":  {"label": "Moderate Density Residential",  "max_units_per_acre": 7,  "mf_units_per_acre": 7},    # SF: 5,900 sqft min lot; MF: same
    "MFR":  {"label": "Multiple Family Residential",   "max_units_per_acre": 5,  "mf_units_per_acre": 30},   # SF: 9,000 sqft min lot; MF: 30 u/ac confirmed
    "DR":   {"label": "Dune Residential",              "max_units_per_acre": 3,  "mf_units_per_acre": 3},    # SF/MF: same; Critical Dune constraints
    "NS":   {"label": "North Shore",                   "max_units_per_acre": 4,  "mf_units_per_acre": 4},    # SF/MF: same
    # Mixed use / neighborhood districts
    "NMU":  {"label": "Neighborhood Mixed Use",        "max_units_per_acre": 5,  "mf_units_per_acre": 20},   # SF: 9,000 sqft min lot; MF: 20 u/ac confirmed
    "OS":   {"label": "Office Service",                "max_units_per_acre": 5,  "mf_units_per_acre": 20},   # SF: 9,000 sqft min lot; MF: 20 u/ac confirmed
    "OT":   {"label": "Old Town",                      "max_units_per_acre": 7,  "mf_units_per_acre": 7},    # SF/MF: same; 5,900 sqft min lot
    "S":    {"label": "Southside",                     "max_units_per_acre": 7,  "mf_units_per_acre": 7},    # SF/MF: same
    "E":    {"label": "Eastown",                       "max_units_per_acre": 7,  "mf_units_per_acre": 7},    # SF/MF: same
    # Commercial zones
    "CB":   {"label": "Central Business",              "max_units_per_acre": 0,  "mf_units_per_acre": 30},   # SF: not viable; MF: estimated 30 u/ac
    "C":    {"label": "Commercial",                    "max_units_per_acre": 0,  "mf_units_per_acre": 10},   # SF: not viable; MF: estimated 10 u/ac
    "B":    {"label": "Beechtree",                     "max_units_per_acre": 0,  "mf_units_per_acre": 8},    # SF: not viable; MF: estimated 8 u/ac
    "WF2":  {"label": "Waterfront 2",                  "max_units_per_acre": 0,  "mf_units_per_acre": 12},   # SF: not viable; MF: estimated 12 u/ac
    "PD":   {"label": "Planned Development",           "max_units_per_acre": 20, "mf_units_per_acre": 20},   # SF/MF: same; case-by-case
    # Non-residential
    "WF":   {"label": "Waterfront",                    "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "TI":   {"label": "Transitional Industrial",       "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "I":    {"label": "Industrial",                    "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "CC":   {"label": "Civic Center",                  "max_units_per_acre": 0,  "mf_units_per_acre": 0},
}

# ── Grand Haven Township zoning density lookup ────────────────────────────────
# Codes confirmed from live Ottawa County Layer 0 query (Zon_Code field).
# Density estimates — VERIFY against GH Township Zoning Ordinance before use.
# Contact grandhavencity.org or ottawa.org (or request ordinance from the township).
#
# Codes observed in township bbox (from pipeline run):
#   R-1, R-2, R-3, R-4, RR, PUD  — standard Michigan township codes (confirmed)
#   SFR, RG1, RG2, RG3            — GH Township-specific residential codes (estimated)
#   SAO                            — Special Areas Overlay (estimated)
#   IL-O                           — Industrial Light Overlay (0 density)
#   Also: MDR, LDR, MFR, etc. from Grand Haven city parcels that fall in the bbox
GH_TOWNSHIP_ZONING = {
    # Official Grand Haven Charter Township zone codes — confirmed from GHT Zoning
    # Ordinance 2020 (as amended Oct 5, 2025). Source: ghtmi.gov
    # Densities derived from minimum lot area per DU; no explicit units/acre in ordinance.
    # Large-scale rule: 9+ units (8+ apartments) in RR/R-1/R-2 MUST be PUD (Sec. 14.01).

    # Agricultural / rural — no residential development pathway to ≥3 u/ac
    "AG":   {"label": "Agricultural",                         "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "RP":   {"label": "Rural Preserve",                       "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "RR":   {"label": "Rural Residential",                    "max_units_per_acre": 1,  "mf_units_per_acre": 1},
    "R-1":  {"label": "Single Family Residential",            "max_units_per_acre": 2,  "mf_units_per_acre": 2},
    "R-2":  {"label": "Single Family and Two-Family Residential", "max_units_per_acre": 3, "mf_units_per_acre": 3},
    "R-3":  {"label": "Multiple Family Residential",          "max_units_per_acre": 13, "mf_units_per_acre": 13},  # explicit formula; same for SF/MF
    "R-4":  {"label": "Manufactured Housing Park",            "max_units_per_acre": 8,  "mf_units_per_acre": 8},
    "PUD":  {"label": "Planned Unit Development",             "max_units_per_acre": 12, "mf_units_per_acre": 12},
    "C-1":  {"label": "Neighborhood Commercial",              "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "C-2":  {"label": "Regional Commercial",                  "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "I-1":  {"label": "Industrial",                           "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "SFR":  {"label": "SFR (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "RG1":  {"label": "RG1 (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "RG2":  {"label": "RG2 (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "RG3":  {"label": "RG3 (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "SAO":  {"label": "SAO (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "IL-O": {"label": "IL-O (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "A-1":  {"label": "A-1 (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "A-2":  {"label": "A-2 (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "C-3":  {"label": "C-3 (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "M-1":  {"label": "M-1 (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "M-2":  {"label": "M-2 (external municipality — not GHT)", "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "RM":   {"label": "RM (external municipality — not GHT)",  "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "OS":   {"label": "OS (external municipality — not GHT)",  "max_units_per_acre": 0, "mf_units_per_acre": 0},
    "P":    {"label": "P (external municipality — not GHT)",   "max_units_per_acre": 0, "mf_units_per_acre": 0},
}

# ── Spring Lake Township zoning density lookup ────────────────────────────────
# Spring Lake Township uses similar Michigan township codes.
# Verify against Spring Lake Township Zoning Ordinance — contact sprglktownship.com.
SPRING_LAKE_TWP_ZONING = {
    # Codes confirmed from Spring Lake Township Zoning Ordinance (updated Aug 14, 2023).
    # Densities derived from minimum lot area per DU — no explicit units/acre in ordinance.
    # Source: https://springlaketwp.org (PDF) and Municode.
    # All MF requires public water + sewer (Section 355).
    # 8+ lot developments in RR/R-1/R-2/R-3/R-4 must proceed as PUD (Section 332).

    # Agricultural / Rural — no residential development pathway
    "AG":   {"label": "Agricultural",                         "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "RR":   {"label": "Rural Residential",                    "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "R-1":  {"label": "Low Density Residential-Resource",     "max_units_per_acre": 2,  "mf_units_per_acre": 2},
    "R-2":  {"label": "Medium Density Residential-Suburban",  "max_units_per_acre": 3,  "mf_units_per_acre": 3},
    "R-3":  {"label": "Medium Density Residential-Cottage",   "max_units_per_acre": 3,  "mf_units_per_acre": 3},
    "R-4":  {"label": "High Density Residential",             "max_units_per_acre": 0,  "mf_units_per_acre": 12},  # SF: no standard; MF: 3,500 sqft/DU
    "NC":   {"label": "Neighborhood Commercial",              "max_units_per_acre": 0,  "mf_units_per_acre": 12},  # SF: not viable; MF: R-4 proxy
    "MU":   {"label": "Mixed Use Commercial",                 "max_units_per_acre": 0,  "mf_units_per_acre": 12},  # SF: not viable; MF: 3,500 sqft/DU
    "GC":   {"label": "General Commercial",                   "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "LI":   {"label": "Light Industrial",                     "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "I":    {"label": "Industrial",                           "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "PR":   {"label": "Public/Recreation",                    "max_units_per_acre": 0,  "mf_units_per_acre": 0},
    "MH":   {"label": "Mobile Home District",                 "max_units_per_acre": 0,  "mf_units_per_acre": 0},
}

# ── Ottawa County Master Plan FLU lookup ─────────────────────────────────────
# Keys = Stan_Class values from MasterPlanZoning/FeatureServer/2 (confirmed live).
# Stan_Class is the county-standardized classification — consistent across all
# Ottawa County municipalities (Grand Haven city, townships, Spring Lake, etc.)
# so this table works for Phase 4 expansion without modification.
#
# max_units_per_acre: residential density equivalent for rezoning comparison.
# Based on Ottawa County / Grand Haven ordinance density ranges; verify against
# each municipality's current ordinance before using for actual projects.
#
# Also includes Mast_Class aliases for the most common Grand Haven-specific labels.
GRAND_HAVEN_FUTURE_LU = {
    # ── Residential (Stan_Class values) ───────────────────────────────────────
    "Low Density Residential A (LDR A)":   {"label": "Low Density Residential A",    "max_units_per_acre": 4},
    "Low Density Residential B":           {"label": "Low Density Residential B",    "max_units_per_acre": 6},
    "Traditional Residential Neighborhoods (TRN)": {"label": "Traditional Residential Neighborhoods", "max_units_per_acre": 8},
    "Medium Density Residential A (MDR A)": {"label": "Medium Density Residential A", "max_units_per_acre": 12},
    "Medium Density Residential B (MDR B)": {"label": "Medium Density Residential B", "max_units_per_acre": 16},
    "High Density Residential (HDR A)":    {"label": "High Density Residential",     "max_units_per_acre": 22},
    # ── Mixed use / commercial with residential component ─────────────────────
    "Mixed Use (MU)":                      {"label": "Mixed Use",                    "max_units_per_acre": 18},
    "Central Business District (CBD)":     {"label": "Central Business District",    "max_units_per_acre": 30},
    "Neighborhood Commercial (NC)":        {"label": "Neighborhood Commercial",      "max_units_per_acre": 10},
    "Commercial (C)":                      {"label": "Commercial",                   "max_units_per_acre": 10},
    "Limited Business Overlay (LBO)":      {"label": "Limited Business Overlay",     "max_units_per_acre": 8},
    "Marina District (MD)":                {"label": "Marina District",              "max_units_per_acre": 8},
    "Special Planning Areas (SPA)":        {"label": "Special Planning Areas",       "max_units_per_acre": 15},
    # ── Non-residential — no housing density ──────────────────────────────────
    "Parks, Recreation, Natural Areas (P)": {"label": "Parks, Recreation, Natural Areas", "max_units_per_acre": 0},
    "Public/Quasi-Public (PQP)":           {"label": "Public / Quasi-Public",        "max_units_per_acre": 0},
    "Light Industrial (LI)":              {"label": "Light Industrial",             "max_units_per_acre": 0},
    "Port Industrial (PI)":               {"label": "Port Industrial",              "max_units_per_acre": 0},
    # ── Mast_Class aliases — Grand Haven city-specific labels ─────────────────
    # These appear in Grand Haven's local Master Plan alongside the Stan_Class values.
    # Included as fallback if mast_class field is used instead of stan_class.
    "Downtown":                           {"label": "Downtown",                     "max_units_per_acre": 30},
    "Moderate to High Density Residential": {"label": "Moderate to High Density Residential", "max_units_per_acre": 16},
    "Low to Moderate Density Residential": {"label": "Low to Moderate Density Residential",  "max_units_per_acre": 8},
    "Traditional Neighborhood Mixed Use":  {"label": "Traditional Neighborhood Mixed Use",   "max_units_per_acre": 18},
    "Mixed Use Redevelopment":            {"label": "Mixed Use Redevelopment",      "max_units_per_acre": 18},
    "Service / Commercial":               {"label": "Service / Commercial",         "max_units_per_acre": 10},
    "Service / Residential":              {"label": "Service / Residential",        "max_units_per_acre": 8},
    "Public/Semi-Public (P)":             {"label": "Public / Semi-Public",         "max_units_per_acre": 0},
    "Parks and Preservation (PP)":        {"label": "Parks and Preservation",       "max_units_per_acre": 0},
    "Natural Area / Open Space":          {"label": "Natural Area / Open Space",    "max_units_per_acre": 0},
    "Industrial":                         {"label": "Industrial",                   "max_units_per_acre": 0},
    # Neighborhood overlay districts (score conservatively — verify with city)
    "Southwest Business District":        {"label": "SW Business District",         "max_units_per_acre": 10},
    "Center Town":                        {"label": "Center Town Overlay",          "max_units_per_acre": 18},
    "Beechtree":                          {"label": "Beechtree Neighborhood",       "max_units_per_acre": 8},
    "North Beechtree":                    {"label": "North Beechtree",              "max_units_per_acre": 8},
    "Waterfront Strategic Plan":          {"label": "Waterfront Strategic Plan",    "max_units_per_acre": 12},
    "Washington Square":                  {"label": "Washington Square",            "max_units_per_acre": 14},
    "Robbins Road":                       {"label": "Robbins Road Corridor",        "max_units_per_acre": 10},
}

# ── Target geographies ────────────────────────────────────────────────────────
# Bounding boxes as (min_lon, min_lat, max_lon, max_lat) in WGS84
#
# Per-city service keys:
#   parcel_service:   ArcGIS REST URL for parcel polygons (None = use cache / not configured)
#   zoning_service:   ArcGIS REST URL for current zoning districts (None = not configured)
#   zoning_table:     dict mapping zone codes → max_units_per_acre (must match actual codes)
#   flu_service:      REST URL for Future Land Use / master plan layer
#   flu_code_field:   column name in the FLU layer (None = autodetected)
#   flu_lu_table:     dict mapping FLU codes → max_units_per_acre
CITIES = {
    # ── Grand Haven city ──────────────────────────────────────────────────────
    "grand_haven": {
        "label":          "Grand Haven",
        "bbox":           (-86.28, 43.020, -86.18, 43.095),
        "county":         "ottawa",
        "min_acres":      1.5,   # dense urban core — 2.0 yielded 0 vacant parcels; 1.5 floor (smaller = not developable for WR-Dev)
        # Data services
        "parcel_service": GH_PARCEL_SERVICE,      # wwplanning ArcGIS org (city-specific)
        "zoning_service": GH_ZONING_SERVICE,       # zZoning2020 layer — ZONE_ = short code
        "zoning_table":   GRAND_HAVEN_ZONING,
        # Future Land Use (Ottawa County Master Plan — confirmed live)
        "flu_service":    GH_MASTERPLAN_SERVICE,
        "flu_code_field": "stan_class",
        "flu_lu_table":   GRAND_HAVEN_FUTURE_LU,
    },

    # ── Grand Haven Township ──────────────────────────────────────────────────
    # Surrounds Grand Haven city to the south, east, and inland.
    # Ottawa County zoning + master plan both covered by gis.miottawa.org services.
    # Parcel service: TODO — find Ottawa County-wide parcel layer URL and set
    #   GH_OC_PARCEL_SERVICE in config, then add "parcel_service": GH_OC_PARCEL_SERVICE here.
    # Until then: place a GeoJSON at data/raw/gh_township_parcels.geojson to use cache.
    "gh_township": {
        "label":          "Grand Haven Township",
        "bbox":           (-86.32, 42.93, -86.14, 43.12),
        "county":         "ottawa",
        "govt_unit":      "GRAND HAVEN TOWNSHIP",  # filter county parcels to this municipality
        # min_acres uses global MIN_ACRES (2.0) — more rural/suburban land
        "parcel_service": GH_OC_PARCEL_SERVICE,   # Ottawa County parcels (filtered by govt_unit)
        "zoning_service": GH_OC_ZONING_SERVICE,   # Ottawa County Layer 0 (confirmed live)
        "zoning_table":   GH_TOWNSHIP_ZONING,
        # Future Land Use — same Ottawa County service as Grand Haven city
        "flu_service":    GH_MASTERPLAN_SERVICE,
        "flu_code_field": "stan_class",
        "flu_lu_table":   GRAND_HAVEN_FUTURE_LU,
    },

    # ── Spring Lake Township ──────────────────────────────────────────────────
    # North and northeast of Grand Haven, along Spring Lake.
    # Spring Lake Village (a separate municipality) sits within the township bbox.
    # Same Ottawa County parcel/zoning/FLU situation as GH Township above.
    "spring_lake_twp": {
        "label":          "Spring Lake Township",
        "bbox":           (-86.28, 43.02, -86.07, 43.22),
        "county":         "ottawa",
        "govt_unit":      "SPRING LAKE TOWNSHIP",  # filter county parcels to this municipality
        "parcel_service": GH_OC_PARCEL_SERVICE,   # Ottawa County parcels (filtered by govt_unit)
        "zoning_service": GH_OC_ZONING_SERVICE,   # Ottawa County Layer 0 (confirmed live)
        "zoning_table":   SPRING_LAKE_TWP_ZONING,
        # Future Land Use
        "flu_service":    GH_MASTERPLAN_SERVICE,
        "flu_code_field": "stan_class",
        "flu_lu_table":   GRAND_HAVEN_FUTURE_LU,
    },

    # ── Future cities (Phase 4) ───────────────────────────────────────────────
    # "holland":  {"label": "Holland",  "bbox": (-86.155, 42.755, -86.075, 42.815),
    #              "county": "ottawa",  "parcel_service": None, "zoning_service": None,
    #              "zoning_table": {}, "flu_service": None, "flu_code_field": None,
    #              "flu_lu_table": {}},
    # "muskegon": {"label": "Muskegon", "bbox": (-86.295, 43.185, -86.195, 43.265),
    #              "county": "muskegon", "parcel_service": None, "zoning_service": None,
    #              "zoning_table": {}, "flu_service": None, "flu_code_field": None,
    #              "flu_lu_table": {}},
}

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — MARKET FEASIBILITY (Phase 1.1: demographics & affordability)
# ══════════════════════════════════════════════════════════════════════════════

# ── Census ACS configuration ──────────────────────────────────────────────────
# Most recent ACS 5-year release (confirmed live). 5-year used (not 1-year) so
# small geographies — townships — are covered. BASELINE is the non-overlapping
# 5-year sample used for population-growth comparison.
ACS_DATASET        = "acs/acs5"
ACS_YEAR           = 2024
ACS_BASELINE_YEAR  = 2019
CENSUS_BASE_URL    = "https://api.census.gov/data"

# ACS detail-table variables (acs/acs5 endpoint).
ACS_VARS = {
    "median_hh_income":  "B19013_001E",   # median household income ($)
    "median_gross_rent": "B25064_001E",   # median gross rent ($/mo)
    "median_home_value": "B25077_001E",   # median value, owner-occupied homes ($)
    "median_age":        "B01002_001E",   # median age (years)
    "population":        "B01003_001E",    # total population
    "tenure_total":      "B25003_001E",   # occupied housing units
    "tenure_renter":     "B25003_003E",   # renter-occupied units
    "occ_total":         "B25002_001E",   # all housing units
    "occ_occupied":      "B25002_002E",   # occupied units
    "burden_total":      "B25070_001E",   # renters w/ computed rent-to-income
    "burden_30_35":      "B25070_007E",   # 30.0–34.9% of income on rent
    "burden_35_40":      "B25070_008E",   # 35.0–39.9%
    "burden_40_50":      "B25070_009E",   # 40.0–49.9%
    "burden_50_plus":    "B25070_010E",   # 50.0%+
}
# Variables only available on the Data-Profile endpoint (acs/acs5/profile).
# `_moe` entries are the 90% margin of error for the matching estimate; used to
# flag low-reliability small-sample estimates (e.g. townships) in the UI.
ACS_PROFILE_VARS = {
    "rental_vacancy_rate": "DP04_0005E",   # rental vacancy rate (%)
    "rental_vacancy_moe":  "DP04_0005M",   # ± margin of error on the rate
}

# ── Affordability rule (WR-Dev BTR) ───────────────────────────────────────────
# Max affordable monthly housing payment = median HH income / 12 × 30%.
AFFORDABILITY_INCOME_SHARE = 0.30

# ── Submarkets & competition geography ────────────────────────────────────────
# Submarkets = the existing land-screener cities (Census county-subdivision FIPS,
# all within Ottawa County 26139). Counties = tri-county competition/context band.
# `screener_key` links a submarket to its CITIES entry (Market → Land carry-fwd).
MARKET_SUBMARKETS = [
    {"key": "grand_haven",    "label": "Grand Haven",        "screener_key": "grand_haven",
     "geo": {"type": "cousub", "state": "26", "county": "139", "cousub": "33340"}},
    {"key": "gh_township",    "label": "Grand Haven Twp",    "screener_key": "gh_township",
     "geo": {"type": "cousub", "state": "26", "county": "139", "cousub": "33360"}},
    {"key": "spring_lake_twp","label": "Spring Lake Twp",    "screener_key": "spring_lake_twp",
     "geo": {"type": "cousub", "state": "26", "county": "139", "cousub": "75840"}},
]
MARKET_COUNTIES = [
    {"key": "ottawa",   "label": "Ottawa County",   "geo": {"type": "county", "state": "26", "county": "139"}},
    {"key": "kent",     "label": "Kent County",     "geo": {"type": "county", "state": "26", "county": "081"}},
    {"key": "muskegon", "label": "Muskegon County", "geo": {"type": "county", "state": "26", "county": "121"}},
    {"key": "allegan",  "label": "Allegan County",  "geo": {"type": "county", "state": "26", "county": "005"}},
]

# ── FRED (Federal Reserve Economic Data) — pricing/momentum layer ────────────
# County HPI (FHFA All-Transactions Index, annual): series `ATNHPIUS<FIPS>A`.
# State HPI baseline (quarterly): `MISTHPI`, annualized for comparison.
# County building permits (Census BPS, annual, residential-only): `BPPRIV0<FIPS>`.
# National 30-yr mortgage rate (weekly): `MORTGAGE30US`.
# All county series IDs are built from MARKET_COUNTIES' FIPS codes at request
# time (see market/fred.py) — a county added there is picked up automatically,
# no new series ID to hardcode. Verified live against the FRED API 2026-07-16.
FRED_BASE_URL          = "https://api.stlouisfed.org/fred"
FRED_STATE_HPI_SERIES  = "MISTHPI"
FRED_MORTGAGE_SERIES   = "MORTGAGE30US"

# Momentum badge: % of a county's 5-yr HNA unit gap already covered by permits
# issued so far within that study period. Below RED_MAX = red (Underserved),
# up to YELLOW_MAX = yellow (Responding), above = green (Saturating). Matches
# the existing heat map's red=more-need / green=less-need convention.
FRED_MOMENTUM_RED_MAX    = 50
FRED_MOMENTUM_YELLOW_MAX = 100

# ── Demand-score weights (Phase 1.1) ──────────────────────────────────────────
# 0–100 housing-need / BTR-demand score. Each signal is normalized 0–1 over the
# band below, then weighted. Weights sum to 100. Higher score = more need.
#   tightness     — inverted rental vacancy rate (low vacancy = tight = demand)
#   cost_burden   — % of renters paying >30% of income on rent
#   growth        — population growth since the baseline ACS sample
#   renter_share  — renter-occupied share (size of the rental market)
#   rent_pressure — median rent ÷ max-affordable rent (rent vs local incomes)
DEMAND_WEIGHTS = {
    "tightness":     30,
    "cost_burden":   25,
    "growth":        20,
    "renter_share":  15,
    "rent_pressure": 10,
}
# Normalization bands: (value_at_score_0, value_at_score_1). Clamped to [0,1].
DEMAND_BANDS = {
    "rental_vacancy_rate": (10.0, 0.0),    # 10%+ vacancy → 0 ; 0% → 1 (inverted)
    "cost_burden_pct":     (20.0, 60.0),   # 20% burdened → 0 ; 60%+ → 1
    "pop_growth_pct":      (-5.0, 20.0),   # −5% → 0 ; +20% → 1
    "renter_share_pct":    (15.0, 55.0),   # 15% renters → 0 ; 55%+ → 1
    "rent_to_afford":      (0.40, 1.00),   # rent 40% of affordable → 0 ; 100%+ → 1
}

# ── Census TIGERweb boundary service (choropleth geometries) ──────────────────
# tigerWMS_Current MapServer. Layer ids are discovered at runtime by name match
# (they shift between vintages), so only the base URL is pinned here.
TIGERWEB_BASE = ("https://tigerweb.geo.census.gov/arcgis/rest/services/"
                 "TIGERweb/tigerWMS_Current/MapServer")
