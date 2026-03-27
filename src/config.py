"""
Central config: paths, bounding boxes, API endpoints, and zoning constants.
"""
from pathlib import Path

# ── Project paths ─────────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent.parent
DATA_RAW   = ROOT / "data" / "raw"
DATA_PROC  = ROOT / "data" / "processed"
OUTPUT_DIR = ROOT / "output"

ORDINANCE_DIR = ROOT / "data" / "ordinance"

for d in (DATA_RAW, DATA_PROC, OUTPUT_DIR, ORDINANCE_DIR):
    d.mkdir(parents=True, exist_ok=True)

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

# ── Parcel filtering thresholds ───────────────────────────────────────────────
MIN_ACRES            = 4.0     # hard filter: below this is too small
MAX_FLOOD_PCT        = 0.25    # hard filter: >25% floodplain = disqualify
WETLAND_PENALTY_PCT  = 0.10    # soft: penalize wetland coverage above 10%

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
    "LDR":  {"label": "Low Density Residential",       "max_units_per_acre": 5},   # implied: 8,700 sqft/unit
    "MDR":  {"label": "Moderate Density Residential",  "max_units_per_acre": 7},   # implied: 5,900 sqft/unit (Sec. 40-404 confirmed 5,900; summary table shows 5,800 — use detailed section)
    "MFR":  {"label": "Multiple Family Residential",   "max_units_per_acre": 30},  # confirmed: 30 u/a or 12/structure
    "DR":   {"label": "Dune Residential",              "max_units_per_acre": 3},   # implied: 10,500 sqft/unit; Critical Dune constraints
    "NS":   {"label": "North Shore",                   "max_units_per_acre": 4},   # implied: 10,500 sqft/unit
    # Mixed use / neighborhood districts
    "NMU":  {"label": "Neighborhood Mixed Use",        "max_units_per_acre": 20},  # confirmed: 20 u/a or 6/structure
    "OS":   {"label": "Office Service",                "max_units_per_acre": 20},  # confirmed: 20 u/a or 6/structure
    "OT":   {"label": "Old Town",                      "max_units_per_acre": 7},   # implied: 5,900 sqft/unit
    "S":    {"label": "Southside",                     "max_units_per_acre": 7},   # implied: 5,900 sqft/unit
    "E":    {"label": "Eastown",                       "max_units_per_acre": 7},   # implied: 5,900 sqft/unit
    # Commercial with residential potential
    "CB":   {"label": "Central Business",              "max_units_per_acre": 30},  # estimated — no explicit cap found
    "C":    {"label": "Commercial",                    "max_units_per_acre": 10},  # estimated — no explicit cap found
    "B":    {"label": "Beechtree",                     "max_units_per_acre": 8},   # estimated — no explicit cap found
    "WF2":  {"label": "Waterfront 2",                  "max_units_per_acre": 12},  # estimated — no explicit cap found
    "PD":   {"label": "Planned Development",           "max_units_per_acre": 20},  # guidance: 1,000 sqft/unit land area; 50% coverage cap → ~20 practical
    # Non-residential
    "WF":   {"label": "Waterfront",                    "max_units_per_acre": 0},   # no residential uses in WF land use table
    "TI":   {"label": "Transitional Industrial",       "max_units_per_acre": 0},
    "I":    {"label": "Industrial",                    "max_units_per_acre": 0},
    "CC":   {"label": "Civic Center",                  "max_units_per_acre": 0},
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
    "AG":   {"label": "Agricultural",                         "max_units_per_acre": 0},   # 20 ac min; incidental residential only
    "RP":   {"label": "Rural Preserve",                       "max_units_per_acre": 0},   # 5 ac min; 0.2 u/ac; no path to ≥3
    "RR":   {"label": "Rural Residential",                    "max_units_per_acre": 1},   # 45,000 sqft min; 0.97 u/ac; PUD max 1.2 u/ac — below threshold
    # Residential
    # R-1: 15,000 sqft min → 2.9 u/ac (floor → 2); PUD density bonus → 3.6 u/ac on ≥5 ac
    "R-1":  {"label": "Single Family Residential",            "max_units_per_acre": 2},
    # R-2: 13,000 sqft min → 3.35 u/ac (floor → 3); duplexes by right; ≥3 u/ac
    "R-2":  {"label": "Single Family and Two-Family Residential", "max_units_per_acre": 3},
    # R-3: explicit 1 unit/3,250 sqft → 13.4 u/ac; MF by right
    "R-3":  {"label": "Multiple Family Residential",          "max_units_per_acre": 13},
    # R-4: 5,000 sqft/unit → 8.7 u/ac; manufactured housing only
    "R-4":  {"label": "Manufactured Housing Park",            "max_units_per_acre": 8},
    # PUD: site-specific; density governed by PUD Agreement
    "PUD":  {"label": "Planned Unit Development",             "max_units_per_acre": 12},
    # Commercial — residential as SLU (upper floors) only
    "C-1":  {"label": "Neighborhood Commercial",              "max_units_per_acre": 0},
    "C-2":  {"label": "Regional Commercial",                  "max_units_per_acre": 0},
    # Industrial
    "I-1":  {"label": "Industrial",                           "max_units_per_acre": 0},
    # ── Codes from neighboring municipalities appearing in Ottawa County GIS bbox ──
    # These are NOT Grand Haven Township codes — they belong to Spring Lake, Ferrysburg,
    # or Grand Haven City parcels that fall within the township bounding box.
    # Set to 0 so they fail the density hard filter and are excluded from results.
    "SFR":  {"label": "SFR (external municipality — not GHT)", "max_units_per_acre": 0},
    "RG1":  {"label": "RG1 (external municipality — not GHT)", "max_units_per_acre": 0},
    "RG2":  {"label": "RG2 (external municipality — not GHT)", "max_units_per_acre": 0},
    "RG3":  {"label": "RG3 (external municipality — not GHT)", "max_units_per_acre": 0},
    "SAO":  {"label": "SAO (external municipality — not GHT)", "max_units_per_acre": 0},
    "IL-O": {"label": "IL-O (external municipality — not GHT)", "max_units_per_acre": 0},
    "A-1":  {"label": "A-1 (external municipality — not GHT)", "max_units_per_acre": 0},
    "A-2":  {"label": "A-2 (external municipality — not GHT)", "max_units_per_acre": 0},
    "C-3":  {"label": "C-3 (external municipality — not GHT)", "max_units_per_acre": 0},
    "M-1":  {"label": "M-1 (external municipality — not GHT)", "max_units_per_acre": 0},
    "M-2":  {"label": "M-2 (external municipality — not GHT)", "max_units_per_acre": 0},
    "RM":   {"label": "RM (external municipality — not GHT)",  "max_units_per_acre": 0},
    "OS":   {"label": "OS (external municipality — not GHT)",  "max_units_per_acre": 0},
    "P":    {"label": "P (external municipality — not GHT)",   "max_units_per_acre": 0},
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
    "AG":   {"label": "Agricultural",                         "max_units_per_acre": 0},
    "RR":   {"label": "Rural Residential",                    "max_units_per_acre": 0},
    # Residential (with-sewer densities — development assumption)
    # R-1: 15,000 sqft min → 2.9 u/ac (floor → 2); PUD w/ density bonus → 3.6 u/ac
    "R-1":  {"label": "Low Density Residential-Resource",     "max_units_per_acre": 2},
    # R-2: 12,000 sqft min → 3.6 u/ac (floor → 3); by right ≥3 u/ac threshold
    "R-2":  {"label": "Medium Density Residential-Suburban",  "max_units_per_acre": 3},
    # R-3: 12,000 sqft min → 3.6 u/ac (floor → 3); cottage district (Strawberry Point)
    "R-3":  {"label": "Medium Density Residential-Cottage",   "max_units_per_acre": 3},
    # R-4: 3,500 sqft/DU → 12.4 u/ac; MF by right
    "R-4":  {"label": "High Density Residential",             "max_units_per_acre": 12},
    # Commercial with residential
    "NC":   {"label": "Neighborhood Commercial",              "max_units_per_acre": 12},  # MF by right; R-4 density proxy — unconfirmed
    "MU":   {"label": "Mixed Use Commercial",                 "max_units_per_acre": 12},  # MF by right; 3,500 sqft/DU = ~12.4 u/ac
    # Non-residential
    "GC":   {"label": "General Commercial",                   "max_units_per_acre": 0},
    "LI":   {"label": "Light Industrial",                     "max_units_per_acre": 0},
    "I":    {"label": "Industrial",                           "max_units_per_acre": 0},
    "PR":   {"label": "Public/Recreation",                    "max_units_per_acre": 0},
    "MH":   {"label": "Mobile Home District",                 "max_units_per_acre": 0},   # state-regulated
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
        "bbox":           (-86.275, 43.045, -86.195, 43.095),
        "county":         "ottawa",
        "min_acres":      2.0,   # urban city — lower threshold; global default is MIN_ACRES (4.0)
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
        "bbox":           (-86.32, 43.00, -86.14, 43.12),
        "county":         "ottawa",
        # min_acres uses global MIN_ACRES (4.0) — more rural/suburban land
        "parcel_service": GH_OC_PARCEL_SERVICE,   # None — not yet configured (see TODO above)
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
        "bbox":           (-86.25, 43.07, -86.07, 43.22),
        "county":         "ottawa",
        "parcel_service": GH_OC_PARCEL_SERVICE,   # None — not yet configured (see TODO above)
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
