"""
County housing-gap ("units needed") data for the market-feasibility heat map.

Figures are transcribed from the county Housing Needs Assessments covering the
six market counties — all produced by Bowen National Research using the same
5-year housing-gap methodology, so they're directly comparable (with a minor
caveat that Ottawa/Kent are 2024–2029 while Allegan/Muskegon/Grand Traverse/
Antrim are 2022–2027). Grand Traverse and Antrim differ in one structural way:
they have no county-commissioned study of their own, and instead come from the
per-county addenda of Bowen's 2023 10-county Northern Michigan regional HNA.

Each county's total gap = rental + for-sale units needed over the 5-year period.
`rental_by_income` breaks the rental gap into the report's own AMI/rent bands
(Ottawa, Allegan, Grand Traverse & Antrim use 4 bands; Kent & Muskegon use 5).
`forsale_by_income`
breaks the for-sale gap into the SAME AMI bands (paired with price points
instead of rents) — each county's report bands both tenures identically, so
rental_by_income[i]["units"] + forsale_by_income[i]["units"] is a legitimate
per-band "total units needed", not an approximation. Kent is reported as two
areas (Grand Rapids + Balance-of-County); the values here are the county total
(the sum of both), which matches the report's own summed figures — confirmed
for for-sale too: Grand Rapids ($106,400/$177,333/$283,733/$425,600 price
breaks) and Balance-of-County use identical price bands, so summing is exact,
not a mismatched-bucket approximation.

`population_growth`/`household_growth` (added 2026-08-06) are 4-point series
from each report's own "Total Population"/"Total Households" tables: 2010 and
2020 Census actuals, a current-year estimate, and an end-of-study-period
projection — all from the same report, source "2010, 2020 Census; ESRI;
[Urban Decision Group;] Bowen National Research" (transcribed from the exact
tables, page-cited in each county's comment below). Deliberately NOT a smooth
annual series — there's a 10-year gap between the first two real points, so a
line chart across all 4 would misrepresent it as continuous. The last point
is a projection, not a measured value; rendered accordingly (see render.py).

Public API:
    HOUSING_NEEDS                       # raw dict keyed by county key
    load_housing_needs(acs_df) -> df    # merges ACS households → intensity
"""
import pandas as pd

# key → county gap record. `units` are 5-year "units needed" from each HNA.
HOUSING_NEEDS = {
    "ottawa": {
        "label": "Ottawa County",
        "study_period": "2024–2029",
        "report": "Bowen National Research HNA, 2025 (commissioned by Housing Next)",
        "total_units": 16_464,
        "rental_units": 3_938,
        "forsale_units": 12_526,
        "rental_by_income": [
            {"ami": "≤50%",    "rent": "≤ $1,285",       "units": 2_289},
            {"ami": "51–80%",  "rent": "$1,286–$2,055",  "units": 997},
            {"ami": "81–120%", "rent": "$2,056–$3,084",  "units": 400},
            {"ami": "121%+",   "rent": "$3,085+",        "units": 252},
        ],
        "forsale_by_income": [
            {"ami": "≤50%",    "price": "≤ $171,333",         "units": 259},
            {"ami": "51–80%",  "price": "$171,334–$274,133",  "units": 2_794},
            {"ami": "81–120%", "price": "$274,134–$411,200",  "units": 6_973},
            {"ami": "121%+",   "price": "$411,201+",          "units": 2_500},
        ],
        # Total Population p.IV-2 / Total Households p.IV-15, Ottawa County
        # 2025 HNA. Source: "2010, 2020 Census; ESRI; Bowen National Research."
        "population_growth": [
            {"year": "2010", "value": 263_801},
            {"year": "2020", "value": 296_200},
            {"year": "2024", "value": 306_943},
            {"year": "2029", "value": 315_675, "projected": True},
        ],
        "household_growth": [
            {"year": "2010", "value": 93_775},
            {"year": "2020", "value": 107_239},
            {"year": "2024", "value": 112_718},
            {"year": "2029", "value": 117_957, "projected": True},
        ],
    },
    "kent": {
        "label": "Kent County",
        "study_period": "2024–2029",
        "report": "Bowen National Research HNA, 2025 (Grand Rapids + Balance of County, summed)",
        "total_units": 33_914,
        "rental_units": 11_775,
        "forsale_units": 22_139,
        "rental_by_income": [
            {"ami": "≤30%",    "rent": "≤ $798",         "units": 1_992},
            {"ami": "31–50%",  "rent": "$799–$1,330",    "units": 1_875},
            {"ami": "51–80%",  "rent": "$1,331–$2,128",  "units": 3_248},
            {"ami": "81–120%", "rent": "$2,129–$3,192",  "units": 2_691},
            {"ami": "121%+",   "rent": "$3,193+",        "units": 1_969},
        ],
        # Grand Rapids (6,333) + Balance of Kent County (15,806) = 22,139,
        # summed per band — both areas use identical price bands (2025 HUD
        # limits for the Grand Rapids-Wyoming MSA), so this is exact.
        "forsale_by_income": [
            {"ami": "≤30%",    "price": "≤ $106,400",         "units": 0},
            {"ami": "31–50%",  "price": "$106,401–$177,333",  "units": 1_425},
            {"ami": "51–80%",  "price": "$177,334–$283,733",  "units": 6_328},
            {"ami": "81–120%", "price": "$283,734–$425,600",  "units": 7_506},
            {"ami": "121%+",   "price": "$425,601+",          "units": 6_880},
        ],
        # Total Population p.IV-2 / Total Households p.IV-14, Kent County HNA.
        # Source: "2010, 2020 Census; ESRI; Bowen National Research." County
        # totals are Grand Rapids (PSA) + Balance of County (SSA) summed —
        # the report gives the county-total row directly, not something
        # derived here.
        "population_growth": [
            {"year": "2010", "value": 602_622},
            {"year": "2020", "value": 657_974},
            {"year": "2024", "value": 669_956},
            {"year": "2029", "value": 677_526, "projected": True},
        ],
        "household_growth": [
            {"year": "2010", "value": 227_239},
            {"year": "2020", "value": 251_658},
            {"year": "2024", "value": 258_621},
            {"year": "2029", "value": 264_992, "projected": True},
        ],
    },
    "muskegon": {
        "label": "Muskegon County",
        "study_period": "2022–2027",
        "report": "Bowen National Research HNA (22-501), 2022",
        "total_units": 9_184,
        "rental_units": 3_043,
        "forsale_units": 6_141,
        "rental_by_income": [
            {"ami": "≤30%",    "rent": "≤ $567",         "units": 724},
            {"ami": "31–50%",  "rent": "$568–$946",      "units": 698},
            {"ami": "51–80%",  "rent": "$947–$1,513",    "units": 677},
            {"ami": "81–120%", "rent": "$1,514–$2,271",  "units": 460},
            {"ami": "121%+",   "rent": "$2,272+",        "units": 484},
        ],
        "forsale_by_income": [
            {"ami": "≤30%",    "price": "≤ $75,700",         "units": 519},
            {"ami": "31–50%",  "price": "$75,701–$126,167",  "units": 239},
            {"ami": "51–80%",  "price": "$126,168–$201,867", "units": 624},
            {"ami": "81–120%", "price": "$201,868–$302,800", "units": 2_420},
            {"ami": "121%+",   "price": "$302,801+",         "units": 2_339},
        ],
        # Total Population p.IV-2 / Total Households p.IV-14, Muskegon County
        # HNA. Source: "2010, 2020 Census; ESRI; Urban Decision Group; Bowen
        # National Research." Only county in this tool where the projection
        # is a DECLINE, not growth — population -222 (-0.1%) and households
        # only +217 (+0.3%) over 2022-2027, "identical to the projected
        # statewide rate" per the report's own text.
        "population_growth": [
            {"year": "2010", "value": 172_188},
            {"year": "2020", "value": 175_824},
            {"year": "2022", "value": 175_859},
            {"year": "2027", "value": 175_637, "projected": True},
        ],
        "household_growth": [
            {"year": "2010", "value": 65_616},
            {"year": "2020", "value": 68_610},
            {"year": "2022", "value": 68_822},
            {"year": "2027", "value": 69_039, "projected": True},
        ],
    },
    "allegan": {
        "label": "Allegan County",
        "study_period": "2022–2027",
        "report": "Bowen National Research HNA, 2023",
        "total_units": 6_214,
        "rental_units": 1_885,
        "forsale_units": 4_329,
        "rental_by_income": [
            {"ami": "≤50%",    "rent": "≤ $1,096",       "units": 989},
            {"ami": "51–80%",  "rent": "$1,097–$1,754",  "units": 355},
            {"ami": "81–120%", "rent": "$1,755–$2,847",  "units": 395},
            {"ami": "121%+",   "rent": "$2,848+",        "units": 146},
        ],
        "forsale_by_income": [
            {"ami": "≤50%",    "price": "≤ $146,166",         "units": 219},
            {"ami": "51–80%",  "price": "$146,167–$233,866",  "units": 709},
            {"ami": "81–120%", "price": "$233,867–$379,600",  "units": 1_657},
            {"ami": "121%+",   "price": "$379,601+",          "units": 1_744},
        ],
        # Total Population p.IV-2 / Total Households p.IV-14, Allegan County
        # HNA. Source: "2010, 2020 Census; ESRI; Urban Decision Group; Bowen
        # National Research."
        "population_growth": [
            {"year": "2010", "value": 111_408},
            {"year": "2020", "value": 120_502},
            {"year": "2022", "value": 121_956},
            {"year": "2027", "value": 123_322, "projected": True},
        ],
        "household_growth": [
            {"year": "2010", "value": 42_018},
            {"year": "2020", "value": 45_545},
            {"year": "2022", "value": 46_126},
            {"year": "2027", "value": 46_691, "projected": True},
        ],
    },
    # ── Northern Michigan (added 2026-08-07) ─────────────────────────────────
    # Both counties come from ONE report — Bowen's 2023 10-county Northern
    # Michigan regional HNA — rather than a county-commissioned study of their
    # own, so their figures live in that report's per-county addenda (Grand
    # Traverse = Addendum G, Antrim = Addendum C) instead of a standalone PDF.
    # Same 5-year gap methodology and the same 4 AMI bands as Ottawa/Allegan,
    # so they're directly comparable; study period matches Muskegon/Allegan.
    "grand_traverse": {
        "label": "Grand Traverse County",
        "study_period": "2022–2027",
        "report": "Bowen National Research, Northern Michigan HNA, 2023 (Addendum G)",
        "total_units": 11_361,
        "rental_units": 3_569,
        "forsale_units": 7_792,
        "rental_by_income": [
            {"ami": "≤50%",    "rent": "≤ $1,123",       "units": 2_358},
            {"ami": "51–80%",  "rent": "$1,124–$1,797",  "units": 733},
            {"ami": "81–120%", "rent": "$1,798–$2,697",  "units": 288},
            {"ami": "121%+",   "rent": "$2,698+",        "units": 190},
        ],
        "forsale_by_income": [
            {"ami": "≤50%",    "price": "≤ $149,833",         "units": 1_798},
            {"ami": "51–80%",  "price": "$149,834–$239,733",  "units": 1_384},
            {"ami": "81–120%", "price": "$239,734–$359,600",  "units": 2_569},
            {"ami": "121%+",   "price": "$359,601+",          "units": 2_041},
        ],
        # Total Population p.G-2 / Total Households p.G-3 (Addendum G).
        # Source: "2010, 2020 Census; ESRI; Urban Decision Group; Bowen
        # National Research." Strongest growth of the six counties.
        "population_growth": [
            {"year": "2010", "value": 86_986},
            {"year": "2020", "value": 95_238},
            {"year": "2022", "value": 96_832},
            {"year": "2027", "value": 98_662, "projected": True},
        ],
        "household_growth": [
            {"year": "2010", "value": 35_328},
            {"year": "2020", "value": 39_819},
            {"year": "2022", "value": 40_604},
            {"year": "2027", "value": 41_553, "projected": True},
        ],
    },
    "antrim": {
        "label": "Antrim County",
        "study_period": "2022–2027",
        "report": "Bowen National Research, Northern Michigan HNA, 2023 (Addendum C)",
        "total_units": 1_771,
        "rental_units": 321,
        "forsale_units": 1_450,
        "rental_by_income": [
            {"ami": "≤50%",    "rent": "≤ $981",         "units": 114},
            {"ami": "51–80%",  "rent": "$982–$1,569",    "units": 114},
            {"ami": "81–120%", "rent": "$1,570–$2,355",  "units": 66},
            {"ami": "121%+",   "rent": "$2,356+",        "units": 27},
        ],
        "forsale_by_income": [
            {"ami": "≤50%",    "price": "≤ $130,833",         "units": 265},
            {"ami": "51–80%",  "price": "$130,834–$209,333",  "units": 239},
            {"ami": "81–120%", "price": "$209,334–$314,000",  "units": 504},
            {"ami": "121%+",   "price": "$314,001+",          "units": 442},
        ],
        # Total Population p.C-2 / Total Households p.C-3 (Addendum C).
        # Source: "2010, 2020 Census; ESRI; Urban Decision Group; Bowen
        # National Research." Second declining-population county in the tool
        # (with Muskegon) — and the only one declining in BOTH the measured
        # 2010→2020 window and the projection, where Muskegon's decline is
        # projection-only. Households still tick up (+20).
        "population_growth": [
            {"year": "2010", "value": 23_580},
            {"year": "2020", "value": 23_431},
            {"year": "2022", "value": 23_171},
            {"year": "2027", "value": 23_077, "projected": True},
        ],
        "household_growth": [
            {"year": "2010", "value": 9_890},
            {"year": "2020", "value": 10_147},
            {"year": "2022", "value": 10_073},
            {"year": "2027", "value": 10_093, "projected": True},
        ],
    },
}


def load_housing_needs(acs_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Return one row per county with total/rental/for-sale units needed. When an
    ACS metrics frame is supplied, merge each county's household count and add
    intensity columns (units needed per 1,000 existing households) so acute need
    in a small county isn't hidden by raw-count comparisons.
    """
    rows = []
    hh_by_key = {}
    if acs_df is not None and not acs_df.empty:
        counties = acs_df[acs_df["tier"] == "county"]
        hh_by_key = counties.set_index("key")["households"].to_dict()

    for key, rec in HOUSING_NEEDS.items():
        hh = hh_by_key.get(key)
        row = {
            "key":            key,
            "label":          rec["label"],
            "study_period":   rec["study_period"],
            "report":         rec["report"],
            "total_units":    rec["total_units"],
            "rental_units":   rec["rental_units"],
            "forsale_units":  rec["forsale_units"],
            "households":     hh,
            "intensity_total":  (rec["total_units"]  / hh * 1000) if hh else None,
            "intensity_rental": (rec["rental_units"] / hh * 1000) if hh else None,
        }
        rows.append(row)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from market.demographics import load_market_metrics
    df = load_housing_needs(load_market_metrics())
    pd.set_option("display.width", 200)
    print(df[["label", "study_period", "total_units", "rental_units",
              "forsale_units", "households", "intensity_total",
              "intensity_rental"]].round(1).to_string(index=False))
