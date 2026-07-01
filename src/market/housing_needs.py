"""
County housing-gap ("units needed") data for the market-feasibility heat map.

Figures are transcribed from the four West Michigan county Housing Needs
Assessments — all produced by Bowen National Research using the same 5-year
housing-gap methodology, so they're directly comparable (with a minor caveat
that Ottawa/Kent are 2024–2029 and Allegan/Muskegon are 2022–2027).

Each county's total gap = rental + for-sale units needed over the 5-year period.
`rental_by_income` breaks the rental gap into the report's own AMI/rent bands
(Ottawa & Allegan use 4 bands; Kent & Muskegon use 5). Kent is reported as two
areas (Grand Rapids + Balance-of-County); the values here are the county total
(the sum of both), which matches the report's own summed figures.

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
