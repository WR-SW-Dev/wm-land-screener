"""
Transportation accessibility: nearest-neighbor distance to the nearest
qualifying thoroughfare and to the nearest Interstate-or-Freeway/Expressway
facility (straight-line distance — a deliberate design choice, not a
placeholder for future drive-time routing; see plan notes).

This is a minor, supportive "nice to know" signal, not a scored filter or a
cross-parcel comparison tool. The weighted score below is computed purely to
classify each parcel into a plain-language rating (Excellent/Very Good/Good/
Fair/Limited) — the numeric 0-100 is kept in the output for QA but is never
shown in the UI.
"""
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd

from overlay import _to_area_crs
from config import (
    MDOT_NFC_LABELS, QUALIFYING_THOROUGHFARE_NFC, COLLECTOR_FALLBACK_NFC,
    THOROUGHFARE_FALLBACK_RADIUS_MI, REGIONAL_HWY_NFC,
    TRANSPORT_SCORE_WEIGHTS, TRANSPORT_SCORE_BANDS, MDOT_FUNCTIONAL_CLASS_POINTS,
    TRANSPORT_RATING_BANDS,
)

METERS_PER_MILE = 1609.344


def _epoch_ms_to_year(val):
    """AADTYear comes back as epoch-ms and is frequently null even when AADT
    is populated (confirmed live) — return None gracefully, don't error."""
    if val is None or pd.isna(val):
        return None
    try:
        return datetime.fromtimestamp(int(val) / 1000, tz=timezone.utc).year
    except (ValueError, OverflowError, OSError):
        return None


def _nearest_join(parcels_proj: gpd.GeoDataFrame, candidates_proj: gpd.GeoDataFrame,
                   cand_cols: list) -> pd.DataFrame:
    """gpd.sjoin_nearest wrapper — one nearest match per parcel (by _pidx),
    with distance converted to miles."""
    if candidates_proj.empty:
        return pd.DataFrame(columns=["_dist_mi"] + cand_cols)
    joined = gpd.sjoin_nearest(
        parcels_proj[["_pidx", "geometry"]],
        candidates_proj[cand_cols + ["geometry"]],
        how="left", distance_col="_dist_m",
    )
    joined = joined.sort_values("_dist_m").drop_duplicates("_pidx", keep="first")
    joined["_dist_mi"] = joined["_dist_m"] / METERS_PER_MILE
    return joined.set_index("_pidx")


def add_nearest_thoroughfare(parcels: gpd.GeoDataFrame, roads: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Nearest qualifying thoroughfare (NFC 1-4: Interstate / Freeway-Expressway /
    Principal Arterial / Minor Arterial). Falls back to NFC 1-6 (collectors
    included) only when nothing in the arterial+ tier is within
    THOROUGHFARE_FALLBACK_RADIUS_MI (10 mi).

    Adds: thoroughfare_name, thoroughfare_route, thoroughfare_class,
    thoroughfare_nfc, thoroughfare_dist_mi, thoroughfare_fallback_tier,
    thoroughfare_aadt, thoroughfare_aadt_year.

    Runs on the full parcel set, independent of land-score pass/fail.
    """
    n = len(parcels)
    names, routes, classes, nfcs = [""] * n, [""] * n, [""] * n, [0] * n
    dists, tiers, aadts, aadt_years = [None] * n, [""] * n, [None] * n, [None] * n

    if roads is not None and not roads.empty and "nfc" in roads.columns:
        cand_cols = [c for c in ["fename", "rt1", "nfc", "aadt", "aadtyear"] if c in roads.columns]
        roads_use = roads.to_crs(parcels.crs) if roads.crs != parcels.crs else roads

        p_proj = _to_area_crs(parcels[["geometry"]].copy()).reset_index(drop=True)
        p_proj["_pidx"] = p_proj.index

        tier1_proj = _to_area_crs(
            roads_use.loc[roads_use["nfc"].isin(QUALIFYING_THOROUGHFARE_NFC), cand_cols + ["geometry"]]
        )
        tier2_proj = _to_area_crs(
            roads_use.loc[roads_use["nfc"].isin(COLLECTOR_FALLBACK_NFC), cand_cols + ["geometry"]]
        )
        t1 = _nearest_join(p_proj, tier1_proj, cand_cols)
        t2 = _nearest_join(p_proj, tier2_proj, cand_cols)

        for pidx in range(n):
            if pidx in t1.index and t1.loc[pidx, "_dist_mi"] <= THOROUGHFARE_FALLBACK_RADIUS_MI:
                row, tier = t1.loc[pidx], "arterial+"
            elif pidx in t2.index:
                row, tier = t2.loc[pidx], "collector_fallback"
            else:
                continue

            nfc = int(row.get("nfc", 0) or 0)
            names[pidx]   = str(row.get("fename") or "")
            routes[pidx]  = str(row.get("rt1") or "").lstrip("0")
            classes[pidx] = MDOT_NFC_LABELS.get(nfc, "Unclassified")
            nfcs[pidx]    = nfc
            dists[pidx]   = round(float(row["_dist_mi"]), 2)
            tiers[pidx]   = tier
            aadt = row.get("aadt")
            aadts[pidx]      = int(aadt) if pd.notna(aadt) else None
            aadt_years[pidx] = _epoch_ms_to_year(row.get("aadtyear"))

    result = parcels.copy()
    result["thoroughfare_name"] = names
    result["thoroughfare_route"] = routes
    result["thoroughfare_class"] = classes
    result["thoroughfare_nfc"] = nfcs
    result["thoroughfare_dist_mi"] = dists
    result["thoroughfare_fallback_tier"] = tiers
    result["thoroughfare_aadt"] = aadts
    result["thoroughfare_aadt_year"] = aadt_years
    return result


def add_nearest_regional_highway(parcels: gpd.GeoDataFrame, roads: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Distance to the nearest Interstate-or-Freeway/Expressway facility (NFC 1
    or 2), found by functional classification — not a hardcoded list of named
    routes. Revised from an I-96/I-196-only design after checking real Grand
    Haven data: US-31 (NFC 2) is often under 0.1mi away while I-96 (NFC 1) is
    5-7mi away, so "nearest true Interstate" was answering a less useful
    question than "nearest freeway-grade regional corridor." This also means
    no per-city named-route list — whichever freeway/Interstate is actually
    closest wins, which works the same way once Holland/Muskegon are added.

    Adds: regional_hwy_name (raw road name, e.g. "S US 31"), regional_hwy_class
    ("Interstate"/"Freeway/Expressway"), regional_hwy_dist_mi.
    """
    n = len(parcels)
    names, classes, dists = [""] * n, [""] * n, [None] * n

    if roads is not None and not roads.empty and "nfc" in roads.columns:
        cand_cols = [c for c in ["fename", "nfc"] if c in roads.columns]
        roads_use = roads.to_crs(parcels.crs) if roads.crs != parcels.crs else roads
        candidates = roads_use.loc[roads_use["nfc"].isin(REGIONAL_HWY_NFC), cand_cols + ["geometry"]]

        if not candidates.empty:
            candidates_proj = _to_area_crs(candidates)
            p_proj = _to_area_crs(parcels[["geometry"]].copy()).reset_index(drop=True)
            p_proj["_pidx"] = p_proj.index

            nearest = _nearest_join(p_proj, candidates_proj, cand_cols)
            for pidx in range(n):
                if pidx not in nearest.index:
                    continue
                row = nearest.loc[pidx]
                nfc = int(row.get("nfc", 0) or 0)
                names[pidx]   = str(row.get("fename") or "")
                classes[pidx] = MDOT_NFC_LABELS.get(nfc, "Unclassified")
                dists[pidx]   = round(float(row["_dist_mi"]), 2)

    result = parcels.copy()
    result["regional_hwy_name"] = names
    result["regional_hwy_class"] = classes
    result["regional_hwy_dist_mi"] = dists
    return result


def _norm(value, band) -> float:
    """Linear-normalize a value into [0,1] given a (value_at_0, value_at_1)
    band — same convention as market/market_scoring.py's DEMAND_BANDS."""
    if pd.isna(value):
        return 0.0
    lo, hi = band
    if hi == lo:
        return 0.0
    return max(0.0, min(1.0, (value - lo) / (hi - lo)))


def score_components(row) -> dict:
    """Returns the 4 weighted point components; sums to transportation_access_score."""
    w = TRANSPORT_SCORE_WEIGHTS
    thoroughfare_frac = _norm(row.get("thoroughfare_dist_mi"),  TRANSPORT_SCORE_BANDS["thoroughfare_dist_mi"])
    aadt_frac         = _norm(row.get("thoroughfare_aadt"),     TRANSPORT_SCORE_BANDS["aadt"])
    regional_hwy_frac = _norm(row.get("regional_hwy_dist_mi"),  TRANSPORT_SCORE_BANDS["regional_hwy_dist_mi"])
    nfc = int(row.get("thoroughfare_nfc", 0) or 0)
    class_pts = MDOT_FUNCTIONAL_CLASS_POINTS.get(nfc, 0)

    return {
        "pts_thoroughfare_proximity": round(thoroughfare_frac * w["thoroughfare_proximity"], 1),
        "pts_aadt":                   round(aadt_frac * w["aadt"], 1),
        "pts_functional_class":       round(min(class_pts, w["functional_class"]), 1),
        "pts_regional_hwy_proximity": round(regional_hwy_frac * w["regional_hwy_proximity"], 1),
    }


def _rating(score: float) -> str:
    for threshold, label in TRANSPORT_RATING_BANDS:
        if score >= threshold:
            return label
    return "Limited"


def add_transportation_score(parcels: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Adds the 4 pts_* components, transportation_access_score (0-100, kept for
    QA but not shown in the UI), and transportation_rating (the only
    score-derived field the UI displays, as a plain-language badge).
    Fully independent of the land `score` column.
    """
    p = parcels.copy()
    breakdown = p.apply(score_components, axis=1, result_type="expand")
    for col in breakdown.columns:
        p[col] = breakdown[col]
    p["transportation_access_score"] = breakdown.sum(axis=1).round(1)
    p["transportation_rating"] = p["transportation_access_score"].apply(_rating)
    return p
