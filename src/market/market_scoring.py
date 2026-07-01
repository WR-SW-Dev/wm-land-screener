"""
0–100 housing-need / BTR-demand score for Section 1 market feasibility.

Each signal is normalized to 0–1 over a documented band (config.DEMAND_BANDS),
then combined with config.DEMAND_WEIGHTS. The per-component contributions are
returned alongside the total so the Analyst view can show the breakdown and the
Executive heat map can shade polygons by `demand_score`.

Public API:
    add_demand_score(df) -> df   # adds demand_score + pts_<component> columns
    score_components(row) -> dict
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DEMAND_WEIGHTS, DEMAND_BANDS  # noqa: E402


def _norm(value, band) -> float:
    """Linear-normalize `value` over (lo→0, hi→1), clamped to [0,1]. None→0."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    lo, hi = band
    if hi == lo:
        return 0.0
    frac = (value - lo) / (hi - lo)
    return max(0.0, min(1.0, frac))


# Map each weight component → (metric column, normalization band).
_COMPONENTS = {
    "tightness":     ("rental_vacancy_rate", DEMAND_BANDS["rental_vacancy_rate"]),
    "cost_burden":   ("cost_burden_pct",     DEMAND_BANDS["cost_burden_pct"]),
    "growth":        ("pop_growth_pct",      DEMAND_BANDS["pop_growth_pct"]),
    "renter_share":  ("renter_share_pct",    DEMAND_BANDS["renter_share_pct"]),
    "rent_pressure": ("rent_to_afford",      DEMAND_BANDS["rent_to_afford"]),
}


def score_components(row) -> dict:
    """Return {component: points} for one metrics row. Points sum to demand_score."""
    out = {}
    for comp, (col, band) in _COMPONENTS.items():
        frac = _norm(row.get(col) if isinstance(row, dict) else row[col], band)
        out[comp] = round(frac * DEMAND_WEIGHTS[comp], 1)
    return out


def add_demand_score(df: pd.DataFrame) -> pd.DataFrame:
    """Add `pts_<component>` columns and a `demand_score` (0–100) to the frame."""
    df = df.copy()
    comp_cols = []
    for comp in _COMPONENTS:
        col = f"pts_{comp}"
        df[col] = df.apply(lambda r, c=comp: score_components(r)[c], axis=1)
        comp_cols.append(col)
    df["demand_score"] = df[comp_cols].sum(axis=1).round(1)
    return df


if __name__ == "__main__":
    from demographics import load_market_metrics
    frame = add_demand_score(load_market_metrics())
    cols = (["label", "tier", "demand_score"] +
            [f"pts_{c}" for c in DEMAND_WEIGHTS])
    pd.set_option("display.width", 200)
    print(frame[cols].to_string(index=False))
