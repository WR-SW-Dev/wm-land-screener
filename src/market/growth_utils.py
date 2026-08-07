"""
Shared growth-metric helpers: collapse monthly observations into annual
averages, then compute year-over-year %. Used by any monthly series feeding
the paired metric-tiles + YoY-growth-chart pattern (ZORI rent, ZHVI home
value, FRED employment) — extracted here once a third consumer needed the
exact same two functions that used to live only in zillow.py.
"""
from __future__ import annotations

from collections import defaultdict


def annualize_observations(obs: list) -> list:
    """Collapse monthly {date, value} observations into annual averages.
    Returns [{"date": "YYYY-01-01", "value": avg, "months": n}, ...]. A
    partial current year still becomes its own (lower-confidence) point
    rather than being dropped — same spirit as the permits chart's YTD bar.
    `months` records how many months went into that average, so a partial
    final year can be flagged downstream instead of silently compared to
    full prior years as if it were one."""
    by_year = defaultdict(list)
    for o in obs:
        if o.get("value") is not None:
            by_year[o["date"][:4]].append(o["value"])
    return [{"date": f"{y}-01-01", "value": sum(vals) / len(vals), "months": len(vals)}
            for y, vals in sorted(by_year.items())]


def yoy_frame(obs: list) -> list:
    """Year-over-year growth, one row per year: {year, pct, provisional}.
    Deliberately the same shape as the permits chart's growth_rows in
    render.py, so render._render_yoy_growth_chart() can render any of these
    series without knowing which one it is. `provisional` marks a final
    year built from fewer than 12 months, matching the permits chart's YTD
    bar treatment, rather than presenting it as a full-year figure."""
    rows = []
    for i in range(1, len(obs)):
        y0, y1 = obs[i - 1], obs[i]
        if not y0["value"]:
            continue
        pct = (y1["value"] / y0["value"] - 1) * 100
        provisional = y1.get("months", 12) < 12
        rows.append({"year": y1["date"][:4], "pct": pct, "provisional": provisional})
    return rows
