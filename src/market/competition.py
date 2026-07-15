"""
Competition mapping — tracks competing residential/BTR development projects in
the market submarkets (config.MARKET_SUBMARKETS: Grand Haven, Grand Haven
Township, Spring Lake Township).

Same review-queue mechanism as econ_dev.py (Google News RSS scan + human
Keep/Skip, "Scan now" button, nothing kept until approved), but a distinct
data domain with a richer schema — specific projects (address, builder, unit
count, construction timeline, parcel #, acres) rather than general news
announcements.

CopperBay (built by Allen Edwin) is WR-Dev's only direct BTR competitor in
Ottawa County and is always included in the keyword set by name, the same
seeding approach used for the named retailers in econ_dev.py's retail category.

Stage taxonomy (STAGES), earliest to latest:
    proposed -> planned (approved) -> under_construction -> lease_up -> existing
Per the analyst's classification rule, a pre-construction lead is "planned"
if its description contains approval language ("approved"/"approval"),
else "proposed" — see classify_stage().

Public API:
    run_scan()          -> (new_count, pending_count, is_catchup)
    load_queue()         -> dict {id: record}
    set_status(id, s)    -> None                          # 'approved'|'rejected'|'pending'
    classify_stage(text) -> 'planned' | 'proposed'
"""
import hashlib
import json
import re
import sys
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import ROOT, MARKET_SUBMARKETS  # noqa: E402

_QUEUE = ROOT / "data" / "competition_queue.json"
_META = ROOT / "data" / "competition_meta.json"

# Scan window logic — identical reasoning to econ_dev.py: first scan pulls a
# year of history, later scans are incremental since the last run.
_CATCHUP_DAYS = 365
_OVERLAP_DAYS = 3

# Stage taxonomy, ordered earliest -> latest in the development pipeline.
STAGES = {
    "proposed": "Proposed",
    "planned": "Planned (approved)",
    "under_construction": "Under construction",
    "lease_up": "Lease-up",
    "existing": "Existing",
}
DEFAULT_STAGE = "proposed"

# Approval language that flips a pre-construction lead from Proposed to
# Planned (approved) — analyst's classification rule.
_APPROVED_WORDS = re.compile(r"\bapprov(ed|al)\b", re.I)


def classify_stage(text: str) -> str:
    """Proposed vs Planned (approved) for a pre-construction lead: approval
    language in the description -> planned, else proposed."""
    return "planned" if _APPROVED_WORDS.search(text or "") else "proposed"


# Google News query keywords — run as SEPARATE single-keyword queries per
# submarket, NOT one big OR-chain. Confirmed via direct testing: a long
# OR-chain of keywords drowns out the location filter and Google News returns
# generic national real-estate-industry news (Richmond, Tolland CT, Palm
# Beach County...) instead of local signal. A single keyword + location stays
# on-target and matched real known projects (South Village, Winsor Place,
# Blueberry Woods) directly in testing.
#
# "CopperBay" (Allen Edwin's brand for WR-Dev's one direct BTR competitor in
# Ottawa County) is deliberately NOT a keyword here — bare "CopperBay" collides
# with an unrelated rum/cocktail brand of the same name and returns nothing
# but drink-industry noise. "Allen Edwin" (the builder) is the reliable
# stand-in; add CopperBay-specific coverage via "Add a link manually" instead.
_KEYWORDS = (
    "apartment", "townhomes", "subdivision", '"site plan"', "rezoning",
    '"planned development"', "multifamily", '"build-to-rent"',
    '"single-family rental"', '"housing development"', '"Allen Edwin"',
)

# Secondary relevance gate on title+snippet — trims obvious off-topic hits.
_SIGNAL = re.compile(
    r"\b(copperbay|allen edwin|apartments?|townhomes?|subdivision|site plan|"
    r"rezon\w*|planned development|multi-?family|build.to.rent|"
    r"single.family rental|housing development|residential development)\b", re.I)

# CopperBay/Allen Edwin get a flag so the UI can surface WR-Dev's one direct
# BTR competitor distinctly from general residential-development noise.
_DIRECT_COMPETITOR = re.compile(r"\b(copperbay|allen edwin)\b", re.I)


def is_direct_competitor(text: str) -> bool:
    return bool(_DIRECT_COMPETITOR.search(text or ""))


def _news_rss(query: str) -> ET.Element:
    url = ("https://news.google.com/rss/search?q="
           + urllib.parse.quote(query) + "&hl=en-US&gl=US&ceid=US:en")
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
    r.raise_for_status()
    return ET.fromstring(r.content)


def _norm_id(title: str) -> str:
    key = re.sub(r"\W+", " ", (title or "").lower()).strip()
    return hashlib.sha1(key.encode()).hexdigest()[:12]


def _parse_date(s: str):
    try:
        return parsedate_to_datetime(s)
    except Exception:                                # noqa: BLE001
        return None


def scan_candidates(cutoff) -> dict:
    """One Google-News query per (submarket x keyword); return {id: record}
    of hits newer than `cutoff`. See _KEYWORDS comment for why this runs
    single-keyword queries rather than one combined OR-chain per submarket."""
    seen = {}
    for sm in MARKET_SUBMARKETS:
        label = sm["label"]                           # e.g. "Grand Haven"
        for kw in _KEYWORDS:
            query = f'{kw} "{label}" Michigan'
            try:
                root = _news_rss(query)
            except Exception:                        # noqa: BLE001
                continue
            for it in root.findall(".//item"):
                title = (it.findtext("title") or "").strip()
                link = (it.findtext("link") or "").strip()
                if not title or not link:
                    continue
                blob = title + " " + (it.findtext("description") or "")
                if not _SIGNAL.search(blob):
                    continue
                iid = _norm_id(title)
                if iid in seen:
                    continue
                src_el = it.find("{*}source")
                source = (src_el.text if src_el is not None else "") or ""
                pub = it.findtext("pubDate") or ""
                dt = _parse_date(pub)
                if dt is not None and dt < cutoff:   # older than the recency window
                    continue
                seen[iid] = {
                    "id": iid, "title": title, "link": link, "source": source,
                    "published": pub, "published_ts": dt.isoformat() if dt else "",
                    "submarket_key": sm["key"], "submarket_label": label,
                    "stage": classify_stage(blob),
                    "is_direct_competitor": is_direct_competitor(blob),
                }
    return seen


def load_queue() -> dict:
    if _QUEUE.exists():
        return json.loads(_QUEUE.read_text())
    return {}


def _save_queue(q: dict):
    _QUEUE.parent.mkdir(parents=True, exist_ok=True)
    # Keep a one-version backup before overwriting, so analyst-entered details
    # can be recovered if a save goes wrong.
    if _QUEUE.exists():
        try:
            _QUEUE.with_suffix(".bak.json").write_text(_QUEUE.read_text())
        except Exception:                            # noqa: BLE001
            pass
    _QUEUE.write_text(json.dumps(q, indent=2))


def _load_meta() -> dict:
    if _META.exists():
        return json.loads(_META.read_text())
    return {}


def _save_meta(m: dict):
    _META.parent.mkdir(parents=True, exist_ok=True)
    _META.write_text(json.dumps(m, indent=2))


_GEOCODE_CACHE = ROOT / "data" / "competition_geocode_cache.json"


def _load_geocode_cache() -> dict:
    if _GEOCODE_CACHE.exists():
        return json.loads(_GEOCODE_CACHE.read_text())
    return {}


def _save_geocode_cache(cache: dict):
    _GEOCODE_CACHE.parent.mkdir(parents=True, exist_ok=True)
    _GEOCODE_CACHE.write_text(json.dumps(cache, indent=2))


def geocode_address(address: str, city: str, state: str = "MI"):
    """
    (lat, lon) for a street address via the free Census Geocoder (no API key —
    distinct from the Census data API's key), or None if it can't be resolved
    (vague/partial addresses like "Between Comstock St & Robbins Rd" often
    won't geocode — callers should fall back to a submarket-center point).
    Cached to disk (including misses, so a bad address isn't re-queried on
    every page load) since these results never change for a fixed address.
    """
    address = (address or "").strip()
    if not address:
        return None
    key = f"{address}, {city}, {state}".lower()
    cache = _load_geocode_cache()
    if key in cache:
        return tuple(cache[key]) if cache[key] else None
    try:
        r = requests.get(
            "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress",
            params={"address": f"{address}, {city}, {state}",
                    "benchmark": "Public_AR_Current", "format": "json"},
            timeout=15)
        r.raise_for_status()
        matches = r.json().get("result", {}).get("addressMatches", [])
        result = None
        if matches:
            c = matches[0]["coordinates"]
            result = (c["y"], c["x"])                 # (lat, lon)
    except Exception:                                # noqa: BLE001
        result = None
    cache[key] = list(result) if result else None
    _save_geocode_cache(cache)
    return result


def last_scan_ts():
    """ISO timestamp of the previous scan, or None if never scanned."""
    return _load_meta().get("last_scan_ts")


def _scan_cutoff():
    """(cutoff_datetime, is_catchup) — incremental since last scan, else catch-up."""
    now = datetime.now(timezone.utc)
    last = last_scan_ts()
    if last:
        try:
            return datetime.fromisoformat(last) - timedelta(days=_OVERLAP_DAYS), False
        except Exception:                            # noqa: BLE001
            pass
    return now - timedelta(days=_CATCHUP_DAYS), True


def run_scan() -> tuple[int, int, bool]:
    """
    Fetch candidates and add NEW ones as pending. First run = catch-up (history);
    later runs = only items since the last scan. Returns (new, pending, is_catchup).
    """
    cutoff, is_catchup = _scan_cutoff()
    q = load_queue()
    new = 0
    for iid, rec in scan_candidates(cutoff).items():
        if iid not in q:                             # never re-surface a decided item
            rec["status"] = "pending"
            q[iid] = rec
            new += 1
    _save_queue(q)
    _save_meta({"last_scan_ts": datetime.now(timezone.utc).isoformat()})
    pending = sum(1 for v in q.values() if v.get("status") == "pending")
    return new, pending, is_catchup


def set_status(iid: str, status: str):
    q = load_queue()
    if iid in q:
        q[iid]["status"] = status
        _save_queue(q)


# Analyst-entered detail fields that feed the Executive summary. `approved_on`
# is the date a rezoning/site-plan/special-land-use item cleared its planning
# commission or board approval — an earlier lifecycle milestone than
# construction_start, and the most reliable signal for the proposed-vs-planned
# split when it's known (vs. guessing from free text via classify_stage()).
#
# effective_rent/occupancy_pct/avg_sqft/year_built are for EXISTING/lease-up
# comps (e.g. from RealPage Explore) — a stabilized or lease-up property has
# performance data instead of a construction timeline.
DETAIL_FIELDS = ("project_name", "address", "stage", "type", "total_units",
                  "approved_on", "construction_start", "construction_end",
                  "builder", "parcel_number", "acres", "notes",
                  "is_direct_competitor", "effective_rent", "occupancy_pct",
                  "avg_sqft", "year_built")


def update_record(iid: str, **fields):
    """Set analyst-entered detail fields (see DETAIL_FIELDS)."""
    q = load_queue()
    if iid not in q:
        return
    for k, v in fields.items():
        if k in DETAIL_FIELDS:
            if k == "stage" and v not in STAGES:
                continue                              # ignore unknown stage keys
            q[iid][k] = v
    _save_queue(q)


def add_manual(url: str, submarket_key: str, submarket_label: str,
               title: str = None, source: str = None,
               stage: str = DEFAULT_STAGE) -> tuple[str, bool]:
    """
    Manually add a project the scanner missed. Lands directly in the kept
    items (status='approved') so it shows in the editable table to fill in.
    Returns (id, added) — added=False if it was already present.
    """
    parsed = urllib.parse.urlparse(url)
    if not title:
        slug = parsed.path.rstrip("/").split("/")[-1]
        title = slug.replace("-", " ").strip().title() or url
    if not source:
        source = parsed.netloc.replace("www.", "")
    iid = _norm_id(title + url)
    q = load_queue()
    if iid in q:
        return iid, False
    q[iid] = {"id": iid, "title": title, "link": url, "source": source,
              "published": "", "published_ts": "",
              "submarket_key": submarket_key, "submarket_label": submarket_label,
              "stage": stage if stage in STAGES else DEFAULT_STAGE,
              "is_direct_competitor": False,
              "status": "approved", "manual": True}
    _save_queue(q)
    return iid, True


def add_existing_property(project_name: str, submarket_key: str, submarket_label: str,
                          address: str = "", stage: str = "existing",
                          **detail_fields) -> tuple[str, bool]:
    """
    Add an existing/lease-up comp (e.g. from RealPage Explore) directly — no
    source URL, since these are property records, not news articles. Lands
    straight in the kept items (status='approved') so it shows in the
    editable table. detail_fields: any of DETAIL_FIELDS (effective_rent,
    occupancy_pct, avg_sqft, year_built, total_units, type, notes, ...).
    Returns (id, added) — added=False if it was already present.
    """
    iid = _norm_id(f"property|{project_name}|{address}")
    q = load_queue()
    if iid in q:
        return iid, False
    rec = {"id": iid, "title": project_name, "link": "", "source": "RealPage Explore",
           "published": "", "published_ts": "",
           "submarket_key": submarket_key, "submarket_label": submarket_label,
           "stage": stage if stage in STAGES else DEFAULT_STAGE,
           "is_direct_competitor": False, "status": "approved", "manual": True,
           "project_name": project_name, "address": address}
    for k, v in detail_fields.items():
        if k in DETAIL_FIELDS:
            rec[k] = v
    q[iid] = rec
    _save_queue(q)
    return iid, True


def summary_by_submarket() -> dict:
    """Aggregate APPROVED items per submarket: project count, total units,
    direct-competitor (CopperBay/Allen Edwin) project count, and a breakdown
    by stage."""
    out = {}
    for v in load_queue().values():
        if v.get("status") != "approved":
            continue
        s = out.setdefault(v["submarket_key"], {
            "projects": 0, "total_units": 0, "direct_competitor_projects": 0,
            "by_stage": {k: 0 for k in STAGES},
        })
        s["projects"] += 1
        units = v.get("total_units")
        if isinstance(units, (int, float)) and units == units:
            s["total_units"] += int(units)
        if v.get("is_direct_competitor"):
            s["direct_competitor_projects"] += 1
        stage = v.get("stage", DEFAULT_STAGE)
        if stage in s["by_stage"]:
            s["by_stage"][stage] += 1
    return out


if __name__ == "__main__":
    new, pending, catchup = run_scan()
    print(f"{new} new; {pending} pending (catch-up={catchup})")
    q = load_queue()
    print(f"{pending} pending after scan (total tracked: {len(q)})")
    for v in sorted(q.values(), key=lambda r: r.get("published_ts", ""), reverse=True)[:12]:
        flag = "★" if v.get("is_direct_competitor") else " "
        print(f"  [{v['status']:<8}]{flag} {v['submarket_label']:<16} "
              f"{STAGES.get(v.get('stage', DEFAULT_STAGE)):<20} {v['title'][:60]}")
