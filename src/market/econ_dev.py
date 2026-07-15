"""
Economic-development / market-signal news scanner (Phase A).

On-demand ("Scan now") scan for expansion / new-jobs / investment / new-retail
announcements in the market counties (config.MARKET_COUNTIES — locked to
Ottawa/Kent/Muskegon/Allegan today, auto-expands with that list). Uses Google
News RSS: free, no API key, no third-party service, nothing leaves your
systems. Google News already indexes MiBiz, MLive, Crain's, Bridge Michigan,
and the EDO press releases.

Each item is tagged with a `category` (see CATEGORIES) so the review UI and
Executive summary can distinguish employer expansions from new-retail signals;
more categories (water/sewer, parks, community) land the same way later.

Findings land in a review queue (status="pending"); the analyst approves/rejects
in-app. Nothing is "kept" until approved — matching the full-review-queue choice.

Public API:
    run_scan()          -> (new_count, pending_count, is_catchup)
    load_queue()        -> dict {id: record}
    set_status(id, s)   -> None                          # 'approved'|'rejected'|'pending'
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
from config import ROOT, MARKET_COUNTIES  # noqa: E402

_QUEUE = ROOT / "data" / "econ_dev_queue.json"
_META  = ROOT / "data" / "econ_dev_meta.json"

# Scan window logic:
#   • First scan (no prior scan recorded) → CATCH-UP: pull as much history as
#     Google News exposes (capped by the 365-day filter; RSS itself favors recent
#     + notable, so this grabs the available backlog, not a guaranteed archive).
#   • Later scans → INCREMENTAL: only items published since the last scan, minus
#     a small overlap so late-indexed articles aren't missed. The queue's per-item
#     dedup then ensures nothing already seen is re-added.
_CATCHUP_DAYS = 365
_OVERLAP_DAYS = 3

# Google News query keywords (broad net; the signal filter + human review refine).
_EMPLOYER_KEYWORDS = ('"new jobs" OR expansion OR investment OR headquarters OR '
             'groundbreaking OR hiring OR manufacturing OR "new facility" OR '
             '"economic development"')

# Secondary relevance gate on title+snippet — trims obvious off-topic hits.
_EMPLOYER_SIGNAL = re.compile(
    r"\b(jobs?|expansion|expand|invest|investment|headquarters|hq|groundbreaking|"
    r"hir(e|ing)|manufactur\w*|facilit\w+|plant|campus|million|billion|"
    r"develop\w*|relocat\w+|break ground|new site|square feet|sq\.? ?ft|workforce|"
    r"employ\w*|abatement|MEDC)\b", re.I)

# New large-retailer / anchor-store signals — a separate category from employer
# expansions, but same scan mechanism (one Google News query per county).
_RETAIL_KEYWORDS = ('Costco OR Target OR Meijer OR "Trader Joe\'s" OR "Whole Foods" OR '
             'Aldi OR "grand opening" OR "new store" OR "breaks ground" OR '
             '"retail development"')
_RETAIL_SIGNAL = re.compile(
    r"\b(costco|target|meijer|trader joe|whole foods|aldi|grand opening|"
    r"new store|breaks? ground|storefront|retailer|shopping (center|plaza)|"
    r"square feet|sq\.? ?ft)\b", re.I)

# Water/sewer infrastructure expansion. Municipal SRF/EGLE project lists are
# PDF-only annual documents with no clean structured export (confirmed via
# research) — cheaper to catch these via the news coverage that award/project
# announcements already generate than to build a PDF scraper. Includes the
# funding-program names (often named explicitly in coverage) and the
# engineering firms behind the township utility PDFs already in this repo.
_WATER_SEWER_KEYWORDS = ('"water main" OR sewer OR wastewater OR "lift station" OR '
             '"water system" OR "State Revolving Fund" OR "EGLE grant" OR '
             '"Prein & Newhof" OR "Fleis & VandenBrink"')
_WATER_SEWER_SIGNAL = re.compile(
    r"\b(water main|sewer|wastewater|lift station|force main|pump station|"
    r"water (system|tower|treatment)|infrastructure|utilit(y|ies)|"
    r"state revolving fund|\bsrf\b|egle|prein ?(&|and) ?newhof|vandenbrink)\b", re.I)

# Parks & recreation improvements. Same reasoning as water/sewer — DNR Trust
# Fund/LWCF award lists are annual press-release style pages, not structured
# data; news coverage of the same awards is the cheaper signal.
_PARKS_KEYWORDS = ('"DNR Trust Fund" OR "Land and Water Conservation Fund" OR '
             '"new park" OR "park expansion" OR "trail expansion" OR '
             '"park bond" OR "recreation grant"')
_PARKS_SIGNAL = re.compile(
    r"\b(park|trail|recreation|dnr trust fund|land and water conservation|"
    r"playground|greenway|boat launch|nature preserve)\b", re.I)

# category key -> (Google News keywords, secondary signal regex, display label)
CATEGORIES = {
    "employer":    (_EMPLOYER_KEYWORDS,    _EMPLOYER_SIGNAL,    "Employer expansion"),
    "retail":      (_RETAIL_KEYWORDS,      _RETAIL_SIGNAL,      "New retail"),
    "water_sewer": (_WATER_SEWER_KEYWORDS, _WATER_SEWER_SIGNAL, "Water/sewer infrastructure"),
    "parks":       (_PARKS_KEYWORDS,       _PARKS_SIGNAL,       "Parks & recreation"),
}
DEFAULT_CATEGORY = "employer"          # older queue records predate the field


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


def scan_candidates(cutoff, category=DEFAULT_CATEGORY) -> dict:
    """One Google-News query per county for `category`; return {id: record} of hits newer than `cutoff`."""
    keywords, signal, _label = CATEGORIES[category]
    seen = {}
    for c in MARKET_COUNTIES:
        label = c["label"]                           # e.g. "Ottawa County"
        query = f'{keywords} "{label}" Michigan'
        try:
            root = _news_rss(query)
        except Exception:                            # noqa: BLE001
            continue
        for it in root.findall(".//item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            if not title or not link:
                continue
            blob = title + " " + (it.findtext("description") or "")
            if not signal.search(blob):
                continue
            iid = _norm_id(title)
            if iid in seen:
                continue
            src_el = it.find("{*}source")
            source = (src_el.text if src_el is not None else "") or ""
            pub = it.findtext("pubDate") or ""
            dt = _parse_date(pub)
            if dt is not None and dt < cutoff:       # older than the recency window
                continue
            seen[iid] = {
                "id": iid, "title": title, "link": link, "source": source,
                "published": pub, "published_ts": dt.isoformat() if dt else "",
                "county_key": c["key"], "county_label": label,
                "category": category,
            }
    return seen


def load_queue() -> dict:
    if _QUEUE.exists():
        return json.loads(_QUEUE.read_text())
    return {}


def _save_queue(q: dict):
    _QUEUE.parent.mkdir(parents=True, exist_ok=True)
    # Keep a one-version backup before overwriting, so analyst-entered details
    # (employer/jobs/investment/city) can be recovered if a save goes wrong.
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


def _scan_ts_by_category() -> dict:
    """{category: iso_ts} of each category's last scan, migrating the old flat
    (pre-category) format where `last_scan_ts` was a single string that only
    ever meant the employer scan."""
    raw = _load_meta().get("last_scan_ts")
    if isinstance(raw, str):
        return {DEFAULT_CATEGORY: raw}
    return raw or {}


def last_scan_ts(category=None):
    """ISO timestamp of the most recent scan. Pass a category for that
    category's own last-scan time (None if never scanned); omit it for the
    most recent scan across all categories (None if nothing's ever run)."""
    by_cat = _scan_ts_by_category()
    if category is not None:
        return by_cat.get(category)
    return max(by_cat.values()) if by_cat else None


def _scan_cutoff(category):
    """(cutoff_datetime, is_catchup) for `category` — incremental since that
    category's own last scan, else a one-time catch-up over _CATCHUP_DAYS.
    Adding a new category later automatically gets its own catch-up run even
    though other categories have already been scanned many times."""
    now = datetime.now(timezone.utc)
    last = last_scan_ts(category)
    if last:
        try:
            return datetime.fromisoformat(last) - timedelta(days=_OVERLAP_DAYS), False
        except Exception:                            # noqa: BLE001
            pass
    return now - timedelta(days=_CATCHUP_DAYS), True


def run_scan() -> tuple[int, int, bool]:
    """
    Fetch candidates across all CATEGORIES and add NEW ones as pending, each
    category incremental since its OWN last scan (or a one-time catch-up if
    it's never been scanned before). Returns (new, pending, any_catchup).
    """
    q = load_queue()
    new = 0
    any_catchup = False
    for category in CATEGORIES:
        cutoff, is_catchup = _scan_cutoff(category)
        any_catchup = any_catchup or is_catchup
        for iid, rec in scan_candidates(cutoff, category).items():
            if iid not in q:                          # never re-surface a decided item
                rec["status"] = "pending"
                q[iid] = rec
                new += 1
    _save_queue(q)
    now_iso = datetime.now(timezone.utc).isoformat()
    _save_meta({"last_scan_ts": {c: now_iso for c in CATEGORIES}})
    pending = sum(1 for v in q.values() if v.get("status") == "pending")
    return new, pending, any_catchup


def set_status(iid: str, status: str):
    q = load_queue()
    if iid in q:
        q[iid]["status"] = status
        _save_queue(q)


# Analyst-entered detail fields that feed the Executive summary. `category` is
# included so the analyst can recategorize a scan that landed under the wrong
# keyword match (e.g. a dog-park article matched the retail query).
DETAIL_FIELDS = ("employer", "jobs", "investment_musd", "city", "notes", "category")


def update_record(iid: str, **fields):
    """Set analyst-entered detail fields (employer/jobs/investment_musd/city/notes/category)."""
    q = load_queue()
    if iid not in q:
        return
    for k, v in fields.items():
        if k in DETAIL_FIELDS:
            if k == "category" and v not in CATEGORIES:
                continue                              # ignore unknown category keys
            q[iid][k] = v
    _save_queue(q)


def add_manual(url: str, county_key: str, county_label: str,
               title: str = None, source: str = None,
               category: str = DEFAULT_CATEGORY) -> tuple[str, bool]:
    """
    Manually add an announcement the scanner missed. Lands directly in the kept
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
              "county_key": county_key, "county_label": county_label,
              "status": "approved", "manual": True, "category": category}
    _save_queue(q)
    return iid, True


def summary_by_county() -> dict:
    """Aggregate APPROVED items per county → jobs, projects, employers, investment."""
    out = {}
    for v in load_queue().values():
        if v.get("status") != "approved":
            continue
        s = out.setdefault(v["county_key"],
                           {"jobs": 0, "projects": 0, "employers": set(),
                            "investment_musd": 0.0})
        s["projects"] += 1
        j = v.get("jobs")
        if isinstance(j, (int, float)) and j == j:
            s["jobs"] += int(j)
        inv = v.get("investment_musd")
        if isinstance(inv, (int, float)) and inv == inv:
            s["investment_musd"] += float(inv)
        # "employer" doubles as a free-text project description for the
        # market-attractiveness categories (retail/water_sewer/parks) — only
        # count it toward unique employers for actual employer-expansion items.
        if v.get("category", DEFAULT_CATEGORY) == "employer":
            emp = (v.get("employer") or "").strip()
            if emp:
                s["employers"].add(emp.lower())
    for s in out.values():
        s["employers"] = len(s["employers"])
    return out


if __name__ == "__main__":
    new, pending, catchup = run_scan()
    print(f"{new} new; {pending} pending (catch-up={catchup})")
    q = load_queue()
    print(f"{pending} pending after scan (total tracked: {len(q)})")
    for v in sorted(q.values(), key=lambda r: r.get("published_ts", ""), reverse=True)[:12]:
        cat = v.get("category", DEFAULT_CATEGORY)
        print(f"  [{v['status']:<8}][{cat:<8}] {v['county_label']:<16} {v['title'][:70]}")
