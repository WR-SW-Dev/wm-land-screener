"""
Economic-development / employer-expansion news scanner (Phase A).

On-demand ("Scan now") scan for expansion / new-jobs / investment announcements
in the market counties (config.MARKET_COUNTIES — locked to Ottawa/Kent/Muskegon/
Allegan today, auto-expands with that list). Uses Google News RSS: free, no API
key, no third-party service, nothing leaves your systems. Google News already
indexes MiBiz, MLive, Crain's, Bridge Michigan, and the EDO press releases.

Findings land in a review queue (status="pending"); the analyst approves/rejects
in-app. Nothing is "kept" until approved — matching the full-review-queue choice.

Public API:
    run_scan()          -> (new_count, pending_count)   # fetch + queue new items
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

# Only surface reasonably recent announcements so each scan stays focused on new
# activity rather than months of Google News history.
_DAYS_BACK = 60

# Google News query keywords (broad net; the signal filter + human review refine).
_KEYWORDS = ('"new jobs" OR expansion OR investment OR headquarters OR '
             'groundbreaking OR hiring OR manufacturing OR "new facility" OR '
             '"economic development"')

# Secondary relevance gate on title+snippet — trims obvious off-topic hits.
_SIGNAL = re.compile(
    r"\b(jobs?|expansion|expand|invest|investment|headquarters|hq|groundbreaking|"
    r"hir(e|ing)|manufactur\w*|facilit\w+|plant|campus|million|billion|"
    r"develop\w*|relocat\w+|break ground|new site|square feet|sq\.? ?ft|workforce|"
    r"employ\w*|abatement|MEDC)\b", re.I)


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


def scan_candidates() -> dict:
    """One Google-News query per county; return {id: record} of relevant hits."""
    seen = {}
    cutoff = datetime.now(timezone.utc) - timedelta(days=_DAYS_BACK)
    for c in MARKET_COUNTIES:
        label = c["label"]                           # e.g. "Ottawa County"
        query = f'{_KEYWORDS} "{label}" Michigan'
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
            if not _SIGNAL.search(blob):
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
            }
    return seen


def load_queue() -> dict:
    if _QUEUE.exists():
        return json.loads(_QUEUE.read_text())
    return {}


def _save_queue(q: dict):
    _QUEUE.parent.mkdir(parents=True, exist_ok=True)
    _QUEUE.write_text(json.dumps(q, indent=2))


def run_scan() -> tuple[int, int]:
    """Fetch candidates, add NEW ones as pending. Returns (new_count, pending_count)."""
    q = load_queue()
    new = 0
    for iid, rec in scan_candidates().items():
        if iid not in q:                             # never re-surface a decided item
            rec["status"] = "pending"
            q[iid] = rec
            new += 1
    _save_queue(q)
    pending = sum(1 for v in q.values() if v.get("status") == "pending")
    return new, pending


def set_status(iid: str, status: str):
    q = load_queue()
    if iid in q:
        q[iid]["status"] = status
        _save_queue(q)


# Analyst-entered detail fields that feed the Executive summary.
DETAIL_FIELDS = ("employer", "jobs", "investment_musd", "city", "notes")


def update_record(iid: str, **fields):
    """Set analyst-entered detail fields (employer/jobs/investment_musd/city/notes)."""
    q = load_queue()
    if iid not in q:
        return
    for k, v in fields.items():
        if k in DETAIL_FIELDS:
            q[iid][k] = v
    _save_queue(q)


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
        emp = (v.get("employer") or "").strip()
        if emp:
            s["employers"].add(emp.lower())
    for s in out.values():
        s["employers"] = len(s["employers"])
    return out


if __name__ == "__main__":
    new, pending = run_scan()
    print(f"{new} new; {pending} pending")
    q = load_queue()
    print(f"{pending} pending after scan (total tracked: {len(q)})")
    for v in sorted(q.values(), key=lambda r: r.get("published_ts", ""), reverse=True)[:12]:
        print(f"  [{v['status']:<8}] {v['county_label']:<16} {v['title'][:70]}")
