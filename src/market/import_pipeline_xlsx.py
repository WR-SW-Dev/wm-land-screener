"""
One-time importer: seeds data/competition_queue.json from the analyst's
existing pipeline-tracking spreadsheet — two sections on one sheet: a
confirmed "Pipeline" table, and a raw "Planning Commission Meeting Minutes"
leads table. Not run automatically; the source file lives in the analyst's
OneDrive at a machine-specific path, so it's passed as a CLI argument rather
than hardcoded.

Every imported row lands as an already-approved, historical=True record
(the analyst already vetted this data by hand) — reruns are idempotent,
since each row's id is a hash of its own content and already-present ids
are skipped rather than duplicated.

Stage classification:
  - Pipeline section: Status == "Construction" -> under_construction;
    Status == "Planning" -> classify_stage(notes) (text heuristic, per the
    analyst's rule: "approved"-type language in the notes -> planned, else
    proposed — no more reliable signal exists in this section).
  - Meeting Minutes section: has an explicit "Approved On" date column, a far
    more reliable signal than guessing from text -> planned if a date is
    present, else proposed.

Usage:
    python3 src/market/import_pipeline_xlsx.py "/path/to/WRII Pipeline.xlsx"
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from market import competition as c   # noqa: E402

# Location-name (as it appears in the sheet, lowercased) -> (submarket_key,
# submarket_label). Anything not matched here is outside the current
# 3-submarket scope but still imported, tagged "other" with its real name —
# nothing from the analyst's own tracking gets silently dropped.
_LOCATION_MAP = {
    "grand haven": ("grand_haven", "Grand Haven"),
    "graned haven": ("grand_haven", "Grand Haven"),           # typo in source
    "city of grand haven": ("grand_haven", "Grand Haven"),
    "grand haven township": ("gh_township", "Grand Haven Twp"),
    "grand haven charter township": ("gh_township", "Grand Haven Twp"),
    "spring lake township": ("spring_lake_twp", "Spring Lake Twp"),
    "village of spring lake": ("spring_lake_twp", "Spring Lake Twp"),
}


def _resolve_location(name):
    key = (name or "").strip().lower()
    if key in _LOCATION_MAP:
        return _LOCATION_MAP[key]
    return ("other", (name or "Unknown").strip())


def _clean(v):
    """NaN -> None; strip strings; leave numbers alone."""
    if pd.isna(v):
        return None
    if isinstance(v, str):
        v = v.strip()
        return v or None
    return v


def _date_str(v):
    v = _clean(v)
    if v is None:
        return None
    # A bare small number (e.g. 2021) is a plain year, not a timestamp —
    # pd.Timestamp(2021) silently misreads it as nanoseconds-since-epoch and
    # produces a bogus 1970-ish date. Keep it as plain text instead.
    if isinstance(v, (int, float)) and 1900 <= v <= 2100:
        return str(int(v))
    try:
        return pd.Timestamp(v).date().isoformat()
    except Exception:                                # noqa: BLE001
        return str(v)


def import_pipeline_section(df) -> list:
    """Columns: Project Name | Address | City | Status | Type | Total Units |
    Construction Start | Construction End | Builder | Notes."""
    records = []
    for _, row in df.iterrows():
        project_name = _clean(row[0])
        address = _clean(row[1])
        city = _clean(row[2])
        status = _clean(row[3])
        rtype = _clean(row[4])
        total_units = _clean(row[5])
        constr_start = _date_str(row[6])
        constr_end = _date_str(row[7])
        builder = _clean(row[8])
        notes = _clean(row[9])
        if not any([project_name, address, city, builder]):
            continue                                  # skip blank separator rows
        sm_key, sm_label = _resolve_location(city)
        if status and status.strip().lower() == "construction":
            stage = "under_construction"
        else:
            stage = c.classify_stage(notes or "")
        blob = " ".join(str(x) for x in (project_name, address, notes) if x)
        title = project_name or f"{rtype or 'Project'} at {address or city or 'unknown address'}"
        iid = c._norm_id(f"pipeline|{project_name}|{address}")
        records.append({
            "id": iid, "title": title, "link": "", "source": "WRII Pipeline.xlsx",
            "published": "", "published_ts": "",
            "submarket_key": sm_key, "submarket_label": sm_label,
            "stage": stage, "status": "approved", "historical": True,
            "is_direct_competitor": c.is_direct_competitor(blob),
            "project_name": project_name or "", "address": address or "",
            "type": rtype or "", "total_units": total_units,
            "construction_start": constr_start, "construction_end": constr_end,
            "builder": builder or "", "notes": notes or "",
        })
    return records


def import_minutes_section(df) -> list:
    """Columns: Description | Address | Parcel # | Proposed Zoning | Type |
    Number of Units | Acres | Approved On | Township | Notes."""
    records = []
    for _, row in df.iterrows():
        description = _clean(row[0])
        address = _clean(row[1])
        parcel = _clean(row[2])
        zoning = _clean(row[3])
        rtype = _clean(row[4])
        units = _clean(row[5])
        acres = _clean(row[6])
        approved_on = _date_str(row[7])
        township = _clean(row[8])
        notes = _clean(row[9])
        if not any([description, address, township]):
            continue
        sm_key, sm_label = _resolve_location(township)
        stage = "planned" if approved_on else "proposed"
        blob = " ".join(str(x) for x in (description, notes, zoning) if x)
        title = description or f"Lead at {address or township or 'unknown address'}"
        iid = c._norm_id(f"minutes|{description}|{address}")
        full_notes = " / ".join(x for x in (
            f"Proposed zoning: {zoning}" if zoning else None, notes) if x)
        records.append({
            "id": iid, "title": title, "link": "", "source": "WRII Pipeline.xlsx",
            "published": "", "published_ts": "",
            "submarket_key": sm_key, "submarket_label": sm_label,
            "stage": stage, "status": "approved", "historical": True,
            "is_direct_competitor": c.is_direct_competitor(blob),
            "project_name": description or "", "address": address or "",
            "type": rtype or "", "total_units": units,
            "approved_on": approved_on, "parcel_number": parcel or "",
            "acres": acres, "builder": "", "notes": full_notes or "",
        })
    return records


def main(path: str):
    xls_path = Path(path).expanduser()
    if not xls_path.exists():
        print(f"File not found: {xls_path}")
        sys.exit(1)

    raw = pd.read_excel(xls_path, sheet_name=0, header=None)

    # Locate the two section header rows by their first-column label, then
    # slice the data rows between/after them.
    pipeline_hdr = raw[raw[0] == "Project Name"].index[0]
    minutes_marker = raw[raw[0] == "Planning Commission Meeting Minutes"].index[0]
    minutes_hdr = raw[raw[0] == "Description"].index[0]

    pipeline_rows = raw.iloc[pipeline_hdr + 1: minutes_marker]
    minutes_rows = raw.iloc[minutes_hdr + 1:]

    new_records = (import_pipeline_section(pipeline_rows)
                   + import_minutes_section(minutes_rows))

    q = c.load_queue()
    added, skipped = 0, 0
    for rec in new_records:
        if rec["id"] in q:
            skipped += 1
            continue
        q[rec["id"]] = rec
        added += 1
    c._save_queue(q)

    print(f"Imported {added} historical record(s); {skipped} already present.")
    by_stage, by_sm = {}, {}
    for rec in new_records:
        by_stage[rec["stage"]] = by_stage.get(rec["stage"], 0) + 1
        by_sm[rec["submarket_label"]] = by_sm.get(rec["submarket_label"], 0) + 1
    print("By stage:", by_stage)
    print("By submarket:", by_sm)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 import_pipeline_xlsx.py <path-to-WRII-Pipeline.xlsx>")
        sys.exit(1)
    main(sys.argv[1])
