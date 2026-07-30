"""Aerial thumbnail + parcel-outline overlay, for the per-parcel PDF card.

Fetches a real Esri World Imagery export for a parcel's bbox (padded and
aspect-capped so narrow/flag-shaped rural lots don't letterbox), reprojects
the parcel polygon to Web Mercator for a distortion-free pixel transform, and
draws the outline with Pillow. Proven against real Grand Haven / Spring Lake
Township parcels — see chat history 2026-07-29 for the PoC that validated
this approach (including the flag-lot aspect-ratio fix).
"""
import io

import requests
from PIL import Image, ImageDraw
from pyproj import Transformer

_WEB_MERCATOR = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
_EXPORT_URL = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export"
_ATTRIBUTION = "Imagery: Esri World Imagery"


def _largest_ring(geometry):
    """Exterior ring (list of (lon, lat)) of a shapely Polygon/MultiPolygon —
    the largest sub-polygon by area if it's a MultiPolygon."""
    geom = geometry
    if geom.geom_type == "MultiPolygon":
        geom = max(geom.geoms, key=lambda g: g.area)
    return list(geom.exterior.coords)


def fetch_parcel_thumbnail(geometry, label="", pad_frac=0.4, max_aspect=1.6,
                           out_long_edge=900, timeout=20):
    """Return PNG bytes: aerial image of `geometry`'s surroundings with its
    outline drawn on top. `geometry` is a shapely Polygon/MultiPolygon in
    EPSG:4326 (lon/lat, standard GeoJSON convention). Returns None if the
    imagery fetch fails — callers should treat that as "no thumbnail
    available" rather than a hard error (this hits a third-party service)."""
    ring = _largest_ring(geometry)
    merc_ring = [_WEB_MERCATOR.transform(lon, lat) for lon, lat in ring]

    xs = [p[0] for p in merc_ring]
    ys = [p[1] for p in merc_ring]
    w0, s0, e0, n0 = min(xs), min(ys), max(xs), max(ys)
    pad_x = (e0 - w0) * pad_frac
    pad_y = (n0 - s0) * pad_frac
    west, south, east, north = w0 - pad_x, s0 - pad_y, e0 + pad_x, n0 + pad_y

    # Cap the aspect ratio for long/narrow (flag-lot) parcels — expand the
    # shorter axis around the parcel's centroid rather than letterboxing.
    bbox_w, bbox_h = east - west, north - south
    cx, cy = (west + east) / 2, (south + north) / 2
    if bbox_w / bbox_h > max_aspect:
        bbox_h = bbox_w / max_aspect
        south, north = cy - bbox_h / 2, cy + bbox_h / 2
    elif bbox_h / bbox_w > max_aspect:
        bbox_w = bbox_h / max_aspect
        west, east = cx - bbox_w / 2, cx + bbox_w / 2

    if bbox_w >= bbox_h:
        out_w, out_h = out_long_edge, max(1, round(out_long_edge * bbox_h / bbox_w))
    else:
        out_h, out_w = out_long_edge, max(1, round(out_long_edge * bbox_w / bbox_h))

    try:
        resp = requests.get(_EXPORT_URL, params={
            "bbox": f"{west},{south},{east},{north}",
            "bboxSR": 3857, "imageSR": 3857,
            "size": f"{out_w},{out_h}",
            "format": "png", "f": "image",
        }, timeout=timeout)
        resp.raise_for_status()
        img = Image.open(io.BytesIO(resp.content)).convert("RGB")
    except Exception:                              # noqa: BLE001 — third-party service
        return None

    def to_px(mx, my):
        px = (mx - west) / bbox_w * out_w
        py = (north - my) / bbox_h * out_h
        return px, py

    draw = ImageDraw.Draw(img)
    poly_px = [to_px(mx, my) for mx, my in merc_ring]
    draw.line(poly_px + [poly_px[0]], fill=(255, 210, 0), width=5)

    if label:
        draw.rectangle([8, 8, 8 + 11 * len(label) + 12, 30], fill=(0, 0, 0))
        draw.text((14, 12), label, fill=(255, 255, 255))

    draw.rectangle([0, out_h - 20, 8 + 8 * len(_ATTRIBUTION), out_h], fill=(0, 0, 0))
    draw.text((6, out_h - 18), _ATTRIBUTION, fill=(220, 220, 220))

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()
