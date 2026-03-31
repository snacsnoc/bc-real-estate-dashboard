from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

import requests

from .http import build_session, request_with_retries
from .output import iso_now, write_json

BASE_URL = "https://api.remax.ca/api/v1/listings/gallery/"

DEFAULT_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-CA,en-US;q=0.7,en;q=0.3",
    "Origin": "https://www.remax.ca",
    "Referer": "https://www.remax.ca/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Sec-GPC": "1",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) "
        "Gecko/20100101 Firefox/146.0"
    ),
}

DEFAULT_EXCLUDED_LISTING_TYPE_IDS = [101, 103, 107, 108, 112, 130]

BBox = Tuple[float, float, float, float]
ParamList = List[Tuple[str, str]]
ExtraParams = Optional[Dict[str, Union[str, int, float, bool]]]


def build_params(
    *,
    bbox: BBox,
    from_index: int,
    size: int,
    zoom: int,
    sort_key: int,
    sort_direction: int,
    excluded_listing_type_ids: Iterable[int],
    extra_params: ExtraParams = None,
) -> ParamList:
    """Build query params list for the gallery endpoint."""
    lat_min, lat_max, lon_min, lon_max = bbox
    params: ParamList = [
        ("from", str(from_index)),
        ("size", str(size)),
        ("zoom", str(zoom)),
        ("north", str(lat_max)),
        ("south", str(lat_min)),
        ("east", str(lon_max)),
        ("west", str(lon_min)),
        ("sortKey", str(sort_key)),
        ("sortDirection", str(sort_direction)),
    ]
    for value in excluded_listing_type_ids:
        params.append(("features.excludedListingTypeIds", str(value)))
    if extra_params:
        for key, value in extra_params.items():
            params.append((key, str(value).lower() if isinstance(value, bool) else str(value)))
    return params


def fetch_gallery_page(
    session: requests.Session,
    params: ParamList,
) -> Dict[str, object]:
    """Call the gallery endpoint with prepared params."""
    response = request_with_retries(session, "GET", BASE_URL, params=params)
    return response.json()


def fetch_gallery(
    out_dir: Path,
    *,
    bbox: BBox,
    from_index: int = 0,
    size: int = 20,
    zoom: int = 12,
    sort_key: int = 1,
    sort_direction: int = 0,
    excluded_listing_type_ids: Optional[List[int]] = None,
    all_pages: bool = False,
    max_pages: Optional[int] = None,
    user_agent: Optional[str] = None,
    accept_language: Optional[str] = None,
    debug: bool = False,
    extra_params: ExtraParams = None,
    dataset: Optional[str] = None,
) -> Dict[str, object]:
    """Fetch one or more gallery pages and persist responses + metadata."""
    session = build_session()
    session.headers.update(DEFAULT_HEADERS)
    if user_agent:
        session.headers["User-Agent"] = user_agent
    if accept_language:
        session.headers["Accept-Language"] = accept_language

    excluded = excluded_listing_type_ids or DEFAULT_EXCLUDED_LISTING_TYPE_IDS
    base_dir = out_dir / "remax"
    if dataset:
        base_dir = base_dir / dataset
    gallery_dir = base_dir / "gallery"

    page = 0
    total_hits: Optional[int] = None
    pages: List[Dict[str, object]] = []

    while True:
        params = build_params(
            bbox=bbox,
            from_index=from_index,
            size=size,
            zoom=zoom,
            sort_key=sort_key,
            sort_direction=sort_direction,
            excluded_listing_type_ids=excluded,
            extra_params=extra_params,
        )
        data = fetch_gallery_page(session, params)
        page_path = gallery_dir / f"page_{page}.json"
        write_json(page_path, {
            "fetched_at": iso_now(),
            "params": params,
            "data": data,
        })
        pages.append({"page": page, "file": str(page_path)})

        result = data.get("result") or {}
        if total_hits is None:
            total_hits = result.get("totalHits")
            if debug:
                print(f"[remax] total_hits={total_hits} size={size}")

        if not all_pages:
            break

        if total_hits is None:
            break

        page += 1
        if max_pages is not None and page >= max_pages:
            break

        next_from = from_index + size
        if next_from >= int(total_hits):
            break
        from_index = next_from
        if debug:
            print(f"[remax] page={page} from={from_index}")

    metadata = {
        "fetched_at": iso_now(),
        "bbox": bbox,
        "from": from_index,
        "size": size,
        "zoom": zoom,
        "sort_key": sort_key,
        "sort_direction": sort_direction,
        "excluded_listing_type_ids": excluded,
        "extra_params": extra_params or {},
        "total_hits": total_hits,
        "pages": pages,
        "source_url": BASE_URL,
    }
    write_json(base_dir / "metadata.json", metadata)
    return metadata
