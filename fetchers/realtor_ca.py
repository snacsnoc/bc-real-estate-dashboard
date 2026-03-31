from __future__ import annotations

import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from .output import iso_now, safe_filename, write_json

# API behavior and header patterns are adapted from public Realtor.ca scrapers
# (pyRealtor and mls-real-estate-scraper-for-realtor.ca, MIT licensed).

SEARCH_URL = "https://api2.realtor.ca/Listing.svc/PropertySearch_Post"
DETAILS_URL = "https://api2.realtor.ca/Listing.svc/PropertyDetails"

DEFAULT_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-CA,en-US;q=0.7,en;q=0.3",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "DNT": "1",
    "Host": "api2.realtor.ca",
    "Origin": "https://www.realtor.ca",
    "Pragma": "no-cache",
    "Referer": "https://www.realtor.ca/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Sec-GPC": "1",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) "
        "Gecko/20100101 Firefox/146.0"
    ),
}

BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-CA,en-US;q=0.7,en;q=0.3",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) "
        "Gecko/20100101 Firefox/146.0"
    ),
}

TRANSACTION_TYPE_MAP = {
    "for_sale": "2",
    "for_rent": "3",
}

SORT_MAP = {
    "listing_price": "1",
    "listing_date_posted": "6",
}

BBox = Tuple[float, float, float, float]
SearchPayload = Dict[str, Any]


class RealtorCaClient:
    def __init__(
        self,
        *,
        cookie: Optional[str] = None,
        user_agent: Optional[str] = None,
        accept_language: Optional[str] = None,
    ) -> None:
        self.session = requests.Session()
        self.cookie = cookie
        self.user_agent = user_agent
        self.accept_language = accept_language
        self._apply_headers()

    def _apply_headers(self) -> None:
        self.session.headers.update(DEFAULT_HEADERS)
        if self.user_agent:
            self.session.headers["User-Agent"] = self.user_agent
        if self.accept_language:
            self.session.headers["Accept-Language"] = self.accept_language
        if self.cookie:
            self.session.headers["Cookie"] = self.cookie

    def prime_session(self) -> None:
        """Attempt to warm the session with browser-like requests."""
        self._apply_headers()
        try:
            self.session.get("https://www.realtor.ca/", headers=BROWSER_HEADERS, timeout=20)
        except requests.RequestException:
            return

        try:
            response = self.session.post(
                "https://www.realtor.ca/dnight-Exit-shall-Braith-Then-why-vponst-is-proc",
                json={
                    "solution": {"interrogation": None, "version": "beta"},
                    "old_token": None,
                    "error": None,
                    "performance": {"interrogation": 1897},
                },
                params={"d": "www.realtor.ca"},
                timeout=20,
            )
            if response.status_code == 200:
                token = response.json().get("token")
                if token and not self.cookie:
                    self.session.headers.update({"Cookie": f"reese84={token};"})
        except requests.RequestException:
            return

    def _request_with_reprime(
        self,
        method: str,
        url: str,
        *,
        retry_on_403: bool,
        **kwargs: Any,
    ) -> requests.Response:
        response = self.session.request(method, url, **kwargs)
        if retry_on_403 and response.status_code == 403:
            self.prime_session()
            response = self.session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    def search(self, payload: SearchPayload) -> Dict[str, Any]:
        """Execute a listing search."""
        response = self._request_with_reprime(
            "POST",
            SEARCH_URL,
            retry_on_403=True,
            data=payload,
            timeout=30,
        )
        return response.json()

    def fetch_details(self, property_id: str, mls_number: str) -> Dict[str, Any]:
        """Fetch property details by ID + MLS number."""
        params = {
            "ApplicationId": "1",
            "CultureId": "1",
            "PropertyID": property_id,
            "ReferenceNumber": mls_number,
        }
        response = self._request_with_reprime(
            "GET",
            DETAILS_URL,
            retry_on_403=True,
            params=params,
            timeout=30,
        )
        return response.json()

    def sub_area_search(self, area: str, page: int = 1) -> Dict[str, Any]:
        params = {
            "Area": area,
            "ApplicationId": "1",
            "CultureId": "1",
            "Version": "7.0",
            "CurrentPage": str(page),
        }
        url = "https://api2.realtor.ca/Location.svc/SubAreaSearch"
        response = self._request_with_reprime(
            "GET",
            url,
            retry_on_403=True,
            params=params,
            timeout=30,
        )
        return response.json()

def geocode_bbox(place: str) -> BBox:
    params = {"q": place, "format": "json", "limit": 1}
    headers = {"User-Agent": "bc-real-estate-dashboard/0.1"}
    response = requests.get(
        "https://nominatim.openstreetmap.org/search",
        params=params,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if not data:
        raise ValueError(f"No geocode results for place: {place}")
    bbox = data[0].get("boundingbox")
    if not bbox or len(bbox) != 4:
        raise ValueError(f"Unexpected bounding box result for place: {place}")
    lat_min, lat_max, lon_min, lon_max = (float(value) for value in bbox)
    return lat_min, lat_max, lon_min, lon_max


def build_search_payload(
    bbox: BBox,
    *,
    page: int,
    records_per_page: int,
    price_min: int,
    price_max: int,
    transaction_type: str,
    sort: str,
    ascending: bool,
    max_results: int,
    extra_params: Optional[Dict[str, str]] = None,
    zoom_level: int = 9,
    sold_within_days: Optional[int] = None,
    listed_within_days: Optional[int] = None,
) -> Dict[str, Any]:
    lat_min, lat_max, lon_min, lon_max = bbox
    payload: Dict[str, Any] = {
        "LatitudeMin": lat_min,
        "LatitudeMax": lat_max,
        "LongitudeMin": lon_min,
        "LongitudeMax": lon_max,
        "PriceMin": price_min,
        "PriceMax": price_max,
        "RecordsPerPage": records_per_page,
        "CurrentPage": page,
        "ApplicationId": "1",
        "CultureId": "1",
        "Version": "7.0",
        "Currency": "CAD",
        "MaximumResults": max_results,
        "ZoomLevel": zoom_level,
        "PropertyTypeGroupID": "1",
        "PropertySearchTypeId": "0",
        "IncludeHiddenListings": "false",
    }

    if sold_within_days is not None:
        payload["SoldWithinDays"] = str(sold_within_days)
    if listed_within_days is not None:
        payload["NumberOfDays"] = str(listed_within_days)

    transaction_id = TRANSACTION_TYPE_MAP.get(transaction_type)
    if not transaction_id:
        raise ValueError("transaction_type must be for_sale or for_rent")
    payload["TransactionTypeId"] = transaction_id

    sort_id = SORT_MAP.get(sort)
    if sort_id:
        direction = "-A" if ascending else "-D"
        payload["Sort"] = f"{sort_id}{direction}"

    if extra_params:
        payload.update(extra_params)

    return payload


def _parse_page_count(data: Dict[str, Any]) -> Optional[int]:
    paging = data.get("Paging") or {}
    total = paging.get("TotalRecords")
    per_page = paging.get("RecordsPerPage")
    if not total or not per_page:
        return None
    try:
        return int(math.ceil(int(total) / int(per_page)))
    except (TypeError, ValueError):
        return None


def fetch_search_pages(
    out_dir: Path,
    *,
    bbox: BBox,
    cookie: Optional[str] = None,
    user_agent: Optional[str] = None,
    accept_language: Optional[str] = None,
    dataset: Optional[str] = None,
    max_pages: int,
    all_pages: bool,
    records_per_page: int,
    price_min: int,
    price_max: int,
    transaction_type: str,
    sort: str,
    ascending: bool,
    max_results: int,
    extra_params: Optional[Dict[str, str]] = None,
    zoom_level: int = 9,
    sold_within_days: Optional[int] = None,
    listed_within_days: Optional[int] = None,
    sleep_seconds: float = 0.0,
    include_details: bool = False,
) -> Dict[str, Any]:
    client = RealtorCaClient(
        cookie=cookie,
        user_agent=user_agent,
        accept_language=accept_language,
    )
    client.prime_session()

    base_dir = out_dir / "realtor_ca"
    if dataset:
        base_dir = base_dir / dataset
    search_dir = base_dir / "search"
    details_dir = base_dir / "details"

    metadata: Dict[str, Any] = {
        "fetched_at": iso_now(),
        "bbox": bbox,
        "records_per_page": records_per_page,
        "price_min": price_min,
        "price_max": price_max,
        "transaction_type": transaction_type,
        "sort": sort,
        "ascending": ascending,
        "max_results": max_results,
        "zoom_level": zoom_level,
        "sold_within_days": sold_within_days,
        "listed_within_days": listed_within_days,
        "cookie_provided": bool(cookie),
        "extra_params": extra_params or {},
        "pages": [],
        "details": {"enabled": include_details, "items": 0},
    }

    page = 1
    resolved_max_pages = max_pages

    while page <= resolved_max_pages:
        client.prime_session()
        payload = build_search_payload(
            bbox,
            page=page,
            records_per_page=records_per_page,
            price_min=price_min,
            price_max=price_max,
            transaction_type=transaction_type,
            sort=sort,
            ascending=ascending,
            max_results=max_results,
            extra_params=extra_params,
            zoom_level=zoom_level,
            sold_within_days=sold_within_days,
            listed_within_days=listed_within_days,
        )
        data = client.search(payload)
        page_path = search_dir / f"page_{page}.json"
        write_json(page_path, {
            "fetched_at": iso_now(),
            "payload": payload,
            "data": data,
        })
        metadata["pages"].append({
            "page": page,
            "file": str(page_path),
        })

        if page == 1 and all_pages:
            parsed_pages = _parse_page_count(data)
            if parsed_pages:
                resolved_max_pages = parsed_pages

        if include_details:
            results = data.get("Results") or []
            for item in results:
                property_id = str(item.get("Id", "")).strip()
                mls_number = str(item.get("MlsNumber", "")).strip()
                if not property_id or not mls_number:
                    continue
                details_payload = client.fetch_details(property_id, mls_number)
                file_slug = safe_filename(f"{property_id}_{mls_number}")
                write_json(details_dir / f"{file_slug}.json", {
                    "fetched_at": iso_now(),
                    "property_id": property_id,
                    "mls_number": mls_number,
                    "data": details_payload,
                })
                metadata["details"]["items"] += 1
                if sleep_seconds:
                    time.sleep(sleep_seconds)

        page += 1
        if sleep_seconds:
            time.sleep(sleep_seconds)

    write_json(base_dir / "metadata.json", metadata)
    return metadata
