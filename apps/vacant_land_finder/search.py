from __future__ import annotations

from datetime import datetime, timezone
import math
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

APP_DIR = Path(__file__).resolve().parent
for candidate in (APP_DIR, *APP_DIR.parents):
    if (candidate / "fetchers").is_dir():
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
        break

from fetchers import remax_ca, realtor_ca
from fetchers.http import build_session

BBox = Tuple[float, float, float, float]

DEFAULT_CENTER_LAT = 44.32307163206222
DEFAULT_CENTER_LNG = -78.32923679935357
DEFAULT_RADIUS_KM = 50.0

REALTOR_VACANT_LAND_TYPE_ID = 303
REMAX_LAND_TYPE_IDS = [102, 180]
REALTOR_VACANT_LAND_SEARCH_TYPE_ID = "6"
REALTOR_PROPERTY_TYPE_GROUP_ID = "1"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int_price(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    cleaned = "".join(ch for ch in str(value) if ch.isdigit() or ch == ".")
    if not cleaned:
        return None
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0088
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_km * c


def bbox_from_center_radius(lat: float, lng: float, radius_km: float) -> BBox:
    lat_delta = radius_km / 111.32
    lon_delta = radius_km / (111.32 * max(abs(math.cos(math.radians(lat))), 1e-6))

    lat_min = max(-90.0, lat - lat_delta)
    lat_max = min(90.0, lat + lat_delta)
    lon_min = max(-180.0, lng - lon_delta)
    lon_max = min(180.0, lng + lon_delta)
    return lat_min, lat_max, lon_min, lon_max


def _absolute_url(base_url: str, path_or_url: Optional[str]) -> Optional[str]:
    if not path_or_url:
        return None
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    return f"{base_url.rstrip('/')}/{path_or_url.lstrip('/')}"


def _extract_city_from_address_text(address_text: Optional[str]) -> Optional[str]:
    if not address_text:
        return None
    parts = [part.strip() for part in address_text.split(",") if part.strip()]
    if len(parts) >= 3:
        return parts[-2]
    if len(parts) == 2:
        return parts[-1]
    return None


def _text_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"0", "0.0", "none", "null", "n/a", "na", "-"}:
        return None
    return text


def _realtor_land_size(land: Dict[str, Any]) -> Optional[str]:
    for key in ("SizeTotal", "SizeIrregular"):
        value = _text_or_none(land.get(key))
        if value:
            return value

    frontage = _text_or_none(land.get("SizeFrontage"))
    depth = _text_or_none(land.get("SizeDepth"))
    if frontage and depth:
        return f"{frontage} x {depth}"
    return frontage or depth


def _remax_land_size(item: Dict[str, Any]) -> Optional[str]:
    for key in (
        "lotSizeRaw",
        "lotSize",
        "lotSizeText",
        "lotDimensions",
        "lotDimensionsText",
        "landSize",
        "acreage",
    ):
        value = _text_or_none(item.get(key))
        if value:
            return value

    total_acres = _to_float(item.get("totalAcres"))
    if total_acres is not None and total_acres > 0:
        return f"{total_acres:g} acres"

    sq_ft = _to_float(item.get("sqFtSearch"))
    if sq_ft is not None and sq_ft > 0:
        return f"{sq_ft:g} sqft"
    return None


def _is_realtor_vacant_land(listing: Dict[str, Any]) -> bool:
    prop = listing.get("Property") or {}
    type_name = str(prop.get("Type") or "").strip().lower()
    type_id = _to_int_price(prop.get("TypeId"))

    if type_id == REALTOR_VACANT_LAND_TYPE_ID:
        return True
    return type_name == "vacant land" or "vacant land" in type_name


def fetch_realtor_vacant_land(
    *,
    center_lat: float,
    center_lng: float,
    radius_km: float,
    bbox: BBox,
    max_pages: int,
    records_per_page: int,
    cookie: Optional[str],
) -> List[Dict[str, Any]]:
    client = realtor_ca.RealtorCaClient(cookie=cookie)
    client.prime_session()

    listings: List[Dict[str, Any]] = []
    page = 1
    try:
        while page <= max_pages:
            payload = realtor_ca.build_search_payload(
                bbox,
                page=page,
                records_per_page=records_per_page,
                price_min=0,
                price_max=25_000_000,
                transaction_type="for_sale",
                sort="listing_price",
                ascending=True,
                max_results=600,
                zoom_level=10,
                extra_params={
                    "PropertyTypeGroupID": REALTOR_PROPERTY_TYPE_GROUP_ID,
                    "PropertySearchTypeId": REALTOR_VACANT_LAND_SEARCH_TYPE_ID,
                },
            )
            data = client.search(payload)
            results = data.get("Results") or []

            for item in results:
                if not _is_realtor_vacant_land(item):
                    continue

                prop = item.get("Property") or {}
                address = prop.get("Address") or {}
                land = item.get("Land") or {}

                lat = _to_float(address.get("Latitude"))
                lng = _to_float(address.get("Longitude"))
                if lat is None or lng is None:
                    continue

                distance_km = haversine_km(center_lat, center_lng, lat, lng)
                if distance_km > radius_km:
                    continue

                listings.append(
                    {
                        "source": "realtor_ca",
                        "source_id": str(item.get("Id") or "").strip() or None,
                        "mls_number": str(item.get("MlsNumber") or "").strip() or None,
                        "property_type": prop.get("Type") or "Vacant Land",
                        "address": address.get("AddressText"),
                        "city": _extract_city_from_address_text(address.get("AddressText")),
                        "province": item.get("ProvinceName"),
                        "price": _to_int_price(
                            prop.get("PriceUnformattedValue") or prop.get("Price")
                        ),
                        "distance_km": round(distance_km, 3),
                        "lat": lat,
                        "lng": lng,
                        "land_size": _realtor_land_size(land),
                        "listing_url": _absolute_url(
                            "https://www.realtor.ca", item.get("RelativeURLEn")
                        ),
                        "raw_price_text": prop.get("Price"),
                    }
                )

            paging = data.get("Paging") or {}
            total_pages = _to_int_price(paging.get("TotalPages"))
            if total_pages is None or page >= total_pages:
                break
            page += 1
    finally:
        client.session.close()

    return listings


def fetch_remax_vacant_land(
    *,
    center_lat: float,
    center_lng: float,
    radius_km: float,
    bbox: BBox,
    max_pages: int,
    page_size: int,
) -> List[Dict[str, Any]]:
    session = build_session()
    session.headers.update(remax_ca.DEFAULT_HEADERS)

    listings: List[Dict[str, Any]] = []
    try:
        for page in range(max_pages):
            from_index = page * page_size
            params = remax_ca.build_params(
                bbox=bbox,
                from_index=from_index,
                size=page_size,
                zoom=12,
                sort_key=0,
                sort_direction=0,
                excluded_listing_type_ids=[],
            )
            for listing_type_id in REMAX_LAND_TYPE_IDS:
                params.append(("features.listingTypeIds", str(listing_type_id)))
            data = remax_ca.fetch_gallery_page(session, params)
            result = data.get("result") or {}
            rows = result.get("results") or []

            for item in rows:
                lat = _to_float(item.get("lat"))
                lng = _to_float(item.get("lng"))
                if lat is None or lng is None:
                    continue

                distance_km = haversine_km(center_lat, center_lng, lat, lng)
                if distance_km > radius_km:
                    continue

                listings.append(
                    {
                        "source": "remax_ca",
                        "source_id": str(item.get("listingId") or "").strip() or None,
                        "mls_number": str(item.get("mlsNum") or "").strip() or None,
                        "property_type": "Vacant Land",
                        "address": item.get("address") or item.get("mlsAddress"),
                        "city": item.get("city") or item.get("mlsCity"),
                        "province": item.get("province") or item.get("mlsProvince"),
                        "price": _to_int_price(item.get("listPrice")),
                        "distance_km": round(distance_km, 3),
                        "lat": lat,
                        "lng": lng,
                        "land_size": _remax_land_size(item),
                        "listing_url": _absolute_url(
                            "https://www.remax.ca", item.get("detailUrl")
                        ),
                        "raw_price_text": item.get("listPrice"),
                    }
                )

            total_hits = _to_int_price(result.get("totalHits"))
            if total_hits is not None and (from_index + page_size) >= total_hits:
                break
            if not rows:
                break
    finally:
        session.close()

    return listings


def search_vacant_land(
    *,
    center_lat: float,
    center_lng: float,
    radius_km: float,
    max_results: int,
    max_pages: int,
    include_realtor: bool,
    include_remax: bool,
    realtor_cookie: Optional[str] = None,
) -> Dict[str, Any]:
    bbox = bbox_from_center_radius(center_lat, center_lng, radius_km)
    combined: List[Dict[str, Any]] = []
    errors: Dict[str, str] = {}

    if include_realtor:
        try:
            combined.extend(
                fetch_realtor_vacant_land(
                    center_lat=center_lat,
                    center_lng=center_lng,
                    radius_km=radius_km,
                    bbox=bbox,
                    max_pages=max_pages,
                    records_per_page=200,
                    cookie=realtor_cookie,
                )
            )
        except Exception as exc:  # noqa: BLE001
            errors["realtor_ca"] = str(exc)

    if include_remax:
        try:
            combined.extend(
                fetch_remax_vacant_land(
                    center_lat=center_lat,
                    center_lng=center_lng,
                    radius_km=radius_km,
                    bbox=bbox,
                    max_pages=max_pages,
                    page_size=50,
                )
            )
        except Exception as exc:  # noqa: BLE001
            errors["remax_ca"] = str(exc)

    requested_sources = {
        "realtor_ca": include_realtor,
        "remax_ca": include_remax,
    }
    requested_source_keys = [name for name, enabled in requested_sources.items() if enabled]
    failed_source_keys = [name for name in requested_source_keys if name in errors]
    all_sources_failed = bool(requested_source_keys) and (
        len(failed_source_keys) == len(requested_source_keys)
    )

    return {
        "generated_at": _iso_now(),
        "all_sources_failed": all_sources_failed,
        "errors": errors,
        "results": combined[:max_results],
    }
