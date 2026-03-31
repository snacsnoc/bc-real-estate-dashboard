#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Ensure project root is on sys.path when running as a script.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fetchers import realtor_ca
from fetchers.output import ensure_parent, iso_now, write_json

BBox = Tuple[float, float, float, float]
ExtraParams = Dict[str, str]


PAGE_RE = re.compile(r"page_(\d+)")


def normalize_cookie(cookie: str) -> str:
    cleaned = " ".join(part.strip() for part in cookie.splitlines() if part.strip())
    try:
        cleaned.encode("latin-1")
    except UnicodeEncodeError as exc:
        raise SystemExit(
            "Cookie contains non-ASCII characters (e.g. ellipsis). "
            "Use the exact Cookie header value with no truncation or ellipsis."
        ) from exc
    return cleaned


def parse_bbox(values: Optional[List[float]]) -> Optional[BBox]:
    """Parse a bounding box argument list into a tuple."""
    if not values:
        return None
    if len(values) != 4:
        raise SystemExit("--bbox requires four values: LAT_MIN LAT_MAX LON_MIN LON_MAX")
    return tuple(values)  # type: ignore[return-value]


def parse_params(items: List[str]) -> Dict[str, str]:
    """Parse repeated key=value params from CLI flags."""
    extra_params: Dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise SystemExit("Extra param must be in key=value form.")
        key, value = item.split("=", 1)
        extra_params[key] = value
    return extra_params


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_jsonl(path: Path, items: Iterable[Dict[str, Any]]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        for item in items:
            handle.write(json.dumps(item, ensure_ascii=True))
            handle.write("\n")


def parse_page_number(path: Path) -> int:
    match = PAGE_RE.search(path.stem)
    if not match:
        return 0
    return int(match.group(1))


def load_details(details_dir: Path) -> Dict[str, Dict[str, Any]]:
    details: Dict[str, Dict[str, Any]] = {}
    if not details_dir.exists():
        return details
    for path in details_dir.glob("*.json"):
        payload = load_json(path)
        property_id = str(payload.get("property_id") or "").strip()
        if not property_id:
            continue
        details[property_id] = {
            "fetched_at": payload.get("fetched_at"),
            "data": payload.get("data"),
        }
    return details


def normalize_listing(
    listing: Dict[str, Any],
    *,
    source: Dict[str, Any],
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    prop = listing.get("Property") or {}
    address = prop.get("Address") or {}
    building = listing.get("Building") or {}
    land = listing.get("Land") or {}

    payload: Dict[str, Any] = {
        "id": listing.get("Id"),
        "mls_number": listing.get("MlsNumber"),
        "status_id": listing.get("StatusId"),
        "postal_code": listing.get("PostalCode"),
        "province_name": listing.get("ProvinceName"),
        "public_remarks": listing.get("PublicRemarks"),
        "inserted_date_utc": listing.get("InsertedDateUTC"),
        "time_on_realtor": listing.get("TimeOnRealtor"),
        "relative_details_url": listing.get("RelativeDetailsURL"),
        "relative_url_en": listing.get("RelativeURLEn"),
        "relative_url_fr": listing.get("RelativeURLFr"),
        "price": prop.get("Price"),
        "price_unformatted_value": prop.get("PriceUnformattedValue"),
        "short_value": prop.get("ShortValue"),
        "property_type": prop.get("Type"),
        "property_type_id": prop.get("TypeId"),
        "ownership_type": prop.get("OwnershipType"),
        "ownership_type_group_ids": prop.get("OwnershipTypeGroupIds"),
        "address_text": address.get("AddressText"),
        "latitude": address.get("Latitude"),
        "longitude": address.get("Longitude"),
        "permit_show_address": address.get("PermitShowAddress"),
        "building": building,
        "land": land,
        "business": listing.get("Business") or {},
        "media": listing.get("Media") or [],
        "tags": listing.get("Tags") or [],
        "photo_change_date_utc": listing.get("PhotoChangeDateUTC"),
        "has_new_image_update": listing.get("HasNewImageUpdate"),
        "distance": listing.get("Distance"),
        "uploaded_by": listing.get("UploadedBy"),
        "source": source,
        "raw": listing,
    }

    if details is not None:
        payload["details"] = details

    return payload


def collect_listings(
    search_dir: Path,
    *,
    details_map: Dict[str, Dict[str, Any]],
    dedupe: bool,
) -> List[Dict[str, Any]]:
    listings: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    page_files = sorted(search_dir.glob("page_*.json"), key=parse_page_number)
    for page_file in page_files:
        payload = load_json(page_file)
        data = payload.get("data") or {}
        results = data.get("Results") or []
        source = {
            "page": parse_page_number(page_file),
            "page_file": str(page_file),
            "fetched_at": payload.get("fetched_at"),
            "payload": payload.get("payload"),
        }
        for listing in results:
            listing_id = str(listing.get("Id") or "").strip()
            if dedupe and listing_id and listing_id in seen_ids:
                continue
            if listing_id:
                seen_ids.add(listing_id)
            details = details_map.get(listing_id)
            listings.append(
                normalize_listing(listing, source=source, details=details)
            )

    return listings


def build_summary(
    listings: List[Dict[str, Any]],
    *,
    search_dir: Path,
    details_map: Dict[str, Dict[str, Any]],
    dedupe: bool,
) -> Dict[str, Any]:
    ids = [item.get("id") for item in listings if item.get("id")]
    unique_ids = len(set(ids))
    return {
        "generated_at": iso_now(),
        "search_dir": str(search_dir),
        "listings": len(listings),
        "unique_listing_ids": unique_ids,
        "dedupe": dedupe,
        "details_available": len(details_map),
        "details_matched": sum(1 for item in listings if item.get("details")),
    }


def normalize_dataset(
    *,
    raw_dirs: List[Path],
    out_dir: Path,
    with_details: bool,
    snapshot: bool,
    dedupe: bool = True,
    province_filter: Optional[str] = None,
    debug: bool = False,
) -> Dict[str, Any]:
    listings: List[Dict[str, Any]] = []
    details_map: Dict[str, Dict[str, Any]] = {}

    for raw_dir in raw_dirs:
        search_dir = raw_dir / "search"
        details_dir = raw_dir / "details"

        if not search_dir.exists():
            raise SystemExit(f"Missing search directory: {search_dir}")

        if with_details:
            details_map.update(load_details(details_dir))

        listings.extend(
            collect_listings(
                search_dir,
                details_map=details_map,
                dedupe=dedupe,
            )
        )

    if dedupe:
        seen_ids: set[str] = set()
        deduped: List[Dict[str, Any]] = []
        for item in listings:
            listing_id = str(item.get("id") or "").strip()
            if listing_id and listing_id in seen_ids:
                continue
            if listing_id:
                seen_ids.add(listing_id)
            deduped.append(item)
        listings = deduped

    if province_filter:
        expected = province_filter.strip().lower()
        before = len(listings)
        listings = [
            item
            for item in listings
            if str(item.get("province_name") or "").strip().lower() == expected
        ]
        if debug:
            print(
                f"[normalize] province_filter='{province_filter}' "
                f"kept={len(listings)} removed={before - len(listings)}"
            )

    write_jsonl(out_dir / "listings.jsonl", listings)
    summary = build_summary(
        listings,
        search_dir=raw_dirs[0] / "search",
        details_map=details_map,
        dedupe=dedupe,
    )
    summary["raw_dirs"] = [str(path) for path in raw_dirs]
    write_json(out_dir / "summary.json", summary)

    if snapshot:
        snapshot_dir = out_dir / "snapshots"
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        snapshot_path = snapshot_dir / f"listings_{timestamp}.jsonl"
        write_jsonl(snapshot_path, listings)
    return summary


def read_paging(raw_dir: Path) -> Optional[Dict[str, Any]]:
    page = raw_dir / "search" / "page_1.json"
    if not page.exists():
        return None
    data = load_json(page)
    payload = data.get("data") or {}
    return payload.get("Paging")


def is_truncated(paging: Optional[Dict[str, Any]]) -> bool:
    if not paging:
        return False
    total = paging.get("TotalRecords")
    max_records = paging.get("MaxRecords")
    if total is None or max_records is None:
        return False
    try:
        return int(total) > int(max_records)
    except (TypeError, ValueError):
        return False


def format_paging(paging: Optional[Dict[str, Any]]) -> str:
    if not paging:
        return "paging unavailable"
    total = paging.get("TotalRecords")
    max_records = paging.get("MaxRecords")
    pages = paging.get("TotalPages")
    per_page = paging.get("RecordsPerPage")
    return f"total={total} max={max_records} pages={pages} per_page={per_page}"


def split_bbox(
    bbox: BBox,
    rows: int,
    cols: int,
) -> List[Tuple[BBox, str]]:
    lat_min, lat_max, lon_min, lon_max = bbox
    lat_step = (lat_max - lat_min) / rows
    lon_step = (lon_max - lon_min) / cols
    tiles = []
    for row in range(rows):
        for col in range(cols):
            tile_lat_min = lat_min + row * lat_step
            tile_lat_max = lat_min + (row + 1) * lat_step
            tile_lon_min = lon_min + col * lon_step
            tile_lon_max = lon_min + (col + 1) * lon_step
            label = f"r{row+1}c{col+1}"
            tiles.append(((tile_lat_min, tile_lat_max, tile_lon_min, tile_lon_max), label))
    return tiles


def fetch_with_tiling(
    *,
    raw_root: Path,
    dataset: Optional[str],
    bbox: BBox,
    fetch_kwargs: Dict[str, Any],
    tile_rows: int,
    tile_cols: int,
    max_depth: int,
    debug: bool,
) -> List[Path]:
    raw_dirs: List[Path] = []
    stats = {"tiles": 0, "truncated": 0}

    def fetch_tile(tile_bbox: BBox, label: str, depth: int) -> None:
        dataset_name = dataset or "active"
        if label:
            dataset_name = f"{dataset_name}_{label}"

        if debug:
            print(f"[fetch] dataset={dataset_name} bbox={tile_bbox} depth={depth}")

        realtor_ca.fetch_search_pages(
            raw_root,
            bbox=tile_bbox,
            dataset=dataset_name,
            **fetch_kwargs,
        )

        raw_dir = raw_root / "realtor_ca" / dataset_name
        paging = read_paging(raw_dir)
        truncated = is_truncated(paging)
        stats["tiles"] += 1
        if truncated:
            stats["truncated"] += 1
        if debug:
            print(f"[fetch] {dataset_name} {format_paging(paging)} truncated={truncated}")

        if truncated and depth < max_depth:
            tiles = split_bbox(tile_bbox, tile_rows, tile_cols)
            for child_bbox, child_label in tiles:
                fetch_tile(child_bbox, f"{label}_{child_label}" if label else child_label, depth + 1)
            return
        if truncated and depth >= max_depth and debug:
            print(
                f"[warn] {dataset_name} still truncated at max depth {max_depth}; "
                "consider increasing tile depth or narrowing the bbox."
            )

        raw_dirs.append(raw_dir)

    fetch_tile(bbox, "", 0)
    if debug:
        print(
            f"[fetch] tiles={stats['tiles']} truncated_tiles={stats['truncated']} max_depth={max_depth}"
        )
    return raw_dirs


def parser_defaults(parser: argparse.ArgumentParser) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {}
    for action in parser._actions:
        if action.dest == "help":
            continue
        defaults[action.dest] = action.default
    return defaults


def resolve_config_path(path: Optional[str]) -> Optional[Path]:
    if path:
        return Path(path)
    candidate = Path("config/realtor_pipeline.json")
    if candidate.exists():
        return candidate
    candidate = Path("realtor_pipeline.json")
    if candidate.exists():
        return candidate
    return None


def load_config(path: Optional[Path]) -> Dict[str, Any]:
    if not path:
        return {}
    if not path.exists():
        raise SystemExit(f"Config file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def merge_config(args: argparse.Namespace, config: Dict[str, Any], defaults: Dict[str, Any]) -> None:
    for key, value in config.items():
        if not hasattr(args, key):
            continue
        if getattr(args, key) == defaults.get(key):
            setattr(args, key, value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch active + sold Realtor.ca listings and normalize results."
    )
    parser.add_argument(
        "--config",
        help="Path to a JSON config file (default: config/realtor_pipeline.json if present).",
    )
    parser.add_argument(
        "--out-raw",
        default="data/raw",
        help="Raw output root (default: data/raw).",
    )
    parser.add_argument(
        "--out-normalized",
        default="data/normalized",
        help="Normalized output root (default: data/normalized).",
    )
    parser.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("LAT_MIN", "LAT_MAX", "LON_MIN", "LON_MAX"),
        help="Bounding box coordinates for the search.",
    )
    parser.add_argument(
        "--place",
        help="Place name for OSM geocoding (alternative to --bbox).",
    )
    parser.add_argument(
        "--cookie",
        help="Raw Cookie header value for realtor.ca requests.",
    )
    parser.add_argument(
        "--cookie-file",
        help="Path to a file containing a raw Cookie header value.",
    )
    parser.add_argument(
        "--user-agent",
        help="Override User-Agent header.",
    )
    parser.add_argument(
        "--accept-language",
        help="Override Accept-Language header.",
    )
    parser.add_argument(
        "--records-per-page",
        type=int,
        default=200,
        help="Fallback records per page (default: 200).",
    )
    parser.add_argument(
        "--active-records-per-page",
        type=int,
        help="Records per page for active listings.",
    )
    parser.add_argument(
        "--sold-records-per-page",
        type=int,
        help="Records per page for sold listings.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="Number of pages to fetch (default: 1).",
    )
    parser.add_argument(
        "--all-pages",
        action="store_true",
        help="Fetch all pages based on API paging metadata.",
    )
    parser.add_argument(
        "--price-min",
        type=int,
        default=0,
        help="Minimum price filter (default: 0).",
    )
    parser.add_argument(
        "--price-max",
        type=int,
        default=10000000,
        help="Maximum price filter (default: 10000000).",
    )
    parser.add_argument(
        "--transaction-type",
        choices=["for_sale", "for_rent"],
        default="for_sale",
        help="Transaction type (default: for_sale).",
    )
    parser.add_argument(
        "--sort",
        choices=["listing_date_posted", "listing_price"],
        default="listing_date_posted",
        help="Sort field (default: listing_date_posted).",
    )
    parser.add_argument(
        "--ascending",
        action="store_true",
        help="Sort ascending (default: descending).",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=600,
        help="Maximum results hint for the API (default: 600).",
    )
    parser.add_argument(
        "--sold-within-days",
        type=int,
        default=365,
        help="Fetch sold listings within N days (default: 365).",
    )
    parser.add_argument(
        "--sold-any",
        action="store_true",
        help="Remove SoldWithinDays filter for sold listings.",
    )
    parser.add_argument(
        "--active-listed-within-days",
        type=int,
        help="Filter active listings by NumberOfDays (listed since N days).",
    )
    parser.add_argument(
        "--active-dataset",
        help="Optional dataset name for active listings output.",
    )
    parser.add_argument(
        "--sold-dataset",
        default="sold_365",
        help="Dataset name for sold listings output (default: sold_365).",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Fetch per-listing detail payloads.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Sleep seconds between requests (default: 0).",
    )
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        help="Extra API param in key=value form (repeatable).",
    )
    parser.add_argument(
        "--zoom-level",
        type=int,
        default=9,
        help="Map zoom level hint (default: 9).",
    )
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Write a snapshot of active listings after normalization.",
    )
    parser.add_argument(
        "--province-filter",
        help="Filter listings by ProvinceName (e.g. 'British Columbia').",
    )
    parser.add_argument(
        "--tile-on-cap",
        action="store_true",
        help="Auto-split the bbox if results exceed the 600 record cap.",
    )
    parser.add_argument(
        "--tile-rows",
        type=int,
        default=2,
        help="Rows per tiling split (default: 2).",
    )
    parser.add_argument(
        "--tile-cols",
        type=int,
        default=2,
        help="Columns per tiling split (default: 2).",
    )
    parser.add_argument(
        "--tile-max-depth",
        type=int,
        default=2,
        help="Maximum tiling recursion depth (default: 2).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print verbose fetch and paging diagnostics.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    defaults = parser_defaults(parser)
    args = parser.parse_args()
    config_path = resolve_config_path(args.config)
    config = load_config(config_path)
    merge_config(args, config, defaults)

    bbox = parse_bbox(args.bbox)
    if not bbox and args.place:
        bbox = realtor_ca.geocode_bbox(args.place)
    if not bbox:
        raise SystemExit("Provide --bbox or --place.")

    cookie = args.cookie
    if not cookie and args.cookie_file:
        cookie = Path(args.cookie_file).read_text(encoding="utf-8").strip()
    if cookie and cookie.lower().startswith("cookie:"):
        cookie = cookie.split(":", 1)[1].strip()
    if cookie:
        cookie = normalize_cookie(cookie)

    extra_params = parse_params(args.param)

    raw_root = Path(args.out_raw)
    normalized_root = Path(args.out_normalized)

    active_records_per_page = args.active_records_per_page or args.records_per_page
    sold_records_per_page = args.sold_records_per_page or args.records_per_page
    sold_within_days = None if args.sold_any else args.sold_within_days

    active_fetch_kwargs = {
        "cookie": cookie,
        "user_agent": args.user_agent,
        "accept_language": args.accept_language,
        "max_pages": args.max_pages,
        "all_pages": args.all_pages,
        "records_per_page": active_records_per_page,
        "price_min": args.price_min,
        "price_max": args.price_max,
        "transaction_type": args.transaction_type,
        "sort": args.sort,
        "ascending": args.ascending,
        "max_results": args.max_results,
        "extra_params": extra_params or None,
        "zoom_level": args.zoom_level,
        "listed_within_days": args.active_listed_within_days,
        "sleep_seconds": args.sleep,
        "include_details": args.details,
    }

    sold_fetch_kwargs = {
        "cookie": cookie,
        "user_agent": args.user_agent,
        "accept_language": args.accept_language,
        "max_pages": args.max_pages,
        "all_pages": args.all_pages,
        "records_per_page": sold_records_per_page,
        "price_min": args.price_min,
        "price_max": args.price_max,
        "transaction_type": args.transaction_type,
        "sort": args.sort,
        "ascending": args.ascending,
        "max_results": args.max_results,
        "extra_params": extra_params or None,
        "zoom_level": args.zoom_level,
        "sold_within_days": sold_within_days,
        "sleep_seconds": args.sleep,
        "include_details": args.details,
    }

    if args.debug:
        print(f"[config] bbox={bbox}")
        print(
            "[config] active_records_per_page="
            f"{active_records_per_page} sold_records_per_page={sold_records_per_page}"
        )
        print(
            "[config] sold_within_days="
            f"{sold_within_days} all_pages={args.all_pages} max_pages={args.max_pages}"
        )
        if args.tile_on_cap:
            print(
                "[config] tile_rows="
                f"{args.tile_rows} tile_cols={args.tile_cols} tile_max_depth={args.tile_max_depth}"
            )
    if args.tile_on_cap:
        active_raw_dirs = fetch_with_tiling(
            raw_root=raw_root,
            dataset=args.active_dataset,
            bbox=bbox,
            fetch_kwargs=active_fetch_kwargs,
            tile_rows=args.tile_rows,
            tile_cols=args.tile_cols,
            max_depth=args.tile_max_depth,
            debug=args.debug,
        )
        sold_raw_dirs = fetch_with_tiling(
            raw_root=raw_root,
            dataset=args.sold_dataset,
            bbox=bbox,
            fetch_kwargs=sold_fetch_kwargs,
            tile_rows=args.tile_rows,
            tile_cols=args.tile_cols,
            max_depth=args.tile_max_depth,
            debug=args.debug,
        )
    else:
        realtor_ca.fetch_search_pages(
            raw_root,
            bbox=bbox,
            dataset=args.active_dataset,
            **active_fetch_kwargs,
        )
        realtor_ca.fetch_search_pages(
            raw_root,
            bbox=bbox,
            dataset=args.sold_dataset,
            **sold_fetch_kwargs,
        )
        active_raw_dirs = [
            raw_root / "realtor_ca" / args.active_dataset
            if args.active_dataset
            else raw_root / "realtor_ca"
        ]
        sold_raw_dirs = [raw_root / "realtor_ca" / args.sold_dataset]
        if args.debug:
            active_paging = read_paging(active_raw_dirs[0])
            sold_paging = read_paging(sold_raw_dirs[0])
            print(f"[fetch] active {format_paging(active_paging)} truncated={is_truncated(active_paging)}")
            print(f"[fetch] sold {format_paging(sold_paging)} truncated={is_truncated(sold_paging)}")

    normalized_realtor = normalized_root / "realtor_ca"

    active_out_dir = normalized_realtor / args.active_dataset if args.active_dataset else normalized_realtor
    sold_out_dir = normalized_realtor / args.sold_dataset

    active_summary = normalize_dataset(
        raw_dirs=active_raw_dirs,
        out_dir=active_out_dir,
        with_details=args.details,
        snapshot=args.snapshot,
        province_filter=args.province_filter,
        debug=args.debug,
    )
    sold_summary = normalize_dataset(
        raw_dirs=sold_raw_dirs,
        out_dir=sold_out_dir,
        with_details=args.details,
        snapshot=False,
        province_filter=args.province_filter,
        debug=args.debug,
    )

    if args.debug:
        print(
            "[normalize] active listings="
            f"{active_summary.get('unique_listing_ids')} raw_dirs={len(active_summary.get('raw_dirs', []))}"
        )
        print(
            "[normalize] sold listings="
            f"{sold_summary.get('unique_listing_ids')} raw_dirs={len(sold_summary.get('raw_dirs', []))}"
        )


if __name__ == "__main__":
    main()
