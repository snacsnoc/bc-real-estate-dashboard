#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# Ensure project root is on sys.path when running as a script.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fetchers import remax_ca
from fetchers.output import ensure_parent, iso_now, write_json

BBox = Tuple[float, float, float, float]
ExtraParams = Dict[str, str]

PAGE_RE = re.compile(r"page_(\d+)")


def parse_bbox(values: Optional[List[float]]) -> Optional[BBox]:
    """Parse a bounding box argument list into a tuple."""
    if not values:
        return None
    if len(values) != 4:
        raise SystemExit("--bbox requires four values: LAT_MIN LAT_MAX LON_MIN LON_MAX")
    return tuple(values)  # type: ignore[return-value]


def geocode_bbox(place: str) -> BBox:
    """Geocode a place name using Nominatim."""
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


def parse_params(items: List[str]) -> Dict[str, str]:
    """Parse repeated key=value params from CLI."""
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


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))
    return items


def parse_page_number(path: Path) -> int:
    match = PAGE_RE.search(path.stem)
    if not match:
        return 0
    return int(match.group(1))


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_price(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def price_band(value: Optional[int]) -> str:
    if value is None:
        return "Unknown"
    bands = [
        (0, 250_000, "0-250k"),
        (250_000, 500_000, "250k-500k"),
        (500_000, 750_000, "500k-750k"),
        (750_000, 1_000_000, "750k-1.0M"),
        (1_000_000, 1_500_000, "1.0M-1.5M"),
        (1_500_000, 2_000_000, "1.5M-2.0M"),
        (2_000_000, 3_000_000, "2.0M-3.0M"),
        (3_000_000, 5_000_000, "3.0M-5.0M"),
        (5_000_000, None, "5.0M+"),
    ]
    for low, high, label in bands:
        if low is not None and value < low:
            continue
        if high is not None and value >= high:
            continue
        return label
    return "Unknown"


def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def read_total_hits(raw_dir: Path) -> Optional[int]:
    page = raw_dir / "gallery" / "page_0.json"
    if not page.exists():
        return None
    data = load_json(page)
    result = (data.get("data") or {}).get("result") or {}
    try:
        return int(result.get("totalHits"))
    except (TypeError, ValueError):
        return None


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
    tile_threshold: int,
    debug: bool,
) -> List[Path]:
    raw_dirs: List[Path] = []
    stats = {"tiles": 0, "split": 0}
    dataset_prefix = dataset or "remax"

    def fetch_tile(tile_bbox: BBox, label: str, depth: int) -> None:
        dataset_name = dataset_prefix
        if label:
            dataset_name = f"{dataset_prefix}_{label}"

        if debug:
            print(f"[remax] dataset={dataset_name} bbox={tile_bbox} depth={depth}")

        remax_ca.fetch_gallery(
            raw_root,
            bbox=tile_bbox,
            dataset=dataset_name,
            **fetch_kwargs,
        )

        raw_dir = raw_root / "remax" / dataset_name
        total_hits = read_total_hits(raw_dir)
        stats["tiles"] += 1

        if debug:
            print(f"[remax] {dataset_name} total_hits={total_hits}")

        if total_hits is not None and total_hits > tile_threshold and depth < max_depth:
            stats["split"] += 1
            tiles = split_bbox(tile_bbox, tile_rows, tile_cols)
            for child_bbox, child_label in tiles:
                fetch_tile(child_bbox, f"{label}_{child_label}" if label else child_label, depth + 1)
            return

        raw_dirs.append(raw_dir)

    fetch_tile(bbox, "", 0)
    if debug:
        print(f"[remax] tiles={stats['tiles']} splits={stats['split']} max_depth={max_depth}")
    return raw_dirs


def normalize_listing(
    listing: Dict[str, Any],
    *,
    source: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "id": listing.get("listingId"),
        "listing_id": listing.get("listingId"),
        "mls_num": listing.get("mlsNum"),
        "status": listing.get("status"),
        "address": listing.get("address"),
        "city": listing.get("city") or listing.get("mlsCity"),
        "province": listing.get("province") or listing.get("mlsProvince"),
        "postal_code": listing.get("postalCode") or listing.get("mlsPostalCode"),
        "lat": listing.get("lat"),
        "lng": listing.get("lng"),
        "list_price": listing.get("listPrice"),
        "beds": listing.get("beds"),
        "baths": listing.get("baths"),
        "listing_date": listing.get("listingDate"),
        "last_updated": listing.get("lastUpdated"),
        "detail_url": listing.get("detailUrl"),
        "image_urls": listing.get("imageUrls") or [],
        "is_luxury": listing.get("isLuxury"),
        "is_commercial": listing.get("isCommercial"),
        "is_remax_listing": listing.get("isRemaxListing"),
        "board_name": listing.get("boardName"),
        "remax_office_name": listing.get("remaxOfficeName"),
        "sqft": listing.get("sqFtSearch"),
        "source": source,
        "raw": listing,
    }


def normalize_dataset(
    *,
    raw_dirs: List[Path],
    out_dir: Path,
    province_filter: Optional[str],
    snapshot: bool,
    dedupe: bool = True,
    debug: bool = False,
) -> Dict[str, Any]:
    listings: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    for raw_dir in raw_dirs:
        gallery_dir = raw_dir / "gallery"
        if not gallery_dir.exists():
            raise SystemExit(f"Missing gallery directory: {gallery_dir}")

        page_files = sorted(gallery_dir.glob("page_*.json"), key=parse_page_number)
        for page_file in page_files:
            payload = load_json(page_file)
            data = payload.get("data") or {}
            result = data.get("result") or {}
            results = result.get("results") or []
            source = {
                "page": parse_page_number(page_file),
                "page_file": str(page_file),
                "fetched_at": payload.get("fetched_at"),
                "params": payload.get("params"),
            }
            for listing in results:
                item = normalize_listing(listing, source=source)
                listing_id = str(item.get("id") or "").strip()
                if dedupe and listing_id and listing_id in seen_ids:
                    continue
                if listing_id:
                    seen_ids.add(listing_id)
                listings.append(item)

    if province_filter:
        expected = province_filter.strip().lower()
        before = len(listings)
        listings = [
            item
            for item in listings
            if str(item.get("province") or "").strip().lower() == expected
        ]
        if debug:
            print(
                f"[remax] province_filter='{province_filter}' kept={len(listings)} removed={before - len(listings)}"
            )

    out_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(out_dir / "listings.jsonl", listings)

    unique_ids = len({str(item.get("id") or "").strip() for item in listings if item.get("id")})
    summary = {
        "generated_at": iso_now(),
        "listings": len(listings),
        "unique_listing_ids": unique_ids,
        "raw_dirs": [str(path) for path in raw_dirs],
    }
    write_json(out_dir / "summary.json", summary)

    if snapshot:
        snapshot_dir = out_dir / "snapshots"
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        snapshot_path = snapshot_dir / f"listings_{timestamp}.jsonl"
        write_jsonl(snapshot_path, listings)

    return summary


def determine_as_of(listings: List[Dict[str, Any]]) -> datetime:
    timestamps: List[datetime] = []
    for item in listings:
        fetched_at = (item.get("source") or {}).get("fetched_at")
        dt = parse_iso_datetime(fetched_at)
        if dt:
            timestamps.append(dt)
    if not timestamps:
        return datetime.now(timezone.utc)
    return max(timestamps)


def median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    values_sorted = sorted(values)
    mid = len(values_sorted) // 2
    if len(values_sorted) % 2 == 1:
        return float(values_sorted[mid])
    return (values_sorted[mid - 1] + values_sorted[mid]) / 2


def average(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def group_counts(items: Iterable[Dict[str, Any]], key_fn) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        key = key_fn(item) or "Unknown"
        counts[key] = counts.get(key, 0) + 1
    return counts


def counts_to_list(counts: Dict[str, int]) -> List[Dict[str, Any]]:
    return [
        {"key": key, "count": count}
        for key, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]


def build_inventory_rollup(listings: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "total_listings": len(listings),
        "by_city": counts_to_list(group_counts(listings, lambda item: item.get("city"))),
        "by_province": counts_to_list(group_counts(listings, lambda item: item.get("province"))),
        "by_price_band": counts_to_list(
            group_counts(listings, lambda item: price_band(parse_price(item.get("list_price"))))
        ),
        "by_luxury": counts_to_list(
            group_counts(listings, lambda item: str(item.get("is_luxury")))
        ),
        "by_commercial": counts_to_list(
            group_counts(listings, lambda item: str(item.get("is_commercial")))
        ),
        "by_remax_listing": counts_to_list(
            group_counts(listings, lambda item: str(item.get("is_remax_listing")))
        ),
        "by_status": counts_to_list(group_counts(listings, lambda item: item.get("status"))),
    }


def build_listing_trend(listings: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_month: Dict[str, List[int]] = {}
    for item in listings:
        listing_date = parse_iso_datetime(item.get("listing_date"))
        if not listing_date:
            continue
        price = parse_price(item.get("list_price"))
        if price is None:
            continue
        key = month_key(listing_date)
        by_month.setdefault(key, []).append(price)

    series = []
    for key in sorted(by_month.keys()):
        prices = by_month[key]
        series.append(
            {
                "month": key,
                "count": len(prices),
                "median": median(prices),
                "average": average(prices),
                "min": min(prices),
                "max": max(prices),
            }
        )
    return {"by_month": series}


def build_time_on_market(listings: List[Dict[str, Any]], as_of: datetime) -> Dict[str, Any]:
    values: List[float] = []
    for item in listings:
        listing_date = parse_iso_datetime(item.get("listing_date"))
        if not listing_date:
            continue
        delta = as_of - listing_date
        values.append(delta.days + delta.seconds / 86400)

    return {
        "count": len(values),
        "median_days": median(values) if values else None,
        "average_days": average(values) if values else None,
    }


def rollup(
    *,
    listings: List[Dict[str, Any]],
    out_dir: Path,
) -> None:
    as_of = determine_as_of(listings)
    generated_at = datetime.now(timezone.utc).isoformat()

    inventory = build_inventory_rollup(listings)
    listing_trend = build_listing_trend(listings)
    time_on_market = build_time_on_market(listings, as_of)

    write_json(out_dir / "active_inventory.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **inventory,
    })
    write_json(out_dir / "listing_trend.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **listing_trend,
    })
    write_json(out_dir / "time_on_market.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **time_on_market,
    })


def list_snapshots(snapshot_dir: Path) -> List[Path]:
    return sorted(snapshot_dir.glob("listings_*.jsonl"))


def summarize_snapshot_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": item.get("id"),
        "mls_num": item.get("mls_num"),
        "status": item.get("status"),
        "list_price": item.get("list_price"),
        "address": item.get("address"),
        "city": item.get("city"),
        "province": item.get("province"),
        "listing_date": item.get("listing_date"),
    }


def build_snapshot_diff(
    snapshot_dir: Path,
    out_path: Path,
    *,
    debug: bool = False,
) -> Optional[Dict[str, Any]]:
    snapshots = list_snapshots(snapshot_dir)
    if len(snapshots) < 2:
        if debug:
            print(f"[remax] snapshot diff skipped (found {len(snapshots)} snapshots).")
        return None

    old_path, new_path = snapshots[-2], snapshots[-1]
    old_items = load_jsonl(old_path)
    new_items = load_jsonl(new_path)

    old_map = {str(item.get("id")): item for item in old_items if item.get("id")}
    new_map = {str(item.get("id")): item for item in new_items if item.get("id")}

    old_ids = set(old_map.keys())
    new_ids = set(new_map.keys())

    added_ids = sorted(new_ids - old_ids)
    removed_ids = sorted(old_ids - new_ids)

    payload = {
        "old_snapshot": str(old_path),
        "new_snapshot": str(new_path),
        "added_count": len(added_ids),
        "removed_count": len(removed_ids),
        "added": [summarize_snapshot_item(new_map[item_id]) for item_id in added_ids],
        "removed": [summarize_snapshot_item(old_map[item_id]) for item_id in removed_ids],
    }
    write_json(out_path, payload)
    if debug:
        print(
            "[remax] snapshot diff wrote "
            f"added={payload['added_count']} removed={payload['removed_count']}"
        )
    return payload


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
    candidate = Path("config/remax_pipeline.json")
    if candidate.exists():
        return candidate
    candidate = Path("remax_pipeline.json")
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
        description="Fetch RE/MAX listings and build normalized + derived outputs."
    )
    parser.add_argument(
        "--config",
        help="Path to a JSON config file (default: config/remax_pipeline.json if present).",
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
        "--out-derived",
        default="data/derived",
        help="Derived output root (default: data/derived).",
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
        "--dataset",
        help="Dataset name for output subfolder.",
    )
    parser.add_argument(
        "--from",
        dest="from_index",
        type=int,
        default=0,
        help="Offset for results (default: 0).",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=20,
        help="Results per page (default: 20).",
    )
    parser.add_argument(
        "--zoom",
        type=int,
        default=12,
        help="Zoom level hint (default: 12).",
    )
    parser.add_argument(
        "--sort-key",
        type=int,
        default=1,
        help="Sort key (default: 1).",
    )
    parser.add_argument(
        "--sort-direction",
        type=int,
        default=0,
        help="Sort direction (default: 0).",
    )
    parser.add_argument(
        "--exclude-type",
        action="append",
        type=int,
        default=[],
        help="Exclude listing type ID (repeatable).",
    )
    parser.add_argument(
        "--coming-soon",
        action="store_true",
        help="Include Coming Soon listings (features.comingSoon=true).",
    )
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        help="Extra API param in key=value form (repeatable).",
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
        "--all-pages",
        action="store_true",
        help="Fetch all pages based on totalHits.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        help="Maximum pages to fetch when --all-pages is set.",
    )
    parser.add_argument(
        "--province-filter",
        help="Filter listings by province (e.g. BC).",
    )
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Write a snapshot after normalization.",
    )
    parser.add_argument(
        "--tile-on-hits",
        action="store_true",
        help="Split bbox when totalHits exceeds a threshold.",
    )
    parser.add_argument(
        "--tile-threshold",
        type=int,
        default=2000,
        help="totalHits threshold to trigger tiling (default: 2000).",
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
        help="Print debug output.",
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
        bbox = geocode_bbox(args.place)
    if not bbox:
        raise SystemExit("Provide --bbox or --place.")

    extra_params = parse_params(args.param)
    if args.coming_soon:
        extra_params["features.comingSoon"] = "true"

    excluded = args.exclude_type or remax_ca.DEFAULT_EXCLUDED_LISTING_TYPE_IDS

    raw_root = Path(args.out_raw)
    normalized_root = Path(args.out_normalized)
    derived_root = Path(args.out_derived)

    fetch_kwargs = {
        "from_index": args.from_index,
        "size": args.size,
        "zoom": args.zoom,
        "sort_key": args.sort_key,
        "sort_direction": args.sort_direction,
        "excluded_listing_type_ids": excluded,
        "all_pages": args.all_pages,
        "max_pages": args.max_pages,
        "extra_params": extra_params or None,
        "user_agent": args.user_agent,
        "accept_language": args.accept_language,
        "debug": args.debug,
    }

    if args.debug:
        print(f"[remax] bbox={bbox} sortKey={args.sort_key} sortDirection={args.sort_direction}")
        print(f"[remax] size={args.size} all_pages={args.all_pages} max_pages={args.max_pages}")
        if args.tile_on_hits:
            print(
                f"[remax] tile_threshold={args.tile_threshold} rows={args.tile_rows} cols={args.tile_cols} max_depth={args.tile_max_depth}"
            )

    if args.tile_on_hits:
        raw_dirs = fetch_with_tiling(
            raw_root=raw_root,
            dataset=args.dataset,
            bbox=bbox,
            fetch_kwargs=fetch_kwargs,
            tile_rows=args.tile_rows,
            tile_cols=args.tile_cols,
            max_depth=args.tile_max_depth,
            tile_threshold=args.tile_threshold,
            debug=args.debug,
        )
    else:
        remax_ca.fetch_gallery(
            raw_root,
            bbox=bbox,
            dataset=args.dataset,
            **fetch_kwargs,
        )
        raw_dir = raw_root / "remax" / args.dataset if args.dataset else raw_root / "remax"
        raw_dirs = [raw_dir]
        if args.debug:
            total_hits = read_total_hits(raw_dir)
            print(f"[remax] total_hits={total_hits}")

    normalized_dir = normalized_root / "remax_ca"
    if args.dataset:
        normalized_dir = normalized_dir / args.dataset

    summary = normalize_dataset(
        raw_dirs=raw_dirs,
        out_dir=normalized_dir,
        province_filter=args.province_filter,
        snapshot=args.snapshot,
        dedupe=True,
        debug=args.debug,
    )

    if args.debug:
        print(f"[remax] normalized listings={summary.get('unique_listing_ids')}")

    derived_dir = derived_root / "remax_ca"
    if args.dataset:
        derived_dir = derived_dir / args.dataset

    listings = []
    with (normalized_dir / "listings.jsonl").open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            listings.append(json.loads(line))

    rollup(listings=listings, out_dir=derived_dir)

    diff_path = derived_dir / "diffs" / "latest_diff.json"
    build_snapshot_diff(
        normalized_dir / "snapshots",
        diff_path,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
