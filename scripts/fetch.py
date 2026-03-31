#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Ensure project root is on sys.path when running as a script.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fetchers import boc_valet, crea_stats, interior_realtors, realtor_ca, remax_ca, statcan_wds
from fetchers.output import iso_now, write_json

BBox = Tuple[float, float, float, float]
ExtraParams = Dict[str, str]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch raw data for the BC real estate dashboard."
    )
    parser.add_argument(
        "--out",
        default="data/raw",
        help="Output directory for raw downloads (default: data/raw).",
    )

    subparsers = parser.add_subparsers(dest="source", required=True)

    interior = subparsers.add_parser(
        "interior-realtors", help="Fetch Interior REALTORS PDF links."
    )
    interior.add_argument(
        "--download-pdfs",
        action="store_true",
        help="Download PDFs into the output directory.",
    )

    subparsers.add_parser("crea", help="Fetch CREA stats pages as HTML.")

    boc = subparsers.add_parser("boc", help="Fetch Bank of Canada Valet data.")
    boc.add_argument(
        "--group",
        action="append",
        default=None,
        help="Valet group name to fetch (repeatable).",
    )
    boc.add_argument(
        "--series",
        action="append",
        default=None,
        help="Valet series ID to fetch (repeatable).",
    )

    statcan = subparsers.add_parser(
        "statcan", help="Fetch StatCan WDS vector range."
    )
    statcan.add_argument(
        "--vector",
        type=int,
        required=True,
        help="Vector ID for the StatCan series.",
    )
    statcan.add_argument(
        "--start",
        required=True,
        help="Start reference period (e.g. 2015-01).",
    )
    statcan.add_argument(
        "--end",
        required=True,
        help="End reference period (e.g. 2025-01).",
    )

    realtor = subparsers.add_parser(
        "realtor", help="Fetch Realtor.ca search results for a bounding box."
    )
    realtor.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("LAT_MIN", "LAT_MAX", "LON_MIN", "LON_MAX"),
        help="Bounding box coordinates for the search.",
    )
    realtor.add_argument(
        "--place",
        help="Place name for OSM geocoding (alternative to --bbox).",
    )
    realtor.add_argument(
        "--records-per-page",
        type=int,
        default=200,
        help="Records per page (default: 200).",
    )
    realtor.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="Number of pages to fetch (default: 1).",
    )
    realtor.add_argument(
        "--all-pages",
        action="store_true",
        help="Fetch all pages based on API paging metadata.",
    )
    realtor.add_argument(
        "--price-min",
        type=int,
        default=0,
        help="Minimum price filter (default: 0).",
    )
    realtor.add_argument(
        "--price-max",
        type=int,
        default=10000000,
        help="Maximum price filter (default: 10000000).",
    )
    realtor.add_argument(
        "--transaction-type",
        choices=["for_sale", "for_rent"],
        default="for_sale",
        help="Transaction type (default: for_sale).",
    )
    realtor.add_argument(
        "--sort",
        choices=["listing_date_posted", "listing_price"],
        default="listing_date_posted",
        help="Sort field (default: listing_date_posted).",
    )
    realtor.add_argument(
        "--ascending",
        action="store_true",
        help="Sort ascending (default: descending).",
    )
    realtor.add_argument(
        "--max-results",
        type=int,
        default=600,
        help="Maximum results hint for the API (default: 600).",
    )
    realtor.add_argument(
        "--sold-within-days",
        type=int,
        help="Fetch sold listings within N days (sets SoldWithinDays).",
    )
    realtor.add_argument(
        "--sold-any",
        action="store_true",
        help="Remove SoldWithinDays filter for sold listings.",
    )
    realtor.add_argument(
        "--listed-within-days",
        type=int,
        help="Filter listings by NumberOfDays (listed since N days).",
    )
    realtor.add_argument(
        "--dataset",
        help="Store output under data/raw/realtor_ca/<dataset>/...",
    )
    realtor.add_argument(
        "--details",
        action="store_true",
        help="Fetch per-listing detail payloads.",
    )
    realtor.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Sleep seconds between requests (default: 0).",
    )
    realtor.add_argument(
        "--param",
        action="append",
        default=[],
        help="Extra API param in key=value form (repeatable).",
    )
    realtor.add_argument(
        "--cookie",
        help="Raw Cookie header value for realtor.ca requests.",
    )
    realtor.add_argument(
        "--cookie-file",
        help="Path to a file containing a raw Cookie header value.",
    )
    realtor.add_argument(
        "--user-agent",
        help="Override User-Agent header.",
    )
    realtor.add_argument(
        "--accept-language",
        help="Override Accept-Language header.",
    )
    realtor.add_argument(
        "--zoom-level",
        type=int,
        default=9,
        help="Map zoom level hint (default: 9).",
    )
    realtor.add_argument(
        "--sub-area",
        help="Realtor.ca sub-area search text (optional, for discovery).",
    )

    remax = subparsers.add_parser("remax", help="Fetch RE/MAX Canada listings.")
    remax.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("LAT_MIN", "LAT_MAX", "LON_MIN", "LON_MAX"),
        help="Bounding box coordinates for the search.",
    )
    remax.add_argument(
        "--place",
        help="Place name for OSM geocoding (alternative to --bbox).",
    )
    remax.add_argument(
        "--from",
        dest="from_index",
        type=int,
        default=0,
        help="Offset for results (default: 0).",
    )
    remax.add_argument(
        "--size",
        type=int,
        default=20,
        help="Results per page (default: 20).",
    )
    remax.add_argument(
        "--zoom",
        type=int,
        default=12,
        help="Zoom level hint (default: 12).",
    )
    remax.add_argument(
        "--sort-key",
        type=int,
        default=1,
        help="Sort key (default: 1).",
    )
    remax.add_argument(
        "--sort-direction",
        type=int,
        default=0,
        help="Sort direction (default: 0).",
    )
    remax.add_argument(
        "--exclude-type",
        action="append",
        type=int,
        default=[],
        help="Exclude listing type ID (repeatable).",
    )
    remax.add_argument(
        "--all-pages",
        action="store_true",
        help="Fetch all pages based on totalHits.",
    )
    remax.add_argument(
        "--max-pages",
        type=int,
        help="Maximum pages to fetch when --all-pages is set.",
    )
    remax.add_argument(
        "--user-agent",
        help="Override User-Agent header.",
    )
    remax.add_argument(
        "--accept-language",
        help="Override Accept-Language header.",
    )
    remax.add_argument(
        "--debug",
        action="store_true",
        help="Print debug output.",
    )
    remax.add_argument(
        "--param",
        action="append",
        default=[],
        help="Extra API param in key=value form (repeatable).",
    )
    remax.add_argument(
        "--coming-soon",
        action="store_true",
        help="Include Coming Soon listings (features.comingSoon=true).",
    )
    remax.add_argument(
        "--dataset",
        help="Store output under data/raw/remax/<dataset>/...",
    )

    return parser


def normalize_cookie(cookie: str) -> str:
    """Validate and flatten a Cookie header into a single ASCII line."""
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
    if not values:
        return None
    if len(values) != 4:
        raise SystemExit("--bbox requires four values: LAT_MIN LAT_MAX LON_MIN LON_MAX")
    return tuple(values)  # type: ignore[return-value]


def parse_extra_params(items: List[str]) -> ExtraParams:
    params: ExtraParams = {}
    for item in items:
        if "=" not in item:
            raise SystemExit("Extra param must be in key=value form.")
        key, value = item.split("=", 1)
        params[key] = value
    return params


def resolve_cookie(raw: Optional[str], cookie_file: Optional[str]) -> Optional[str]:
    cookie = raw
    if not cookie and cookie_file:
        cookie = Path(cookie_file).read_text(encoding="utf-8").strip()
    if cookie and cookie.lower().startswith("cookie:"):
        cookie = cookie.split(":", 1)[1].strip()
    if cookie:
        cookie = normalize_cookie(cookie)
    return cookie


def resolve_bbox(bbox: Optional[BBox], place: Optional[str]) -> Optional[BBox]:
    if bbox:
        return bbox
    if place:
        return realtor_ca.geocode_bbox(place)
    return None


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    out_dir = Path(args.out)

    if args.source == "interior-realtors":
        interior_realtors.fetch_all(out_dir, download=args.download_pdfs)
        return

    if args.source == "crea":
        crea_stats.fetch_pages(out_dir)
        return

    if args.source == "boc":
        groups: Optional[List[str]] = args.group if args.group else None
        series: Optional[List[str]] = args.series if args.series else None
        boc_valet.fetch_all(out_dir, groups=groups, series=series)
        return

    if args.source == "statcan":
        statcan_wds.fetch_vector_range(
            out_dir,
            vector_id=args.vector,
            start_ref_period=args.start,
            end_ref_period=args.end,
        )
        return

    if args.source == "realtor":
        bbox = resolve_bbox(parse_bbox(args.bbox), args.place)
        cookie = resolve_cookie(args.cookie, args.cookie_file)
        extra_params = parse_extra_params(args.param)

        if args.sub_area:
            client = realtor_ca.RealtorCaClient(
                cookie=cookie,
                user_agent=args.user_agent,
                accept_language=args.accept_language,
            )
            result = client.sub_area_search(args.sub_area, page=1)
            out_dir_path = Path(args.out)
            write_json(
                out_dir_path / "realtor_ca" / "sub_area_search.json",
                {"fetched_at": iso_now(), "query": args.sub_area, "data": result},
            )
            if bbox is None:
                return

        if bbox is None:
            raise SystemExit("Provide --bbox or --place for realtor search.")

        realtor_ca.fetch_search_pages(
            out_dir,
            bbox=bbox,  # type: ignore[arg-type]
            cookie=cookie,
            user_agent=args.user_agent,
            accept_language=args.accept_language,
            dataset=args.dataset,
            max_pages=args.max_pages,
            all_pages=args.all_pages,
            records_per_page=args.records_per_page,
            price_min=args.price_min,
            price_max=args.price_max,
            transaction_type=args.transaction_type,
            sort=args.sort,
            ascending=args.ascending,
            max_results=args.max_results,
            extra_params=extra_params or None,
            zoom_level=args.zoom_level,
            sold_within_days=None if args.sold_any else args.sold_within_days,
            listed_within_days=args.listed_within_days,
            sleep_seconds=args.sleep,
            include_details=args.details,
        )
        return

    if args.source == "remax":
        bbox = resolve_bbox(parse_bbox(args.bbox), args.place)
        if bbox is None:
            raise SystemExit("Provide --bbox or --place for remax search.")

        excluded = args.exclude_type or None
        extra_params = parse_extra_params(args.param)
        if args.coming_soon:
            extra_params["features.comingSoon"] = True

        remax_ca.fetch_gallery(
            out_dir,
            bbox=bbox,  # type: ignore[arg-type]
            from_index=args.from_index,
            size=args.size,
            zoom=args.zoom,
            sort_key=args.sort_key,
            sort_direction=args.sort_direction,
            excluded_listing_type_ids=excluded,
            all_pages=args.all_pages,
            max_pages=args.max_pages,
            user_agent=args.user_agent,
            accept_language=args.accept_language,
            debug=args.debug,
            extra_params=extra_params or None,
            dataset=args.dataset,
        )
        return


if __name__ == "__main__":
    main()
