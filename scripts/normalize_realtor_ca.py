#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

# Ensure project root is on sys.path when running as a script.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fetchers.output import ensure_parent, iso_now, write_json


PAGE_RE = re.compile(r"page_(\d+)")


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize Realtor.ca search pages into JSONL."
    )
    parser.add_argument(
        "--raw",
        default="data/raw/realtor_ca",
        help="Raw realtor_ca directory (default: data/raw/realtor_ca).",
    )
    parser.add_argument(
        "--dataset",
        help="Dataset subfolder under data/raw/realtor_ca (e.g. sold_365).",
    )
    parser.add_argument(
        "--out",
        default="data/normalized/realtor_ca",
        help="Output directory (default: data/normalized/realtor_ca).",
    )
    parser.add_argument(
        "--with-details",
        action="store_true",
        help="Embed details payloads when present.",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Keep duplicate listing IDs across pages.",
    )
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Write a timestamped snapshot copy of listings.jsonl.",
    )
    parser.add_argument(
        "--snapshot-dir",
        help="Override snapshot directory (default: <out>/snapshots).",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    raw_dir = Path(args.raw)
    if args.dataset:
        raw_dir = raw_dir / args.dataset
    out_dir = Path(args.out)
    if args.dataset and args.out == "data/normalized/realtor_ca":
        out_dir = out_dir / args.dataset
    search_dir = raw_dir / "search"
    details_dir = raw_dir / "details"

    if not search_dir.exists():
        raise SystemExit(f"Missing search directory: {search_dir}")

    details_map: Dict[str, Dict[str, Any]] = {}
    if args.with_details:
        details_map = load_details(details_dir)

    listings = collect_listings(
        search_dir,
        details_map=details_map,
        dedupe=not args.no_dedupe,
    )

    write_jsonl(out_dir / "listings.jsonl", listings)

    summary = build_summary(
        listings,
        search_dir=search_dir,
        details_map=details_map,
        dedupe=not args.no_dedupe,
    )
    write_json(out_dir / "summary.json", summary)

    if args.snapshot:
        snapshot_dir = Path(args.snapshot_dir) if args.snapshot_dir else (out_dir / "snapshots")
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        snapshot_path = snapshot_dir / f"listings_{timestamp}.jsonl"
        write_jsonl(snapshot_path, listings)


if __name__ == "__main__":
    main()
