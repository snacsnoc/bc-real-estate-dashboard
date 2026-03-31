#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

import requests

# Ensure project root is on sys.path when running as a script.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fetchers.http import build_session
from fetchers.output import ensure_parent, iso_now, write_json


BASE_URL = "https://www.interiorrealtors.ca/files/"
MONTHS = [
    "JANUARY",
    "FEBRUARY",
    "MARCH",
    "APRIL",
    "MAY",
    "JUNE",
    "JULY",
    "AUGUST",
    "SEPTEMBER",
    "OCTOBER",
    "NOVEMBER",
    "DECEMBER",
]
FILENAME_PATTERNS = [
    "{month} {year} DATA RELEASE - KOOTENAY MEDIA RELEASE.pdf",
    "{month} {year} DATA RELEASE - MEDIA RELEASE KOOTENAY.pdf",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Interior REALTORS Kootenay media release PDFs."
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="Oldest year to attempt (inclusive).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2025,
        help="Newest year to attempt (inclusive).",
    )
    parser.add_argument(
        "--out-dir",
        default="data/raw/interior_realtors/media_releases",
        help="Directory to save downloaded PDFs.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Sleep between requests (seconds).",
    )
    parser.add_argument("--debug", action="store_true", help="Verbose output.")
    return parser.parse_args()


def build_urls(year: int, month: str) -> List[Dict[str, str]]:
    urls = []
    for pattern in FILENAME_PATTERNS:
        filename = pattern.format(month=month, year=year)
        url = f"{BASE_URL}{quote(filename)}"
        urls.append({"filename": filename, "url": url})
    return urls


def fetch_pdf(session: requests.Session, url: str, timeout: int = 30) -> bytes | None:
    try:
        response = session.get(url, timeout=timeout)
    except requests.RequestException:
        return None
    if response.status_code == 200:
        content = response.content
        content_type = response.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not content.startswith(b"%PDF-"):
            return None
        if not content.startswith(b"%PDF-"):
            return None
        return content
    if response.status_code in (403, 404):
        return None
    response.raise_for_status()
    return None


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    ensure_parent(out_dir / "placeholder.txt")
    session = build_session()

    downloaded: List[Dict[str, str]] = []
    skipped: List[Dict[str, str]] = []

    for year in range(args.end_year, args.start_year - 1, -1):
        for month in MONTHS:
            for entry in build_urls(year, month):
                filename = entry["filename"]
                url = entry["url"]
                destination = out_dir / filename
                if destination.exists():
                    skipped.append({"url": url, "file": str(destination), "reason": "exists"})
                    if args.debug:
                        print(f"[media] exists {destination.name}")
                    continue
                content = fetch_pdf(session, url)
                if content is None:
                    skipped.append({"url": url, "file": str(destination), "reason": "missing_or_not_pdf"})
                    if args.debug:
                        print(f"[media] missing {url}")
                    time.sleep(args.sleep)
                    continue
                destination.write_bytes(content)
                downloaded.append({"url": url, "file": str(destination)})
                if args.debug:
                    print(f"[media] downloaded {destination.name}")
                time.sleep(args.sleep)

    manifest = {
        "generated_at": iso_now(),
        "start_year": args.start_year,
        "end_year": args.end_year,
        "downloaded": downloaded,
        "skipped": skipped,
    }
    manifest_path = out_dir / "manifest.json"
    write_json(manifest_path, manifest)

    if args.debug:
        print(f"[media] wrote {manifest_path}")


if __name__ == "__main__":
    main()
