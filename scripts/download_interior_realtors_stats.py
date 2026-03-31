#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote, unquote

import requests

# Ensure project root is on sys.path when running as a script.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fetchers.http import build_session
from fetchers.output import ensure_parent, iso_now, write_json

LinkEntry = Dict[str, str]
Manifest = Dict[str, object]


BASE_URL = "https://www.interiorrealtors.ca/files/"
FILES_PREFIX = "https://www.interiorrealtors.ca/files/"
MONTH_ABBRS: Dict[int, List[str]] = {
    1: ["Jan", "January"],
    2: ["Feb", "February"],
    3: ["Mar", "March"],
    4: ["Apr", "April"],
    5: ["May"],
    6: ["Jun", "June"],
    7: ["Jul", "July"],
    8: ["Aug", "August"],
    9: ["Sept", "Sep", "September"],
    10: ["Oct", "October"],
    11: ["Nov", "November"],
    12: ["Dec", "December"],
}

YEAR_RE = re.compile(r"(20\d{2})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Interior REALTORS Kootenay monthly stats PDFs."
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
        default="data/raw/interior_realtors/monthly_stats",
        help="Directory to save downloaded PDFs.",
    )
    parser.add_argument(
        "--links",
        default="data/raw/interior_realtors/kootenay_links.json",
        help="Optional link list to seed downloads.",
    )
    parser.add_argument(
        "--url-list",
        default="interiorrealtors.ca.urls",
        help="Optional URL list to seed downloads (gau output).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Sleep between requests (seconds).",
    )
    parser.add_argument("--debug", action="store_true", help="Verbose output.")
    return parser.parse_args()


def build_urls(year: int, month: int) -> List[Dict[str, str]]:
    """Construct likely monthly stats filenames for a given month/year."""
    urls = []
    for abbr in MONTH_ABBRS.get(month, []):
        filename = f"{month:02d}-KO Statistics-{abbr}{year}.pdf"
        url = f"{BASE_URL}{quote(filename)}"
        urls.append({"filename": filename, "url": url})
    return urls


def parse_year_from_name(name: str) -> Optional[int]:
    matches = YEAR_RE.findall(name)
    if not matches:
        return None
    try:
        return int(matches[-1])
    except ValueError:
        return None


def load_links(path: Path, start_year: int, end_year: int) -> List[LinkEntry]:
    """Extract candidate URLs from the saved links manifest."""
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries: List[Dict[str, str]] = []
    for section in ("historical", "latest"):
        links = (payload.get(section) or {}).get("pdf_links") or []
        for link in links:
            url = link.get("url") or ""
            if "KO%20Statistics" not in url:
                continue
            filename = unquote(Path(url).name)
            year = parse_year_from_name(filename)
            if year is None or year < start_year or year > end_year:
                continue
            entries.append({"filename": filename, "url": url})
    return entries


def load_url_list(path: Path, start_year: int, end_year: int) -> List[LinkEntry]:
    """Extract candidate URLs from a flat text list (gau output)."""
    if not path.exists():
        return []
    entries: List[Dict[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or not line.startswith(FILES_PREFIX):
            continue
        if "KO%20Statistics" not in line and "KO Statistics" not in line:
            continue
        filename = unquote(Path(line).name)
        year = parse_year_from_name(filename)
        if year is None or year < start_year or year > end_year:
            continue
        entries.append({"filename": filename, "url": line})
    return entries


def fetch_pdf(
    session: requests.Session, url: str, timeout: int = 30
) -> tuple[bytes | None, Optional[int], str]:
    """Download a PDF and validate header/content type."""
    try:
        response = session.get(url, timeout=timeout)
    except requests.RequestException:
        return None, None, "request_error"
    status = response.status_code
    if response.status_code == 200:
        content = response.content
        content_type = response.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not content.startswith(b"%PDF-"):
            return None, status, "not_pdf"
        if not content.startswith(b"%PDF-"):
            return None, status, "not_pdf_header"
        return content, status, "ok"
    if response.status_code in (403, 404):
        return None, status, "missing"
    return None, status, f"status_{status}"


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    ensure_parent(out_dir / "placeholder.txt")
    session = build_session()

    downloaded: List[Dict[str, str]] = []
    skipped: List[Dict[str, str]] = []
    attempted = 0

    if args.debug:
        print(
            f"[stats] output_dir={out_dir} range={args.start_year}-{args.end_year}"
        )

    link_entries = load_links(Path(args.links), args.start_year, args.end_year)
    url_list_entries = load_url_list(Path(args.url_list), args.start_year, args.end_year)

    if args.debug:
        print(f"[stats] link_seed={args.links} matched={len(link_entries)}")
        print(f"[stats] url_list={args.url_list} matched={len(url_list_entries)}")

    candidates = link_entries + url_list_entries
    if not candidates:
        if args.debug:
            print("[stats] no seeded links; using filename patterns")
        for year in range(args.end_year, args.start_year - 1, -1):
            for month in range(1, 13):
                candidates.extend(build_urls(year, month))

    seen = set()
    deduped: List[Dict[str, str]] = []
    for entry in candidates:
        key = entry["url"]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)

    for entry in deduped:
        filename = entry["filename"]
        url = entry["url"]
        destination = out_dir / filename
        attempted += 1
        if destination.exists():
            skipped.append({"url": url, "file": str(destination), "reason": "exists"})
            if args.debug:
                print(f"[stats] exists {destination.name}")
            continue
        content, status, reason = fetch_pdf(session, url)
        if content is None:
            skipped.append(
                {
                    "url": url,
                    "file": str(destination),
                    "reason": reason,
                    "status": str(status) if status is not None else None,
                }
            )
            if args.debug:
                print(
                    f"[stats] skip {filename} status={status} reason={reason}"
                )
            time.sleep(args.sleep)
            continue
        destination.write_bytes(content)
        downloaded.append({"url": url, "file": str(destination)})
        if args.debug:
            size_kb = len(content) / 1024
            print(
                f"[stats] downloaded {filename} status={status} size_kb={size_kb:.1f}"
            )
        time.sleep(args.sleep)

    manifest = {
        "generated_at": iso_now(),
        "start_year": args.start_year,
        "end_year": args.end_year,
        "attempted": attempted,
        "downloaded_count": len(downloaded),
        "skipped_count": len(skipped),
        "downloaded": downloaded,
        "skipped": skipped,
    }
    manifest_path = out_dir / "manifest.json"
    write_json(manifest_path, manifest)

    if args.debug:
        print(f"[stats] wrote {manifest_path}")


if __name__ == "__main__":
    main()
