#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse

# Ensure project root is on sys.path when running as a script.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fetchers.output import ensure_parent, iso_now, write_json

try:
    import pdfplumber
except ImportError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "pdfplumber is required. Install with: pip install pdfplumber"
    ) from exc

StatsRecord = Dict[str, Any]
ParseOutcome = Tuple[Optional[StatsRecord], Optional[str]]
LinksMap = Dict[str, str]
Records = List[StatsRecord]
SkippedRecords = List[Dict[str, str]]


MONTH_MAP = {
    "JANUARY": 1,
    "FEBRUARY": 2,
    "MARCH": 3,
    "APRIL": 4,
    "MAY": 5,
    "JUNE": 6,
    "JULY": 7,
    "AUGUST": 8,
    "SEPTEMBER": 9,
    "SEPT": 9,
    "OCTOBER": 10,
    "NOVEMBER": 11,
    "DECEMBER": 12,
}

MONTH_RE = re.compile(
    r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|SEPT|"
    r"OCTOBER|NOVEMBER|DECEMBER)\s+(\d{4})",
    re.IGNORECASE,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse Interior REALTORS Kootenay media release PDFs."
    )
    parser.add_argument(
        "--pdf-dir",
        default="data/raw/interior_realtors/media_releases",
        help="Directory containing PDFs to parse.",
    )
    parser.add_argument(
        "--pdf-glob",
        default="*.pdf",
        help="Glob pattern for PDFs inside --pdf-dir.",
    )
    parser.add_argument(
        "--links",
        default="data/raw/interior_realtors/kootenay_links.json",
        help="Optional link list to map filenames to source URLs.",
    )
    parser.add_argument(
        "--out-normalized",
        default="data/normalized/interior_realtors/kootenay_monthly.jsonl",
        help="Output JSONL for normalized monthly stats.",
    )
    parser.add_argument(
        "--out-derived",
        default="data/derived/interior_realtors/kootenay_market_stats.json",
        help="Output JSON summary with MOI and SNLR.",
    )
    parser.add_argument("--debug", action="store_true", help="Verbose output.")
    return parser.parse_args()


def load_json(path: Path) -> Dict[str, Any]:
    """Read JSON into a dict."""
    return json.loads(path.read_text(encoding="utf-8"))


def write_jsonl(path: Path, items: Iterable[Dict[str, Any]]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        for item in items:
            handle.write(json.dumps(item, ensure_ascii=True))
            handle.write("\n")


def parse_number(value: str) -> Optional[int]:
    """Parse an integer, ignoring commas; return None on failure."""
    value = value.replace(",", "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    """Parse a float, ignoring currency formatting; return None on failure."""
    value = value.replace("$", "").replace(",", "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_month_label(text: str) -> Optional[Tuple[str, int, int]]:
    match = MONTH_RE.search(text)
    if not match:
        return None
    month_name = match.group(1).upper()
    year = int(match.group(2))
    month = MONTH_MAP.get(month_name)
    if not month:
        return None
    return f"{month_name} {year}", year, month


def parse_month_from_filename(path: Path) -> Optional[Tuple[str, int, int]]:
    stem = path.stem.replace("_", " ")
    return parse_month_label(stem)


def parse_summary_line(line: str) -> Optional[Dict[str, Any]]:
    """Parse the summary line that follows a KOOTENAY header."""
    tokens = line.replace("$", "$").split()
    if len(tokens) < 4:
        return None
    sold = parse_number(tokens[0])
    dollar_volume = parse_float(tokens[1])
    active = parse_number(tokens[2])
    new = parse_number(tokens[3])
    if sold is None or dollar_volume is None or active is None or new is None:
        return None
    return {
        "sold_units": sold,
        "dollar_volume_millions": dollar_volume,
        "active_listings": active,
        "new_listings": new,
        "raw_line": line,
    }


def extract_kootenay_stats(text: str) -> Optional[Dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for idx, line in enumerate(lines):
        if line.upper() != "KOOTENAY":
            continue
        for offset in range(1, 6):
            if idx + offset >= len(lines):
                break
            candidate = lines[idx + offset]
            if "%" in candidate:
                continue
            if not re.search(r"\d", candidate):
                continue
            parsed = parse_summary_line(candidate)
            if parsed:
                return parsed
    return None


def read_pdf_text(path: Path) -> tuple[str, Optional[str]]:
    """Return first-page text and an optional error code."""
    try:
        with path.open("rb") as handle:
            header = handle.read(5)
        if header != b"%PDF-":
            return "", "not_pdf_header"
        with pdfplumber.open(path) as pdf:
            if not pdf.pages:
                return "", "no_pages"
            return pdf.pages[0].extract_text() or "", None
    except Exception:
        return "", "invalid_pdf"


def build_links_map(path: Path) -> LinksMap:
    if not path.exists():
        return {}
    payload = load_json(path)
    entries: List[Dict[str, Any]] = []
    for section in ("historical", "latest"):
        data = payload.get(section) or {}
        entries.extend(data.get("pdf_links") or [])
    mapping: Dict[str, str] = {}
    for entry in entries:
        url = entry.get("url") or ""
        if not url:
            continue
        filename = Path(unquote(urlparse(url).path)).name
        if filename:
            mapping[filename] = url
    return mapping


def parse_pdf(
    path: Path,
    *,
    links_map: LinksMap,
    debug: bool,
) -> ParseOutcome:
    text, error = read_pdf_text(path)
    if error:
        return None, error
    if not text:
        return None, "empty"
    month_label = parse_month_label(text)
    if not month_label:
        month_label = parse_month_from_filename(path)
    if not month_label:
        return None, "missing_month"
    month_text, year, month = month_label
    stats = extract_kootenay_stats(text)
    if not stats:
        return None, "missing_kootenay"
    reference_month = f"{year:04d}-{month:02d}"
    moi = None
    if stats["sold_units"]:
        moi = stats["active_listings"] / stats["sold_units"]
    snlr = None
    if stats["new_listings"]:
        snlr = stats["sold_units"] / stats["new_listings"]
    record = {
        "reference_month": reference_month,
        "year": year,
        "month": month,
        "month_label": month_text,
        "sold_units": stats["sold_units"],
        "dollar_volume_millions": stats["dollar_volume_millions"],
        "dollar_volume": stats["dollar_volume_millions"] * 1_000_000,
        "active_listings": stats["active_listings"],
        "new_listings": stats["new_listings"],
        "moi": moi,
        "snlr": snlr,
        "source": {
            "file": str(path),
            "url": links_map.get(path.name),
        },
        "raw_line": stats["raw_line"],
        "parsed_at": iso_now(),
    }
    if debug:
        print(f"[interior] {path.name}: {reference_month} sold={stats['sold_units']}")
    return record, None


def main() -> None:
    args = parse_args()
    pdf_dir = Path(args.pdf_dir)
    pdfs = sorted(pdf_dir.glob(args.pdf_glob))
    if not pdfs:
        raise SystemExit(f"No PDFs found in {pdf_dir} (pattern {args.pdf_glob}).")

    links_map = build_links_map(Path(args.links))
    records: List[Dict[str, Any]] = []
    skipped: List[Dict[str, str]] = []

    for pdf in pdfs:
        record, reason = parse_pdf(pdf, links_map=links_map, debug=args.debug)
        if record:
            records.append(record)
        else:
            skipped.append({"file": str(pdf), "reason": reason or "unknown"})
            if args.debug:
                print(f"[interior] skip {pdf.name} reason={reason}")

    records.sort(key=lambda item: item["reference_month"])

    normalized_path = Path(args.out_normalized)
    derived_path = Path(args.out_derived)
    write_jsonl(normalized_path, records)

    summary = {
        "generated_at": iso_now(),
        "records": records,
        "record_count": len(records),
        "skipped": skipped,
    }
    write_json(derived_path, summary)

    if args.debug:
        print(f"[interior] wrote {normalized_path}")
        print(f"[interior] wrote {derived_path}")


if __name__ == "__main__":
    main()
