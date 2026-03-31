#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

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
Records = List[StatsRecord]
SkippedRecords = List[Dict[str, str]]


BASE_URL = "https://www.interiorrealtors.ca/files/"
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
    "SEP": 9,
    "OCTOBER": 10,
    "NOVEMBER": 11,
    "DECEMBER": 12,
}

MONTH_RE = re.compile(
    r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|SEPT|SEP|"
    r"OCTOBER|NOVEMBER|DECEMBER)\s+(\d{4})",
    re.IGNORECASE,
)

FILENAME_RE = re.compile(r"\d{2}-KO Statistics-([A-Za-z]+)(\d{4})")

METRIC_PATTERNS = {
    "sales": re.compile(r"^Sales\s+(.*)$", re.IGNORECASE),
    "new_listings": re.compile(r"^New Listings\s+(.*)$", re.IGNORECASE),
    "current_inventory": re.compile(r"^Current Inventory\s+(.*)$", re.IGNORECASE),
    "sell_inventory_ratio": re.compile(r"^Sell/Inv\. Ratio\s+(.*)$", re.IGNORECASE),
    "days_to_sell": re.compile(r"^Days to Sell\s+(.*)$", re.IGNORECASE),
    "average_price": re.compile(r"^Average Price\s+(.*)$", re.IGNORECASE),
    "median_price": re.compile(r"^Median Price\s+(.*)$", re.IGNORECASE),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse Interior REALTORS Kootenay monthly stats PDFs."
    )
    parser.add_argument(
        "--pdf-dir",
        default="data/raw/interior_realtors/monthly_stats",
        help="Directory containing PDFs to parse.",
    )
    parser.add_argument(
        "--pdf-glob",
        default="*.pdf",
        help="Glob pattern for PDFs inside --pdf-dir.",
    )
    parser.add_argument(
        "--out-normalized",
        default="data/normalized/interior_realtors/kootenay_monthly_stats.jsonl",
        help="Output JSONL for normalized monthly stats.",
    )
    parser.add_argument(
        "--out-derived",
        default="data/derived/interior_realtors/kootenay_monthly_stats.json",
        help="Output JSON summary with property type metrics.",
    )
    parser.add_argument("--debug", action="store_true", help="Verbose output.")
    return parser.parse_args()


def write_jsonl(path: Path, items: Iterable[Dict[str, Any]]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        for item in items:
            handle.write(json.dumps(item, ensure_ascii=True))
            handle.write("\n")


def parse_number(value: str) -> Optional[int]:
    """Parse an integer from a string with optional commas."""
    value = value.replace(",", "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_price(value: str) -> Optional[int]:
    """Parse a currency value into an int; returns None on failure."""
    value = value.replace("$", "").replace(",", "").strip()
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def parse_percent(value: str) -> Optional[float]:
    """Parse a percent string into a decimal fraction (0-1)."""
    value = value.replace("%", "").strip()
    if not value:
        return None
    try:
        return float(value) / 100.0
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
    match = FILENAME_RE.search(path.name)
    if not match:
        return None
    month_token = match.group(1).upper()
    year = int(match.group(2))
    month = MONTH_MAP.get(month_token)
    if not month:
        return None
    month_label = f"{month_token} {year}"
    return month_label, year, month


def read_pdf_text(path: Path) -> tuple[List[str], Optional[str]]:
    """Return all text lines and an optional error code."""
    try:
        with path.open("rb") as handle:
            header = handle.read(5)
        if header != b"%PDF-":
            return [], "not_pdf_header"
        with pdfplumber.open(path) as pdf:
            if not pdf.pages:
                return [], "no_pages"
            lines: List[str] = []
            for page in pdf.pages:
                text = page.extract_text() or ""
                lines.extend([line.strip() for line in text.splitlines() if line.strip()])
            return lines, None
    except Exception:
        return [], "invalid_pdf"


def parse_quick_summary(lines: List[str]) -> Tuple[List[Dict[str, Any]], Optional[float]]:
    start = None
    for idx, line in enumerate(lines):
        if not line.lower().startswith("quick summary part 2"):
            continue
        window = lines[idx + 1 : idx + 30]
        if any(re.match(r"^Sales\s+\d", entry) for entry in window):
            start = idx + 1
            break
    if start is None:
        return [], None

    records: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    ratio_sales_inventory: Optional[float] = None

    def commit_current() -> None:
        if current:
            records.append(current.copy())

    for line in lines[start:]:
        lower = line.lower()
        if lower.startswith("ratio of sales vs inventory"):
            parts = line.split()
            if parts:
                ratio_sales_inventory = parse_percent(parts[-1])
            break
        if lower.startswith("totals include"):
            break
        if lower.startswith("kootenay statistics"):
            continue
        if lower.startswith("quick summary part"):
            continue
        if re.fullmatch(r"\d+", line):
            continue

        matched = False
        for key, pattern in METRIC_PATTERNS.items():
            match = pattern.match(line)
            if not match or current is None:
                continue
            raw = match.group(1)
            if key in ("sales", "new_listings", "current_inventory", "days_to_sell"):
                current[key] = parse_number(raw)
            elif key == "sell_inventory_ratio":
                current[key] = parse_percent(raw)
            elif key in ("average_price", "median_price"):
                current[key] = parse_price(raw)
            matched = True
            break
        if matched:
            continue

        commit_current()
        current = {"property_type": line}

    commit_current()
    return records, ratio_sales_inventory


def parse_pdf(path: Path, debug: bool) -> ParseOutcome:
    lines, error = read_pdf_text(path)
    if error:
        return None, error
    if not lines:
        return None, "empty"
    month_label = parse_month_label(" ".join(lines))
    if not month_label:
        month_label = parse_month_from_filename(path)
    if not month_label:
        return None, "missing_month"
    month_text, year, month = month_label
    property_types, ratio_sales_inventory = parse_quick_summary(lines)
    if not property_types:
        return None, "missing_quick_summary"
    reference_month = f"{year:04d}-{month:02d}"
    record = {
        "reference_month": reference_month,
        "year": year,
        "month": month,
        "month_label": month_text.title(),
        "property_types": property_types,
        "ratio_sales_inventory": ratio_sales_inventory,
        "source": {
            "file": str(path),
            "url": f"{BASE_URL}{quote(path.name)}",
        },
        "parsed_at": iso_now(),
    }
    if debug:
        print(f"[stats] {path.name}: {reference_month} types={len(property_types)}")
    return record, None


def main() -> None:
    args = parse_args()
    pdf_dir = Path(args.pdf_dir)
    pdfs = sorted(pdf_dir.glob(args.pdf_glob))
    if not pdfs:
        raise SystemExit(f"No PDFs found in {pdf_dir} (pattern {args.pdf_glob}).")

    records: List[Dict[str, Any]] = []
    skipped: List[Dict[str, str]] = []

    for pdf in pdfs:
        record, reason = parse_pdf(pdf, debug=args.debug)
        if record:
            records.append(record)
        else:
            skipped.append({"file": str(pdf), "reason": reason or "unknown"})
            if args.debug:
                print(f"[stats] skip {pdf.name} reason={reason}")

    records.sort(key=lambda item: item["reference_month"])

    normalized_path = Path(args.out_normalized)
    derived_path = Path(args.out_derived)
    write_jsonl(normalized_path, records)

    summary = {
        "generated_at": iso_now(),
        "record_count": len(records),
        "records": records,
        "skipped": skipped,
    }
    write_json(derived_path, summary)

    if args.debug:
        print(f"[stats] wrote {normalized_path}")
        print(f"[stats] wrote {derived_path}")


if __name__ == "__main__":
    main()
