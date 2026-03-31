#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))
    return items


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def list_snapshots(snapshot_dir: Path) -> List[Path]:
    return sorted(snapshot_dir.glob("listings_*.jsonl"))


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


def parse_dotnet_ticks(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        ticks = int(value)
    except (TypeError, ValueError):
        return None
    base = datetime(1, 1, 1, tzinfo=timezone.utc)
    return base + timedelta(microseconds=ticks / 10)


def parse_time_on_realtor(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    text = value.strip().lower()
    if text in {"just now", "moments ago"}:
        return 0.0
    match = re.match(r"(\d+)\s+(minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)", text)
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    if unit.startswith("minute"):
        return amount / 1440
    if unit.startswith("hour"):
        return amount / 24
    if unit.startswith("day"):
        return float(amount)
    if unit.startswith("week"):
        return amount * 7.0
    if unit.startswith("month"):
        return amount * 30.0
    if unit.startswith("year"):
        return amount * 365.0
    return None


def parse_price(listing: Dict[str, Any]) -> Optional[int]:
    raw_value = listing.get("price_unformatted_value")
    if raw_value:
        try:
            return int(float(str(raw_value)))
        except ValueError:
            pass
    price_text = listing.get("price")
    if not price_text:
        return None
    digits = re.sub(r"[^0-9]", "", str(price_text))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_city(address_text: Optional[str]) -> Optional[str]:
    if not address_text:
        return None
    parts = address_text.split("|")
    tail = parts[1].strip() if len(parts) > 1 else address_text
    match = re.match(r"([^,]+),", tail)
    if match:
        return match.group(1).strip()
    return None


def listing_event_date(listing: Dict[str, Any], as_of: datetime) -> Optional[datetime]:
    listing_as_of = parse_iso_datetime((listing.get("source") or {}).get("fetched_at")) or as_of
    days = parse_time_on_realtor(listing.get("time_on_realtor"))
    if days is not None:
        return listing_as_of - timedelta(days=days)
    inserted = parse_dotnet_ticks(listing.get("inserted_date_utc"))
    return inserted


def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def week_key(dt: datetime) -> str:
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"


def price_band(value: Optional[int]) -> str:
    if value is None:
        return "Unknown"
    bands: List[Tuple[Optional[int], Optional[int], str]] = [
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


def build_inventory_summary(listings: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(listings)
    by_property_type = group_counts(listings, lambda item: item.get("property_type"))
    by_price_band = group_counts(listings, lambda item: price_band(parse_price(item)))
    by_area = group_counts(listings, lambda item: parse_city(item.get("address_text")))

    return {
        "total_listings": total,
        "by_property_type": counts_to_list(by_property_type),
        "by_price_band": counts_to_list(by_price_band),
        "by_area": counts_to_list(by_area),
    }


def build_sold_momentum(listings: List[Dict[str, Any]], as_of: datetime) -> Dict[str, Any]:
    by_month: Dict[str, int] = {}
    by_week: Dict[str, int] = {}
    recent_counts = {"7d": 0, "30d": 0, "90d": 0}

    for item in listings:
        event_dt = listing_event_date(item, as_of)
        if not event_dt:
            continue
        by_month[month_key(event_dt)] = by_month.get(month_key(event_dt), 0) + 1
        by_week[week_key(event_dt)] = by_week.get(week_key(event_dt), 0) + 1

        delta = as_of - event_dt
        if delta.days <= 7:
            recent_counts["7d"] += 1
        if delta.days <= 30:
            recent_counts["30d"] += 1
        if delta.days <= 90:
            recent_counts["90d"] += 1

    by_month_list = [
        {"month": key, "count": count}
        for key, count in sorted(by_month.items())
    ]
    by_week_list = [
        {"week": key, "count": count}
        for key, count in sorted(by_week.items())
    ]

    return {
        "recent_counts": recent_counts,
        "by_week": by_week_list,
        "by_month": by_month_list,
    }


def build_new_listing_momentum(
    listings: List[Dict[str, Any]],
    as_of: datetime,
) -> Dict[str, Any]:
    by_month: Dict[str, int] = {}
    by_week: Dict[str, int] = {}
    recent_counts = {"7d": 0, "30d": 0, "90d": 0}

    for item in listings:
        event_dt = listing_event_date(item, as_of)
        if not event_dt:
            continue
        by_month[month_key(event_dt)] = by_month.get(month_key(event_dt), 0) + 1
        by_week[week_key(event_dt)] = by_week.get(week_key(event_dt), 0) + 1

        delta = as_of - event_dt
        if delta.days <= 7:
            recent_counts["7d"] += 1
        if delta.days <= 30:
            recent_counts["30d"] += 1
        if delta.days <= 90:
            recent_counts["90d"] += 1

    by_month_list = [
        {"month": key, "count": count}
        for key, count in sorted(by_month.items())
    ]
    by_week_list = [
        {"week": key, "count": count}
        for key, count in sorted(by_week.items())
    ]

    return {
        "recent_counts": recent_counts,
        "by_week": by_week_list,
        "by_month": by_month_list,
    }


def build_market_balance(
    sold_momentum: Dict[str, Any],
    new_momentum: Dict[str, Any],
) -> Dict[str, Any]:
    sold_by_month = {row["month"]: row["count"] for row in sold_momentum.get("by_month", [])}
    new_by_month = {row["month"]: row["count"] for row in new_momentum.get("by_month", [])}
    months = sorted(set(sold_by_month) | set(new_by_month))

    balance_rows = []
    for month in months:
        sold_count = sold_by_month.get(month, 0)
        new_count = new_by_month.get(month, 0)
        ratio = round(sold_count / new_count, 4) if new_count else None
        balance_rows.append(
            {
                "month": month,
                "sold": sold_count,
                "new": new_count,
                "snlr": ratio,
            }
        )

    sold_recent = sold_momentum.get("recent_counts", {})
    new_recent = new_momentum.get("recent_counts", {})
    sold_30d = sold_recent.get("30d", 0)
    new_30d = new_recent.get("30d", 0)
    snlr_30d = round(sold_30d / new_30d, 4) if new_30d else None

    return {
        "recent_counts": {
            "sold_30d": sold_30d,
            "new_30d": new_30d,
            "snlr_30d": snlr_30d,
        },
        "by_month": balance_rows,
        "notes": [
            "New listings are inferred from active listing dates (InsertedDateUTC/TimeOnRealtor).",
        ],
    }


def build_price_trends(
    sold_listings: List[Dict[str, Any]],
    active_listings: List[Dict[str, Any]],
    as_of: datetime,
) -> Dict[str, Any]:
    sold_by_month: Dict[str, List[int]] = {}
    sold_band_by_month: Dict[str, Dict[str, int]] = {}

    for item in sold_listings:
        event_dt = listing_event_date(item, as_of)
        if not event_dt:
            continue
        key = month_key(event_dt)
        price = parse_price(item)
        if price is None:
            continue
        sold_by_month.setdefault(key, []).append(price)
        band = price_band(price)
        sold_band_by_month.setdefault(key, {})
        sold_band_by_month[key][band] = sold_band_by_month[key].get(band, 0) + 1

    sold_series = []
    for key in sorted(sold_by_month.keys()):
        prices = sold_by_month[key]
        sold_series.append(
            {
                "month": key,
                "count": len(prices),
                "median": median(prices),
                "average": average(prices),
                "min": min(prices),
                "max": max(prices),
            }
        )

    sold_band_shares = []
    for key in sorted(sold_band_by_month.keys()):
        counts = sold_band_by_month[key]
        total = sum(counts.values())
        shares = {
            band: round(count / total, 4) if total else 0
            for band, count in sorted(counts.items())
        }
        sold_band_shares.append({"month": key, "shares": shares, "total": total})

    active_by_month: Dict[str, List[int]] = {}
    for item in active_listings:
        event_dt = listing_event_date(item, as_of)
        if not event_dt:
            continue
        key = month_key(event_dt)
        price = parse_price(item)
        if price is None:
            continue
        active_by_month.setdefault(key, []).append(price)

    active_series = []
    for key in sorted(active_by_month.keys()):
        prices = active_by_month[key]
        active_series.append(
            {
                "month": key,
                "count": len(prices),
                "median": median(prices),
                "average": average(prices),
                "min": min(prices),
                "max": max(prices),
            }
        )

    return {
        "sold_by_month": sold_series,
        "sold_price_band_shares": sold_band_shares,
        "active_list_price_by_month": active_series,
    }


def build_absorption(
    sold_listings: List[Dict[str, Any]],
    active_listings: List[Dict[str, Any]],
    as_of: datetime,
) -> Dict[str, Any]:
    window_start = as_of - timedelta(days=30)

    active_counts: Dict[Tuple[str, str], int] = {}
    for item in active_listings:
        category = (
            item.get("property_type") or "Unknown",
            price_band(parse_price(item)),
        )
        active_counts[category] = active_counts.get(category, 0) + 1

    sold_counts: Dict[Tuple[str, str], int] = {}
    for item in sold_listings:
        event_dt = listing_event_date(item, as_of)
        if not event_dt or event_dt < window_start:
            continue
        category = (
            item.get("property_type") or "Unknown",
            price_band(parse_price(item)),
        )
        sold_counts[category] = sold_counts.get(category, 0) + 1

    rows = []
    categories = set(active_counts) | set(sold_counts)
    for category in sorted(categories):
        active = active_counts.get(category, 0)
        sold = sold_counts.get(category, 0)
        ratio = round(sold / active, 4) if active else None
        rows.append(
            {
                "property_type": category[0],
                "price_band": category[1],
                "active_count": active,
                "sold_30d": sold,
                "absorption_ratio": ratio,
            }
        )

    return {
        "window_days": 30,
        "rows": rows,
    }


def build_time_on_market(
    active_listings: List[Dict[str, Any]],
    sold_listings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    def summarize(listings: List[Dict[str, Any]]) -> Dict[str, Any]:
        values = [
            value
            for item in listings
            if (value := parse_time_on_realtor(item.get("time_on_realtor"))) is not None
        ]
        return {
            "count": len(values),
            "median_days": median(values) if values else None,
            "average_days": average(values) if values else None,
        }

    def summarize_by_type(listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        buckets: Dict[str, List[float]] = {}
        for item in listings:
            value = parse_time_on_realtor(item.get("time_on_realtor"))
            if value is None:
                continue
            key = item.get("property_type") or "Unknown"
            buckets.setdefault(key, []).append(value)
        rows = []
        for key, values in sorted(buckets.items()):
            rows.append(
                {
                    "property_type": key,
                    "count": len(values),
                    "median_days": median(values),
                    "average_days": average(values),
                }
            )
        return rows

    return {
        "active": {
            "overall": summarize(active_listings),
            "by_property_type": summarize_by_type(active_listings),
        },
        "sold": {
            "overall": summarize(sold_listings),
            "by_property_type": summarize_by_type(sold_listings),
        },
    }


def build_snapshot_change_metrics(
    snapshot_dir: Path,
    sold_listings: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    snapshots = list_snapshots(snapshot_dir)
    if len(snapshots) < 2:
        return None

    old_path, new_path = snapshots[-2], snapshots[-1]
    old_items = load_jsonl(old_path)
    new_items = load_jsonl(new_path)

    old_ids = {str(item.get("id")) for item in old_items if item.get("id")}
    new_ids = {str(item.get("id")) for item in new_items if item.get("id")}

    added_ids = new_ids - old_ids
    removed_ids = old_ids - new_ids

    sold_ids = {str(item.get("id")) for item in sold_listings if item.get("id")}
    removed_sold = len([item_id for item_id in removed_ids if item_id in sold_ids])
    removed_delisted = len(removed_ids) - removed_sold

    return {
        "old_snapshot": str(old_path),
        "new_snapshot": str(new_path),
        "added_count": len(added_ids),
        "removed_count": len(removed_ids),
        "removed_sold_count": removed_sold,
        "removed_delisted_count": removed_delisted,
        "net_change": len(added_ids) - len(removed_ids),
        "notes": [
            "Removed listings are tagged as sold if the listing ID appears in the sold dataset; otherwise assumed delisted.",
        ],
    }


def build_price_cut_metrics(snapshot_dir: Path) -> Optional[Dict[str, Any]]:
    snapshots = list_snapshots(snapshot_dir)
    if len(snapshots) < 2:
        return None

    old_path, new_path = snapshots[-2], snapshots[-1]
    old_items = load_jsonl(old_path)
    new_items = load_jsonl(new_path)

    old_prices = {
        str(item.get("id")): parse_price(item)
        for item in old_items
        if item.get("id")
    }
    new_prices = {
        str(item.get("id")): parse_price(item)
        for item in new_items
        if item.get("id")
    }

    common_ids = set(old_prices) & set(new_prices)
    deltas = []
    increases = 0
    for listing_id in common_ids:
        old_price = old_prices.get(listing_id)
        new_price = new_prices.get(listing_id)
        if old_price is None or new_price is None:
            continue
        if new_price < old_price:
            deltas.append((old_price - new_price, (old_price - new_price) / old_price))
        elif new_price > old_price:
            increases += 1

    cut_amounts = [delta[0] for delta in deltas]
    cut_percents = [delta[1] for delta in deltas]
    total_with_price = len(
        [
            listing_id
            for listing_id in common_ids
            if old_prices[listing_id] is not None and new_prices[listing_id] is not None
        ]
    )
    cut_rate = len(deltas) / total_with_price if total_with_price else None

    return {
        "old_snapshot": str(old_path),
        "new_snapshot": str(new_path),
        "total_common_with_price": total_with_price,
        "price_cut_count": len(deltas),
        "price_increase_count": increases,
        "price_cut_rate": round(cut_rate, 4) if cut_rate is not None else None,
        "median_cut_amount": median(cut_amounts),
        "average_cut_amount": average(cut_amounts),
        "median_cut_percent": median(cut_percents),
        "average_cut_percent": average(cut_percents),
    }


def build_market_state(
    inventory: Dict[str, Any],
    sold_momentum: Dict[str, Any],
    market_balance: Dict[str, Any],
    time_on_market: Dict[str, Any],
    price_cuts: Optional[Dict[str, Any]],
    snapshot_changes: Optional[Dict[str, Any]],
    official_stats: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    active_total = inventory.get("total_listings")
    by_month = sold_momentum.get("by_month") or []
    sold_last_month = by_month[-1]["count"] if by_month else None

    moi = None
    if sold_last_month:
        moi = round(active_total / sold_last_month, 2) if active_total else None

    snlr_30d = (market_balance.get("recent_counts") or {}).get("snlr_30d")
    official_snlr = (official_stats or {}).get("snlr")
    official_moi = (official_stats or {}).get("moi")
    official_month = (official_stats or {}).get("reference_month")
    dom_median = (((time_on_market.get("active") or {}).get("overall") or {}).get("median_days"))

    snlr_low = 0.4
    snlr_high = 0.6
    moi_high = 6.0
    moi_mid = 4.0
    dom_high = 60.0
    dom_low = 30.0

    state_key = "insufficient_data"
    label = "Insufficient data"
    description = "Need SNLR, MOI, and DOM to classify market state."

    snlr_value = official_snlr if official_snlr is not None else snlr_30d
    moi_value = official_moi if official_moi is not None else moi

    if snlr_value is not None and moi_value is not None and dom_median is not None:
        if snlr_value < snlr_low and moi_value > moi_high and dom_median > dom_high:
            state_key = "buyer_leaning"
            label = "Buyer-leaning"
            description = "Soft market with heavy supply and slower absorption."
        elif snlr_value > snlr_high and moi_value < moi_mid and dom_median < dom_low:
            state_key = "seller_leaning"
            label = "Seller-leaning"
            description = "Tight market with fast sales and limited inventory."
        elif (snlr_low <= snlr_value <= snlr_high) or (moi_mid <= moi_value <= moi_high):
            state_key = "balanced"
            label = "Balanced"
            description = "Supply and demand are closer to equilibrium."
        else:
            state_key = "mixed"
            label = "Mixed signals"
            description = "Indicators point in different directions."

    return {
        "state": {
            "key": state_key,
            "label": label,
            "description": description,
        },
        "metrics": {
            "active_total": active_total,
            "sold_last_month": sold_last_month,
            "moi_latest": moi,
            "snlr_30d": snlr_30d,
            "moi_official": official_moi,
            "snlr_official": official_snlr,
            "official_month": official_month,
            "dom_median_days": dom_median,
            "price_cut_rate": (price_cuts or {}).get("price_cut_rate"),
            "price_cut_median_amount": (price_cuts or {}).get("median_cut_amount"),
            "price_cut_median_percent": (price_cuts or {}).get("median_cut_percent"),
            "snapshot_added": (snapshot_changes or {}).get("added_count"),
            "snapshot_removed": (snapshot_changes or {}).get("removed_count"),
            "snapshot_removed_sold": (snapshot_changes or {}).get("removed_sold_count"),
            "snapshot_removed_delisted": (snapshot_changes or {}).get("removed_delisted_count"),
            "snapshot_net": (snapshot_changes or {}).get("net_change"),
        },
        "thresholds": {
            "snlr_low": snlr_low,
            "snlr_high": snlr_high,
            "moi_mid": moi_mid,
            "moi_high": moi_high,
            "dom_low": dom_low,
            "dom_high": dom_high,
        },
        "notes": [
            "SNLR and new listings are inferred from active listing dates; treat as a proxy.",
            "Official MOI/SNLR from Interior REALTORS are used when available.",
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Roll up Realtor.ca listings into summary metrics."
    )
    parser.add_argument(
        "--active",
        default="data/normalized/realtor_ca/listings.jsonl",
        help="Active listings JSONL path.",
    )
    parser.add_argument(
        "--sold",
        default="data/normalized/realtor_ca/sold_730/listings.jsonl",
        help="Sold listings JSONL path.",
    )
    parser.add_argument(
        "--out",
        default="data/derived/realtor_ca",
        help="Output directory for rollups.",
    )
    return parser


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


def load_official_stats(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records") or []
    if not records:
        return None
    records = sorted(records, key=lambda item: item.get("reference_month") or "")
    latest = records[-1]
    if not latest:
        return None
    return {
        "reference_month": latest.get("reference_month"),
        "moi": latest.get("moi"),
        "snlr": latest.get("snlr"),
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    active_path = Path(args.active)
    sold_path = Path(args.sold)

    if not active_path.exists():
        raise SystemExit(f"Active listings file not found: {active_path}")
    if not sold_path.exists():
        raise SystemExit(f"Sold listings file not found: {sold_path}")

    active_listings = load_jsonl(active_path)
    sold_listings = load_jsonl(sold_path)

    as_of = determine_as_of(active_listings + sold_listings)
    generated_at = datetime.now(timezone.utc).isoformat()

    inventory = build_inventory_summary(active_listings)
    sold_momentum = build_sold_momentum(sold_listings, as_of)
    new_momentum = build_new_listing_momentum(active_listings, as_of)
    market_balance = build_market_balance(sold_momentum, new_momentum)
    price_trends = build_price_trends(sold_listings, active_listings, as_of)
    absorption = build_absorption(sold_listings, active_listings, as_of)
    time_on_market = build_time_on_market(active_listings, sold_listings)

    out_dir = Path(args.out)
    write_json(out_dir / "active_inventory.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **inventory,
    })
    write_json(out_dir / "sold_momentum.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **sold_momentum,
    })
    write_json(out_dir / "new_momentum.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **new_momentum,
    })
    write_json(out_dir / "market_balance.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **market_balance,
    })
    write_json(out_dir / "price_trends.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **price_trends,
    })
    write_json(out_dir / "absorption.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **absorption,
    })
    write_json(out_dir / "time_on_market.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **time_on_market,
    })

    snapshot_dir = active_path.parent / "snapshots"
    snapshot_changes = None
    price_cuts = None
    if snapshot_dir.exists():
        snapshot_changes = build_snapshot_change_metrics(snapshot_dir, sold_listings)
        price_cuts = build_price_cut_metrics(snapshot_dir)

    official_stats = load_official_stats(
        Path("data/derived/interior_realtors/kootenay_market_stats.json")
    )

    if snapshot_changes:
        write_json(out_dir / "snapshot_changes.json", {
            "generated_at": generated_at,
            "as_of": as_of.isoformat(),
            **snapshot_changes,
        })
    if price_cuts:
        write_json(out_dir / "price_cuts.json", {
            "generated_at": generated_at,
            "as_of": as_of.isoformat(),
            **price_cuts,
        })

    market_state = build_market_state(
        inventory,
        sold_momentum,
        market_balance,
        time_on_market,
        price_cuts,
        snapshot_changes,
        official_stats,
    )
    write_json(out_dir / "market_state.json", {
        "generated_at": generated_at,
        "as_of": as_of.isoformat(),
        **market_state,
    })


if __name__ == "__main__":
    main()
