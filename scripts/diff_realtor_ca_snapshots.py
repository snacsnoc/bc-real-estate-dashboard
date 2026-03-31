#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


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


def summarize(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": item.get("id"),
        "mls_number": item.get("mls_number"),
        "price": item.get("price"),
        "price_unformatted_value": item.get("price_unformatted_value"),
        "property_type": item.get("property_type"),
        "address_text": item.get("address_text"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Diff two Realtor.ca listing snapshots."
    )
    parser.add_argument(
        "--dataset",
        help="Dataset subfolder under data/normalized/realtor_ca.",
    )
    parser.add_argument(
        "--old",
        help="Older listings JSONL path.",
    )
    parser.add_argument(
        "--new",
        help="Newer listings JSONL path.",
    )
    parser.add_argument(
        "--snapshot-dir",
        help="Snapshot directory (to auto-pick newest two).",
    )
    parser.add_argument(
        "--out",
        default="data/derived/realtor_ca/diffs/latest_diff.json",
        help="Output JSON path (default: data/derived/realtor_ca/diffs/latest_diff.json).",
    )
    return parser


def resolve_paths(args: argparse.Namespace) -> Tuple[Path, Path]:
    if args.old and args.new:
        return Path(args.old), Path(args.new)
    if args.snapshot_dir or args.dataset:
        if args.snapshot_dir:
            snapshot_dir = Path(args.snapshot_dir)
        else:
            base = Path("data/normalized/realtor_ca")
            snapshot_dir = base / args.dataset / "snapshots" if args.dataset else (base / "snapshots")
        snapshots = list_snapshots(snapshot_dir)
        if len(snapshots) < 2:
            raise SystemExit(f"Need at least two snapshots in {snapshot_dir}")
        return snapshots[-2], snapshots[-1]
    raise SystemExit("Provide --old/--new or --snapshot-dir")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    old_path, new_path = resolve_paths(args)
    if not old_path.exists() or not new_path.exists():
        raise SystemExit("Snapshot file(s) missing.")

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
        "added": [summarize(new_map[item_id]) for item_id in added_ids],
        "removed": [summarize(old_map[item_id]) for item_id in removed_ids],
    }

    write_json(Path(args.out), payload)


if __name__ == "__main__":
    main()
