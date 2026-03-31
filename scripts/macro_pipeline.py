#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure project root is on sys.path when running as a script.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fetchers import boc_valet, statcan_wds
from fetchers.output import iso_now, write_json


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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
    candidate = Path("config/macro_pipeline.json")
    if candidate.exists():
        return candidate
    candidate = Path("macro_pipeline.json")
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
        description="Fetch and normalize macro data (BoC rates + StatCan unemployment)."
    )
    parser.add_argument(
        "--config",
        help="Path to a JSON config file (default: config/macro_pipeline.json if present).",
    )
    parser.add_argument(
        "--out-raw",
        default="data/raw",
        help="Raw output root (default: data/raw).",
    )
    parser.add_argument(
        "--out-derived",
        default="data/derived",
        help="Derived output root (default: data/derived).",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Skip fetching raw data; only build derived outputs.",
    )
    parser.add_argument(
        "--boc-groups",
        nargs="*",
        default=None,
        help="BoC Valet group names to fetch.",
    )
    parser.add_argument(
        "--boc-series",
        nargs="*",
        default=None,
        help="BoC Valet series IDs to fetch.",
    )
    parser.add_argument(
        "--boc-alias",
        action="append",
        default=[],
        help="Alias mapping in name=SERIES_ID form (repeatable).",
    )
    parser.add_argument(
        "--statcan-vector",
        type=int,
        default=None,
        help="StatCan vector ID for unemployment (optional).",
    )
    parser.add_argument(
        "--statcan-start",
        help="StatCan start reference period (YYYY-MM).",
    )
    parser.add_argument(
        "--statcan-end",
        help="StatCan end reference period (YYYY-MM).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug output.",
    )
    return parser


def parse_aliases(items: List[str]) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise SystemExit("Alias must be in name=SERIES_ID form.")
        key, value = item.split("=", 1)
        aliases[key] = value
    return aliases


def debug_print(enabled: bool, message: str) -> None:
    if enabled:
        print(message)


def unwrap_boc_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def latest_observation(observations: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for obs in reversed(observations):
        if obs.get("value") is not None:
            return obs
    return None


def extract_boc_series(
    payload: Dict[str, Any],
    series_id: str,
) -> Optional[Dict[str, Any]]:
    data = unwrap_boc_payload(payload)
    series_detail = (data.get("seriesDetail") or {}).get(series_id, {})
    observations_raw = data.get("observations") or []
    observations: List[Dict[str, Any]] = []
    for row in observations_raw:
        date = row.get("d") or row.get("date")
        if not date:
            continue
        series_row = row.get(series_id) or {}
        value = parse_float(series_row.get("v"))
        observations.append({"date": date, "value": value})
    if not observations:
        return None
    latest = latest_observation(observations)
    return {
        "series_id": series_id,
        "label": series_detail.get("label") or series_detail.get("description"),
        "description": series_detail.get("description"),
        "unit": series_detail.get("unit"),
        "observations": observations,
        "latest": latest,
    }


def load_boc_sources(raw_dir: Path) -> Dict[str, Dict[str, Any]]:
    sources: Dict[str, Dict[str, Any]] = {}
    for path in sorted(raw_dir.glob("group_*.json")):
        sources[path.stem] = load_json(path)
    for path in sorted(raw_dir.glob("series_*.json")):
        sources[path.stem] = load_json(path)
    return sources


def build_rates_output(
    raw_dir: Path,
    aliases: Dict[str, str],
    *,
    debug: bool = False,
) -> Dict[str, Any]:
    sources = load_boc_sources(raw_dir)
    resolved: Dict[str, Any] = {}
    missing: List[str] = []

    for alias, series_id in aliases.items():
        series_payload = None
        series_path = raw_dir / f"series_{series_id}.json"
        if series_path.exists():
            series_payload = load_json(series_path)
        else:
            for payload in sources.values():
                data = unwrap_boc_payload(payload)
                if series_id in (data.get("seriesDetail") or {}):
                    series_payload = payload
                    break
        if series_payload is None:
            missing.append(alias)
            if debug:
                print(f"[macro] missing BoC series {alias} ({series_id})")
            continue
        extracted = extract_boc_series(series_payload, series_id)
        if not extracted:
            missing.append(alias)
            debug_print(debug, f"[macro] empty BoC series {alias} ({series_id})")
            continue
        resolved[alias] = extracted
        if debug:
            latest = extracted.get("latest") or {}
            debug_print(
                debug,
                "[macro] rates "
                f"{alias} series={series_id} obs={len(extracted.get('observations', []))} "
                f"latest={latest.get('date')} value={latest.get('value')}",
            )

    return {
        "generated_at": iso_now(),
        "aliases": aliases,
        "series": resolved,
        "missing": missing,
    }


def extract_statcan_series(payload: Dict[str, Any]) -> Dict[str, Any]:
    data: Any = payload.get("data") or {}
    if isinstance(data, list):
        data = next((item for item in data if isinstance(item, dict)), {}) or {}
    obj = data.get("object") or data.get("data") or data.get("result") or data
    vector_data = obj.get("vectorData") or obj.get("vectorDataPoint") or []
    observations = []
    for row in vector_data:
        ref_period = row.get("refPer") or row.get("refPeriod")
        value = parse_float(row.get("value"))
        if not ref_period:
            continue
        observations.append({"ref_period": ref_period, "value": value})
    latest = None
    for row in reversed(observations):
        if row.get("value") is not None:
            latest = row
            break
    return {
        "vector_id": obj.get("vectorId") or payload.get("vector_id"),
        "product_id": obj.get("productId"),
        "coordinate": obj.get("coordinate"),
        "observations": observations,
        "latest": latest,
    }


def build_unemployment_output(
    raw_dir: Path,
    vector_id: int,
    *,
    debug: bool = False,
) -> Optional[Dict[str, Any]]:
    path = raw_dir / f"vector_{vector_id}.json"
    if not path.exists():
        return None
    payload = load_json(path)
    parsed = extract_statcan_series(payload)
    parsed["generated_at"] = iso_now()
    parsed["source_url"] = payload.get("source_url")
    parsed["payload"] = payload.get("payload")
    if debug:
        latest = parsed.get("latest") or {}
        debug_print(
            debug,
            "[macro] unemployment "
            f"vector={vector_id} obs={len(parsed.get('observations', []))} "
            f"latest={latest.get('ref_period')} value={latest.get('value')}",
        )
    return parsed


def main() -> None:
    parser = build_parser()
    defaults = parser_defaults(parser)
    args = parser.parse_args()
    config_path = resolve_config_path(args.config)
    config = load_config(config_path)
    merge_config(args, config, defaults)

    debug_print(args.debug, f"[macro] config={config_path or 'none'}")

    aliases = {}
    if isinstance(config.get("boc_aliases"), dict):
        aliases.update(config["boc_aliases"])
    aliases.update(parse_aliases(args.boc_alias))

    boc_groups = args.boc_groups
    if boc_groups is None and isinstance(config.get("boc_groups"), list):
        boc_groups = config["boc_groups"]
    boc_series = args.boc_series
    if boc_series is None and isinstance(config.get("boc_series"), list):
        boc_series = config["boc_series"]

    statcan_vector = args.statcan_vector
    if statcan_vector is None:
        statcan_vector = config.get("statcan_vector")

    statcan_start = args.statcan_start or config.get("statcan_start")
    statcan_end = args.statcan_end or config.get("statcan_end")
    if not statcan_end:
        statcan_end = datetime.now(timezone.utc).strftime("%Y-%m")

    debug_print(args.debug, f"[macro] out_raw={args.out_raw} out_derived={args.out_derived}")
    debug_print(args.debug, f"[macro] no_fetch={args.no_fetch}")
    debug_print(args.debug, f"[macro] boc_groups={boc_groups or []}")
    debug_print(args.debug, f"[macro] boc_series={boc_series or []}")
    if aliases:
        debug_print(args.debug, f"[macro] boc_aliases={aliases}")
    debug_print(args.debug, f"[macro] statcan_vector={statcan_vector}")
    if statcan_vector:
        debug_print(args.debug, f"[macro] statcan_range={statcan_start}..{statcan_end}")

    raw_root = Path(args.out_raw)
    derived_root = Path(args.out_derived)

    if not args.no_fetch:
        if boc_groups or boc_series:
            boc_valet.fetch_all(
                raw_root,
                groups=boc_groups,
                series=boc_series,
            )
        if statcan_vector:
            if not statcan_start:
                raise SystemExit("StatCan start period is required when fetching.")
            statcan_wds.fetch_vector_range(
                raw_root,
                vector_id=int(statcan_vector),
                start_ref_period=statcan_start,
                end_ref_period=statcan_end,
            )

    derived_dir = derived_root / "macro"
    derived_dir.mkdir(parents=True, exist_ok=True)

    if aliases:
        rates = build_rates_output(raw_root / "boc", aliases, debug=args.debug)
        write_json(derived_dir / "rates.json", rates)
        debug_print(args.debug, f"[macro] wrote {derived_dir / 'rates.json'}")
    else:
        debug_print(args.debug, "[macro] skipping rates output (no aliases configured).")

    if statcan_vector:
        unemployment = build_unemployment_output(
            raw_root / "statcan",
            int(statcan_vector),
            debug=args.debug,
        )
        if unemployment:
            write_json(derived_dir / "unemployment.json", unemployment)
            debug_print(args.debug, f"[macro] wrote {derived_dir / 'unemployment.json'}")
        else:
            debug_print(
                args.debug,
                "[macro] unemployment vector data missing; run with --no-fetch=off.",
            )


if __name__ == "__main__":
    main()
