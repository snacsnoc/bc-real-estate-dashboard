from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import requests

from .http import build_session, get_json
from .output import iso_now, write_json

BASE = "https://www.bankofcanada.ca/valet"

DEFAULT_GROUPS = [
    "chartered_bank_interest",
    "bond_yields_benchmark",
]

DEFAULT_SERIES = [
    "V39079",  # Target overnight rate
]


def fetch_group(session: requests.Session, group_name: str) -> Dict[str, object]:
    """Download a Valet group as JSON."""
    url = f"{BASE}/observations/group/{group_name}/json"
    data = get_json(session, url)
    return {
        "group": group_name,
        "source_url": url,
        "data": data,
    }


def fetch_series(session: requests.Session, series_id: str) -> Dict[str, object]:
    """Download a single Valet series as JSON."""
    url = f"{BASE}/observations/{series_id}/json"
    data = get_json(session, url)
    return {
        "series": series_id,
        "source_url": url,
        "data": data,
    }


def fetch_all(
    out_dir: Path,
    groups: Iterable[str] | None = None,
    series: Iterable[str] | None = None,
) -> Dict[str, object]:
    session = build_session()
    group_list = list(groups or DEFAULT_GROUPS)
    series_list = list(series or DEFAULT_SERIES)

    payload: Dict[str, object] = {
        "fetched_at": iso_now(),
        "groups": [],
        "series": [],
    }

    for group_name in group_list:
        result = fetch_group(session, group_name)
        write_json(out_dir / "boc" / f"group_{group_name}.json", result)
        payload["groups"].append({"group": group_name})

    for series_id in series_list:
        result = fetch_series(session, series_id)
        write_json(out_dir / "boc" / f"series_{series_id}.json", result)
        payload["series"].append({"series": series_id})

    write_json(out_dir / "boc" / "metadata.json", payload)
    return payload
