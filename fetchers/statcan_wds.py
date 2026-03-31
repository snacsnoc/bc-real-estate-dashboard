from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable

from .http import build_session, get_json
from .output import iso_now, write_json

WDS_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"


def get_data_from_vector_by_reference_period_range(
    vector_id: int,
    start_ref_period: str,
    end_ref_period: str,
) -> Dict[str, object]:
    session = build_session()
    url = f"{WDS_BASE}/getDataFromVectorByReferencePeriodRange"

    def normalize_period(value: str) -> str:
        value = value.strip()
        if len(value) == 7 and value.count("-") == 1:
            return f"{value}-01"
        return value

    start_norm = normalize_period(start_ref_period)
    end_norm = normalize_period(end_ref_period)

    def candidate_params() -> Iterable[Dict[str, str]]:
        quoted_vector = f"\"{vector_id}\""
        vector_as_str = str(vector_id)
        return [
            {
                "vectorIds": quoted_vector,
                "startRefPeriod": start_norm,
                "endReferencePeriod": end_norm,
            },
            {
                "vectorIds": quoted_vector,
                "startRefPeriod": start_norm,
                "endRefPeriod": end_norm,
            },
            {
                "vectorIds": quoted_vector,
                "startRefPeriod": start_norm,
                "endDataPointReleaseDate": end_norm,
            },
            {
                "vectorIds": vector_as_str,
                "startRefPeriod": start_norm,
                "endReferencePeriod": end_norm,
            },
            {
                "vectorId": vector_as_str,
                "startRefPeriod": start_norm,
                "endReferencePeriod": end_norm,
            },
        ]

    last_error: Exception | None = None
    for params in candidate_params():
        try:
            data = get_json(session, url, params=params)
            payload = params
            return {
                "vector_id": vector_id,
                "source_url": url,
                "payload": payload,
                "data": data,
            }
        except Exception as exc:
            last_error = exc
            continue

    if last_error:
        raise last_error
    raise RuntimeError("StatCan WDS request failed without a response.")


def fetch_vector_range(
    out_dir: Path,
    vector_id: int,
    start_ref_period: str,
    end_ref_period: str,
) -> Dict[str, object]:
    result = get_data_from_vector_by_reference_period_range(
        vector_id,
        start_ref_period,
        end_ref_period,
    )
    result["fetched_at"] = iso_now()
    write_json(out_dir / "statcan" / f"vector_{vector_id}.json", result)
    return result
