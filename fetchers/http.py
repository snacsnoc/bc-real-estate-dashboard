from __future__ import annotations

import time
from typing import Any, Dict, Mapping, MutableMapping, Optional

import requests

DEFAULT_TIMEOUT = 30
DEFAULT_HEADERS = {
    "User-Agent": "bc-real-estate-dashboard/0.1 (fetchers)"
}


def build_session() -> requests.Session:
    """Create a requests session with default headers applied."""
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    json_body: Optional[Mapping[str, Any]] = None,
    headers: Optional[MutableMapping[str, str]] = None,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = 3,
    backoff: float = 1.5,
) -> requests.Response:
    """Perform an HTTP request with simple retry + exponential backoff."""
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        try:
            response = session.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt == retries - 1:
                break
            time.sleep(backoff ** attempt)
    if last_error is None:
        raise RuntimeError("Request failed without exception")
    raise last_error


def get_text(session: requests.Session, url: str, **kwargs: Any) -> str:
    response = request_with_retries(session, "GET", url, **kwargs)
    response.encoding = response.encoding or "utf-8"
    return response.text


def get_bytes(session: requests.Session, url: str, **kwargs: Any) -> bytes:
    response = request_with_retries(session, "GET", url, **kwargs)
    return response.content


def get_json(session: requests.Session, url: str, **kwargs: Any) -> Dict[str, Any]:
    response = request_with_retries(session, "GET", url, **kwargs)
    return response.json()


def post_json(
    session: requests.Session,
    url: str,
    payload: Mapping[str, Any],
    **kwargs: Any,
) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    response = request_with_retries(
        session,
        "POST",
        url,
        json_body=payload,
        headers=headers,
        **kwargs,
    )
    return response.json()
