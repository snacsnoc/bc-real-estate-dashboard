from __future__ import annotations

from pathlib import Path
from typing import Dict

from .http import build_session, get_text
from .output import iso_now, write_json, write_text

PAGES = {
    "board_overview": "https://creastats.crea.ca/board/koot/",
    "residential_activity": "https://stats.crea.ca/mls/koot-residential-activity/",
    "market_conditions": "https://stats.crea.ca/mls/koot-market-conditions/",
}


def fetch_pages(out_dir: Path) -> Dict[str, object]:
    """Fetch and persist HTML for a small set of CREA stats pages."""
    session = build_session()
    fetched = {
        "fetched_at": iso_now(),
        "pages": {},
    }
    for name, url in PAGES.items():
        html = get_text(session, url)
        write_text(out_dir / "crea" / f"{name}.html", html)
        fetched["pages"][name] = {"url": url}

    write_json(out_dir / "crea" / "metadata.json", fetched)
    return fetched
