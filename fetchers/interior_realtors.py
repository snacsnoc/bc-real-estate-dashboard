from __future__ import annotations

from pathlib import Path
from typing import Dict, List
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .http import build_session, get_bytes, get_text
from .output import iso_now, safe_filename, write_bytes, write_json

HISTORICAL_URL = (
    "https://www.interiorrealtors.ca/board-news/market-stats/"
    "historical-data/kootenay/"
)
LATEST_URL = "https://www.interiorrealtors.ca/board-news/market-stats/new"

PdfLink = Dict[str, str]
FetchResult = Dict[str, object]


def extract_pdf_links(html: str, base_url: str) -> List[PdfLink]:
    """Extract PDF anchors from HTML, resolving relative URLs."""
    soup = BeautifulSoup(html, "html.parser")
    found: Dict[str, PdfLink] = {}
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if ".pdf" not in href.lower():
            continue
        url = urljoin(base_url, href)
        text = anchor.get_text(strip=True)
        found[url] = {"url": url, "text": text}
    return list(found.values())


def fetch_links(source_url: str) -> FetchResult:
    """Fetch a page and collect any PDF links."""
    session = build_session()
    html = get_text(session, source_url)
    links = extract_pdf_links(html, source_url)
    return {
        "source_url": source_url,
        "fetched_at": iso_now(),
        "pdf_links": links,
    }


def download_pdfs(links: List[PdfLink], out_dir: Path) -> List[str]:
    """Download PDF content if not already present on disk."""
    session = build_session()
    downloaded: List[str] = []
    for link in links:
        url = link["url"]
        filename = Path(urlparse(url).path).name
        filename = safe_filename(filename)
        destination = out_dir / filename
        if destination.exists():
            continue
        content = get_bytes(session, url)
        write_bytes(destination, content)
        downloaded.append(str(destination))
    return downloaded


def fetch_all(out_dir: Path, download: bool = False) -> FetchResult:
    historical = fetch_links(HISTORICAL_URL)
    latest = fetch_links(LATEST_URL)
    payload = {
        "historical": historical,
        "latest": latest,
    }
    write_json(out_dir / "interior_realtors" / "kootenay_links.json", payload)

    if download:
        pdf_dir = out_dir / "interior_realtors" / "pdfs"
        download_pdfs(historical["pdf_links"], pdf_dir)
        download_pdfs(latest["pdf_links"], pdf_dir)
    return payload
