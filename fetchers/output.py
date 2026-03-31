from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data: Any) -> None:
    ensure_parent(path)
    payload = json.dumps(data, indent=2, ensure_ascii=True)
    path.write_text(payload, encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    ensure_parent(path)
    path.write_text(text, encoding="utf-8")


def write_bytes(path: Path, content: bytes) -> None:
    ensure_parent(path)
    path.write_bytes(content)


def safe_filename(name: str) -> str:
    cleaned = []
    for char in name:
        if char.isalnum() or char in ("-", "_", "."):
            cleaned.append(char)
        else:
            cleaned.append("_")
    return "".join(cleaned).strip("_") or "file"
