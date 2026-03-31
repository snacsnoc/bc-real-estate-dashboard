#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional
import sys

from flask import Flask, jsonify, make_response, render_template, request

APP_DIR = Path(__file__).resolve().parent


def _find_repo_root(start_dir: Path) -> Path:
    for candidate in (start_dir, *start_dir.parents):
        if (candidate / "fetchers").is_dir():
            return candidate
    return start_dir


ROOT_DIR = _find_repo_root(APP_DIR)
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

try:
    from apps.vacant_land_finder import search
except ModuleNotFoundError:
    import search  # type: ignore[no-redef]

STATIC_DIR = APP_DIR / "static"
TEMPLATE_DIR = APP_DIR / "templates"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787
DEFAULT_COOKIE_FILE = str(ROOT_DIR / "cookiefile.txt")
MAX_RADIUS_KM = 250.0
MAX_RESULTS = 500
MAX_PAGES = 10


def _as_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_read_file(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    try:
        content = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return content or None


def load_cookie(cookie: Optional[str], cookie_file: Optional[str]) -> Optional[str]:
    if cookie:
        return cookie.strip()

    cookie_path = os.getenv("REALTOR_COOKIE_FILE") or cookie_file
    if cookie_path:
        loaded = _safe_read_file(Path(cookie_path))
        if loaded:
            return loaded
    return None


def create_app(
    *,
    realtor_cookie: Optional[str] = None,
    realtor_cookie_file: Optional[str] = DEFAULT_COOKIE_FILE,
) -> Flask:
    app = Flask(
        __name__,
        static_folder=str(STATIC_DIR),
        template_folder=str(TEMPLATE_DIR),
    )
    app.config["REALTOR_COOKIE"] = load_cookie(realtor_cookie, realtor_cookie_file)

    @app.get("/api/health")
    def health() -> object:
        return jsonify({"ok": True, "service": "vacant_land_finder"})

    @app.get("/api/search")
    def api_search() -> object:
        try:
            center_lat = float(request.args.get("lat", str(search.DEFAULT_CENTER_LAT)))
            center_lng = float(request.args.get("lng", str(search.DEFAULT_CENTER_LNG)))
            radius_km = float(request.args.get("radius_km", str(search.DEFAULT_RADIUS_KM)))
            max_results = int(request.args.get("max_results", "75"))
            max_pages = int(request.args.get("max_pages", "4"))
        except ValueError as exc:
            return jsonify({"error": f"Invalid numeric parameter: {exc}"}), 400

        include_realtor = _as_bool(request.args.get("include_realtor", "true"), True)
        include_remax = _as_bool(request.args.get("include_remax", "true"), True)

        if radius_km <= 0:
            return jsonify({"error": "radius_km must be greater than 0"}), 400
        if radius_km > MAX_RADIUS_KM:
            return jsonify({"error": f"radius_km must be <= {MAX_RADIUS_KM}"}), 400
        if max_results <= 0:
            return jsonify({"error": "max_results must be greater than 0"}), 400
        if max_results > MAX_RESULTS:
            return jsonify({"error": f"max_results must be <= {MAX_RESULTS}"}), 400
        if max_pages <= 0:
            return jsonify({"error": "max_pages must be greater than 0"}), 400
        if max_pages > MAX_PAGES:
            return jsonify({"error": f"max_pages must be <= {MAX_PAGES}"}), 400
        if not include_realtor and not include_remax:
            return jsonify({"error": "At least one source must be enabled."}), 400

        payload = search.search_vacant_land(
            center_lat=center_lat,
            center_lng=center_lng,
            radius_km=radius_km,
            max_results=max_results,
            max_pages=max_pages,
            include_realtor=include_realtor,
            include_remax=include_remax,
            realtor_cookie=app.config.get("REALTOR_COOKIE"),
        )
        status_code = 502 if payload.get("all_sources_failed") else 200
        if status_code == 502 and "error" not in payload:
            payload["error"] = "All requested listing sources failed."
        response = make_response(jsonify(payload), status_code)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/")
    def index() -> object:
        return render_template(
            "index.html",
            app_title="Vacant Land Radius Finder",
            defaults={
                "lat": search.DEFAULT_CENTER_LAT,
                "lng": search.DEFAULT_CENTER_LNG,
                "radius_km": search.DEFAULT_RADIUS_KM,
                "max_results": 75,
                "max_pages": 4,
                "include_realtor": True,
                "include_remax": True,
            },
            sources={"realtor_ca": "Realtor.ca", "remax_ca": "RE/MAX"},
        )

    return app


app = create_app()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Vacant land finder Flask app server")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Bind host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Bind port (default: {DEFAULT_PORT})")
    parser.add_argument(
        "--realtor-cookie",
        help="Optional raw Realtor.ca Cookie header value",
    )
    parser.add_argument(
        "--realtor-cookie-file",
        default=DEFAULT_COOKIE_FILE,
        help="Optional file containing raw Realtor.ca Cookie header value",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    run_app = create_app(
        realtor_cookie=args.realtor_cookie,
        realtor_cookie_file=args.realtor_cookie_file,
    )

    print(f"[vacant-land-finder] serving on http://{args.host}:{args.port}")
    print("[vacant-land-finder] template root:", TEMPLATE_DIR)
    print("[vacant-land-finder] static root:", STATIC_DIR)
    if run_app.config.get("REALTOR_COOKIE"):
        print("[vacant-land-finder] Realtor.ca cookie loaded.")
    else:
        print("[vacant-land-finder] Realtor.ca cookie not set (requests may be blocked).")

    run_app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
