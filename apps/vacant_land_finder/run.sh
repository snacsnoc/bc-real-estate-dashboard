#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

export FLASK_APP="app:app"
export FLASK_DEBUG="1"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8787}"

exec python3 -m flask run --host "$HOST" --port "$PORT"
