#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${ROOT_DIR}/venv/bin/python"

if [[ ! -x "${PYTHON}" ]]; then
  echo "Missing venv python at ${PYTHON}. Activate venv or adjust path."
  exit 1
fi

echo "[daily] $(date -u +"%Y-%m-%dT%H:%M:%SZ") start"

"${PYTHON}" "${ROOT_DIR}/scripts/realtor_pipeline.py" --sold-within-days 730
"${PYTHON}" "${ROOT_DIR}/scripts/rollup_realtor_ca.py"
"${PYTHON}" "${ROOT_DIR}/scripts/remax_pipeline.py"
"${PYTHON}" "${ROOT_DIR}/scripts/macro_pipeline.py"
"${PYTHON}" "${ROOT_DIR}/scripts/interior_realtors_pipeline.py"
"${PYTHON}" "${ROOT_DIR}/scripts/interior_realtors_stats_pipeline.py"

echo "[daily] $(date -u +"%Y-%m-%dT%H:%M:%SZ") done"
