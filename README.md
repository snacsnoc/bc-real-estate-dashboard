# BC Real Estate Dashboard - Fetchers

Lightweight fetchers for the Kootenay / BC Interior dashboard data sources.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Fetch raw data

Interior REALTORS (PDF link list, optional downloads):

```bash
python scripts/fetch.py interior-realtors
python scripts/fetch.py interior-realtors --download-pdfs
```

Download media release PDFs (2025..2020 by default):

```bash
python scripts/download_interior_realtors_media.py --debug
```

Download monthly statistics PDFs (2025..2020 by default):

```bash
python scripts/download_interior_realtors_stats.py --debug
```

The stats downloader will use `data/raw/interior_realtors/kootenay_links.json` if present
to avoid guessing filenames. Remove or override `--links` to force pattern-based downloads.

If you have a harvested URL list (e.g., `interiorrealtors.ca.urls`), the downloader will
also scan it for `KO Statistics` PDFs and prefer those URLs.

Interior REALTORS stats codes:

- `CO`: Central Okanagan
- `NO`: North Okanagan
- `SO`: South Okanagan
- `SPR`: South Peace River
- `SH`: Shuswap / Revelstoke
- `KO`: Kootenay

Standard download folders:

- `data/raw/interior_realtors/media_releases/`
- `data/raw/interior_realtors/monthly_stats/`

CREA HTML pages:

```bash
python scripts/fetch.py crea
```

Bank of Canada Valet (defaults: chartered bank interest + benchmark bond yields + V39079):

```bash
python scripts/fetch.py boc
python scripts/fetch.py boc --group chartered_bank_interest --series V39079
```

StatCan WDS (vector range):

```bash
python scripts/fetch.py statcan --vector 1234567 --start 2018-01 --end 2025-01
```

Realtor.ca search (bounding box or place name):

```bash
python scripts/fetch.py realtor --bbox 49.0 50.2 -117.8 -115.0 --max-pages 2
python scripts/fetch.py realtor --place "Kootenay, British Columbia" --all-pages
python scripts/fetch.py realtor --place "Kootenay, British Columbia" --details --sleep 1
```

Realtor.ca search with cookies (to avoid 403s):

```bash
python scripts/fetch.py realtor --place "Kootenay, British Columbia" --cookie "nlbi_...; reese84=...;" --max-pages 1
python scripts/fetch.py realtor --place "Kootenay, British Columbia" --cookie-file /path/to/realtor_cookie.txt --all-pages
```

Realtor.ca sold listings (store under a dataset subfolder):

```bash
python scripts/fetch.py realtor --place "Kootenay, British Columbia" \
  --cookie-file /path/to/realtor_cookie.txt \
  --sold-within-days 365 \
  --dataset sold_365 \
  --max-pages 1
```

Sub-area discovery (optional):

```bash
python scripts/fetch.py realtor --sub-area "kootenay boundary" --cookie-file /path/to/realtor_cookie.txt
```

RE/MAX gallery listings (Creston example, oldest first):

```bash
python scripts/fetch.py remax --bbox 49.0167950656 49.1741604147 -116.685169277 -116.341846523 \\
  --sort-key 1 --sort-direction 0 --all-pages
```

Outputs are written to `data/raw/`.

## Parse Interior REALTORS PDFs (Kootenay MOI)

Parse the monthly media release PDFs to extract sold units, dollar volume,
active listings, and new listings (with MOI + SNLR computed).

```bash
python scripts/interior_realtors_pipeline.py
python scripts/interior_realtors_pipeline.py --pdf-dir data/raw/interior_realtors/media_releases
python scripts/interior_realtors_pipeline.py --pdf-dir . --pdf-glob "*KOOTENAY MEDIA RELEASE.pdf" --debug
```

Outputs:

- `data/normalized/interior_realtors/kootenay_monthly.jsonl`
- `data/derived/interior_realtors/kootenay_market_stats.json`

Parse the monthly stats PDFs to extract price/DOM by property type:

```bash
python scripts/interior_realtors_stats_pipeline.py
python scripts/interior_realtors_stats_pipeline.py --pdf-dir data/raw/interior_realtors/monthly_stats --debug
```

Outputs:

- `data/normalized/interior_realtors/kootenay_monthly_stats.jsonl`
- `data/derived/interior_realtors/kootenay_monthly_stats.json`

## Normalize Realtor.ca listings

```bash
python scripts/normalize_realtor_ca.py
python scripts/normalize_realtor_ca.py --with-details
python scripts/normalize_realtor_ca.py --dataset sold_365
python scripts/normalize_realtor_ca.py --snapshot
```

Normalized outputs are written to `data/normalized/realtor_ca/`.
If you pass `--dataset sold_365`, outputs land under `data/normalized/realtor_ca/sold_365/`.

## Realtor.ca pipeline (active + sold + normalize)

```bash
python scripts/realtor_pipeline.py --place "Kootenay, British Columbia" \\
  --cookie-file /path/to/realtor_cookie.txt \\
  --active-records-per-page 200 \\
  --sold-within-days 365 \\
  --snapshot
```

Or use the config file at `config/realtor_pipeline.json`:

```bash
python scripts/realtor_pipeline.py
```

Default config uses a Kootenay bounding box (see `config/realtor_pipeline.json`).
`tile_on_cap` is enabled to auto-split the bbox if results exceed the 600 record cap.
The default bbox is `[48.3, 51.3, -118.9, -114.0]` (lat_min, lat_max, lon_min, lon_max).
Listings are filtered to `ProvinceName = British Columbia` to avoid Alberta bleed.

## Roll up Realtor.ca metrics

```bash
python scripts/rollup_realtor_ca.py
python scripts/rollup_realtor_ca.py --active data/normalized/realtor_ca/listings.jsonl --sold data/normalized/realtor_ca/sold_730/listings.jsonl
```

If `data/derived/interior_realtors/kootenay_market_stats.json` is present, the Market State
classification uses the official IR MOI/SNLR values instead of the Realtor.ca proxy.
The viewer shows a red→green SNLR/MOI scale legend to explain buyer/balanced/seller ranges.

## Daily run

Use the convenience script below (expects `./venv`):

```bash
bash scripts/daily_run.sh
```

Outputs are written to `data/derived/realtor_ca/`.
Additional outputs:
- `new_momentum.json`, `market_balance.json` (SNLR proxy based on listing dates).
- `snapshot_changes.json` (adds/removals between the latest two active snapshots).
- `price_cuts.json` (price-change stats between the latest two active snapshots).
- `market_state.json` (buyer/balanced/seller state from SNLR, MOI, DOM).

## Diff Realtor.ca snapshots

```bash
python scripts/diff_realtor_ca_snapshots.py --snapshot-dir data/normalized/realtor_ca/snapshots
python scripts/diff_realtor_ca_snapshots.py --dataset sold_365
```

## Web viewer

```bash
python -m http.server
```

Then open `http://localhost:8000/web/` to view the dashboard.
Use the dropdown to switch between Realtor.ca and RE/MAX (active-only).
You can also open `http://localhost:8000/web/?source=remax` to load RE/MAX by default.

## Macro pipeline (rates + unemployment)

```bash
python scripts/macro_pipeline.py
```

This uses `config/macro_pipeline.json` by default. Notes:
- Raw outputs: `data/raw/boc/` and `data/raw/statcan/`
- Derived outputs: `data/derived/macro/rates.json`, `data/derived/macro/unemployment.json`
- Set `statcan_vector` in `config/macro_pipeline.json` to enable unemployment data.
- Kootenay unemployment vector: `1642733899` (table `1410046201`); BC-wide fallback: `1642733779`.
- Run with `--no-fetch` to rebuild derived outputs from existing raw files.

## RE/MAX pipeline (fetch + normalize + rollup)

```bash
python scripts/remax_pipeline.py
```

This uses `config/remax_pipeline.json` by default. Notes:
- Raw outputs: `data/raw/remax/<dataset>/gallery/` + `data/raw/remax/<dataset>/metadata.json`
- Normalized: `data/normalized/remax_ca/<dataset>/listings.jsonl` + `summary.json`
- Derived (active-only): `data/derived/remax_ca/<dataset>/active_inventory.json`, `listing_trend.json`, `time_on_market.json`, `diffs/latest_diff.json` (requires snapshots)
- Oldest listings: `sort_key=1` and `sort_direction=0`
- Lowest price: `sort_key=0`
- Highest price: `sort_key=0` and `sort_direction=1`
- Coming soon: `features.comingSoon=true` via `--coming-soon` or `"features.comingSoon": true` in config params
- Province filter expects `BC` (matches `mlsProvince`/`province`).
- The web viewer expects RE/MAX data under `data/derived/remax_ca/kootenay_active/` unless you update `web/app.js`.
