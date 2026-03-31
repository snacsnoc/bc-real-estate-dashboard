# Vacant land finder - shut up and take my money (for land)!

Local-first Flask app to find vacant-land listings around a center point.

## What it does

- Queries Realtor.ca + RE/MAX listing endpoints.
- Filters to vacant land.
- Returns normalized listing results.
- Frontend handles view sorting/filtering (distance/price/source/max price).
- If one source fails, results from the other source still render.

## One run command

From repo root:

```bash
bash apps/vacant_land_finder/run.sh
```

Open:

- [http://127.0.0.1:8787/](http://127.0.0.1:8787/)

## Active app structure

- `server.py` - Flask routes + validation
- `search.py` - source fetch + normalization
- `templates/` - Jinja templates
- `static/` - JS/CSS

## Cookie setup (Realtor.ca)

Use one file-based mechanism (plus optional direct override):

1. `--realtor-cookie` (direct CLI override)
2. `REALTOR_COOKIE_FILE` (env var file path)
3. `--realtor-cookie-file` (defaults to repo `cookiefile.txt`)

## API

`GET /api/search`

Query params:

- `lat` (float)
- `lng` (float)
- `radius_km` (float, max 250)
- `max_results` (int, max 500)
- `max_pages` (int, max 10)
- `include_realtor` (`true|false`)
- `include_remax` (`true|false`)

Response fields used by UI:

- `generated_at`
- `results`
- `errors`
- `all_sources_failed`

If all enabled sources fail, API returns HTTP `502`.
