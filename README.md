# xxxclub-scraper

Scrapes torrent metadata from xxxclub.to into a PostgreSQL database. Written in Python, runs in Docker.

## How it works

Three independently toggleable components, all started via `entrypoint.py`:

- **Watcher** (`WATCHER=true`) — polls `/torrents/browse/all/` on a fixed interval; also runs the ThePornDB metadata fetcher when `PORNDB_API_KEY` is set
- **API server** (`API_SERVER=true`) — generic FastAPI REST API (`/api/v1/scenes`, `/api/v1/torrents`, `/api/v1/categories`) that serves JSON from the database
- **Web UI** (`WEB_UI=true`) — scene-first HTML frontend; fetches all data from the API server (set `API_URL` if the API runs elsewhere)

One-shot tools:

- **`import_all.py`** — full backfill, walks every page of the browse index from newest to oldest
- **`meta_extract.py`** — re-extracts structured metadata (site, date, resolution) from all torrent titles

## Data stored per torrent

| Column | Description |
|---|---|
| `info_hash` | SHA-1 info hash (primary key) |
| `title` | Torrent name |
| `magnet` | Full magnet URI |
| `size_bytes` | File size in bytes |
| `category` | Quality category (SD, FullHD, UHD, …) |
| `date_added` | Upload date from the site |
| `uploader` | Uploader username |
| `seeders` | Seeder count at time of scrape |
| `leechers` | Leecher count at time of scrape |
| `source` | `browse` or `watcher` |
| `image_url` | Full-resolution preview image URL |
| `scraped_at` | Timestamp of last scrape |
| `metadata_attempted_at` | When ThePornDB lookup was last attempted |

## Scene metadata (ThePornDB)

When `PORNDB_API_KEY` is set, a background fetcher enriches each torrent with scene metadata from [ThePornDB](https://theporndb.net). Matched data is stored in two additional tables:

- **`scenes`** — one row per ThePornDB scene: title, description, poster, date, duration, site/network name and logo, performers (JSONB), tags (JSONB)
- **`torrent_scenes`** — many-to-many link between torrents and scenes; stores `match_score` so you can audit match quality

The fetcher processes up to 100 unmatched torrents every `METADATA_INTERVAL` seconds and marks each attempt so failures are only retried after 7 days.

### How matching works

Structured fields from `torrent_meta` (populated by `meta_extract.py`) drive the search:

1. **Pass 1** — ThePornDB query scoped to site + date + title (most specific)
2. **Pass 2** — site + title (no date)
3. **Pass 3** — global title search (fallback)

Each pass returns up to 5 candidates. Every candidate is scored against the torrent's structured data:

| Component | Weight | Logic |
|---|---|---|
| Title / performer | 50% | `difflib` ratio between `meta_title` and TPDB scene title **or** any performer name (whichever is higher — torrent titles often contain the performer rather than the scene title) |
| Site | 30% | 1.0 if site names match after normalising whitespace/dashes (e.g. `Hunt4K` ↔ `Hunt 4K`), else 0 |
| Date | 20% | 1.0 exact, 0.5 within 30 days, 0 otherwise; redistributed to title weight when no date is available |

The highest-scoring candidate is accepted only if `score >= METADATA_MIN_SCORE` (default `0.6`). The score is stored in `torrent_scenes.match_score`.

### Dry run

Preview matches without writing anything to the database:

```bash
docker compose exec xxxclub_scraper python metadata_fetcher.py --dry-run --limit 20
```

Prints for each torrent: the extracted site/title/date, each TPDB candidate with its full score breakdown (title sim, site sim, date sim, total, performers, scene date), and whether the best candidate cleared the threshold.

### Force re-attempt all

```bash
docker compose exec xxxclub_scraper python metadata_fetcher.py --all --limit 500
```

Resets `metadata_attempted_at` for every torrent and re-attempts matching.

## Setup

```bash
cp .env.example .env
# Edit .env if needed, then:
docker compose up -d
```

## Configuration (`.env`)

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `xxxclub_scraper` | Postgres username |
| `POSTGRES_PASSWORD` | `xxxclub_scraper` | Postgres password |
| `POSTGRES_DB` | `xxxclub` | Postgres database name |
| `POSTGRES_HOST` | `db` | Postgres host (inside Docker: `db`) |
| `POSTGRES_HOST_PORT` | `5432` | Host-side port mapping for the `db` container |
| `BASE_URL` | `https://xxxclub.to` | Site base URL |
| `SCRAPE_INTERVAL` | `3600` | Seconds between page watcher polls |
| `MAX_PAGES` | `10` | Max pages to paginate per poll cycle |
| `WATCHER` | `false` | Set to `true` to start the page watcher and metadata fetcher |
| `API_SERVER` | `false` | Set to `true` to start the REST API server |
| `WEB_UI` | `false` | Set to `true` to start the HTML web UI |
| `WEB_PORT` | `5000` | Port the web UI listens on |
| `API_PORT` | `5001` | Port the REST API server listens on |
| `API_URL` | `http://localhost:{API_PORT}` | API base URL used by the web UI (override for remote API) |
| `PORNDB_API_KEY` | _(unset)_ | ThePornDB API key; metadata fetcher is disabled if absent |
| `METADATA_INTERVAL` | `300` | Seconds between metadata fetch cycles |
| `METADATA_MIN_SCORE` | `0.65` | Minimum match score (0.0–1.0); lower = more matches, higher = fewer false positives |
| `REDIS_URL` | _(unset)_ | Optional Redis URL (e.g. `redis://redis:6379`); enables API response caching |

## Redis caching (optional)

The API server supports optional Redis-backed response caching. When `REDIS_URL` is set, all read endpoints cache their results — repeated requests for the same parameters are served from Redis without hitting the database.

A `redis` service is included in `docker-compose.yml` but not required. To enable it:

1. Add `REDIS_URL=redis://redis:6379` to your `.env`
2. Bring the stack up — the scraper will connect to Redis automatically

```bash
docker compose up -d
```

If `REDIS_URL` is not set, or if Redis is unreachable, the API falls back to querying the database on every request with no error.

### What gets cached

| Endpoint | TTL | Notes |
|---|---|---|
| `GET /api/v1/stats` | 10 min | Aggregate counts and timelines |
| `GET /api/v1/categories` | 1 hour | Rarely changes |
| `GET /api/v1/scenes` | 10 min | Skipped when `q` is set |
| `GET /api/v1/torrents` | 10 min | Skipped when `q` is set |
| `GET /api/v1/sites` | 1 hour | Skipped when `q` is set |
| `GET /api/v1/sites/{uuid}` | 1 hour | Per-UUID key |
| `GET /api/v1/performers` | 1 hour | Skipped when `q` is set |
| `GET /api/v1/performers/{uuid}` | 1 hour | Per-UUID key |

Free-text search requests (`q=...`) are never cached since they produce unique result sets per query.

## Web UI

Set `WEB_UI=true` and `API_SERVER=true` in `.env` and restart the container. The scene browser will be at `http://localhost:5000` and the API at `http://localhost:5001`.

To run them in separate containers, set `API_URL=http://<api-host>:5001` in the web UI container's environment.

Features:
- Full-text search on title
- Filter by category
- Sort by date added, seeders, leechers, title, or size
- Quick-filter links for latest and top per category
- 25 / 50 / 100 results per page
- Thumbnail preview with hover zoom (ThePornDB poster used when matched)
- Scene metadata inline: site name/logo, performers, tags, description snippet
- One-click magnet links

## Running a full backfill

```bash
docker compose exec xxxclub_scraper python import_all.py
```

This walks all pages newest-to-oldest and can be interrupted safely — all committed pages are preserved. Re-running after an interruption will upsert duplicates harmlessly.

To resume after an interruption:

```bash
docker compose exec xxxclub_scraper python import_all.py --cursor [last url from logfile]
```

## Re-extracting torrent metadata

After the database is populated (or when the extraction logic in `meta_extract.py` changes), re-run extraction over all existing rows:

```bash
docker compose exec xxxclub_scraper python meta_extract.py
```

This processes all torrents in batches of 500 and logs progress. Safe to re-run at any time — it upserts, so existing rows are overwritten with fresh values.
