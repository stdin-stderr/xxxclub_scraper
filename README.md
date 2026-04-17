# xxxclub-scraper

Scrapes torrent metadata from xxxclub.to into a PostgreSQL database. Written in Python, runs in Docker.

## How it works

- **`page_watcher.py`** — long-running service, polls `/torrents/browse/all/` on a fixed interval, paginates forward through new entries and stops as soon as a known torrent is encountered (or `MAX_PAGES` pages)
- **`import_all.py`** — one-shot full backfill, walks every page of the browse index from newest to oldest
- **`web_ui.py`** — optional FastAPI web interface for searching and browsing the database

Both the watcher, the metadata fetcher, and the web UI run in the same container, started via `entrypoint.py`.

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
- **`torrent_scenes`** — many-to-many link between torrents and scenes (multiple quality variants of the same release share one scene record)

The fetcher processes up to 100 unmatched torrents every `METADATA_INTERVAL` seconds and marks each attempt so failures are only retried after 7 days.

### Dry run

Preview matches without writing anything to the database:

```bash
docker compose exec xxxclub_scraper python metadata_fetcher.py --dry-run --limit 20
```

Each torrent prints its matched scene (site, title, performers, tag count) or `NO MATCH`.

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
| `WEB_UI` | `false` | Set to `true` to enable the web UI |
| `WEB_PORT` | `5000` | Port the web UI listens on |
| `PORNDB_API_KEY` | _(unset)_ | ThePornDB API key; metadata fetcher is disabled if absent |
| `METADATA_INTERVAL` | `300` | Seconds between metadata fetch cycles |

## Web UI

Set `WEB_UI=true` in `.env` and restart the container. The UI will be available at `http://localhost:5000` (or whichever `WEB_PORT` you set).

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
