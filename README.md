# xxxclub-scraper

Scrapes torrent metadata from xxxclub.to into a PostgreSQL database. Written in Python, runs in Docker.

## How it works

Two scripts share the same database:

- **`page_watcher.py`** — long-running service, polls `/torrents/browse/all/` on a fixed interval, paginates forward through new entries and stops as soon as a known torrent is encountered (or the 10th page)
- **`import_all.py`** — one-shot full backfill, walks every page of the browse index from newest to oldest

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
| `POSTGRES_HOST_PORT` | `5432` | Host-side port mapping for the `db` container |
| `BASE_URL` | `https://xxxclub.to` | Site base URL |
| `SCRAPE_INTERVAL` | `3600` | Seconds between page watcher polls |

## Running a full backfill

```bash
docker compose exec xxxclub_scraper python import_all.py
```

This walks all pages newest-to-oldest and can be interrupted safely — all committed pages are preserved. Re-running after an interruption will upsert duplicates harmlessly.

If you interupt the backfill, continue with:

```bash
docker compose exec xxxclub_scraper python import_all.py --cursor [last url from logfile]
```
  
