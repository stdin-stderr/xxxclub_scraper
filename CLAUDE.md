# xxxclub-scraper

Scrapes torrent metadata from xxxclub.to into PostgreSQL. Python, runs in Docker.

## Architecture

```
scraper_utils.py   shared HTTP session, HTML parsers, helpers
db.py              schema, upsert, title-based update
import_all.py      one-shot full backfill (all pages, newest → oldest)
page_watcher.py    long-running service: polls browse + top100 on an interval
```

## Database

Single table `torrents`:

| Column | Type | Notes |
|---|---|---|
| `info_hash` | TEXT PK | 40-char hex SHA-1 |
| `title` | TEXT | |
| `magnet` | TEXT NOT NULL | `magnet:?xt=urn:btih:{hash}&dn=...` |
| `size_bytes` | BIGINT | |
| `category` | TEXT | SD / FullHD / UHD / … |
| `date_added` | TIMESTAMPTZ | |
| `uploader` | TEXT | |
| `seeders` | INT | updated on every upsert |
| `leechers` | INT | updated on every upsert |
| `source` | TEXT | `browse`, `watcher`, or `top100` |
| `image_url` | TEXT | full-res (`/p/…`), converted from thumbnail (`/ps/…`) |
| `scraped_at` | TIMESTAMPTZ | |

Connection is built from env vars (not a single `DATABASE_URL`):
`POSTGRES_HOST` (injected by compose as `db`) / `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` / port hardcoded to 5432.

## Site quirks

### Browse pages (`/torrents/browse/all/`)
- Torrent links: `<a id="#i{40-char-hex-hash}">` — info hash is in the `id`
- Image: `<img class="floaterimg" id="i{hash}">` — note no `#`, `id` matches hash
- Pagination: dozens of decoy `<a class="page-link" title="Next Page">` links with a fixed fake cursor; the **real** next-page link has `data-no-instant` and no `class` or a different style

### Top100 pages (`/torrents/top100/0` … `/torrents/top100/7`)
- Same HTML structure but `id="#i{numeric_id}"` — NOT an info hash
- `parse_top100_page()` is used instead of `parse_page()`
- Seeder/leecher counts are updated via `db.update_counts_by_title()` (title-based match)
- Category tag uses correctly-spelled `<label>` (browse pages use `<lable>`)

### Magnet URIs
- `xt=urn:btih:{hash}` — colons must NOT be percent-encoded
- `dn` is URL-encoded with `urllib.parse.quote`

## Running

```bash
# Start everything
docker compose up -d

# Full backfill
docker compose exec xxxclub_scraper python import_all.py

# Resume backfill from a cursor URL (copy from last log line)
docker compose exec xxxclub_scraper python import_all.py \
  --cursor https://xxxclub.to/torrents/browse/all/7296816424212350398

# Fix any existing magnets with encoded colons
docker compose exec db psql -U xxxclub_scraper -d xxxclub -c \
  "UPDATE torrents SET magnet = replace(magnet, 'xt=urn%3Abtih%3A', 'xt=urn:btih:') WHERE magnet LIKE '%xt=urn%3Abtih%3A%';"
```

## page_watcher behaviour per poll cycle

1. **Browse** — fetches up to `MAX_PAGES` pages newest-first; upserts all rows on each page; stops paginating when a known hash is seen on the page
2. **Top100** — fetches all 8 category pages; updates seeders/leechers by title match

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `xxxclub_scraper` | |
| `POSTGRES_PASSWORD` | `xxxclub_scraper` | |
| `POSTGRES_DB` | `xxxclub` | |
| `POSTGRES_HOST_PORT` | `5432` | Host-side port mapping only |
| `BASE_URL` | `https://xxxclub.to` | |
| `SCRAPE_INTERVAL` | `3600` | Seconds between watcher polls |
| `MAX_PAGES` | `10` | Max browse pages per watcher poll |

## Useful queries

```sql
-- Row count by category
SELECT category, count(*) FROM torrents GROUP BY category ORDER BY count(*) DESC;

-- Top 100 by seeders
SELECT info_hash FROM torrents ORDER BY seeders DESC NULLS LAST LIMIT 100;

-- Count and size by category for a specific day
SELECT category, COUNT(*), pg_size_pretty(SUM(size_bytes))
FROM torrents WHERE date_added::date = '2026-04-17'
GROUP BY category ORDER BY count DESC;
```
