# xxxclub-scraper

Scrapes torrent metadata from xxxclub.to into PostgreSQL. Python, runs in Docker.

## Architecture

```
scraper_utils.py   shared HTTP session, HTML parsers, helpers
db.py              schema, upsert, title-based update
import_all.py      one-shot full backfill (all pages, newest → oldest)
page_watcher.py    long-running service: polls browse + top100 on an interval
meta_extract.py    regex extraction of site/date/title from torrent titles → torrent_meta
metadata_fetcher.py  ThePornDB scene matching using torrent_meta structured fields
api.py             generic REST API server (FastAPI, JSON); routes under /api/v1/
web_ui.py          scene-first HTML frontend; calls api.py via HTTP (not db directly)
stremio_addon.py   Stremio addon (FastAPI APIRouter); mounted on web_ui at /stremio/ when STREMIO=true
entrypoint.py      starts watcher + metadata threads, then API server (thread) + web UI (main)
```

### Web layer request flow

```
Browser → web_ui (WEB_PORT, default 5000)
              └─ HTTP → api (API_PORT, default 5001)
                            └─ psycopg2 → PostgreSQL
```

### API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/scenes` | Paginated scene search; params: q, site, date_from, date_to, sort_by, sort_order, per_page, page |
| GET | `/api/v1/scenes/{id}` | Single scene by ThePornDB ID (must have at least one linked torrent) |
| GET | `/api/v1/torrents` | Paginated torrent search; params: q, site, date_from, date_to, category, sort_by, sort_order, per_page, page |
| GET | `/api/v1/categories` | List distinct torrent categories |

All responses: `{ total, page, per_page, total_pages, items[] }` (or `{ categories[] }` for categories).

### Web UI routes

| Path | Description |
|------|-------------|
| `/` | Redirect → `/scenes` |
| `/scenes` | Scene browser (primary / default landing) |
| `/torrents` | Torrent search |

## Database

Table `torrents`:

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
| `metadata_attempted_at` | TIMESTAMPTZ | last ThePornDB lookup attempt; retried after 7 days |

Table `torrent_meta` — structured fields extracted from the raw torrent title by `meta_extract.py`:

| Column | Type | Notes |
|---|---|---|
| `info_hash` | TEXT PK FK | |
| `title` | TEXT | clean scene title (site/date/codec noise stripped) |
| `resolution` | TEXT | 480p / 720p / 1080p / 2160p |
| `release_date` | DATE | parsed from title |
| `sitename` | TEXT | first meaningful word of title |

Table `scenes` — ThePornDB scene records (title, description, poster, background, date, duration, site/network info, performers JSONB, tags JSONB).

Table `torrent_scenes` — many-to-many join between `torrents` and `scenes`:

| Column | Type | Notes |
|---|---|---|
| `info_hash` | TEXT FK | |
| `scene_id` | TEXT FK | |
| `matched_at` | TIMESTAMPTZ | |
| `match_score` | REAL | composite score 0.0–1.0 from metadata_fetcher |

Connection is built from env vars (not a single `DATABASE_URL`):
`POSTGRES_HOST` (injected by compose as `db`) / `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` / `POSTGRES_PORT` (default 5432).

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
| `POSTGRES_PORT` | `5432` | Container-internal port |
| `POSTGRES_HOST_PORT` | `5432` | Host-side port mapping only |
| `BASE_URL` | `https://xxxclub.to` | |
| `SCRAPE_INTERVAL` | `3600` | Seconds between watcher polls |
| `MAX_PAGES` | `10` | Max browse pages per watcher poll |
| `PORNDB_API_KEY` | _(unset)_ | Enables ThePornDB metadata fetcher |
| `METADATA_INTERVAL` | `300` | Seconds between metadata fetch cycles |
| `METADATA_MIN_SCORE` | `0.65` | Minimum composite score to accept a TPDB match |
| `WATCHER` | _(unset)_ | Set to `true` to start the page watcher + metadata fetcher |
| `API_SERVER` | _(unset)_ | Set to `true` to start the REST API server in entrypoint |
| `WEB_UI` | _(unset)_ | Set to `true` to start the HTML web UI in entrypoint |
| `REDIS_URL` | _(unset)_ | Optional Redis URL (e.g. `redis://redis:6379`); enables API response caching |
| `API_PORT` | `5001` | Port the REST API server listens on |
| `WEB_PORT` | `5000` | Port the web UI listens on |
| `API_URL` | `http://localhost:{API_PORT}` | Base URL the web UI uses to reach the API |
| `STREMIO` | _(unset)_ | Set to `true` to enable the Stremio addon routes at `/stremio/` on the web UI |

All three flags are independent — set any combination. `WATCHER` and web servers all run in the same process; web servers block the main thread (API in a background thread if both are set). When only `WEB_UI=true`, set `API_URL` to point at a running API server elsewhere. If no flags are set the process exits immediately.

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

-- Match score distribution
SELECT round(match_score::numeric, 1) AS bucket, count(*)
FROM torrent_scenes GROUP BY bucket ORDER BY bucket;

-- Low-confidence matches (candidates for manual review or threshold tuning)
SELECT t.title, s.title AS scene, ts.match_score
FROM torrent_scenes ts
JOIN torrents t ON t.info_hash = ts.info_hash
JOIN scenes s ON s.id = ts.scene_id
WHERE ts.match_score < 0.75
ORDER BY ts.match_score;
```

## Migrations

```sql
-- Add match_score column (run once on existing databases)
ALTER TABLE torrent_scenes ADD COLUMN IF NOT EXISTS match_score REAL;
```

```sql
-- Normalize sites/networks/performers (run migrate_normalize.sql first to populate tables)
ALTER TABLE scenes ADD COLUMN IF NOT EXISTS site_uuid TEXT;

-- After verifying scene_performers and performers are populated:
ALTER TABLE scenes DROP COLUMN IF EXISTS performers;
ALTER TABLE scenes
    ADD CONSTRAINT fk_scenes_site_uuid
    FOREIGN KEY (site_uuid) REFERENCES sites(uuid) ON DELETE SET NULL;

-- After the metadata fetcher has cycled through all torrents, clean up synthetic
-- md5-based performer records that have been replaced by real ThePornDB UUIDs:
DELETE FROM performers p1
WHERE p1.uuid = md5(p1.name)::text
  AND EXISTS (
    SELECT 1 FROM performers p2
    WHERE p2.name = p1.name
      AND p2.uuid != p1.uuid
  );
```
