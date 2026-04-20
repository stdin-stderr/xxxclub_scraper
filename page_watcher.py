"""Page watcher: polls /torrents/browse/all/ on a fixed interval and stores
new torrents, stopping as soon as a known entry is encountered."""

import logging
import os
import random
import time

import db
import meta_extract
from scraper_utils import BASE_URL, make_session, parse_page, parse_top100_page

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BROWSE_URL = f"{BASE_URL}/torrents/browse/all/"
TOP100_URLS = [f"{BASE_URL}/torrents/top100/{i}" for i in range(8)]

try:
    SCRAPE_INTERVAL = int(os.environ.get("SCRAPE_INTERVAL", 3600))
except ValueError:
    log.warning("Invalid SCRAPE_INTERVAL value, defaulting to 3600s")
    SCRAPE_INTERVAL = 3600

try:
    MAX_PAGES = int(os.environ.get("MAX_PAGES", 10))
except ValueError:
    log.warning("Invalid MAX_PAGES value, defaulting to 10")
    MAX_PAGES = 10


def poll_once(session, conn) -> int:
    """Paginate from newest, upsert new rows, stop when a known hash is hit.
    Returns the total number of rows upserted."""
    url = BROWSE_URL
    visited_urls: set[str] = set()
    total = 0
    while url and len(visited_urls) < MAX_PAGES:
        if url in visited_urls:
            break
        visited_urls.add(url)

        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            log.error("HTTP %d fetching %s", resp.status_code, url)
            break

        rows, next_url = parse_page(resp.text)
        if not rows:
            break

        all_hashes = [r["info_hash"] for r in rows]
        seen = db.known_hashes(conn, all_hashes)

        # Stop paginating once we hit a known hash, but upsert everything on this page
        done = any(r["info_hash"] in seen for r in rows)
        new_count = sum(1 for r in rows if r["info_hash"] not in seen)

        for row in rows:
            row["source"] = "watcher"
        db.upsert_torrents(conn, rows)
        meta_extract.upsert_torrent_meta(conn, rows)
        total += len(rows)
        log.info("Upserted %d row(s) from %s (%d new)", len(rows), url, new_count)

        if done:
            break

        url = next_url
        if url:
            time.sleep(random.uniform(0.5, 1.5))

    if total == 0:
        log.info("No torrents found")
    return total


def poll_top100(session, conn) -> int:
    """Fetch all top100 pages and update seeder/leecher counts by title match.
    Returns the total number of rows updated."""
    total = 0
    for url in TOP100_URLS:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            log.error("HTTP %d fetching %s", resp.status_code, url)
            continue

        rows = parse_top100_page(resp.text)
        if not rows:
            continue

        updated = db.update_counts_by_title(conn, rows)
        total += updated
        log.info("top100 %s: updated %d/%d row(s)", url, updated, len(rows))
        time.sleep(random.uniform(0.5, 1.5))

    return total


def _get_conn():
    conn = db.get_connection()
    db.init_schema(conn)
    return conn


def run():
    session = make_session()
    conn = _get_conn()

    log.info("Page watcher started — polling every %ds", SCRAPE_INTERVAL)
    while True:
        try:
            age = db.seconds_since_last_scrape(conn)
            if age is not None and age < SCRAPE_INTERVAL:
                wait = SCRAPE_INTERVAL - age
                log.info("Last scrape was %.0fs ago, skipping poll — sleeping %.0fs", age, wait)
                time.sleep(wait)
                continue

            poll_once(session, conn)
            poll_top100(session, conn)
        except Exception as exc:
            log.error("Error during poll: %s", exc, exc_info=True)
            log.info("Reconnecting to database...")
            try:
                conn.close()
            except Exception:
                pass
            conn = _get_conn()
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    run()
