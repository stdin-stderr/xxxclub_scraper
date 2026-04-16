"""Page watcher: polls /torrents/browse/all/ on a fixed interval and stores
new torrents, stopping as soon as a known entry is encountered."""

import logging
import os
import random
import time

import db
from scraper_utils import BASE_URL, make_session, parse_page

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BROWSE_URL = f"{BASE_URL}/torrents/browse/all/"

try:
    SCRAPE_INTERVAL = int(os.environ.get("SCRAPE_INTERVAL", 300))
except ValueError:
    log.warning("Invalid SCRAPE_INTERVAL value, defaulting to 300s")
    SCRAPE_INTERVAL = 300


def poll_once(session, conn) -> int:
    """Paginate from newest, upsert new rows, stop when a known hash is hit.
    Returns the total number of rows upserted."""
    url = BROWSE_URL
    visited_urls: set[str] = set()
    total = 0
    max_pages = 10

    while url and len(visited_urls) < max_pages:
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

        new_rows = []
        done = False
        for row in rows:
            if row["info_hash"] in seen:
                done = True
                break
            row["source"] = "watcher"
            new_rows.append(row)

        if new_rows:
            db.upsert_torrents(conn, new_rows)
            total += len(new_rows)
            log.info("Upserted %d new torrent(s) from %s", len(new_rows), url)

        if done:
            break

        url = next_url
        if url:
            time.sleep(random.uniform(0.5, 1.5))

    if total == 0:
        log.info("No new torrents")
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
            poll_once(session, conn)
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
