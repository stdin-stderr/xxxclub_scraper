"""Page watcher: polls /torrents/browse/all/ on a fixed interval and stores
new torrents, stopping as soon as a known entry is encountered."""

import logging
import os
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
    """Fetch the browse page and upsert new rows until a known hash is hit.
    Returns the number of rows upserted."""
    resp = session.get(BROWSE_URL, timeout=30)
    if resp.status_code != 200:
        log.error("HTTP %d fetching browse page", resp.status_code)
        return 0

    rows, _ = parse_page(resp.text)
    if not rows:
        log.info("No torrents found on page")
        return 0

    # Check which hashes are already in the DB
    all_hashes = [r["info_hash"] for r in rows]
    seen = db.known_hashes(conn, all_hashes)

    new_rows = []
    for row in rows:
        if row["info_hash"] in seen:
            log.debug("Known hash %s — stopping", row["info_hash"])
            break
        new_rows.append(row)

    if not new_rows:
        log.info("No new torrents")
        return 0

    # Set source for watcher-originated rows
    for row in new_rows:
        row["source"] = "watcher"

    db.upsert_torrents(conn, new_rows)
    log.info("Upserted %d new torrent(s)", len(new_rows))
    return len(new_rows)


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
