"""One-shot full backfill: walks all pages of /torrents/browse/all/ and
stores every torrent in the database."""

import argparse
import logging
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

START_URL = f"{BASE_URL}/torrents/browse/all/"
PAGE_DELAY = (0.5, 1.5)  # seconds, random range


def run(start_url: str | None = None):
    session = make_session()
    conn = db.get_connection()
    db.init_schema(conn)

    url = start_url or START_URL
    page_num = 0
    total_upserted = 0

    try:
        while url:
            page_num += 1
            log.info("Fetching page %d: %s", page_num, url)

            resp = session.get(url, timeout=30)
            if resp.status_code != 200:
                log.error("HTTP %d on %s — stopping", resp.status_code, url)
                break

            rows, next_url = parse_page(resp.text)

            if not rows:
                log.info("No torrents found on page %d — done", page_num)
                break

            for row in rows:
                row["source"] = "browse"
            db.upsert_torrents(conn, rows)
            total_upserted += len(rows)
            log.info("Page %d: upserted %d rows (total so far: %d)", page_num, len(rows), total_upserted)

            url = next_url
            if url:
                time.sleep(random.uniform(*PAGE_DELAY))
    finally:
        conn.close()

    log.info("Finished. Total rows upserted: %d", total_upserted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cursor",
        metavar="URL",
        help="Resume from this URL (copy from the last log line before interruption)",
    )
    args = parser.parse_args()
    run(start_url=args.cursor)
