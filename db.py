import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS torrents (
    info_hash   TEXT PRIMARY KEY,
    title       TEXT,
    magnet      TEXT NOT NULL,
    size_bytes  BIGINT,
    category    TEXT,
    date_added  TIMESTAMPTZ,
    uploader    TEXT,
    seeders     INT,
    leechers    INT,
    source      TEXT,
    image_url   TEXT,
    scraped_at  TIMESTAMPTZ DEFAULT NOW()
);
"""

UPSERT = """
INSERT INTO torrents
    (info_hash, title, magnet, size_bytes, category, date_added, uploader,
     seeders, leechers, source, image_url, scraped_at)
VALUES %s
ON CONFLICT (info_hash) DO UPDATE SET
    seeders    = EXCLUDED.seeders,
    leechers   = EXCLUDED.leechers,
    image_url  = COALESCE(EXCLUDED.image_url, torrents.image_url),
    scraped_at = EXCLUDED.scraped_at;
"""

COLS = (
    "info_hash",
    "title",
    "magnet",
    "size_bytes",
    "category",
    "date_added",
    "uploader",
    "seeders",
    "leechers",
    "source",
    "image_url",
    "scraped_at",
)


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=5432,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def init_schema(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE)
    conn.commit()


def known_hashes(conn, hashes: list[str]) -> set[str]:
    """Return the subset of hashes that already exist in the database."""
    if not hashes:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT info_hash FROM torrents WHERE info_hash = ANY(%s)",
            (hashes,),
        )
        return {row[0] for row in cur.fetchall()}


def update_counts_by_title(conn, rows: list[dict]) -> int:
    """Update seeders/leechers/scraped_at for rows matched by exact title.
    Used for top100 pages where no info hash is available.
    Returns the number of rows updated."""
    if not rows:
        return 0
    updated = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """UPDATE torrents
                      SET seeders    = %s,
                          leechers   = %s,
                          scraped_at = %s
                    WHERE title = %s""",
                (row["seeders"], row["leechers"], row["scraped_at"], row["title"]),
            )
            updated += cur.rowcount
    conn.commit()
    return updated


def upsert_torrents(conn, rows: list[dict]):
    """Bulk upsert a list of torrent dicts. Each dict must have at least
    info_hash and magnet."""
    if not rows:
        return
    tuples = [tuple(r.get(c) for c in COLS) for r in rows]
    with conn.cursor() as cur:
        execute_values(cur, UPSERT, tuples)
    conn.commit()
