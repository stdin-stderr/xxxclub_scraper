import os
import psycopg2
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv

load_dotenv()

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS torrents (
    info_hash              TEXT PRIMARY KEY,
    title                  TEXT,
    magnet                 TEXT NOT NULL,
    size_bytes             BIGINT,
    category               TEXT,
    date_added             TIMESTAMPTZ,
    uploader               TEXT,
    seeders                INT,
    leechers               INT,
    source                 TEXT,
    image_url              TEXT,
    scraped_at             TIMESTAMPTZ DEFAULT NOW(),
    metadata_attempted_at  TIMESTAMPTZ
);
"""

CREATE_SCENES = """
CREATE TABLE IF NOT EXISTS scenes (
    id               TEXT PRIMARY KEY,
    title            TEXT,
    description      TEXT,
    poster_url       TEXT,
    background_url   TEXT,
    date             DATE,
    duration_seconds INT,
    site_name        TEXT,
    site_slug        TEXT,
    site_logo_url    TEXT,
    network_name     TEXT,
    network_slug     TEXT,
    network_logo_url TEXT,
    performers       JSONB,
    tags             JSONB,
    fetched_at       TIMESTAMPTZ DEFAULT NOW()
);
"""

CREATE_TORRENT_SCENES = """
CREATE TABLE IF NOT EXISTS torrent_scenes (
    info_hash    TEXT NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
    scene_id     TEXT NOT NULL REFERENCES scenes(id) ON DELETE CASCADE,
    matched_at   TIMESTAMPTZ DEFAULT NOW(),
    match_score  REAL,
    PRIMARY KEY (info_hash, scene_id)
);
"""

CREATE_TORRENT_META = """
CREATE TABLE IF NOT EXISTS torrent_meta (
    info_hash    TEXT PRIMARY KEY REFERENCES torrents(info_hash) ON DELETE CASCADE,
    title        TEXT,
    resolution   TEXT,
    release_date DATE,
    sitename     TEXT,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
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

ALLOWED_SORT_COLS = {"seeders", "leechers", "date_added", "title", "size_bytes", "release_date"}

# Columns that live on torrent_meta rather than torrents
_META_SORT_COLS = {"release_date"}

ALLOWED_SCENE_SORT_COLS = {"date", "title", "site_name", "seeders"}


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def init_schema(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE)
        cur.execute(CREATE_SCENES)
        cur.execute(CREATE_TORRENT_SCENES)
        cur.execute(CREATE_TORRENT_META)
    conn.commit()


def seconds_since_last_scrape(conn) -> float | None:
    """Return seconds since the most recently scraped row, or None if the table is empty."""
    from datetime import datetime, timezone
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(scraped_at) FROM torrents")
        last = cur.fetchone()[0]
    if last is None:
        return None
    return (datetime.now(timezone.utc) - last).total_seconds()


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


def list_categories(conn) -> list[str]:
    """Return distinct non-null categories sorted alphabetically."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT category FROM torrents "
            "WHERE category IS NOT NULL ORDER BY category"
        )
        return [row[0] for row in cur.fetchall()]


def count_torrents(
    conn,
    query: str | None = None,
    category: str | None = None,
    site: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> int:
    """Count rows matching the given filters."""
    needs_meta = site or date_from or date_to
    sql = "SELECT COUNT(*) FROM torrents t"
    if needs_meta:
        sql += " LEFT JOIN torrent_meta tm ON tm.info_hash = t.info_hash"
    sql += " WHERE TRUE"
    params: list = []
    if query:
        sql += " AND t.title ILIKE %s"
        params.append(f"%{query}%")
    if category:
        sql += " AND t.category = %s"
        params.append(category)
    if site:
        sql += " AND tm.sitename ILIKE %s"
        params.append(f"%{site}%")
    if date_from:
        sql += " AND tm.release_date >= %s"
        params.append(date_from)
    if date_to:
        sql += " AND tm.release_date <= %s"
        params.append(date_to)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def search_torrents(
    conn,
    query: str | None = None,
    category: str | None = None,
    site: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    sort_by: str = "seeders",
    sort_order: str = "desc",
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Search torrents with optional filters, sorting, and pagination.
    Includes scene metadata and torrent_meta when available."""
    if sort_by not in ALLOWED_SORT_COLS:
        sort_by = "seeders"
    sort_order = "ASC" if sort_order.lower() == "asc" else "DESC"
    order_col = f"tm.{sort_by}" if sort_by in _META_SORT_COLS else f"t.{sort_by}"

    sql = (
        "SELECT t.info_hash, t.title, t.magnet, t.size_bytes, t.category, "
        "t.date_added, t.uploader, t.seeders, t.leechers, t.source, t.image_url, t.scraped_at, "
        "s.title AS scene_title, s.description AS scene_description, "
        "s.poster_url, s.site_name, s.site_logo_url, "
        "s.network_name, s.network_logo_url, s.performers, s.tags, "
        "tm.sitename, tm.title AS meta_title, tm.resolution, tm.release_date "
        "FROM torrents t "
        "LEFT JOIN LATERAL ("
        "  SELECT scene_id FROM torrent_scenes WHERE info_hash = t.info_hash LIMIT 1"
        ") ts ON true "
        "LEFT JOIN scenes s ON s.id = ts.scene_id "
        "LEFT JOIN torrent_meta tm ON tm.info_hash = t.info_hash "
        "WHERE TRUE"
    )
    params: list = []
    if query:
        sql += " AND t.title ILIKE %s"
        params.append(f"%{query}%")
    if category:
        sql += " AND t.category = %s"
        params.append(category)
    if site:
        sql += " AND tm.sitename ILIKE %s"
        params.append(f"%{site}%")
    if date_from:
        sql += " AND tm.release_date >= %s"
        params.append(date_from)
    if date_to:
        sql += " AND tm.release_date <= %s"
        params.append(date_to)
    sql += f" ORDER BY {order_col} {sort_order} NULLS LAST"
    sql += " LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def count_scenes(
    conn,
    query: str | None = None,
    site: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> int:
    """Count distinct scenes that have at least one linked torrent."""
    sql = (
        "SELECT COUNT(DISTINCT s.id) FROM scenes s "
        "INNER JOIN torrent_scenes ts ON ts.scene_id = s.id "
        "WHERE TRUE"
    )
    params: list = []
    if query:
        sql += " AND s.title ILIKE %s"
        params.append(f"%{query}%")
    if site:
        # Match regardless of internal spaces (e.g. "Tiny4K" ↔ "Tiny 4K")
        sql += " AND (s.site_name ILIKE %s OR replace(s.site_name, ' ', '') ILIKE %s)"
        params.append(f"%{site}%")
        params.append(f"%{site.replace(' ', '')}%")
    if date_from:
        sql += " AND s.date >= %s"
        params.append(date_from)
    if date_to:
        sql += " AND s.date <= %s"
        params.append(date_to)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def search_scenes(
    conn,
    query: str | None = None,
    site: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    sort_by: str = "date",
    sort_order: str = "desc",
    limit: int = 25,
    offset: int = 0,
) -> list[dict]:
    """Return scenes that have at least one linked torrent, with torrents aggregated as JSON.
    Only scenes with linked torrents are returned (INNER JOIN on torrent_scenes)."""
    if sort_by not in ALLOWED_SCENE_SORT_COLS:
        sort_by = "date"
    sort_order = "ASC" if sort_order.lower() == "asc" else "DESC"
    # seeders is an aggregate over joined torrents, not a scenes column
    order_col = "MAX(t.seeders)" if sort_by == "seeders" else f"s.{sort_by}"

    sql = (
        "SELECT s.id, s.title, s.description, s.background_url, s.poster_url, "
        "s.date, s.duration_seconds, "
        "s.site_name, s.site_logo_url, "
        "s.network_name, s.network_logo_url, "
        "s.performers, s.tags, "
        "json_agg(json_build_object("
        "  'info_hash', t.info_hash,"
        "  'title', t.title,"
        "  'magnet', t.magnet,"
        "  'size_bytes', t.size_bytes,"
        "  'category', t.category,"
        "  'seeders', t.seeders,"
        "  'leechers', t.leechers,"
        "  'resolution', tm.resolution,"
        "  'date_added', t.date_added"
        ") ORDER BY t.seeders DESC NULLS LAST) AS torrents "
        "FROM scenes s "
        "INNER JOIN torrent_scenes ts ON ts.scene_id = s.id "
        "INNER JOIN torrents t ON t.info_hash = ts.info_hash "
        "LEFT JOIN torrent_meta tm ON tm.info_hash = t.info_hash "
        "WHERE TRUE"
    )
    params: list = []
    if query:
        sql += " AND s.title ILIKE %s"
        params.append(f"%{query}%")
    if site:
        # Match regardless of internal spaces (e.g. "Tiny4K" ↔ "Tiny 4K")
        sql += " AND (s.site_name ILIKE %s OR replace(s.site_name, ' ', '') ILIKE %s)"
        params.append(f"%{site}%")
        params.append(f"%{site.replace(' ', '')}%")
    if date_from:
        sql += " AND s.date >= %s"
        params.append(date_from)
    if date_to:
        sql += " AND s.date <= %s"
        params.append(date_to)
    # GROUP BY PK — PostgreSQL infers functional dependency for other columns
    sql += " GROUP BY s.id"
    sql += f" ORDER BY {order_col} {sort_order} NULLS LAST"
    sql += " LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def upsert_scene(conn, scene: dict):
    """Insert or update a ThePornDB scene."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scenes (
                id, title, description, poster_url, background_url, date,
                duration_seconds, site_name, site_slug, site_logo_url,
                network_name, network_slug, network_logo_url,
                performers, tags, fetched_at
            ) VALUES (
                %(id)s, %(title)s, %(description)s, %(poster_url)s, %(background_url)s, %(date)s,
                %(duration_seconds)s, %(site_name)s, %(site_slug)s, %(site_logo_url)s,
                %(network_name)s, %(network_slug)s, %(network_logo_url)s,
                %(performers)s, %(tags)s, NOW()
            )
            ON CONFLICT (id) DO UPDATE SET
                title            = EXCLUDED.title,
                description      = EXCLUDED.description,
                poster_url       = EXCLUDED.poster_url,
                background_url   = EXCLUDED.background_url,
                date             = EXCLUDED.date,
                duration_seconds = EXCLUDED.duration_seconds,
                site_name        = EXCLUDED.site_name,
                site_slug        = EXCLUDED.site_slug,
                site_logo_url    = EXCLUDED.site_logo_url,
                network_name     = EXCLUDED.network_name,
                network_slug     = EXCLUDED.network_slug,
                network_logo_url = EXCLUDED.network_logo_url,
                performers       = EXCLUDED.performers,
                tags             = EXCLUDED.tags,
                fetched_at       = NOW()
            """,
            {**scene, "performers": Json(scene["performers"]), "tags": Json(scene["tags"])},
        )
    conn.commit()


def link_torrent_scene(conn, info_hash: str, scene_id: str, match_score: float | None = None):
    """Link a torrent to a scene (idempotent). Updates match_score if re-inserted."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO torrent_scenes (info_hash, scene_id, matched_at, match_score)
            VALUES (%s, %s, NOW(), %s)
            ON CONFLICT (info_hash, scene_id) DO UPDATE SET
                match_score = EXCLUDED.match_score,
                matched_at  = EXCLUDED.matched_at
            """,
            (info_hash, scene_id, match_score),
        )
    conn.commit()


def mark_metadata_attempted(conn, info_hash: str):
    """Record that metadata lookup was attempted for this torrent."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE torrents SET metadata_attempted_at = NOW() WHERE info_hash = %s",
            (info_hash,),
        )
    conn.commit()


def unlink_torrent_scenes(conn, info_hash: str) -> int:
    """Remove all scene links for a torrent. Returns number of rows deleted."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM torrent_scenes WHERE info_hash = %s",
            (info_hash,),
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted


def fetch_by_hashes(conn, hashes: list[str]) -> list[dict]:
    """Return torrent + torrent_meta rows for the given info hashes.
    Hashes not present in the database are silently skipped.
    Returns same shape as fetch_unmatched."""
    if not hashes:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.info_hash, t.title,
                   tm.title AS meta_title, tm.sitename, tm.release_date
            FROM torrents t
            LEFT JOIN torrent_meta tm ON tm.info_hash = t.info_hash
            WHERE t.info_hash = ANY(%s)
            ORDER BY t.date_added DESC NULLS LAST
            """,
            (hashes,),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_unmatched(conn, limit: int = 100, retry_days: int = 7) -> list[dict]:
    """Return up to `limit` torrents that need a metadata lookup attempt.

    Each row includes structured fields from torrent_meta (may be None if not yet extracted):
    meta_title, sitename, release_date.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT t.info_hash, t.title,
                   tm.title AS meta_title, tm.sitename, tm.release_date
            FROM torrents t
            LEFT JOIN torrent_scenes ts ON ts.info_hash = t.info_hash
            LEFT JOIN torrent_meta tm ON tm.info_hash = t.info_hash
            WHERE ts.scene_id IS NULL
              AND (
                t.metadata_attempted_at IS NULL
                OR t.metadata_attempted_at < NOW() - INTERVAL '{retry_days} days'
              )
            ORDER BY t.date_added DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


UPSERT_META = """
INSERT INTO torrent_meta (info_hash, title, resolution, release_date, sitename)
VALUES %s
ON CONFLICT (info_hash) DO UPDATE SET
    title        = EXCLUDED.title,
    resolution   = EXCLUDED.resolution,
    release_date = EXCLUDED.release_date,
    sitename     = EXCLUDED.sitename,
    extracted_at = NOW();
"""


def upsert_torrent_meta(conn, tuples: list[tuple]):
    """Bulk upsert (info_hash, title, resolution, release_date, sitename) into torrent_meta."""
    if not tuples:
        return
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_META, tuples)
    conn.commit()


def upsert_torrents(conn, rows: list[dict]):
    """Bulk upsert a list of torrent dicts. Each dict must have at least
    info_hash and magnet."""
    if not rows:
        return
    tuples = [tuple(r.get(c) for c in COLS) for r in rows]
    with conn.cursor() as cur:
        execute_values(cur, UPSERT, tuples)
    conn.commit()
