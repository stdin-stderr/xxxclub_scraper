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

CREATE_NETWORKS = """
CREATE TABLE IF NOT EXISTS networks (
    uuid         TEXT PRIMARY KEY,
    tpdb_id      INT UNIQUE,
    slug         TEXT,
    name         TEXT NOT NULL,
    url          TEXT,
    rating       REAL,
    logo_url     TEXT,
    favicon_url  TEXT,
    poster_url   TEXT
);
"""

CREATE_SITES = """
CREATE TABLE IF NOT EXISTS sites (
    uuid         TEXT PRIMARY KEY,
    tpdb_id      INT UNIQUE,
    slug         TEXT,
    name         TEXT NOT NULL,
    url          TEXT,
    description  TEXT,
    rating       REAL,
    logo_url     TEXT,
    favicon_url  TEXT,
    poster_url   TEXT,
    network_uuid TEXT REFERENCES networks(uuid) ON DELETE SET NULL
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
    site_uuid        TEXT REFERENCES sites(uuid) ON DELETE SET NULL,
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

CREATE_PERFORMERS = """
CREATE TABLE IF NOT EXISTS performers (
    uuid              TEXT PRIMARY KEY,
    slug              TEXT,
    name              TEXT NOT NULL,
    full_name         TEXT,
    bio               TEXT,
    gender            TEXT,
    birthday          DATE,
    birthplace        TEXT,
    birthplace_code   TEXT,
    ethnicity         TEXT,
    nationality       TEXT,
    hair_colour       TEXT,
    eye_colour        TEXT,
    height            TEXT,
    weight            TEXT,
    measurements      TEXT,
    cupsize           TEXT,
    fake_boobs        BOOLEAN,
    career_start_year INT,
    career_end_year   INT,
    rating            REAL,
    image_url         TEXT,
    thumbnail_url     TEXT,
    face_url          TEXT,
    links             JSONB,
    fetched_at        TIMESTAMPTZ DEFAULT NOW()
);
"""

CREATE_SCENE_PERFORMERS = """
CREATE TABLE IF NOT EXISTS scene_performers (
    scene_id       TEXT NOT NULL REFERENCES scenes(id) ON DELETE CASCADE,
    performer_uuid TEXT NOT NULL REFERENCES performers(uuid) ON DELETE CASCADE,
    PRIMARY KEY (scene_id, performer_uuid)
);
"""

CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_torrent_meta_sitename      ON torrent_meta(sitename);
CREATE INDEX IF NOT EXISTS idx_torrent_meta_release_date  ON torrent_meta(release_date);
CREATE INDEX IF NOT EXISTS idx_scenes_site_name           ON scenes(site_name);
CREATE INDEX IF NOT EXISTS idx_scenes_date                ON scenes(date);
CREATE INDEX IF NOT EXISTS idx_torrents_date_added        ON torrents(date_added);
CREATE INDEX IF NOT EXISTS idx_torrent_scenes_scene_id    ON torrent_scenes(scene_id);
CREATE INDEX IF NOT EXISTS idx_performers_name            ON performers(name text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_scene_performers_performer ON scene_performers(performer_uuid);
CREATE INDEX IF NOT EXISTS idx_scenes_site_uuid           ON scenes(site_uuid);
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
        cur.execute(CREATE_NETWORKS)
        cur.execute(CREATE_SITES)
        cur.execute(CREATE_SCENES)
        cur.execute(CREATE_TORRENT_SCENES)
        cur.execute(CREATE_TORRENT_META)
        cur.execute(CREATE_PERFORMERS)
        cur.execute(CREATE_SCENE_PERFORMERS)
        cur.execute(CREATE_INDEXES)
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
        "s.background_url, s.poster_url, s.site_name, s.site_logo_url, "
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
    sql += f" ORDER BY {order_col} {sort_order} NULLS LAST, t.info_hash ASC"
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
    performer: str | None = None,
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
    if performer:
        sql += (
            " AND EXISTS (SELECT 1 FROM scene_performers sp2"
            " JOIN performers p2 ON p2.uuid = sp2.performer_uuid"
            " WHERE sp2.scene_id = s.id AND p2.name ILIKE %s)"
        )
        params.append(f"%{performer}%")
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
    limit: int = 30,
    offset: int = 0,
    performer: str | None = None,
) -> list[dict]:
    """Return scenes that have at least one linked torrent, with torrents and performers aggregated as JSON."""
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
        "s.tags, "
        "(SELECT COALESCE(json_agg(json_build_object("
        "    'uuid', p.uuid, 'name', p.name, 'image_url', p.image_url,"
        "    'thumbnail_url', p.thumbnail_url, 'face_url', p.face_url,"
        "    'gender', p.gender"
        "  ) ORDER BY p.name), '[]'::json)"
        "  FROM scene_performers sp JOIN performers p ON p.uuid = sp.performer_uuid"
        "  WHERE sp.scene_id = s.id"
        ") AS performers, "
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
    if performer:
        sql += (
            " AND EXISTS (SELECT 1 FROM scene_performers sp2"
            " JOIN performers p2 ON p2.uuid = sp2.performer_uuid"
            " WHERE sp2.scene_id = s.id AND p2.name ILIKE %s)"
        )
        params.append(f"%{performer}%")
    # GROUP BY PK — PostgreSQL infers functional dependency for other columns
    sql += " GROUP BY s.id"
    sql += f" ORDER BY {order_col} {sort_order} NULLS LAST, s.id ASC"
    sql += " LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _sync_scene_performers(conn, scene_id: str, performers: list[dict]):
    """Upsert performers and rebuild scene_performers links for a single scene."""
    with conn.cursor() as cur:
        for p in performers:
            uuid = p.get("uuid")
            name = (p.get("name") or "").strip()
            if not uuid or not name:
                continue
            cur.execute(
                """
                INSERT INTO performers (
                    uuid, slug, name, full_name, bio, gender, birthday,
                    birthplace, birthplace_code, ethnicity, nationality,
                    hair_colour, eye_colour, height, weight, measurements,
                    cupsize, fake_boobs, career_start_year, career_end_year,
                    rating, image_url, thumbnail_url, face_url, links, fetched_at
                ) VALUES (
                    %(uuid)s, %(slug)s, %(name)s, %(full_name)s, %(bio)s, %(gender)s, %(birthday)s,
                    %(birthplace)s, %(birthplace_code)s, %(ethnicity)s, %(nationality)s,
                    %(hair_colour)s, %(eye_colour)s, %(height)s, %(weight)s, %(measurements)s,
                    %(cupsize)s, %(fake_boobs)s, %(career_start_year)s, %(career_end_year)s,
                    %(rating)s, %(image_url)s, %(thumbnail_url)s, %(face_url)s, %(links)s, NOW()
                )
                ON CONFLICT (uuid) DO UPDATE SET
                    slug              = COALESCE(EXCLUDED.slug, performers.slug),
                    name              = EXCLUDED.name,
                    full_name         = COALESCE(EXCLUDED.full_name, performers.full_name),
                    bio               = COALESCE(EXCLUDED.bio, performers.bio),
                    gender            = COALESCE(EXCLUDED.gender, performers.gender),
                    birthday          = COALESCE(EXCLUDED.birthday, performers.birthday),
                    birthplace        = COALESCE(EXCLUDED.birthplace, performers.birthplace),
                    birthplace_code   = COALESCE(EXCLUDED.birthplace_code, performers.birthplace_code),
                    ethnicity         = COALESCE(EXCLUDED.ethnicity, performers.ethnicity),
                    nationality       = COALESCE(EXCLUDED.nationality, performers.nationality),
                    hair_colour       = COALESCE(EXCLUDED.hair_colour, performers.hair_colour),
                    eye_colour        = COALESCE(EXCLUDED.eye_colour, performers.eye_colour),
                    height            = COALESCE(EXCLUDED.height, performers.height),
                    weight            = COALESCE(EXCLUDED.weight, performers.weight),
                    measurements      = COALESCE(EXCLUDED.measurements, performers.measurements),
                    cupsize           = COALESCE(EXCLUDED.cupsize, performers.cupsize),
                    fake_boobs        = COALESCE(EXCLUDED.fake_boobs, performers.fake_boobs),
                    career_start_year = COALESCE(EXCLUDED.career_start_year, performers.career_start_year),
                    career_end_year   = COALESCE(EXCLUDED.career_end_year, performers.career_end_year),
                    rating            = COALESCE(EXCLUDED.rating, performers.rating),
                    image_url         = COALESCE(EXCLUDED.image_url, performers.image_url),
                    thumbnail_url     = COALESCE(EXCLUDED.thumbnail_url, performers.thumbnail_url),
                    face_url          = COALESCE(EXCLUDED.face_url, performers.face_url),
                    links             = COALESCE(EXCLUDED.links, performers.links),
                    fetched_at        = EXCLUDED.fetched_at
                """,
                {**p, "links": Json(p.get("links") or {})},
            )
            cur.execute(
                "INSERT INTO scene_performers (scene_id, performer_uuid) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (scene_id, uuid),
            )


def upsert_scene(conn, scene: dict):
    """Insert or update a ThePornDB scene, syncing normalized sites/networks/performers."""
    site = scene.get("site") or {}
    network = site.get("network") if site else None

    with conn.cursor() as cur:
        # Upsert network first (sites FK depends on it)
        if network and network.get("uuid"):
            cur.execute(
                """
                INSERT INTO networks (uuid, tpdb_id, slug, name, url, rating, logo_url, favicon_url, poster_url)
                VALUES (%(uuid)s, %(tpdb_id)s, %(slug)s, %(name)s, %(url)s, %(rating)s,
                        %(logo_url)s, %(favicon_url)s, %(poster_url)s)
                ON CONFLICT (uuid) DO UPDATE SET
                    tpdb_id     = COALESCE(EXCLUDED.tpdb_id, networks.tpdb_id),
                    slug        = COALESCE(EXCLUDED.slug, networks.slug),
                    name        = EXCLUDED.name,
                    url         = COALESCE(EXCLUDED.url, networks.url),
                    rating      = COALESCE(EXCLUDED.rating, networks.rating),
                    logo_url    = COALESCE(EXCLUDED.logo_url, networks.logo_url),
                    favicon_url = COALESCE(EXCLUDED.favicon_url, networks.favicon_url),
                    poster_url  = COALESCE(EXCLUDED.poster_url, networks.poster_url)
                """,
                network,
            )

        # Upsert site
        if site and site.get("uuid"):
            cur.execute(
                """
                INSERT INTO sites (uuid, tpdb_id, slug, name, url, description, rating,
                                   logo_url, favicon_url, poster_url, network_uuid)
                VALUES (%(uuid)s, %(tpdb_id)s, %(slug)s, %(name)s, %(url)s, %(description)s, %(rating)s,
                        %(logo_url)s, %(favicon_url)s, %(poster_url)s, %(network_uuid)s)
                ON CONFLICT (uuid) DO UPDATE SET
                    tpdb_id      = COALESCE(EXCLUDED.tpdb_id, sites.tpdb_id),
                    slug         = COALESCE(EXCLUDED.slug, sites.slug),
                    name         = EXCLUDED.name,
                    description  = COALESCE(EXCLUDED.description, sites.description),
                    url          = COALESCE(EXCLUDED.url, sites.url),
                    rating       = COALESCE(EXCLUDED.rating, sites.rating),
                    logo_url     = COALESCE(EXCLUDED.logo_url, sites.logo_url),
                    favicon_url  = COALESCE(EXCLUDED.favicon_url, sites.favicon_url),
                    poster_url   = COALESCE(EXCLUDED.poster_url, sites.poster_url),
                    network_uuid = COALESCE(EXCLUDED.network_uuid, sites.network_uuid)
                """,
                {**site, "network_uuid": network.get("uuid") if network else None},
            )

        # Upsert the scene itself
        cur.execute(
            """
            INSERT INTO scenes (
                id, title, description, poster_url, background_url, date,
                duration_seconds, site_name, site_slug, site_logo_url, site_uuid,
                network_name, network_slug, network_logo_url,
                tags, fetched_at
            ) VALUES (
                %(id)s, %(title)s, %(description)s, %(poster_url)s, %(background_url)s, %(date)s,
                %(duration_seconds)s, %(site_name)s, %(site_slug)s, %(site_logo_url)s, %(site_uuid)s,
                %(network_name)s, %(network_slug)s, %(network_logo_url)s,
                %(tags)s, NOW()
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
                site_uuid        = COALESCE(EXCLUDED.site_uuid, scenes.site_uuid),
                network_name     = EXCLUDED.network_name,
                network_slug     = EXCLUDED.network_slug,
                network_logo_url = EXCLUDED.network_logo_url,
                tags             = EXCLUDED.tags,
                fetched_at       = NOW()
            """,
            {**scene, "tags": Json(scene.get("tags") or [])},
        )

    _sync_scene_performers(conn, scene["id"], scene.get("performers") or [])
    conn.commit()


def list_sites(conn) -> list[dict]:
    """Return all sites with network info, ordered by name."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.uuid, s.slug, s.name, s.url, s.description, s.rating,
                   s.logo_url, s.favicon_url, s.poster_url,
                   n.uuid AS network_uuid, n.name AS network_name, n.logo_url AS network_logo_url
            FROM sites s
            LEFT JOIN networks n ON n.uuid = s.network_uuid
            ORDER BY s.name
            """
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_site(conn, uuid: str) -> dict | None:
    """Return full site record with network info by UUID, or None if not found."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.uuid, s.tpdb_id, s.slug, s.name, s.url, s.description, s.rating,
                   s.logo_url, s.favicon_url, s.poster_url,
                   n.uuid AS network_uuid, n.name AS network_name,
                   n.logo_url AS network_logo_url, n.url AS network_url
            FROM sites s
            LEFT JOIN networks n ON n.uuid = s.network_uuid
            WHERE s.uuid = %s
            """,
            (uuid,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def count_performers(conn, query: str | None = None) -> int:
    sql = "SELECT COUNT(*) FROM performers WHERE TRUE"
    params: list = []
    if query:
        sql += " AND name ILIKE %s"
        params.append(f"%{query}%")
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def list_performers(
    conn,
    query: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Return performers matching query, ordered by name."""
    sql = (
        "SELECT uuid, slug, name, full_name, gender, image_url, thumbnail_url, face_url, rating "
        "FROM performers WHERE TRUE"
    )
    params: list = []
    if query:
        sql += " AND name ILIKE %s"
        params.append(f"%{query}%")
    sql += " ORDER BY name LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_performer(conn, uuid: str) -> dict | None:
    """Return full performer record by UUID, or None if not found."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM performers WHERE uuid = %s", (uuid,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


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
