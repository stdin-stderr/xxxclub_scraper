"""Extract structured metadata (resolution, release date, site name) from torrent titles
and upsert into the torrent_meta table."""

import logging
import re
import time
from datetime import date, datetime

import db

log = logging.getLogger(__name__)

# First-word tokens that are not site names
SITENAME_EXCEPTIONS = {
    "MegaPACK",
    "The", "My", "Yough", "Cum", "Size", "A",
    "StepSiblings", "Step", "What", "I", "It", "Ask", "Mature", "Free",
    "Pack", "Anal", "Busty", "MILF", "DP", "Trans", "Asian", "Dirty",
    "Hot", "Hardcore", "Young", "Family", "Interracial", "Lesbian", "Black",
    "Up", "In", "Teens", "Fuck", "Girl", "Mom",
}

# Normalise common aliases to the canonical Np form
_RESOLUTION_ALIASES = {
    "4K": "2160p",
    "8K": "4320p",
}

_RE_RESOLUTION = re.compile(r"\b(\d{3,4}p)\b", re.IGNORECASE)
_RE_ALIAS = re.compile(r"\b(4K|8K)\b", re.IGNORECASE)

# DD.MM.YYYY
_RE_DATE_DOTTED = re.compile(r"\b(\d{2})\.(\d{2})\.(\d{4})\b")
# DD MM YYYY (space-separated, four-digit year)
_RE_DATE_SPACED_LONG = re.compile(r"\b(\d{2})\s+(\d{2})\s+(\d{4})\b")
# YY MM DD (space-separated two-digit triplet)
_RE_DATE_SPACED = re.compile(r"\b(\d{2})\s+(\d{2})\s+(\d{2})\b")
# Standalone 4-digit year (1970-2099)
_RE_YEAR_ONLY = re.compile(r"\b(19[7-9]\d|20\d\d)\b")

_CATEGORY_RESOLUTION = {
    "SD": "480p",
    "HD": "720p",
    "FullHD": "1080p",
    "UHD": "2160p",
}

# Trailing date patterns: (28.12.2025) or (14.02.2026)
_RE_TRAILING_DATE = re.compile(r"\s*\(\s*\d{2}[./]\d{2}[./]\d{2,4}\s*\)?\s*$")
# Trailing noise from resolution onward: "1080p MP4-WRB [XC]", "WEB-DL 1080p", etc.
_RE_TRAILING_NOISE = re.compile(
    r"\s+(?:\d{3,4}p\b|4K\b|8K\b|WEB[-\s]?DL\b|WEBRip\b|BluRay\b).*$", re.IGNORECASE
)
# Mid-title noise tokens
_RE_MID_NOISE = re.compile(
    r"\b(?:iTALiAN|POLISH|FRENCH|GERMAN|RUSSIAN|CZECH|HUNGARIAN|BTS)\b", re.IGNORECASE
)


def extract_resolution(title: str) -> str | None:
    alias = _RE_ALIAS.search(title)
    if alias:
        return _RESOLUTION_ALIASES[alias.group(1).upper()]
    matches = _RE_RESOLUTION.findall(title)
    if matches:
        return matches[-1].lower()
    return None


def extract_date(title: str) -> date | None:
    m = _RE_DATE_DOTTED.search(title)
    if m:
        dd, mm, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            try:
                return date(yyyy, mm, dd)
            except ValueError:
                pass

    m = _RE_DATE_SPACED_LONG.search(title)
    if m:
        dd, mm, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            try:
                return date(yyyy, mm, dd)
            except ValueError:
                pass

    m = _RE_DATE_SPACED.search(title)
    if m:
        yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            try:
                return date(2000 + yy, mm, dd)
            except ValueError:
                pass

    m = _RE_YEAR_ONLY.search(title)
    if m:
        yyyy = int(m.group(1))
        if 1970 <= yyyy <= datetime.now().year:
            return date(yyyy, 1, 1)

    return None


def extract_sitename(title: str) -> str | None:
    parts = title.split()
    if not parts:
        return None
    first = parts[0]
    if first in SITENAME_EXCEPTIONS:
        return None
    return first


def extract_title(title: str) -> str | None:
    """Extract a clean scene title, stripping site name, date, resolution and codec noise."""
    if not title:
        return None

    # Format B: dash-separated (SiteName - Performers - Title (date))
    if " - " in title:
        parts = title.split(" - ")
        if len(parts) >= 3:
            raw = " - ".join(parts[2:])
        else:
            raw = parts[1]
        raw = _RE_TRAILING_DATE.sub("", raw)
        raw = _RE_TRAILING_NOISE.sub("", raw)
        raw = _RE_MID_NOISE.sub("", raw)
        return raw.strip() or None

    # Format A: SiteName YY MM DD <title> XXX ...
    m = re.search(r"\b\d{2}\s+\d{2}\s+\d{2}\s+(.*?)\s+XXX\b", title, re.IGNORECASE)
    if m:
        raw = _RE_MID_NOISE.sub("", m.group(1))
        return raw.strip() or None

    # Format A2: SiteName YYYY <title> XXX ...
    m = re.search(r"\b(?:19|20)\d{2}\s+(.*?)\s+XXX\b", title, re.IGNORECASE)
    if m:
        raw = _RE_MID_NOISE.sub("", m.group(1))
        return raw.strip() or None

    # Format E: SiteName E<number> <title> XXX ... (episode-numbered series)
    m = re.search(r"\bE\d+\s+(.*?)\s+XXX\b", title, re.IGNORECASE)
    if m:
        raw = _RE_MID_NOISE.sub("", m.group(1))
        return raw.strip() or None

    # Fallback: drop first word (site), skip a bare "com" token if present,
    # cut at XXX if found, otherwise strip trailing noise.
    parts = title.split(None, 1)
    if len(parts) < 2:
        return None
    raw = parts[1]
    if raw.lower().startswith("com "):
        raw = raw[4:]
    # Cut at XXX boundary if present
    xxx_match = re.search(r"\s+XXX\b", raw, re.IGNORECASE)
    if xxx_match:
        raw = raw[:xxx_match.start()]
    else:
        raw = _RE_TRAILING_NOISE.sub("", raw)
    raw = _RE_MID_NOISE.sub("", raw)
    return raw.strip() or None


def extract_meta(title: str) -> dict:
    return {
        "title": extract_title(title),
        "resolution": extract_resolution(title),
        "release_date": extract_date(title),
        "sitename": extract_sitename(title),
    }


def upsert_torrent_meta(conn, rows: list[dict]):
    """Extract meta from titles and upsert into torrent_meta. Rows must have info_hash + title."""
    tuples = []
    for row in rows:
        meta = extract_meta(row.get("title") or "")
        resolution = meta["resolution"] or _CATEGORY_RESOLUTION.get(row.get("category") or "")
        tuples.append((row["info_hash"], meta["title"], resolution, meta["release_date"], meta["sitename"]))
    db.upsert_torrent_meta(conn, tuples)


def reextract_all(conn, batch_size: int = 500):
    """Re-extract meta for every torrent in the database, in batches."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM torrents")
        total = cur.fetchone()[0]

    if total == 0:
        log.info("No torrents found, nothing to do")
        return

    log.info("Re-extracting meta for %d torrents in batches of %d", total, batch_size)
    offset = 0
    batch_num = 0
    start = time.monotonic()

    while True:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT info_hash, title, category FROM torrents ORDER BY info_hash LIMIT %s OFFSET %s",
                (batch_size, offset),
            )
            rows = [{"info_hash": r[0], "title": r[1], "category": r[2]} for r in cur.fetchall()]

        if not rows:
            break

        batch_num += 1
        upsert_torrent_meta(conn, rows)

        processed = min(offset + len(rows), total)
        elapsed = time.monotonic() - start
        rate = processed / elapsed if elapsed > 0 else 0
        log.info(
            "Batch %d: processed %d–%d of %d (%.0f rows/s)",
            batch_num, offset + 1, processed, total, rate,
        )
        offset += len(rows)

    elapsed = time.monotonic() - start
    log.info("Done — %d rows in %.1fs", total, elapsed)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    conn = db.get_connection()
    db.init_schema(conn)
    reextract_all(conn)
    conn.close()
