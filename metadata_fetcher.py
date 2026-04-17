import logging
import os
import re
import time

import requests
from dotenv import load_dotenv

import db

load_dotenv()

_log = logging.getLogger(__name__)

METADATA_INTERVAL = int(os.environ.get("METADATA_INTERVAL", 300))
PORNDB_API_KEY = os.environ.get("PORNDB_API_KEY", "")

# Noise tokens to strip from torrent titles before sending to ThePornDB
_STRIP_RE = re.compile(
    r"\[.*?\]"           # [XC], [release group]
    r"|\bXXX\b"
    r"|\b\d{3,4}p\b"     # 480p 720p 1080p 2160p
    r"|\bMP4-\w+\b"
    r"|\bMKV-\w+\b"
    r"|\bWEB-\w+\b"
    r"|\bWRB\b"
    r"|\bP2P\b"
    r"|\bWEB\b"
    r"|\bHEVC\b"
    r"|\bx264\b"
    r"|\bx265\b"
    r"|\biTALiAN\b"
    r"|\bUNCUT\b"
    r"|\bUNCENSORED\b"
    r"|\bDVDRip\b"
    r"|\bBluRay\b",
    re.IGNORECASE,
)

# Format A: SiteName YY MM DD Scene Title ... XXX
_FORMAT_A = re.compile(r"^(\w+)\s+\d{2}\s+\d{2}\s+\d{2}\s+(.*?)\s+XXX", re.IGNORECASE)

# Format B: SiteName - Performer(s) - Title (DD.MM.YYYY)
_FORMAT_B = re.compile(r"^(.+?)\s+-\s+.+?\s+-\s+(.+?)\s+\(")


def clean_title(title: str) -> tuple[str, str]:
    """Return (site_name, stripped_title) ready for the ThePornDB parse endpoint."""
    m = _FORMAT_A.match(title)
    if m:
        site = m.group(1)
        scene = m.group(2).strip()
        stripped = _STRIP_RE.sub("", f"{site} {scene}").strip()
        return site, stripped

    m = _FORMAT_B.match(title)
    if m:
        site = m.group(1).strip()
        title_part = m.group(2).strip()
        stripped = _STRIP_RE.sub("", f"{site} {title_part}").strip()
        return site, stripped

    # Fallback: first word is site, strip noise from everything
    parts = title.split()
    site = parts[0] if parts else ""
    stripped = _STRIP_RE.sub("", title).strip()
    return site, stripped


class PornDBClient:
    BASE = "https://api.theporndb.net"

    def __init__(self, api_key: str):
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        })

    def search_scenes(
        self, parse: str, site: str | None = None, per_page: int = 5
    ) -> list[dict]:
        params: dict = {"parse": parse, "per_page": per_page}
        if site:
            params["site"] = site
            params["site_operation"] = "Site/Network"
        try:
            r = self._session.get(f"{self.BASE}/scenes", params=params, timeout=15)
            r.raise_for_status()
            return r.json().get("data", [])
        except requests.HTTPError as exc:
            _log.warning("HTTP %s searching scenes (site=%s, parse=%s): %s", exc.response.status_code, site, parse, exc)
            return []
        except requests.RequestException as exc:
            _log.warning("Request error: %s", exc)
            return []


def _str(val) -> str | None:
    """Coerce a value to str, handling cases where the API returns a dict instead of a URL."""
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if isinstance(val, dict):
        return val.get("url") or val.get("src") or val.get("path") or None
    return str(val)


def _performer_image(p: dict) -> str:
    posters = p.get("posters") or []
    if posters:
        return _str(posters[0].get("url") or posters[0]) or ""
    return _str(p.get("image")) or ""


def _extract_scene(raw: dict) -> dict:
    """Map a ThePornDB scene response to our scenes table schema."""
    site = raw.get("site") or {}
    network = site.get("network") or {}

    performers = [
        {"name": p.get("name", ""), "image_url": _performer_image(p)}
        for p in (raw.get("performers") or [])
    ]
    tags = [t.get("name", "") for t in (raw.get("tags") or []) if t.get("name")]

    return {
        "id": _str(raw.get("slug")) or str(raw.get("_id", "")),
        "title": _str(raw.get("title")),
        "description": _str(raw.get("description")),
        "poster_url": _str(raw.get("poster")) or _str(raw.get("image")),
        "background_url": _str(raw.get("background")),
        "date": _str(raw.get("date")),
        "duration_seconds": raw.get("duration"),
        "site_name": _str(site.get("name")),
        "site_slug": _str(site.get("slug")),
        "site_logo_url": _str(site.get("logo")),
        "network_name": _str(network.get("name")),
        "network_slug": _str(network.get("slug")),
        "network_logo_url": _str(network.get("logo")),
        "performers": performers,
        "tags": tags,
    }


def run_once(conn, client: PornDBClient, limit: int = 100, dry_run: bool = False):
    """Attempt metadata lookup for up to `limit` unmatched torrents."""
    torrents = db.fetch_unmatched(conn, limit=limit)
    if not torrents:
        _log.debug("No unmatched torrents to process.")
        return

    label = " (dry run)" if dry_run else ""
    print(f"Processing {len(torrents)} torrents{label}...", flush=True)
    matched = 0

    for i, torrent in enumerate(torrents, 1):
        info_hash = torrent["info_hash"]
        title = torrent["title"] or ""

        site_name, stripped = clean_title(title)
        print(f"[{i}/{len(torrents)}] {title[:70]}", flush=True)

        # Pass 1: site-scoped parse
        results = client.search_scenes(stripped, site=site_name)

        # Pass 2: global fallback
        if not results:
            results = client.search_scenes(stripped)

        if results:
            scene_data = _extract_scene(results[0])
            scene_id = scene_data["id"]
            if scene_id:
                matched += 1
                performers = ", ".join(p["name"] for p in scene_data["performers"][:3])
                print(
                    f"         MATCH → [{scene_data.get('site_name') or '?'}] {scene_data.get('title')} ({scene_id})\n"
                    f"                performers: {performers or '—'}  |  tags: {len(scene_data['tags'])}",
                    flush=True,
                )
                if not dry_run:
                    db.upsert_scene(conn, scene_data)
                    db.link_torrent_scene(conn, info_hash, scene_id)
        else:
            print("         NO MATCH", flush=True)

        if not dry_run:
            db.mark_metadata_attempted(conn, info_hash)
        time.sleep(0.5)

    print(f"\nDone: {matched}/{len(torrents)} matched{label}.", flush=True)


def run_loop():
    """Long-running loop: enrich unmatched torrents every METADATA_INTERVAL seconds."""
    client = PornDBClient(PORNDB_API_KEY)
    _log.info("Metadata fetcher started (interval=%ds).", METADATA_INTERVAL)

    while True:
        conn = db.get_connection()
        try:
            run_once(conn, client)
        except Exception as exc:
            _log.error("Metadata fetch cycle error: %s", exc)
        finally:
            conn.close()
        time.sleep(METADATA_INTERVAL)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Fetch ThePornDB metadata for unmatched torrents.")
    parser.add_argument("--limit", type=int, default=100, help="Max torrents to process (default 100)")
    parser.add_argument("--all", action="store_true", help="Re-attempt all (reset metadata_attempted_at first)")
    parser.add_argument("--dry-run", action="store_true", help="Query ThePornDB and print matches without writing to DB")
    args = parser.parse_args()

    api_key = PORNDB_API_KEY
    if not api_key:
        raise SystemExit("PORNDB_API_KEY not set in environment.")

    client = PornDBClient(api_key)
    print("Connecting to database...", flush=True)
    conn = db.get_connection()
    print("Connected.", flush=True)

    if args.all:
        with conn.cursor() as cur:
            cur.execute("UPDATE torrents SET metadata_attempted_at = NULL")
        conn.commit()
        _log.info("Reset metadata_attempted_at for all torrents.")

    try:
        run_once(conn, client, limit=args.limit, dry_run=args.dry_run)
    finally:
        conn.close()
