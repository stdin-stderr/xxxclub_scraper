import difflib
import logging
import os
import re
import time
from datetime import date

import requests
from dotenv import load_dotenv

import db

load_dotenv()

_log = logging.getLogger(__name__)

METADATA_INTERVAL = int(os.environ.get("METADATA_INTERVAL", 300))
PORNDB_API_KEY = os.environ.get("PORNDB_API_KEY", "")
METADATA_MIN_SCORE = float(os.environ.get("METADATA_MIN_SCORE", "0.65"))


class PornDBClient:
    BASE = "https://api.theporndb.net"

    def __init__(self, api_key: str):
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        })

    def search_scenes(
        self,
        parse: str,
        site: str | None = None,
        date: str | None = None,
        per_page: int = 5,
    ) -> list[dict]:
        params: dict = {"parse": parse, "per_page": per_page}
        if site:
            params["site"] = site
            params["site_operation"] = "Site/Network"
        if date:
            params["date"] = date
        try:
            r = self._session.get(f"{self.BASE}/scenes", params=params, timeout=15)
            r.raise_for_status()
            return r.json().get("data", [])
        except requests.HTTPError as exc:
            _log.warning("HTTP %s searching scenes (site=%s, parse=%s): %s",
                         exc.response.status_code, site, parse, exc)
            return []
        except requests.RequestException as exc:
            _log.warning("Request error: %s", exc)
            return []


def _str(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if isinstance(val, dict):
        return (
            val.get("full")
            or val.get("large")
            or val.get("medium")
            or val.get("small")
            or val.get("url")
            or val.get("src")
            or val.get("path")
            or None
        )
    return str(val)


def _performer_image(p: dict) -> str:
    posters = p.get("posters") or []
    if posters:
        return _str(posters[0].get("url") or posters[0]) or ""
    return _str(p.get("image")) or ""


def _extract_scene(raw: dict) -> dict:
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
        "poster_url": _str(raw.get("posters")) or _str(raw.get("poster")) or _str(raw.get("image")),
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


def _normalize_site(name: str) -> str:
    """Strip spaces, dashes, underscores for fuzzy site comparison."""
    return re.sub(r"[\s\-_]", "", name).lower()


def score_match(torrent: dict, scene: dict) -> tuple[float, float, float, float]:
    """Score a TPDB scene candidate against structured torrent_meta fields.

    meta_title may be a performer name rather than a scene title, so we take
    the max of scene-title similarity and best-performer-name similarity.

    Returns (total, title_or_performer_sim, site_sim, date_sim).
    """
    meta_title = (torrent.get("meta_title") or "").lower()
    sitename = _normalize_site(torrent.get("sitename") or "")
    release_date = torrent.get("release_date")  # datetime.date or None

    scene_title = (scene.get("title") or "").lower()
    scene_site = _normalize_site(scene.get("site_name") or "")
    scene_date_str = scene.get("date") or ""

    # Title similarity — also check against performer names; meta_title may be a performer
    title_sim = difflib.SequenceMatcher(None, meta_title, scene_title).ratio() if meta_title else 0.0
    for p in scene.get("performers") or []:
        performer_sim = difflib.SequenceMatcher(
            None, meta_title, (p.get("name") or "").lower()
        ).ratio()
        if performer_sim > title_sim:
            title_sim = performer_sim

    # Site similarity — normalize to handle "Hunt4K" vs "Hunt 4K"
    if sitename and scene_site:
        site_sim = 1.0 if (sitename in scene_site or scene_site in sitename) else 0.0
    else:
        site_sim = 0.0

    # Date similarity
    date_sim = 0.0
    has_date = release_date is not None
    if has_date and scene_date_str:
        try:
            scene_date = date.fromisoformat(scene_date_str[:10])
            delta = abs((release_date - scene_date).days)
            if delta == 0:
                date_sim = 1.0
            elif delta <= 30:
                date_sim = 0.5
        except ValueError:
            pass

    # Weights — redistribute date weight to title if no date available
    if has_date:
        title_w, site_w, date_w = 0.35, 0.25, 0.4
    else:
        title_w, site_w, date_w = 0.7, 0.3, 0.0

    total = title_w * title_sim + site_w * site_sim + date_w * date_sim
    return total, title_sim, site_sim, date_sim


def _best_candidate(torrent: dict, candidates: list[dict], min_score: float) -> tuple[dict | None, float]:
    """Score all candidates and return (best_scene_data, score) if above threshold, else (None, best_score)."""
    best_scene = None
    best_score = -1.0
    for raw in candidates:
        scene = _extract_scene(raw)
        total, title_sim, site_sim, date_sim = score_match(torrent, scene)
        performers_str = ", ".join(p["name"] for p in scene.get("performers") or [] if p.get("name"))
        _log.debug(
            "  candidate [%s] %r  date=%s  performers=%s  total=%.2f  title=%.2f  site=%.2f  date_sim=%.2f",
            scene.get("site_name") or "?", scene.get("title"), scene.get("date") or "?",
            performers_str or "—", total, title_sim, site_sim, date_sim,
        )
        if total > best_score:
            best_score = total
            best_scene = scene
    if best_score >= min_score:
        return best_scene, best_score
    return None, best_score


def run_once(conn, client: PornDBClient, limit: int = 100, dry_run: bool = False):
    torrents = db.fetch_unmatched(conn, limit=limit)
    if not torrents:
        _log.debug("No unmatched torrents to process.")
        return

    label = " (dry run)" if dry_run else ""
    print(f"Processing {len(torrents)} torrents{label}...", flush=True)
    matched = 0

    for i, torrent in enumerate(torrents, 1):
        info_hash = torrent["info_hash"]
        search_title = torrent.get("meta_title") or torrent["title"] or ""
        sitename = torrent.get("sitename") or ""
        release_date = torrent.get("release_date")
        date_str = release_date.isoformat() if release_date else None

        print(f"[{i}/{len(torrents)}] {(torrent['title'] or '')[:70]}", flush=True)
        print(f"         meta: site={sitename!r}  title={search_title[:50]!r}  date={date_str}", flush=True)

        candidates: list[dict] = []

        # Pass 1: site + date + title
        if not candidates and sitename and date_str:
            candidates = client.search_scenes(search_title, site=sitename, date=date_str)
            if candidates:
                print(f"         pass 1 (site+date+title): {len(candidates)} candidates", flush=True)

        # Pass 2: site + date only (title may be a performer name or otherwise unhelpful)
        if not candidates and sitename and date_str:
            candidates = client.search_scenes(sitename, site=sitename, date=date_str)
            if candidates:
                print(f"         pass 2 (site+date): {len(candidates)} candidates", flush=True)

        # Pass 3: site + title
        if not candidates and sitename:
            candidates = client.search_scenes(search_title, site=sitename)
            if candidates:
                print(f"         pass 3 (site+title): {len(candidates)} candidates", flush=True)

        # Pass 4: global
        if not candidates:
            candidates = client.search_scenes(search_title)
            if candidates:
                print(f"         pass 4 (global): {len(candidates)} candidates", flush=True)

        if candidates:
            best, score = _best_candidate(torrent, candidates, METADATA_MIN_SCORE)
            if best and best.get("id"):
                matched += 1
                performers = ", ".join(p["name"] for p in best["performers"][:3])
                print(
                    f"         MATCH (score={score:.2f}) → [{best.get('site_name') or '?'}] {best.get('title')}\n"
                    f"                performers: {performers or '—'}  |  tags: {len(best['tags'])}",
                    flush=True,
                )
                if not dry_run:
                    db.upsert_scene(conn, best)
                    db.link_torrent_scene(conn, info_hash, best["id"], match_score=score)
            else:
                print(f"         NO MATCH (best score={score:.2f} < {METADATA_MIN_SCORE})", flush=True)
        else:
            print("         NO RESULTS", flush=True)

        if not dry_run:
            db.mark_metadata_attempted(conn, info_hash)
        time.sleep(0.5)

    print(f"\nDone: {matched}/{len(torrents)} matched{label}.", flush=True)


def run_loop():
    client = PornDBClient(PORNDB_API_KEY)
    _log.info("Metadata fetcher started (interval=%ds, min_score=%.2f).", METADATA_INTERVAL, METADATA_MIN_SCORE)

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

    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

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
