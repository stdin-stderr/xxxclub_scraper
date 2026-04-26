import base64
import json
import logging
import os
from urllib.parse import parse_qs
import requests as req_lib
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

import debrid

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/stremio")

_API_PORT = os.environ.get("API_PORT", "5001")
_API_BASE = os.environ.get("API_URL", f"http://localhost:{_API_PORT}")

ID_PREFIX = "xxxclub_"
PAGE_SIZE = 20

MANIFEST = {
    "id": "com.xxxclub.scraper",
    "version": "1.0.0",
    "name": "xxxclub",
    "description": "Scenes and torrents from your local xxxclub.to scraper",
    "resources": ["catalog", "meta", "stream"],
    "types": ["movie"],
    "catalogs": [
        {
            "type": "movie",
            "id": "xxxclub-scenes",
            "name": "New Scenes",
            "extra": [{"name": "search"}, {"name": "skip"}],
        },
        {
            "type": "movie",
            "id": "xxxclub-torrents",
            "name": "New Available",
            "extra": [{"name": "skip"}],
        },
        {
            "type": "movie",
            "id": "xxxclub-popular",
            "name": "Popular",
            "extra": [{"name": "skip"}],
        },
    ],
    "idPrefixes": [ID_PREFIX],
    "behaviorHints": {"adult": True, "configurable": True},
}


def _encode_config(cfg: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(cfg).encode()).decode().rstrip("=")


def _decode_config(s: str) -> dict:
    padding = "=" * (-len(s) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(s + padding).decode())
    except Exception:
        return {}


_DEFAULT_CONFIG = _encode_config({})


def _get_debrid_entries(cfg: dict) -> list[dict]:
    """Return list of {service, key} dicts from config.

    Supports both the new ``debrid`` list format and the legacy
    ``torboxKey`` field so existing install links keep working.
    """
    entries = cfg.get("debrid") or []
    if entries:
        return [e for e in entries if e.get("service") and e.get("key")]
    # Legacy: single TorBox key
    old_key = cfg.get("torboxKey", "")
    if old_key:
        return [{"service": "torbox", "key": old_key}]
    return []


def _size_human(b) -> str:
    if not b:
        return ""
    b = float(b)
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def _api_get(path: str, params: dict = None) -> dict:
    resp = req_lib.get(f"{_API_BASE}{path}", params=params or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _parse_extras(raw: str) -> dict:
    """Parse Stremio extras string (may end in .json) into a dict."""
    raw = raw.rstrip("/")
    if raw.endswith(".json"):
        raw = raw[:-5]
    return {k: v[0] for k, v in parse_qs(raw).items()}


def _scene_to_meta_preview(s: dict) -> dict:
    tags = s.get("tags") or []
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except Exception:
            tags = []
    genres = [t["name"] for t in tags if isinstance(t, dict) and t.get("name")]

    meta = {
        "id": ID_PREFIX + s["id"],
        "type": "movie",
        "name": s.get("title") or "",
        "poster": s.get("poster_url") or "",
    }
    if s.get("description"):
        meta["description"] = s["description"]
    if genres:
        meta["genres"] = genres
    return meta


def _scene_to_meta_full(s: dict, base_url: str = "") -> dict:
    performers = s.get("performers") or []
    if isinstance(performers, str):
        try:
            performers = json.loads(performers)
        except Exception:
            performers = []

    links = [
        {
            "name": p["name"],
            "category": "Cast",
            "url": f"{base_url}/performer/{p['uuid']}",
        }
        for p in performers
        if p.get("name") and p.get("uuid")
    ]

    duration = s.get("duration_seconds")
    runtime = f"{duration // 60} min" if duration else None

    meta = {
        "id": ID_PREFIX + s["id"],
        "type": "movie",
        "name": s.get("title") or "",
        "poster": s.get("poster_url") or "",
        "background": s.get("background_url") or "",
        "description": s.get("description") or "",
        "releaseInfo": str(s["date"]) if s.get("date") else "",
        "links": links,
    }
    if runtime:
        meta["runtime"] = runtime
    return meta


def _torrents_to_streams(torrents, config_b64: str, info_hash: str | None = None) -> list[dict]:
    streams = []
    for t in torrents:
        h = t.get("info_hash", "").lower()
        if not h:
            continue
        category = t.get("category") or ""
        resolution = t.get("resolution") or ""
        size = t.get("size_bytes")
        seeders = t.get("seeders")

        title = t.get("title") or ""
        label_parts = [p for p in [resolution, category] if p]
        meta_parts = [p for p in [
            _size_human(size),
            f"{seeders} seeders" if seeders is not None else None,
        ] if p]
        desc_lines = [p for p in [title, " · ".join(meta_parts)] if p]

        res_px = int(resolution.lower().rstrip("p")) if resolution and resolution.lower().rstrip("p").isdigit() else 0

        streams.append({
            "_hash": h,
            "_seeders": seeders or 0,
            "_cached": False,
            "_res": res_px,
            "name": f"xxxclub\n{' · '.join(label_parts)}" if label_parts else "xxxclub",
            "description": "\n".join(desc_lines),
            "infoHash": h,
            "behaviorHints": {"videoSize": size, "notWebReady": True} if size else {"notWebReady": True},
        })
    return streams


# ─── Manifest ────────────────────────────────────────────────────────────────

def _build_manifest(cfg: dict) -> dict:
    site_uuids: list = cfg.get("sites") or []
    if not site_uuids:
        return MANIFEST

    try:
        all_sites = _api_get("/api/v1/sites").get("sites", [])
        site_map = {s["uuid"]: s["name"] for s in all_sites}
    except Exception:
        return MANIFEST

    catalogs = list(MANIFEST["catalogs"])
    for uuid in site_uuids:
        name = site_map.get(uuid, uuid)
        catalogs.append({
            "type": "movie",
            "id": f"xxxclub-site-{uuid}",
            "name": name,
            "extra": [{"name": "search"}, {"name": "skip"}],
        })
    return {**MANIFEST, "catalogs": catalogs}


@router.get("/manifest.json")
def manifest_toplevel():
    return JSONResponse(content=MANIFEST)


@router.get("/{config}/manifest.json")
def manifest(config: str):
    return JSONResponse(content=_build_manifest(_decode_config(config)))



# ─── Configure ───────────────────────────────────────────────────────────────

_REDIRECT_TO_CONFIG_HTML = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Redirecting…</title></head><body>
<script>
(function(){{
  var cfg = {config_json};
  if (cfg.debrid && cfg.debrid.length) {{
    var e = cfg.debrid[0];
    localStorage.setItem('debrid_config', JSON.stringify({{service: e.service, key: e.key}}));
  }}
  if (cfg.sites && cfg.sites.length) {{
    localStorage.setItem('stremio_sites', JSON.stringify(cfg.sites));
  }} else {{
    localStorage.removeItem('stremio_sites');
  }}
  location.replace('/configure');
}})();
</script>
<p>Redirecting…</p>
</body></html>"""


@router.get("/configure", response_class=HTMLResponse)
def configure_toplevel():
    html = _REDIRECT_TO_CONFIG_HTML.format(config_json=json.dumps({}))
    return HTMLResponse(html)


@router.get("/{config}/configure", response_class=HTMLResponse)
def configure(config: str):
    html = _REDIRECT_TO_CONFIG_HTML.format(config_json=json.dumps(_decode_config(config)))
    return HTMLResponse(html)


# ─── Catalog ─────────────────────────────────────────────────────────────────

def _fetch_catalog(catalog_id: str, cfg: dict, skip: int = 0, search: str = "") -> list[dict]:
    site_filter = ""

    if catalog_id.startswith("xxxclub-site-"):
        site_uuid = catalog_id[len("xxxclub-site-"):]
        sort_by = "date"
        try:
            site_data = _api_get(f"/api/v1/sites/{site_uuid}")
            site_filter = site_data.get("name", "")
        except Exception:
            pass
    elif catalog_id == "xxxclub-torrents":
        sort_by = "date_added"
    elif catalog_id == "xxxclub-popular":
        sort_by = "seeders"
    else:
        sort_by = "date"

    page = skip // PAGE_SIZE + 1
    params: dict = {
        "sort_by": sort_by,
        "sort_order": "desc",
        "per_page": PAGE_SIZE,
        "page": page,
    }
    if search:
        params["q"] = search
    if site_filter:
        params["site"] = site_filter

    data = _api_get("/api/v1/scenes", params)
    return [_scene_to_meta_preview(s) for s in data.get("items", [])]


@router.get("/{config}/catalog/movie/{catalog_id}.json")
def catalog(config: str, catalog_id: str):
    cfg = _decode_config(config)
    metas = _fetch_catalog(catalog_id, cfg)
    return JSONResponse(content={"metas": metas})


@router.get("/{config}/catalog/movie/{catalog_id}/{extras:path}")
def catalog_with_extras(config: str, catalog_id: str, extras: str):
    cfg = _decode_config(config)
    parsed = _parse_extras(extras)
    skip = int(parsed.get("skip", 0))
    search = parsed.get("search", "")
    metas = _fetch_catalog(catalog_id, cfg, skip=skip, search=search)
    return JSONResponse(content={"metas": metas})


# ─── Meta ─────────────────────────────────────────────────────────────────────

@router.get("/{config}/meta/movie/{stremio_id}.json")
def meta(config: str, stremio_id: str, request: Request):
    if not stremio_id.startswith(ID_PREFIX):
        raise HTTPException(status_code=404, detail="Unknown ID prefix")
    scene_id = stremio_id[len(ID_PREFIX):]
    scene = _api_get(f"/api/v1/scenes/{scene_id}")
    base = str(request.base_url).rstrip("/")
    return JSONResponse(content={"meta": _scene_to_meta_full(scene, base)})


# ─── Stream ───────────────────────────────────────────────────────────────────

@router.get("/{config}/stream/movie/{stremio_id}.json")
def stream(config: str, stremio_id: str, request: Request):
    if not stremio_id.startswith(ID_PREFIX):
        raise HTTPException(status_code=404, detail="Unknown ID prefix")
    scene_id = stremio_id[len(ID_PREFIX):]
    cfg = _decode_config(config)
    debrid_entries = _get_debrid_entries(cfg)

    scene = _api_get(f"/api/v1/scenes/{scene_id}")
    torrents = scene.get("torrents") or []
    if isinstance(torrents, str):
        try:
            torrents = json.loads(torrents)
        except Exception:
            torrents = []

    base_streams = _torrents_to_streams(torrents, config)

    if not base_streams:
        return JSONResponse(content={"streams": []})

    # No debrid configured — plain magnet streams, highest resolution first
    if not debrid_entries:
        base_streams.sort(key=lambda s: -(s.get("_res") or 0))
        for s in base_streams:
            s.pop("_hash", None)
            s.pop("_cached", None)
            s.pop("_seeders", None)
            s.pop("_res", None)
        return JSONResponse(content={"streams": base_streams})

    # Build hash → base stream lookup
    hash_to_stream = {s["_hash"]: s for s in base_streams}
    all_hashes = list(hash_to_stream.keys())
    base_url = str(request.base_url).rstrip("/")

    # Check cache for every configured service.
    # Result: hash → {service: bool}
    cache_by_hash: dict[str, dict[str, bool]] = {h: {} for h in all_hashes}
    for entry in debrid_entries:
        svc = entry["service"]
        key = entry["key"]
        try:
            client = debrid.DebridClient(svc, key)
            results = client.check_cached(all_hashes)
            for h, is_cached in results.items():
                cache_by_hash.setdefault(h, {})[svc] = is_cached
        except Exception as e:
            _log.warning(f"Error checking {svc} cache: {e}")

    output_streams: list[dict] = []

    for h, base_s in hash_to_stream.items():
        seeders = base_s["_seeders"] or 0
        label_line = base_s["name"].split("\n", 1)[-1]  # resolution · category part

        res = base_s.get("_res", 0)

        # One stream row per configured service, cached (⚡) or not (⏳)
        for entry in debrid_entries:
            svc = entry["service"]
            icon = debrid.DebridClient.SUPPORTED_SERVICES.get(svc, {}).get("icon", svc.upper())
            is_cached = cache_by_hash.get(h, {}).get(svc, False)
            badge = "⚡" if is_cached else "⏳"
            resolve_url = f"{base_url}/stremio/{config}/resolve/{svc}/{h}"
            output_streams.append({
                "name": f"[{icon}{badge}] xxxclub\n{label_line}".strip(),
                "description": base_s["description"],
                "url": resolve_url,
                "behaviorHints": base_s.get("behaviorHints") or {},
                "_cached": is_cached,
                "_res": res,
            })

    # Cached first, then highest resolution first
    output_streams.sort(key=lambda s: (0 if s["_cached"] else 1, -(s["_res"] or 0)))

    for s in output_streams:
        s.pop("_cached", None)
        s.pop("_res", None)

    return JSONResponse(content={"streams": output_streams})


# ─── Resolve ─────────────────────────────────────────────────────────────────

def _error_video(name: str) -> RedirectResponse:
    url = f"{debrid.STREMTHRU_URL}/v0/store/_/static/{name}.mp4"
    return RedirectResponse(url=url, status_code=302)


def do_resolve(service: str, api_key: str, info_hash: str):
    """Resolve an info_hash to a playable URL via debrid. Returns a Response."""
    try:
        torrent = _api_get(f"/api/v1/torrents/{info_hash}")
    except Exception:
        raise HTTPException(status_code=404, detail="Torrent not found in local database")
    magnet = torrent.get("magnet", "")
    if not magnet:
        raise HTTPException(status_code=404, detail="No magnet link available")

    try:
        client = debrid.DebridClient(service, api_key)
        dl_url = client.get_stream_url(magnet)
        if not dl_url:
            return _error_video("download_failed")
        return RedirectResponse(url=dl_url, status_code=301)
    except debrid.DebridPendingError:
        return _error_video("downloading")
    except debrid.DebridAuthError:
        return _error_video("401")
    except debrid.DebridLinkGenerationError:
        return _error_video("download_failed")
    except debrid.DebridError:
        return _error_video("download_failed")
    except Exception as e:
        _log.warning(f"resolve {service}/{info_hash}: {e}")
        return _error_video("500")


@router.get("/{config}/resolve/{service}/{info_hash}")
def resolve(config: str, service: str, info_hash: str):
    """Generate a playable link for a specific debrid service."""
    cfg = _decode_config(config)
    debrid_entries = _get_debrid_entries(cfg)

    api_key = next((e["key"] for e in debrid_entries if e["service"] == service), "")
    # Legacy fallback
    if not api_key and service == "torbox":
        api_key = cfg.get("torboxKey", "")
    if not api_key:
        raise HTTPException(status_code=400, detail=f"{service} API key not configured")

    return do_resolve(service, api_key, info_hash)
