import base64
import json
import os
from urllib.parse import parse_qs, quote, unquote

import httpx
import requests as req_lib
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

router = APIRouter(prefix="/stremio")

_API_PORT = os.environ.get("API_PORT", "5001")
_API_BASE = os.environ.get("API_URL", f"http://localhost:{_API_PORT}")

ID_PREFIX = "xxxclub_"
PAGE_SIZE = 20
TORBOX_API = "https://api.torbox.app/v1/api"

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
            "name": "New Torrent Releases",
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

        label_parts = [p for p in [resolution, category] if p]
        desc_parts = [p for p in [
            resolution,
            _size_human(size),
            f"{seeders} seeders" if seeders is not None else None,
        ] if p]

        streams.append({
            "_hash": h,
            "_seeders": seeders or 0,
            "_cached": False,
            "name": f"xxxclub\n{' · '.join(label_parts)}" if label_parts else "xxxclub",
            "description": " · ".join(desc_parts),
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

_CONFIGURE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>xxxclub Stremio Addon — Configure</title>
<style>
  body {{ font-family: system-ui, sans-serif; background: #111; color: #eee;
         max-width: 540px; margin: 60px auto; padding: 0 20px; }}
  h1 {{ font-size: 1.4rem; margin-bottom: 1.5rem; }}
  label {{ display: block; font-size: .85rem; color: #aaa; margin-bottom: 4px; }}
  input[type=text], input[type=password] {{
    width: 100%; padding: 9px 12px; background: #222; border: 1px solid #444;
    border-radius: 6px; color: #eee; font-size: .95rem; box-sizing: border-box;
  }}
  .field {{ margin-bottom: 1.4rem; }}
  #siteSearch {{ width: 100%; padding: 7px 12px; background: #222; border: 1px solid #444;
                 border-bottom: none; border-radius: 6px 6px 0 0; color: #eee;
                 font-size: .9rem; box-sizing: border-box; }}
  #siteSearch:focus {{ outline: none; border-color: #7c3aed; }}
  .sites-list {{ max-height: 260px; overflow-y: auto; background: #1a1a1a;
                 border: 1px solid #444; border-radius: 0 0 6px 6px; padding: 6px; }}
  .site-item {{ display: flex; align-items: center; gap: 10px; padding: 5px 6px;
                font-size: .9rem; cursor: pointer; border-radius: 5px; }}
  .site-item:hover {{ background: #252525; }}
  .site-item input[type=checkbox] {{ width: auto; flex-shrink: 0; accent-color: #7c3aed; cursor: pointer; }}
  .site-logo {{ width: 24px; height: 24px; object-fit: contain; border-radius: 3px; flex-shrink: 0; }}
  .site-logo-placeholder {{ width: 24px; height: 24px; flex-shrink: 0; }}
  .actions {{ display: flex; gap: 10px; margin-bottom: 1rem; }}
  .btn {{ flex: 1; background: #7c3aed; color: #fff; border: none; padding: 11px 16px;
          border-radius: 7px; font-size: .95rem; cursor: pointer; text-align: center;
          text-decoration: none; display: block; }}
  .btn:hover {{ background: #6d28d9; }}
  .btn-secondary {{ background: #2a2a2a; border: 1px solid #444; }}
  .btn-secondary:hover {{ background: #333; }}
  .install-row {{ display: flex; align-items: center; gap: 8px; background: #1a1a1a;
                  border: 1px solid #333; border-radius: 7px; padding: 10px 12px;
                  margin-bottom: 8px; }}
  .install-url {{ flex: 1; font-size: .8rem; color: #999; overflow: hidden;
                  text-overflow: ellipsis; white-space: nowrap; }}
  .copy-btn {{ background: #2a2a2a; border: 1px solid #444; color: #ccc; padding: 5px 12px;
               border-radius: 5px; font-size: .8rem; cursor: pointer; flex-shrink: 0; }}
  .copy-btn:hover {{ background: #333; }}
  .copy-btn.copied {{ color: #4ade80; border-color: #4ade80; }}
  #installBlock {{ display: none; }}
</style>
</head>
<body>
<h1>xxxclub Stremio Addon</h1>

<div class="field">
  <label>TorBox API Key</label>
  <input type="password" id="torboxKey" placeholder="tb_..." value="{torbox_key}">
</div>

<div class="field">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
    <label style="margin:0">Sites</label>
    <button type="button" class="btn btn-secondary" style="flex:none;padding:3px 10px;font-size:.8rem;width:auto" onclick="clearSites()">Clear all</button>
  </div>
  <input type="text" id="siteSearch" placeholder="Search sites…" oninput="filterSites(this.value)">
  <div class="sites-list" id="sitesList">{sites_html}</div>
</div>

<div class="actions">
  <button class="btn" onclick="buildLinks()">Generate Install Link</button>
</div>

<div id="installBlock">
  <div class="install-row">
    <span class="install-url" id="httpUrl"></span>
    <button class="copy-btn" onclick="copyUrl('httpUrl', this)">Copy</button>
  </div>
  <div class="install-row">
    <span class="install-url" id="stremioUrl"></span>
    <button class="copy-btn" onclick="copyUrl('stremioUrl', this)">Copy</button>
    <a class="btn" id="stremioLink" href="#" style="flex:none;padding:5px 14px;font-size:.85rem;">Open in Stremio</a>
  </div>
</div>

<script>
const webPort = location.port || (location.protocol === 'https:' ? '443' : '80');
const base = location.protocol + '//' + location.hostname + (webPort ? ':' + webPort : '');

document.getElementById('torboxKey').addEventListener('input', resetInstall);
document.getElementById('sitesList').addEventListener('change', resetInstall);

function filterSites(q) {{
  const term = q.toLowerCase();
  document.querySelectorAll('#sitesList .site-item').forEach(el => {{
    el.style.display = el.dataset.name.includes(term) ? '' : 'none';
  }});
}}

function resetInstall() {{
  document.getElementById('installBlock').style.display = 'none';
}}

function clearSites() {{
  document.querySelectorAll('#sitesList input[type=checkbox]').forEach(el => el.checked = false);
  resetInstall();
}}

function buildLinks() {{
  const key = document.getElementById('torboxKey').value.trim();
  const checked = [...document.querySelectorAll('#sitesList input:checked')].map(el => el.value);
  const cfg = {{}};
  if (key) cfg.torboxKey = key;
  if (checked.length) cfg.sites = checked;
  const encoded = btoa(JSON.stringify(cfg)).replace(/\\+/g, '-').replace(/\\//g, '_').replace(/=/g, '');
  const httpUrl = base + '/stremio/' + encoded + '/manifest.json';
  const stremioUrl = 'stremio://' + location.hostname + (webPort ? ':' + webPort : '') + '/stremio/' + encoded + '/manifest.json';
  document.getElementById('httpUrl').textContent = httpUrl;
  document.getElementById('stremioUrl').textContent = stremioUrl;
  document.getElementById('stremioLink').href = stremioUrl;
  document.getElementById('installBlock').style.display = 'block';
}}

function copyUrl(id, btn) {{
  const text = document.getElementById(id).textContent;
  navigator.clipboard.writeText(text).then(() => {{
    btn.textContent = 'Copied!';
    btn.classList.add('copied');
    setTimeout(() => {{ btn.textContent = 'Copy'; btn.classList.remove('copied'); }}, 2000);
  }});
}}
</script>
</body>
</html>"""


def _render_sites_html(selected: list[str]) -> str:
    try:
        all_sites = _api_get("/api/v1/sites").get("sites", [])
    except Exception:
        return "<span style='color:#888'>Could not load sites.</span>"
    parts = []
    for s in all_sites:
        uuid = s.get("uuid", "")
        name = s.get("name", "")
        logo_url = s.get("logo_url") or ""
        checked = "checked" if uuid in selected else ""
        logo = (
            f'<img class="site-logo" src="{logo_url}" alt="" loading="lazy" onerror="this.style.display=\'none\'">'
            if logo_url else
            '<span class="site-logo-placeholder"></span>'
        )
        parts.append(
            f'<div class="site-item" data-name="{name.lower()}">'
            f'<input type="checkbox" value="{uuid}" {checked}>'
            f'{logo}'
            f'<span onclick="this.previousElementSibling.previousElementSibling.click()">{name}</span>'
            f'</div>'
        )
    return "\n".join(parts)


@router.get("/configure", response_class=HTMLResponse)
def configure_toplevel():
    return HTMLResponse(_CONFIGURE_HTML.format(torbox_key="", sites_html=_render_sites_html([])))


@router.get("/{config}/configure", response_class=HTMLResponse)
def configure(config: str):
    cfg = _decode_config(config)
    torbox_key = cfg.get("torboxKey", "")
    selected = cfg.get("sites") or []
    return HTMLResponse(_CONFIGURE_HTML.format(torbox_key=torbox_key, sites_html=_render_sites_html(selected)))


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
    torbox_key = cfg.get("torboxKey", "")

    scene = _api_get(f"/api/v1/scenes/{scene_id}")
    torrents = scene.get("torrents") or []
    if isinstance(torrents, str):
        try:
            torrents = json.loads(torrents)
        except Exception:
            torrents = []

    streams = _torrents_to_streams(torrents, config)

    if torbox_key and streams:
        hashes = [s["_hash"] for s in streams]
        try:
            resp = httpx.get(
                f"{TORBOX_API}/torrents/checkcached",
                params={"hash": ",".join(hashes), "format": "object", "list_files": "false"},
                headers={"Authorization": f"Bearer {torbox_key}"},
                timeout=10,
            )
            cached_set: set[str] = set()
            if resp.status_code == 200:
                data = resp.json().get("data") or {}
                for h, v in data.items():
                    if v:
                        cached_set.add(h.lower())
        except Exception:
            cached_set = set()

        for s in streams:
            h = s["_hash"]
            base = str(request.base_url).rstrip("/")
            resolve_url = f"{base}/stremio/{config}/resolve/{h}"
            if h in cached_set:
                s["name"] = "[TB⚡] " + s["name"]
                s["_cached"] = True
            else:
                s["name"] = "[TB⏳] " + s["name"]
            s["url"] = resolve_url
            s.pop("infoHash", None)

        streams.sort(key=lambda s: (0 if s["_cached"] else 1, -(s["_seeders"] or 0)))
    else:
        streams.sort(key=lambda s: -(s["_seeders"] or 0))

    for s in streams:
        s.pop("_hash", None)
        s.pop("_seeders", None)
        s.pop("_cached", None)

    return JSONResponse(content={"streams": streams})


# ─── Resolve (TorBox) ────────────────────────────────────────────────────────

@router.get("/{config}/resolve/{info_hash}")
def resolve(config: str, info_hash: str):
    cfg = _decode_config(config)
    torbox_key = cfg.get("torboxKey", "")
    if not torbox_key:
        raise HTTPException(status_code=400, detail="TorBox API key not configured")

    try:
        torrent = _api_get(f"/api/v1/torrents/{info_hash}")
    except Exception:
        raise HTTPException(status_code=404, detail="Torrent not found in local database")
    magnet = torrent.get("magnet", "")
    if not magnet:
        raise HTTPException(status_code=404, detail="No magnet link available")

    headers = {"Authorization": f"Bearer {torbox_key}"}

    with httpx.Client(timeout=30) as client:
        mylist_resp = client.get(f"{TORBOX_API}/torrents/mylist", headers=headers)
        torrent_id = None
        if mylist_resp.status_code == 200:
            for t in (mylist_resp.json().get("data") or []):
                if (t.get("hash") or "").lower() == info_hash.lower():
                    torrent_id = t["id"]
                    break

        if torrent_id is None:
            create_resp = client.post(
                f"{TORBOX_API}/torrents/createtorrent",
                data={"magnet": magnet},
                headers=headers,
            )
            if create_resp.status_code not in (200, 201):
                raise HTTPException(status_code=502, detail="TorBox: failed to add torrent")
            torrent_id = create_resp.json().get("data", {}).get("torrent_id")
            if not torrent_id:
                raise HTTPException(status_code=502, detail="TorBox: no torrent_id returned")

        detail_resp = client.get(
            f"{TORBOX_API}/torrents/mylist",
            params={"id": torrent_id, "bypass_cache": "true"},
            headers=headers,
        )
        files = []
        if detail_resp.status_code == 200:
            t_detail = detail_resp.json().get("data") or {}
            if isinstance(t_detail, list):
                t_detail = t_detail[0] if t_detail else {}
            files = t_detail.get("files") or []

    video_exts = (".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v")
    video_files = [f for f in files if f.get("name", "").lower().endswith(video_exts)]
    if not video_files:
        video_files = files
    if not video_files:
        raise HTTPException(status_code=404, detail="No files found in torrent")
    best = max(video_files, key=lambda f: f.get("size") or 0)
    file_id = best.get("id")

    dl_url = (
        f"{TORBOX_API}/torrents/requestdl"
        f"?token={torbox_key}&torrent_id={torrent_id}&file_id={file_id}&redirect=true"
    )
    return RedirectResponse(url=dl_url, status_code=301)
