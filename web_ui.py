import json
import os
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlencode

import requests as req_lib
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape

app = FastAPI()

_API_PORT = os.environ.get("API_PORT", "5001")
API_BASE = os.environ.get("API_URL", f"http://localhost:{_API_PORT}")

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
_jinja_env = Environment(
    loader=FileSystemLoader(_TEMPLATE_DIR),
    autoescape=select_autoescape(["html"]),
)
_jinja_env.filters["urlencode"] = quote_plus


def _render(template_name: str, **ctx) -> str:
    return _jinja_env.get_template(template_name).render(**ctx)


def _api_get(path: str, params: dict) -> dict:
    resp = req_lib.get(f"{API_BASE}{path}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def format_duration(seconds) -> str:
    if seconds is None:
        return ""
    h, rem = divmod(int(seconds), 3600)
    m = rem // 60
    if h:
        return f"{h}h {m:02d}m"
    return f"{m}m"


def format_date(dt) -> str:
    if dt is None:
        return ""
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return dt
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now - dt
    seconds = int(delta.total_seconds())
    if seconds < 0:
        return dt.strftime("%Y-%m-%d")
    if seconds < 7 * 86400:
        if seconds < 3600:
            m = max(1, seconds // 60)
            return f"{m}m ago"
        if seconds < 86400:
            h = seconds // 3600
            return f"{h}h ago"
        d = seconds // 86400
        return f"{d}d ago"
    return dt.strftime("%Y-%m-%d")


def format_size(size_bytes) -> str:
    if size_bytes is None:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def page_url(base_path: str, params: dict, page: int) -> str:
    args = {k: v for k, v in params.items() if k != "page" and v}
    args["page"] = page
    return base_path + "?" + urlencode(args)


def _enrich_scenes(scenes: list) -> None:
    for s in scenes:
        s["duration_human"] = format_duration(s.get("duration_seconds"))
        for t in s.get("torrents") or []:
            t["size_human"] = format_size(t.get("size_bytes"))
            t["age_label"] = format_date(t.get("date_added"))
        seeders = [t["seeders"] for t in s.get("torrents") or [] if t.get("seeders") is not None]
        s["max_seeders"] = max(seeders) if seeders else None


VALID_PER_PAGE_SCENES = {30, 60, 90}
VALID_PER_PAGE_TORRENTS = {25, 50, 100}
VALID_PER_PAGE_PERFORMERS = {48, 96, 192}


@app.get("/", response_class=HTMLResponse)
def index():
    return RedirectResponse(url="/scenes", status_code=302)


@app.get("/scenes", response_class=HTMLResponse)
def scenes_ui(
    q: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    sort_by: str = Query(default="date"),
    sort_order: str = Query(default="desc"),
    per_page: int = Query(default=30),
    page: int = Query(default=1, ge=1),
):
    limit = per_page if per_page in VALID_PER_PAGE_SCENES else 30
    data = _api_get("/api/v1/scenes", {
        "q": q, "date_from": date_from, "date_to": date_to,
        "sort_by": sort_by, "sort_order": sort_order, "per_page": limit, "page": page,
    })
    scenes = data["items"]
    total = data["total"]
    total_pages = data["total_pages"]
    _enrich_scenes(scenes)

    base_args = {
        "q": q, "date_from": date_from, "date_to": date_to,
        "sort_by": sort_by, "sort_order": sort_order, "per_page": limit,
    }
    return HTMLResponse(_render(
        "scenes.html",
        active_page="scenes",
        scenes=scenes,
        scenes_json=json.dumps(scenes).replace("</", "<\\/"),
        q=q, date_from=date_from, date_to=date_to,
        sort_by=sort_by, sort_order=sort_order, per_page=limit,
        page=page, total=total, total_pages=total_pages,
        has_prev=page > 1, has_next=page < total_pages,
        prev_url=page_url("/scenes", base_args, page - 1),
        next_url=page_url("/scenes", base_args, page + 1),
    ))


@app.get("/movies", response_class=HTMLResponse)
def movies_ui(
    q: str = Query(default=""),
    site: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    sort_by: str = Query(default="date"),
    sort_order: str = Query(default="desc"),
    per_page: int = Query(default=30),
    page: int = Query(default=1, ge=1),
):
    limit = per_page if per_page in VALID_PER_PAGE_SCENES else 30
    data = _api_get("/api/v1/movies", {
        "q": q, "site": site, "date_from": date_from, "date_to": date_to,
        "sort_by": sort_by, "sort_order": sort_order, "per_page": limit, "page": page,
    })
    movies = data["items"]
    total = data["total"]
    total_pages = data["total_pages"]
    _enrich_scenes(movies)

    base_args = {
        "q": q, "site": site, "date_from": date_from, "date_to": date_to,
        "sort_by": sort_by, "sort_order": sort_order, "per_page": limit,
    }
    return HTMLResponse(_render(
        "movies.html",
        active_page="movies",
        movies=movies,
        movies_json=json.dumps(movies).replace("</", "<\\/"),
        q=q, site=site, date_from=date_from, date_to=date_to,
        sort_by=sort_by, sort_order=sort_order, per_page=limit,
        page=page, total=total, total_pages=total_pages,
        has_prev=page > 1, has_next=page < total_pages,
        prev_url=page_url("/movies", base_args, page - 1),
        next_url=page_url("/movies", base_args, page + 1),
    ))


@app.get("/torrents", response_class=HTMLResponse)
def torrents_ui(
    q: str = Query(default=""),
    site: str = Query(default=""),
    date_from: str = Query(default="2020-01-01"),
    date_to: str = Query(default=""),
    category: str = Query(default=""),
    sort_by: str = Query(default="date_added"),
    sort_order: str = Query(default="desc"),
    per_page: int = Query(default=25),
    page: int = Query(default=1, ge=1),
):
    limit = per_page if per_page in VALID_PER_PAGE_TORRENTS else 25
    data = _api_get("/api/v1/torrents", {
        "q": q, "site": site, "date_from": date_from, "date_to": date_to,
        "category": category, "sort_by": sort_by, "sort_order": sort_order,
        "per_page": limit, "page": page,
    })
    cats = _api_get("/api/v1/categories", {})
    torrents = data["items"]
    total = data["total"]
    total_pages = data["total_pages"]
    for t in torrents:
        t["size_human"] = format_size(t.get("size_bytes"))
        t["date_label"] = format_date(t.get("date_added"))

    base_args = {
        "q": q, "site": site, "date_from": date_from, "date_to": date_to,
        "category": category, "sort_by": sort_by, "sort_order": sort_order, "per_page": limit,
    }
    return HTMLResponse(_render(
        "torrents.html",
        active_page="torrents",
        torrents=torrents,
        categories=cats["categories"],
        q=q, site=site, date_from=date_from, date_to=date_to,
        selected_category=category, sort_by=sort_by, sort_order=sort_order, per_page=limit,
        page=page, total=total, total_pages=total_pages,
        has_prev=page > 1, has_next=page < total_pages,
        prev_url=page_url("/torrents", base_args, page - 1),
        next_url=page_url("/torrents", base_args, page + 1),
    ))


@app.get("/performers", response_class=HTMLResponse)
def performers_ui(
    q: str = Query(default=""),
    per_page: int = Query(default=48),
    page: int = Query(default=1, ge=1),
):
    limit = per_page if per_page in VALID_PER_PAGE_PERFORMERS else 48
    data = _api_get("/api/v1/performers", {"q": q, "per_page": limit, "page": page})
    performers = data["items"]
    total = data["total"]
    total_pages = data["total_pages"]

    base_args = {"q": q, "per_page": limit}
    return HTMLResponse(_render(
        "performers.html",
        active_page="performers",
        performers=performers, q=q, per_page=limit,
        page=page, total=total, total_pages=total_pages,
        has_prev=page > 1, has_next=page < total_pages,
        prev_url=page_url("/performers", base_args, page - 1),
        next_url=page_url("/performers", base_args, page + 1),
    ))


@app.get("/performer/{uuid}", response_class=HTMLResponse)
def performer_ui(
    uuid: str,
    q: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=30),
):
    try:
        performer = _api_get(f"/api/v1/performers/{uuid}", {})
    except req_lib.HTTPError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Performer not found")
        raise
    limit = per_page if per_page in VALID_PER_PAGE_SCENES else 30
    data = _api_get("/api/v1/scenes", {
        "q": q,
        "performer": performer["name"], "per_page": limit, "page": page,
        "sort_by": "date", "sort_order": "desc",
    })
    scenes = data["items"]
    total = data["total"]
    total_pages = data["total_pages"]
    _enrich_scenes(scenes)

    movies_data = _api_get("/api/v1/movies", {
        "performer": performer["name"], "per_page": 30, "page": 1,
        "sort_by": "date", "sort_order": "desc",
    })
    movies = movies_data["items"]
    _enrich_scenes(movies)
    scenes = sorted(scenes + movies, key=lambda x: x.get("date") or "", reverse=True)

    base_args = {"q": q, "per_page": limit}
    return HTMLResponse(_render(
        "performer.html",
        active_page="performers",
        performer=performer, scenes=scenes,
        scenes_json=json.dumps(scenes).replace("</", "<\\/"),
        q=q,
        per_page=limit, page=page, total=total, total_pages=total_pages,
        has_prev=page > 1, has_next=page < total_pages,
        prev_url=page_url(f"/performer/{uuid}", base_args, page - 1),
        next_url=page_url(f"/performer/{uuid}", base_args, page + 1),
    ))


@app.get("/network/{uuid}", response_class=HTMLResponse)
def network_ui(
    uuid: str,
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=30),
):
    try:
        network = _api_get(f"/api/v1/networks/{uuid}", {})
    except req_lib.HTTPError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Network not found")
        raise
    limit = per_page if per_page in VALID_PER_PAGE_SCENES else 30
    scenes_data = _api_get("/api/v1/scenes", {
        "network": uuid, "per_page": limit, "page": page,
        "sort_by": "date", "sort_order": "desc",
    })
    scenes = scenes_data["items"]
    total = scenes_data["total"]
    total_pages = scenes_data["total_pages"]
    _enrich_scenes(scenes)

    movies_data = _api_get("/api/v1/movies", {
        "network": uuid, "per_page": 30, "page": 1,
        "sort_by": "date", "sort_order": "desc",
    })
    movies = movies_data["items"]
    _enrich_scenes(movies)
    scenes = sorted(scenes + movies, key=lambda x: x.get("date") or "", reverse=True)

    base_args = {"per_page": limit}
    return HTMLResponse(_render(
        "network.html",
        active_page="sites",
        network=network,
        scenes=scenes,
        scenes_json=json.dumps(scenes).replace("</", "<\\/"),
        per_page=limit, page=page, total=total, total_pages=total_pages,
        has_prev=page > 1, has_next=page < total_pages,
        prev_url=page_url(f"/network/{uuid}", base_args, page - 1),
        next_url=page_url(f"/network/{uuid}", base_args, page + 1),
    ))


@app.get("/sites", response_class=HTMLResponse)
def sites_ui(q: str = Query(default="")):
    data = _api_get("/api/v1/sites", {"q": q})
    return HTMLResponse(_render(
        "sites.html",
        active_page="sites",
        q=q,
        sites=data.get("sites", []),
    ))


@app.get("/site/{uuid}", response_class=HTMLResponse)
def site_ui(
    uuid: str,
    q: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=30),
):
    try:
        site = _api_get(f"/api/v1/sites/{uuid}", {})
    except req_lib.HTTPError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Site not found")
        raise
    limit = per_page if per_page in VALID_PER_PAGE_SCENES else 30
    data = _api_get("/api/v1/scenes", {
        "q": q,
        "site": site["name"], "per_page": limit, "page": page,
        "sort_by": "date", "sort_order": "desc",
    })
    scenes = data["items"]
    total = data["total"]
    total_pages = data["total_pages"]
    _enrich_scenes(scenes)

    movies_data = _api_get("/api/v1/movies", {
        "site": site["name"], "per_page": 30, "page": 1,
        "sort_by": "date", "sort_order": "desc",
    })
    movies = movies_data["items"]
    _enrich_scenes(movies)
    scenes = sorted(scenes + movies, key=lambda x: x.get("date") or "", reverse=True)

    base_args = {"q": q, "per_page": limit}
    return HTMLResponse(_render(
        "site.html",
        active_page="sites",
        site=site, scenes=scenes,
        scenes_json=json.dumps(scenes).replace("</", "<\\/"),
        q=q,
        per_page=limit, page=page, total=total, total_pages=total_pages,
        has_prev=page > 1, has_next=page < total_pages,
        prev_url=page_url(f"/site/{uuid}", base_args, page - 1),
        next_url=page_url(f"/site/{uuid}", base_args, page + 1),
    ))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("WEB_PORT", 5000))
    uvicorn.run("web_ui:app", host="0.0.0.0", port=port, reload=False)
