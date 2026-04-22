import os
import re
from datetime import date, datetime
from decimal import Decimal

import cache
import db
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="Media Library API", version="1")

MAX_PER_PAGE = 250

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_date(value: str, param_name: str):
    if value and not _DATE_RE.match(value):
        raise HTTPException(status_code=422, detail=f"{param_name} must be YYYY-MM-DD")
    if value:
        try:
            date.fromisoformat(value)
        except ValueError:
            raise HTTPException(status_code=422, detail=f"{param_name} is not a valid date")


def _serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _serial(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serial(i) for i in obj]
    return obj


def _paginated(items, total, page, limit):
    total_pages = max(1, (total + limit - 1) // limit)
    return JSONResponse(content=_serial({
        "total": total,
        "page": page,
        "per_page": limit,
        "total_pages": total_pages,
        "items": items,
    }))


@app.get("/api/v1/scenes")
def list_scenes(
    q: str = Query(default=""),
    site: str = Query(default=""),
    performer: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    sort_by: str = Query(default="date"),
    sort_order: str = Query(default="desc"),
    per_page: int = Query(default=30),
    page: int = Query(default=1, ge=1),
):
    _validate_date(date_from, "date_from")
    _validate_date(date_to, "date_to")
    if sort_order.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=422, detail="sort_order must be 'asc' or 'desc'")
    limit = max(1, min(per_page, MAX_PER_PAGE))
    offset = (page - 1) * limit
    effective_date_to = date_to or date.today().isoformat()
    use_cache = not q
    key = cache.make_key(
        "scenes", site=site, performer=performer,
        date_from=date_from, date_to=effective_date_to,
        sort_by=sort_by, sort_order=sort_order, limit=limit, offset=offset,
    ) if use_cache else None
    if use_cache and (hit := cache.cache_get(key)) is not None:
        return JSONResponse(content=hit)
    conn = db.get_connection()
    try:
        total = db.count_scenes(
            conn, q or None, site or None, date_from or None, effective_date_to,
            performer=performer or None,
        )
        items = db.search_scenes(
            conn,
            query=q or None,
            site=site or None,
            date_from=date_from or None,
            date_to=effective_date_to,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
            performer=performer or None,
        )
    finally:
        conn.close()
    total_pages = max(1, (total + limit - 1) // limit)
    result = _serial({"total": total, "page": page, "per_page": limit, "total_pages": total_pages, "items": items})
    if use_cache:
        cache.cache_set(key, result, ttl=600)
    return JSONResponse(content=result)


@app.get("/api/v1/movies")
def list_movies(
    q: str = Query(default=""),
    site: str = Query(default=""),
    performer: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    sort_by: str = Query(default="date"),
    sort_order: str = Query(default="desc"),
    per_page: int = Query(default=30),
    page: int = Query(default=1, ge=1),
):
    _validate_date(date_from, "date_from")
    _validate_date(date_to, "date_to")
    if sort_order.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=422, detail="sort_order must be 'asc' or 'desc'")
    limit = max(1, min(per_page, MAX_PER_PAGE))
    offset = (page - 1) * limit
    effective_date_to = date_to or date.today().isoformat()
    use_cache = not q
    key = cache.make_key(
        "movies", site=site, performer=performer,
        date_from=date_from, date_to=effective_date_to,
        sort_by=sort_by, sort_order=sort_order, limit=limit, offset=offset,
    ) if use_cache else None
    if use_cache and (hit := cache.cache_get(key)) is not None:
        return JSONResponse(content=hit)
    conn = db.get_connection()
    try:
        total = db.count_movies(
            conn, q or None, site or None, date_from or None, effective_date_to,
            performer=performer or None,
        )
        items = db.search_movies(
            conn,
            query=q or None,
            site=site or None,
            date_from=date_from or None,
            date_to=effective_date_to,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
            performer=performer or None,
        )
    finally:
        conn.close()
    total_pages = max(1, (total + limit - 1) // limit)
    result = _serial({"total": total, "page": page, "per_page": limit, "total_pages": total_pages, "items": items})
    if use_cache:
        cache.cache_set(key, result, ttl=600)
    return JSONResponse(content=result)


@app.get("/api/v1/torrents")
def list_torrents(
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
    _validate_date(date_from, "date_from")
    _validate_date(date_to, "date_to")
    if sort_order.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=422, detail="sort_order must be 'asc' or 'desc'")
    limit = max(1, min(per_page, MAX_PER_PAGE))
    offset = (page - 1) * limit
    effective_date_to = date_to or date.today().isoformat()
    use_cache = not q
    key = cache.make_key(
        "torrents", site=site, category=category,
        date_from=date_from, date_to=effective_date_to,
        sort_by=sort_by, sort_order=sort_order, limit=limit, offset=offset,
    ) if use_cache else None
    if use_cache and (hit := cache.cache_get(key)) is not None:
        return JSONResponse(content=hit)
    conn = db.get_connection()
    try:
        total = db.count_torrents(
            conn, q or None, category or None, site or None,
            date_from or None, effective_date_to,
        )
        items = db.search_torrents(
            conn,
            query=q or None,
            category=category or None,
            site=site or None,
            date_from=date_from or None,
            date_to=effective_date_to,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
        )
    finally:
        conn.close()
    total_pages = max(1, (total + limit - 1) // limit)
    result = _serial({"total": total, "page": page, "per_page": limit, "total_pages": total_pages, "items": items})
    if use_cache:
        cache.cache_set(key, result, ttl=600)
    return JSONResponse(content=result)


@app.get("/api/v1/categories")
def list_categories():
    key = cache.make_key("categories")
    if (hit := cache.cache_get(key)) is not None:
        return JSONResponse(content=hit)
    conn = db.get_connection()
    try:
        cats = db.list_categories(conn)
    finally:
        conn.close()
    result = {"categories": cats}
    cache.cache_set(key, result, ttl=3600)
    return JSONResponse(content=result)


@app.get("/api/v1/stats")
def stats():
    key = cache.make_key("stats")
    if (hit := cache.cache_get(key)) is not None:
        return JSONResponse(content=hit)
    conn = db.get_connection()
    try:
        result = _serial(db.get_stats(conn))
    finally:
        conn.close()
    cache.cache_set(key, result, ttl=600)
    return JSONResponse(content=result)


@app.get("/api/v1/sites")
def list_sites(q: str = Query(default="")):
    use_cache = not q
    key = cache.make_key("sites") if use_cache else None
    if use_cache and (hit := cache.cache_get(key)) is not None:
        return JSONResponse(content=hit)
    conn = db.get_connection()
    try:
        sites = db.list_sites(conn, q or None)
    finally:
        conn.close()
    result = {"sites": _serial(sites)}
    if use_cache:
        cache.cache_set(key, result, ttl=3600)
    return JSONResponse(content=result)


@app.get("/api/v1/sites/{uuid}")
def get_site(uuid: str):
    key = cache.make_key("site", uuid=uuid)
    if (hit := cache.cache_get(key)) is not None:
        return JSONResponse(content=hit)
    conn = db.get_connection()
    try:
        s = db.get_site(conn, uuid)
    finally:
        conn.close()
    if not s:
        raise HTTPException(status_code=404, detail="Site not found")
    result = _serial(s)
    cache.cache_set(key, result, ttl=3600)
    return JSONResponse(content=result)


@app.get("/api/v1/performers")
def list_performers(
    q: str = Query(default=""),
    per_page: int = Query(default=50),
    page: int = Query(default=1, ge=1),
):
    limit = max(1, min(per_page, MAX_PER_PAGE))
    offset = (page - 1) * limit
    use_cache = not q
    key = cache.make_key("performers", limit=limit, offset=offset) if use_cache else None
    if use_cache and (hit := cache.cache_get(key)) is not None:
        return JSONResponse(content=hit)
    conn = db.get_connection()
    try:
        total = db.count_performers(conn, q or None)
        items = db.list_performers(conn, query=q or None, limit=limit, offset=offset)
    finally:
        conn.close()
    total_pages = max(1, (total + limit - 1) // limit)
    result = _serial({"total": total, "page": page, "per_page": limit, "total_pages": total_pages, "items": items})
    if use_cache:
        cache.cache_set(key, result, ttl=3600)
    return JSONResponse(content=result)


@app.get("/api/v1/performers/{uuid}")
def get_performer(uuid: str):
    key = cache.make_key("performer", uuid=uuid)
    if (hit := cache.cache_get(key)) is not None:
        return JSONResponse(content=hit)
    conn = db.get_connection()
    try:
        p = db.get_performer(conn, uuid)
    finally:
        conn.close()
    if not p:
        raise HTTPException(status_code=404, detail="Performer not found")
    result = _serial(p)
    cache.cache_set(key, result, ttl=3600)
    return JSONResponse(content=result)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("API_PORT", 5001))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)
