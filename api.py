import os
from datetime import date, datetime

import db
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="Media Library API", version="1")

VALID_PER_PAGE = {25, 50, 100}


def _serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
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
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    sort_by: str = Query(default="date"),
    sort_order: str = Query(default="desc"),
    per_page: int = Query(default=25),
    page: int = Query(default=1, ge=1),
):
    limit = per_page if per_page in VALID_PER_PAGE else 25
    offset = (page - 1) * limit
    effective_date_to = date_to or date.today().isoformat()
    conn = db.get_connection()
    try:
        total = db.count_scenes(conn, q or None, site or None, date_from or None, effective_date_to)
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
        )
    finally:
        conn.close()
    return _paginated(items, total, page, limit)


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
    limit = per_page if per_page in VALID_PER_PAGE else 25
    offset = (page - 1) * limit
    effective_date_to = date_to or date.today().isoformat()
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
    return _paginated(items, total, page, limit)


@app.get("/api/v1/categories")
def list_categories():
    conn = db.get_connection()
    try:
        cats = db.list_categories(conn)
    finally:
        conn.close()
    return JSONResponse(content={"categories": cats})


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("API_PORT", 5001))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)
