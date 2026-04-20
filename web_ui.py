import os
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlencode

from datetime import date

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from jinja2 import Environment

import db

app = FastAPI()

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Torrent Search</title>
<style>
  body { font-family: monospace; font-size: 14px; margin: 20px; background: #111; color: #ddd; }
  h1 { font-size: 1.2em; margin-bottom: 12px; }
  form { margin-bottom: 16px; display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
  input[type=text] { background: #222; color: #ddd; border: 1px solid #444; padding: 4px 8px; font-family: monospace; font-size: 14px; width: 260px; }
  input[type=date] { background: #222; color: #ddd; border: 1px solid #444; padding: 4px 8px; font-family: monospace; font-size: 14px; }
  select { background: #222; color: #ddd; border: 1px solid #444; padding: 4px 6px; font-family: monospace; font-size: 14px; }
  button { background: #333; color: #ddd; border: 1px solid #555; padding: 4px 14px; font-family: monospace; font-size: 14px; cursor: pointer; }
  button:hover { background: #444; }
  table { border-collapse: collapse; width: 100%; }
  th { text-align: left; border-bottom: 1px solid #444; padding: 5px 8px; color: #aaa; white-space: nowrap; }
  td { padding: 4px 8px; border-bottom: 1px solid #222; vertical-align: top; }
  td.thumb { position: relative; width: 140px; min-width: 140px; height: 104px; }
  td.thumb img { position: absolute; top: 4px; left: 0; width: 136px; height: 96px; object-fit: cover; border-radius: 3px; cursor: zoom-in; transition: width 0.15s ease, height 0.15s ease, box-shadow 0.15s ease; z-index: 1; }
  td.thumb img:hover { width: 500px; height: auto; object-fit: unset; box-shadow: 0 4px 24px #000; z-index: 10; }
  tr:nth-child(even) td { background: #181818; }
  a { color: #7af; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .muted { color: #666; }
  .pagination { margin-top: 14px; display: flex; gap: 16px; align-items: center; }
  .pagination a { color: #7af; }
  .pagination .disabled { color: #555; pointer-events: none; }
  .count { color: #888; margin-bottom: 10px; font-size: 13px; }
  .quickfilters { margin-bottom: 14px; display: flex; flex-wrap: wrap; gap: 6px; font-size: 13px; }
  .quickfilters a { color: #aaa; background: #1e1e1e; border: 1px solid #333; padding: 2px 8px; border-radius: 3px; white-space: nowrap; }
  .quickfilters a:hover { background: #2a2a2a; color: #ddd; text-decoration: none; }
  .quickfilters .sep { color: #444; align-self: center; }
  /* Scene metadata */
  .scene-site { font-size: 11px; color: #888; margin-top: 3px; display: flex; align-items: center; gap: 4px; }
  .scene-site img { height: 14px; width: auto; vertical-align: middle; }
  .scene-title { font-size: 12px; color: #aaa; margin-top: 2px; }
  .scene-performers { font-size: 11px; color: #7af; margin-top: 2px; }
  .scene-tags { margin-top: 3px; display: flex; flex-wrap: wrap; gap: 3px; }
  .scene-tags span { font-size: 10px; background: #1e2a1e; color: #7c7; border: 1px solid #334; padding: 1px 5px; border-radius: 2px; }
  .scene-desc { font-size: 11px; color: #666; margin-top: 3px; max-width: 480px; line-height: 1.4; }
  .site-link { color: #a8d; font-size: 12px; }
</style>
</head>
<body>
<h1><a href="/">Torrent Search</a></h1>
<div class="quickfilters">
  <a href="/?sort_by=date_added&sort_order=desc">Latest</a>
  <a href="/?sort_by=seeders&sort_order=desc">Top All</a>
  <span class="sep">|</span>
  {% for cat in categories %}
  <a href="/?sort_by=seeders&sort_order=desc&category={{ cat | urlencode }}">Top {{ cat }}</a>
  {% endfor %}
</div>
<form method="GET" action="/">
  <input type="text" name="q" value="{{ q }}" placeholder="Search title…">
  <input type="text" name="site" value="{{ site }}" placeholder="Filter by site…">
  <input type="date" name="date_from" value="{{ date_from }}" title="Release date from">
  <input type="date" name="date_to" value="{{ date_to }}" title="Release date to">
  <select name="category">
    <option value="">All categories</option>
    {% for cat in categories %}
    <option value="{{ cat }}"{% if cat == selected_category %} selected{% endif %}>{{ cat }}</option>
    {% endfor %}
  </select>
  <select name="sort_by">
    {% for col, label in [("date_added","Date Added"),("release_date","Release Date"),("seeders","Seeders"),("leechers","Leechers"),("title","Title"),("size_bytes","Size")] %}
    <option value="{{ col }}"{% if col == sort_by %} selected{% endif %}>{{ label }}</option>
    {% endfor %}
  </select>
  <select name="sort_order">
    <option value="desc"{% if sort_order == "desc" %} selected{% endif %}>Desc</option>
    <option value="asc"{% if sort_order == "asc" %} selected{% endif %}>Asc</option>
  </select>
  <select name="per_page">
    {% for n in [25, 50, 100] %}
    <option value="{{ n }}"{% if n == per_page %} selected{% endif %}>{{ n }} / page</option>
    {% endfor %}
  </select>
  <button type="submit">Search</button>
</form>
<div class="count">{{ total }} result{{ "s" if total != 1 else "" }}{% if total > 0 %} &mdash; page {{ page }} of {{ total_pages }}{% endif %}</div>
{% if torrents %}
<table>
  <thead>
    <tr>
      <th></th>
      <th>Title</th>
      <th>Site</th>
      <th>Release</th>
      <th>Category</th>
      <th>Res</th>
      <th>Size</th>
      <th>Seeders</th>
      <th>Leechers</th>
      <th>Date Added</th>
      <th>Uploader</th>
    </tr>
  </thead>
  <tbody>
    {% for t in torrents %}
    <tr>
      <td class="thumb">
        {% set poster = t.poster_url or t.image_url %}
        {% if poster %}<img src="{{ poster }}" alt="">{% endif %}
      </td>
      <td>
        <a href="{{ t.magnet }}" title="{{ t.title }}">{{ t.title }}</a>
        {% if t.scene_title %}
        <div class="scene-title">{{ t.scene_title }}</div>
        {% endif %}
        {% if t.site_name or t.network_name %}
        <div class="scene-site">
          {% set logo = t.site_logo_url or t.network_logo_url %}
          {% if logo %}<img src="{{ logo }}" alt="">{% endif %}
          {{ t.site_name or t.network_name }}
        </div>
        {% endif %}
        {% if t.performers %}
        <div class="scene-performers">{{ t.performers | map(attribute="name") | list | join(", ") }}</div>
        {% endif %}
        {% if t.tags %}
        <div class="scene-tags">{% for tag in t.tags[:8] %}<span>{{ tag }}</span>{% endfor %}</div>
        {% endif %}
        {% if t.scene_description %}
        <div class="scene-desc">{{ t.scene_description[:200] }}{% if t.scene_description | length > 200 %}…{% endif %}</div>
        {% endif %}
      </td>
      <td>
        {% if t.sitename %}
        <a href="/?site={{ t.sitename | urlencode }}" class="site-link">{{ t.sitename }}</a>
        {% endif %}
      </td>
      <td class="muted">{{ t.release_date.isoformat() if t.release_date else "" }}</td>
      <td>{{ t.category or "" }}</td>
      <td class="muted">{{ t.resolution or "" }}</td>
      <td>{{ t.size_human }}</td>
      <td>{{ t.seeders if t.seeders is not none else "" }}</td>
      <td>{{ t.leechers if t.leechers is not none else "" }}</td>
      <td>{{ t.date_label }}</td>
      <td>{{ t.uploader or "" }}</td>
    </tr>
    {% endfor %}
  </tbody>
</table>
<div class="pagination">
  {% if has_prev %}
  <a href="{{ prev_url }}">&larr; Prev</a>
  {% else %}
  <span class="disabled">&larr; Prev</span>
  {% endif %}
  <span>Page {{ page }} / {{ total_pages }}</span>
  {% if has_next %}
  <a href="{{ next_url }}">Next &rarr;</a>
  {% else %}
  <span class="disabled">Next &rarr;</span>
  {% endif %}
</div>
{% else %}
<p class="muted">No results.</p>
{% endif %}
</body>
</html>"""


def format_date(dt) -> str:
    if dt is None:
        return ""
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


def page_url(params: dict, page: int) -> str:
    args = {k: v for k, v in params.items() if k != "page" and v}
    args["page"] = page
    return "/?" + urlencode(args)


VALID_PER_PAGE = {25, 50, 100}

@app.get("/", response_class=HTMLResponse)
def index(
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
        categories = db.list_categories(conn)
        total = db.count_torrents(
            conn, q or None, category or None, site or None,
            date_from or None, effective_date_to,
        )
        torrents = db.search_torrents(
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

    for t in torrents:
        t["size_human"] = format_size(t["size_bytes"])
        t["date_label"] = format_date(t["date_added"])

    total_pages = max(1, (total + limit - 1) // limit)
    base_args = {
        "q": q, "site": site, "date_from": date_from, "date_to": date_to,
        "category": category, "sort_by": sort_by, "sort_order": sort_order, "per_page": limit,
    }

    env = Environment(autoescape=True)
    env.filters["urlencode"] = quote_plus
    tmpl = env.from_string(HTML_TEMPLATE)
    html = tmpl.render(
        torrents=torrents,
        categories=categories,
        q=q,
        site=site,
        date_from=date_from,
        date_to=date_to,
        selected_category=category,
        sort_by=sort_by,
        sort_order=sort_order,
        per_page=limit,
        page=page,
        total=total,
        total_pages=total_pages,
        has_prev=page > 1,
        has_next=page < total_pages,
        prev_url=page_url(base_args, page - 1),
        next_url=page_url(base_args, page + 1),
    )
    return HTMLResponse(content=html)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("WEB_PORT", 5000))
    uvicorn.run("web_ui:app", host="0.0.0.0", port=port, reload=False)
