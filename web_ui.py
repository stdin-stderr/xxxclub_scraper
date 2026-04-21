import json
import os
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlencode

import requests as req_lib
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment

app = FastAPI()

_API_PORT = os.environ.get("API_PORT", "5001")
API_BASE = os.environ.get("API_URL", f"http://localhost:{_API_PORT}")


def _api_get(path: str, params: dict) -> dict:
    resp = req_lib.get(f"{API_BASE}{path}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


SCENES_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Scene Browser</title>
<style>
  *{box-sizing:border-box}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:0;background:#0f0f0f;color:#f1f1f1}
  .topbar{display:flex;align-items:center;gap:12px;padding:10px 20px;background:#161616;border-bottom:1px solid #272727;flex-wrap:wrap}
  .topbar h1{font-size:16px;font-weight:700;margin:0;white-space:nowrap}
  .topbar h1 a{color:#f1f1f1;text-decoration:none}
  .search-form{display:flex;gap:6px;flex-wrap:wrap;align-items:center;flex:1}
  .search-form input[type=text]{background:#121212;color:#f1f1f1;border:1px solid #3f3f3f;border-radius:4px;padding:6px 10px;font-size:13px;min-width:140px;flex:1;max-width:240px}
  .search-form input[type=date]{background:#121212;color:#f1f1f1;border:1px solid #3f3f3f;border-radius:4px;padding:6px 8px;font-size:13px}
  .search-form select{background:#121212;color:#f1f1f1;border:1px solid #3f3f3f;border-radius:4px;padding:6px 8px;font-size:13px}
  .search-form button{background:#272727;color:#f1f1f1;border:1px solid #3f3f3f;border-radius:4px;padding:6px 16px;font-size:13px;cursor:pointer;white-space:nowrap}
  .search-form button:hover{background:#3f3f3f}
  .quickbar{display:flex;gap:8px;padding:8px 20px;flex-wrap:wrap;align-items:center;font-size:13px;border-bottom:1px solid #1e1e1e}
  .quickbar a{color:#aaa;background:#1e1e1e;border:1px solid #2e2e2e;padding:3px 10px;border-radius:16px;text-decoration:none;white-space:nowrap}
  .quickbar a:hover{background:#2e2e2e;color:#f1f1f1}
  .quickbar .sep{color:#333}
  .count-bar{padding:8px 20px;font-size:12px;color:#717171}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px;padding:16px 20px 40px}
  .card{background:#161616;border-radius:8px;overflow:hidden;cursor:pointer;transition:transform 0.15s,box-shadow 0.15s;display:flex;flex-direction:column}
  .card:hover{transform:translateY(-2px);box-shadow:0 8px 32px rgba(0,0,0,0.6)}
  .card-thumb{position:relative;width:100%;padding-top:56.25%;background:#222;flex-shrink:0}
  .card-thumb img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover}
  .card-thumb .no-img{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#555;font-size:12px}
  .card-dur{position:absolute;bottom:6px;right:6px;background:rgba(0,0,0,0.82);color:#fff;font-size:11px;font-weight:600;padding:1px 5px;border-radius:3px}
  .card-body{padding:10px 12px 12px;display:flex;flex-direction:column;flex:1}
  .card-site-logo{height:28px;width:auto;border-radius:2px;margin-bottom:5px;align-self:flex-start;max-width: 100%;}
  .card-title{font-size:14px;font-weight:700;line-height:1.3;color:#f1f1f1;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;margin-bottom:4px}
  .card-performers{font-size:11px;color:#7af;margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .card-desc{font-size:11px;color:#717171;line-height:1.4;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;margin-bottom:6px}
  .card-meta{display:flex;justify-content:space-between;align-items:center;font-size:11px;color:#717171;margin-top:auto}
  .card-seeds{color:#4c4}
  .card-dl-btn{margin-top:8px;width:100%;background:#272727;color:#f1f1f1;border:1px solid #3f3f3f;border-radius:4px;padding:5px 0;font-size:12px;cursor:pointer;transition:background 0.1s}
  .card-dl-btn:hover{background:#3f3f3f}
  .modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.88);z-index:1000;overflow-y:auto;padding:32px 16px}
  .modal-overlay.open{display:flex;justify-content:center;align-items:flex-start}
  .modal{background:#161616;border-radius:10px;max-width:720px;width:100%;position:relative}
  .modal-hero{width:100%;aspect-ratio:16/9;background:#222;overflow:hidden;border-radius:10px 10px 0 0}
  .modal-hero img{width:100%;height:100%;object-fit:cover}
  .modal-close{position:absolute;top:10px;right:10px;background:rgba(0,0,0,0.72);color:#fff;border:none;border-radius:50%;width:32px;height:32px;font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center;z-index:10;line-height:1}
  .modal-body{padding:16px 20px 22px}
  .modal-title{font-size:19px;font-weight:700;line-height:1.3;margin-bottom:8px;color:#f1f1f1}
  .modal-site-row{display:flex;align-items:center;gap:8px;margin-bottom:6px;flex-wrap:wrap}
  .modal-site-logo{height:22px;border-radius:2px}
  .modal-site-name{font-size:13px;font-weight:600;color:#ccc;text-decoration:none}
  .modal-site-name:hover{color:#f1f1f1}
  .modal-network{font-size:12px;color:#717171}
  .modal-dur,.modal-date{font-size:12px;color:#717171}
  .modal-date{margin-left:auto}
  .modal-performers{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:10px}
  .modal-performer{display:flex;flex-direction:column;align-items:center;gap:4px;width:64px}
  .modal-performer img{width:56px;height:56px;border-radius:50%;object-fit:cover;background:#2a2a2a;border:2px solid #2e2e2e}
  .modal-performer span{font-size:10px;text-align:center;color:#7af;line-height:1.2;word-break:break-word}
  .modal-tags{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:10px}
  .modal-tags span{font-size:10px;background:#1e2a1e;color:#7c7;border:1px solid #2a4a2a;padding:2px 7px;border-radius:3px}
  .modal-desc{font-size:13px;color:#aaa;line-height:1.5;margin-bottom:14px}
  .modal-dl-heading{font-size:11px;font-weight:700;color:#717171;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px}
  .modal-torrents table{width:100%;border-collapse:collapse;font-size:12px}
  .modal-torrents th{text-align:left;color:#555;padding:3px 6px;border-bottom:1px solid #2e2e2e;white-space:nowrap}
  .modal-torrents td{padding:4px 6px;border-bottom:1px solid #1e1e1e;vertical-align:middle}
  .modal-torrents tr:last-child td{border-bottom:none}
  .modal-torrents a{color:#7af;text-decoration:none;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:220px;display:inline-block;vertical-align:middle}
  .modal-torrents a:hover{text-decoration:underline}
  .seeds-good{color:#4c4}
  .pagination{display:flex;gap:16px;align-items:center;padding:0 20px 24px;font-size:13px}
  .pagination a{color:#7af;text-decoration:none}
  .pagination a:hover{text-decoration:underline}
  .pagination .disabled{color:#555;pointer-events:none}
</style>
</head>
<body>

<div class="topbar">
  <h1><a href="/scenes">Scenes</a></h1>
  <form class="search-form" method="GET" action="/scenes">
    <input type="text" name="q" value="{{ q }}" placeholder="Search scenes…">
    <input type="text" name="site" value="{{ site }}" placeholder="Site…">
    <input type="date" name="date_from" value="{{ date_from }}" title="From">
    <input type="date" name="date_to" value="{{ date_to }}" title="To">
    <select name="sort_by">
      {% for col, label in [("date","Release Date"),("seeders","Top Seeders"),("title","Title"),("site_name","Site")] %}
      <option value="{{ col }}"{% if col == sort_by %} selected{% endif %}>{{ label }}</option>
      {% endfor %}
    </select>
    <select name="sort_order">
      <option value="desc"{% if sort_order == "desc" %} selected{% endif %}>Desc</option>
      <option value="asc"{% if sort_order == "asc" %} selected{% endif %}>Asc</option>
    </select>
    <select name="per_page">
      {% for n in [30, 60, 90] %}
      <option value="{{ n }}"{% if n == per_page %} selected{% endif %}>{{ n }}/page</option>
      {% endfor %}
    </select>
    <button type="submit">Search</button>
  </form>
</div>

<div class="quickbar">
  <a href="/scenes?sort_by=date&sort_order=desc">Latest</a>
  <a href="/scenes?sort_by=seeders&sort_order=desc">Top Scenes</a>
  <span class="sep">|</span>
  <a href="/torrents" style="color:#7af">Torrents</a>
</div>
<div class="count-bar">{{ total }} scene{{ "s" if total != 1 else "" }}{% if total > 0 %} &mdash; page {{ page }} of {{ total_pages }}{% endif %}</div>

{% if scenes %}
<div class="grid">
  {% for s in scenes %}
  <div class="card" onclick="openModal({{ loop.index0 }})">
    <div class="card-thumb">
      {% set bg = s.background_url or s.poster_url %}
      {% if bg %}<img src="{{ bg }}" alt="" loading="lazy">{% else %}<div class="no-img">No image</div>{% endif %}
      {% if s.duration_human %}<div class="card-dur">{{ s.duration_human }}</div>{% endif %}
    </div>
    <div class="card-body">
      {% if s.site_logo_url %}<a href="/scenes?site={{ s.site_name | urlencode }}" onclick="event.stopPropagation()"><img class="card-site-logo" src="{{ s.site_logo_url }}" alt="{{ s.site_name }}"></a>{% endif %}
      <div class="card-title">{{ s.title or "(untitled)" }}</div>
      {% if s.performers %}<div class="card-performers">{{ s.performers | map(attribute="name") | list | join(", ") }}</div>{% endif %}
      {% if s.description %}<div class="card-desc">{{ s.description }}</div>{% endif %}
      <div class="card-meta">
        <span>{{ s.date or "" }}</span>
        {% if s.max_seeders %}<span class="card-seeds">{{ s.max_seeders }} seeds</span>{% endif %}
      </div>
      {% if s.torrents %}
      <button class="card-dl-btn" onclick="event.stopPropagation();openModal({{ loop.index0 }})">&#8659; Download ({{ s.torrents | length }})</button>
      {% endif %}
    </div>
  </div>
  {% endfor %}
</div>
{% else %}
<p style="padding:20px;color:#717171">No scenes found.</p>
{% endif %}

<div class="pagination">
  {% if has_prev %}<a href="{{ prev_url }}">&larr; Prev</a>{% else %}<span class="disabled">&larr; Prev</span>{% endif %}
  <span style="color:#717171">Page {{ page }} / {{ total_pages }}</span>
  {% if has_next %}<a href="{{ next_url }}">Next &rarr;</a>{% else %}<span class="disabled">Next &rarr;</span>{% endif %}
</div>

<div class="modal-overlay" id="modal-overlay" onclick="closeModalOutside(event)">
  <div class="modal" id="modal">
    <button class="modal-close" onclick="closeModal()">&#x2715;</button>
    <div class="modal-hero" id="modal-hero"><img id="modal-img" src="" alt=""></div>
    <div class="modal-body">
      <div class="modal-title" id="modal-title"></div>
      <div class="modal-site-row">
        <img class="modal-site-logo" id="modal-logo" src="" alt="">
        <a class="modal-site-name" id="modal-site-name" href="#"></a>
        <span class="modal-network" id="modal-network"></span>
        <span class="modal-dur" id="modal-dur"></span>
        <span class="modal-date" id="modal-date"></span>
      </div>
      <div class="modal-performers" id="modal-performers"></div>
      <div class="modal-tags" id="modal-tags"></div>
      <div class="modal-desc" id="modal-desc"></div>
      <div class="modal-dl-heading" id="modal-dl-heading">Downloads</div>
      <div class="modal-torrents" id="modal-torrents"></div>
    </div>
  </div>
</div>

<script id="scenes-data" type="application/json">{{ scenes_json | safe }}</script>
<script>
const scenes = JSON.parse(document.getElementById('scenes-data').textContent);

function fmt(b) {
  if (b == null) return '\u2014';
  const u = ['B','KB','MB','GB','TB'];
  let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return b.toFixed(1) + '\u00a0' + u[i];
}

function esc(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function openModal(idx) {
  const s = scenes[idx];
  const overlay = document.getElementById('modal-overlay');

  const bg = s.background_url || s.poster_url || '';
  const hero = document.getElementById('modal-hero');
  const img = document.getElementById('modal-img');
  if (bg) { img.src = bg; hero.style.display = ''; }
  else { hero.style.display = 'none'; }

  document.getElementById('modal-title').textContent = s.title || '(untitled)';

  const logo = document.getElementById('modal-logo');
  if (s.site_logo_url) { logo.src = s.site_logo_url; logo.style.display = ''; }
  else { logo.style.display = 'none'; }

  const siteLink = document.getElementById('modal-site-name');
  siteLink.textContent = s.site_name || s.network_name || '';
  siteLink.href = '/scenes?site=' + encodeURIComponent(s.site_name || '');

  document.getElementById('modal-network').textContent =
    (s.network_name && s.network_name !== s.site_name) ? '/ ' + s.network_name : '';
  document.getElementById('modal-dur').textContent = s.duration_human || '';
  document.getElementById('modal-date').textContent = s.date || '';

  const perf = document.getElementById('modal-performers');
  perf.innerHTML = '';
  if (s.performers && s.performers.length) {
    s.performers.forEach(p => {
      const chip = document.createElement('div');
      chip.className = 'modal-performer';
      const img = document.createElement('img');
      if (p.image_url) { img.src = p.image_url; }
      else { img.style.background = '#333'; }
      img.alt = p.name || '';
      img.onerror = function() { this.style.display = 'none'; };
      const name = document.createElement('span');
      name.textContent = p.name || '';
      chip.appendChild(img);
      chip.appendChild(name);
      perf.appendChild(chip);
    });
    perf.style.display = '';
  } else { perf.style.display = 'none'; }

  const tagsEl = document.getElementById('modal-tags');
  tagsEl.innerHTML = '';
  (s.tags || []).forEach(t => {
    const sp = document.createElement('span');
    sp.textContent = t;
    tagsEl.appendChild(sp);
  });

  document.getElementById('modal-desc').textContent = s.description || '';

  const dlH = document.getElementById('modal-dl-heading');
  const dlEl = document.getElementById('modal-torrents');
  if (s.torrents && s.torrents.length) {
    dlH.style.display = '';
    let html = '<table><thead><tr><th>Torrent</th><th>Cat</th><th>Res</th><th>Size</th><th>Seeds</th><th>Leech</th><th>Age</th></tr></thead><tbody>';
    s.torrents.forEach(t => {
      const sc = t.seeders > 0 ? ' class="seeds-good"' : '';
      html += `<tr>
        <td><a href="${esc(t.magnet)}" title="${esc(t.title)}">${esc(t.title)}</a></td>
        <td>${esc(t.category||'')}</td>
        <td style="color:#717171">${esc(t.resolution||'')}</td>
        <td>${esc(fmt(t.size_bytes))}</td>
        <td${sc}>${t.seeders != null ? t.seeders : ''}</td>
        <td style="color:#717171">${t.leechers != null ? t.leechers : ''}</td>
        <td style="color:#717171">${esc(t.age_label||'')}</td>
      </tr>`;
    });
    html += '</tbody></table>';
    dlEl.innerHTML = html;
  } else {
    dlH.style.display = 'none';
    dlEl.innerHTML = '';
  }

  overlay.classList.add('open');
  document.body.style.overflow = 'hidden';
}

function closeModal() {
  document.getElementById('modal-overlay').classList.remove('open');
  document.body.style.overflow = '';
}

function closeModalOutside(e) {
  if (e.target === document.getElementById('modal-overlay')) closeModal();
}

document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });
</script>
</body>
</html>"""


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
<h1><a href="/torrents">Torrent Search</a></h1>
<div class="quickfilters">
  <a href="/torrents?sort_by=date_added&sort_order=desc">Latest</a>
  <a href="/torrents?sort_by=seeders&sort_order=desc">Top All</a>
  <span class="sep">|</span>
  <a href="/scenes" style="color:#a8d;">&#9654; Scene Browser</a>
  <span class="sep">|</span>
  {% for cat in categories %}
  <a href="/torrents?sort_by=seeders&sort_order=desc&category={{ cat | urlencode }}">Top {{ cat }}</a>
  {% endfor %}
</div>
<form method="GET" action="/torrents">
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
        {% set poster = t.background_url or t.poster_url or t.image_url %}
        {% if poster %}<img src="{{ poster }}" alt="">{% endif %}
      </td>
      <td>
        <a href="{{ t.magnet }}" title="{{ t.title }}">{{ t.title }}</a>
        {% set display_title = t.scene_title or t.meta_title %}
        {% if display_title %}
        <div class="scene-title">{{ display_title }}</div>
        {% endif %}
        {% set display_site = t.site_name or t.network_name or t.sitename %}
        {% if display_site %}
        <div class="scene-site">
          {% set logo = t.site_logo_url or t.network_logo_url %}
          {% if logo %}<img src="{{ logo }}" alt="">{% endif %}
          {{ display_site }}
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
        <a href="/torrents?site={{ t.sitename | urlencode }}" class="site-link">{{ t.sitename }}</a>
        {% endif %}
      </td>
      <td class="muted">{{ t.release_date or "" }}</td>
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


VALID_PER_PAGE = {30, 60, 90}


@app.get("/", response_class=HTMLResponse)
def index():
    return RedirectResponse(url="/scenes", status_code=302)


@app.get("/scenes", response_class=HTMLResponse)
def scenes_ui(
    q: str = Query(default=""),
    site: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    sort_by: str = Query(default="date"),
    sort_order: str = Query(default="desc"),
    per_page: int = Query(default=30),
    page: int = Query(default=1, ge=1),
):
    limit = per_page if per_page in VALID_PER_PAGE else 30

    data = _api_get("/api/v1/scenes", {
        "q": q, "site": site, "date_from": date_from, "date_to": date_to,
        "sort_by": sort_by, "sort_order": sort_order,
        "per_page": limit, "page": page,
    })

    scenes = data["items"]
    total = data["total"]
    total_pages = data["total_pages"]

    for s in scenes:
        s["duration_human"] = format_duration(s.get("duration_seconds"))
        for t in s.get("torrents") or []:
            t["size_human"] = format_size(t.get("size_bytes"))
            t["age_label"] = format_date(t.get("date_added"))
        seeders = [t["seeders"] for t in s.get("torrents") or [] if t.get("seeders") is not None]
        s["max_seeders"] = max(seeders) if seeders else None

    base_args = {
        "q": q, "site": site, "date_from": date_from, "date_to": date_to,
        "sort_by": sort_by, "sort_order": sort_order, "per_page": limit,
    }

    scenes_json = json.dumps(scenes).replace("</", "<\\/")

    env = Environment(autoescape=True)
    env.filters["urlencode"] = quote_plus
    tmpl = env.from_string(SCENES_TEMPLATE)
    html = tmpl.render(
        scenes=scenes,
        scenes_json=scenes_json,
        q=q,
        site=site,
        date_from=date_from,
        date_to=date_to,
        sort_by=sort_by,
        sort_order=sort_order,
        per_page=limit,
        page=page,
        total=total,
        total_pages=total_pages,
        has_prev=page > 1,
        has_next=page < total_pages,
        prev_url=page_url("/scenes", base_args, page - 1),
        next_url=page_url("/scenes", base_args, page + 1),
    )
    return HTMLResponse(content=html)


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
    limit = per_page if per_page in VALID_PER_PAGE else 30

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

    env = Environment(autoescape=True)
    env.filters["urlencode"] = quote_plus
    tmpl = env.from_string(HTML_TEMPLATE)
    html = tmpl.render(
        torrents=torrents,
        categories=cats["categories"],
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
        prev_url=page_url("/torrents", base_args, page - 1),
        next_url=page_url("/torrents", base_args, page + 1),
    )
    return HTMLResponse(content=html)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("WEB_PORT", 5000))
    uvicorn.run("web_ui:app", host="0.0.0.0", port=port, reload=False)
