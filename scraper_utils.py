import os
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import quote

from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.environ.get("BASE_URL", "https://xxxclub.to")

# Date format used by the site: "16 Apr 2026 14:33:45"
_DATE_FMT = "%d %b %Y %H:%M:%S"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

_SIZE_RE = re.compile(
    r"([\d,.]+)\s*(B|KB|MB|GB|TB)", re.IGNORECASE
)
_UNITS = {"b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3, "tb": 1024**4}


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def extract_info_hash(id_attr: str) -> str | None:
    """Extract info hash from an element id like '#i83b03d140f54...'"""
    if not id_attr:
        return None
    # Strip leading '#i' or 'i'
    clean = id_attr.lstrip("#").lstrip("i")
    if re.fullmatch(r"[0-9a-fA-F]{40}", clean):
        return clean.lower()
    return None


def build_magnet(info_hash: str, title: str = "") -> str:
    magnet = f"magnet:?xt=urn:btih:{info_hash}"
    if title:
        magnet += "&dn=" + quote(title, safe="")
    return magnet


def parse_size(size_str: str) -> int | None:
    """Convert a human-readable size string to bytes."""
    if not size_str:
        return None
    m = _SIZE_RE.search(size_str.replace(",", ""))
    if not m:
        return None
    value = float(m.group(1))
    unit = m.group(2).lower()
    return int(value * _UNITS.get(unit, 1))


def _to_int(s: str) -> int | None:
    try:
        return int(s.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def parse_page(html: str) -> tuple[list[dict], str | None]:
    """Parse a browse page. Returns (rows, next_url).

    Each row is a dict ready for db.upsert_torrents(). next_url is None on the
    last page.
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for torrent_a in soup.find_all("a", id=lambda v: v and v.startswith("#i")):
        info_hash = extract_info_hash(torrent_a["id"])
        if not info_hash:
            continue

        title = torrent_a.get_text(strip=True)
        magnet = build_magnet(info_hash, title)

        li = torrent_a.find_parent("li")
        if li is None:
            continue

        # Category: <span class="catlabe"> → <lable class="catla"> text
        cat_span = li.find("span", class_="catlabe")
        category = (
            cat_span.find("lable").get_text(strip=True)
            if cat_span and cat_span.find("lable")
            else None
        )

        # Date added: <span class="... addedtable">
        date_span = li.find("span", class_="addedtable")
        date_added = None
        if date_span:
            try:
                date_added = datetime.strptime(
                    date_span.get_text(strip=True), _DATE_FMT
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        # Size: <span class="siz ...">
        size_span = li.find("span", class_="siz")
        size_bytes = parse_size(size_span.get_text(strip=True)) if size_span else None

        # Seeders / leechers
        see_span = li.find("span", class_="see")
        lee_span = li.find("span", class_="lee")
        seeders = _to_int(see_span.get_text(strip=True)) if see_span else None
        leechers = _to_int(lee_span.get_text(strip=True)) if lee_span else None

        # Uploader: <span class="uploadertable ...">
        uploader_span = li.find("span", class_="uploadertable")
        uploader = uploader_span.get_text(strip=True) if uploader_span else None

        # Image: thumbnail src with /ps/ → /p/ for full-res
        # The img id is "i{info_hash}" (with leading "i"), not the hash directly
        image_url = None
        img = li.find("img", class_="floaterimg", id=f"i{info_hash}")
        if img and img.get("src"):
            image_url = img["src"].replace("/ps/", "/p/", 1)

        rows.append({
            "info_hash": info_hash,
            "title": title,
            "magnet": magnet,
            "size_bytes": size_bytes,
            "category": category,
            "date_added": date_added,
            "uploader": uploader,
            "seeders": seeders,
            "leechers": leechers,
            "source": None,  # caller sets this (e.g. "browse", "watcher")
            "image_url": image_url,
            "scraped_at": datetime.now(timezone.utc),
        })

    # Pagination: the real "Next Page" link has data-no-instant; decoys do not
    next_url = None
    next_a = soup.find("a", title="Next Page", attrs={"data-no-instant": True})
    if next_a and next_a.get("href"):
        next_url = BASE_URL + next_a["href"]

    return rows, next_url


def parse_top100_page(html: str) -> list[dict]:
    """Parse a top100 page. Returns rows with title/seeders/leechers only —
    top100 pages use numeric IDs so no info hash is available. The caller
    matches rows to existing DB records by title."""
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for li in soup.select("div.divtableinside li"):
        # Skip the header row (contains <span> with plain text like "Name")
        torrent_a = li.find("a", id=lambda v: v and v.startswith("#i"))
        if not torrent_a:
            continue

        title = torrent_a.get_text(strip=True)

        see_span = li.find("span", class_="see")
        lee_span = li.find("span", class_="lee")
        seeders = _to_int(see_span.get_text(strip=True)) if see_span else None
        leechers = _to_int(lee_span.get_text(strip=True)) if lee_span else None

        if title and seeders is not None:
            rows.append({
                "title": title,
                "seeders": seeders,
                "leechers": leechers,
                "scraped_at": datetime.now(timezone.utc),
            })

    return rows
