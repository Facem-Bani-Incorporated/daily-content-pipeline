"""
Wikipedia "On This Day" (OTD) helper.

The REST feed returns Wikipedia's *curated, verified* list of what actually happened on
a given calendar day:
    https://en.wikipedia.org/api/rest_v1/feed/onthisday/{type}/{mm}/{dd}
types: events | births | deaths | selected

We use it two ways:
  * `fetch_otd` — raw entries, for the date validator's ground-truth slug set.
  * `format_reference` — a compact text block seeded into the discovery prompt so the
    model grounds its output in real events instead of hallucinating dates.

Every fetch retries transient failures and logs the exception's repr (httpx timeout/
connect errors often stringify to an empty message, which is why the old log line was
blank).
"""

import asyncio
import httpx

from core.logger import setup_logger

logger = setup_logger("OnThisDay")

WIKI_USER_AGENT = "DailyHistoryApp/2.0 (https://dailyhistory.app; contact@dailyhistory.app)"
_ENDPOINTS = ("events", "births", "deaths", "selected")
_TYPE_LABEL = {"events": "EVENT", "births": "BORN", "deaths": "DIED", "selected": "EVENT"}

# module-level cache: (month, day) -> {ep: [ {year, text, slug} ]}
_cache: dict = {}


async def _get_json(client: httpx.AsyncClient, url: str, ep: str, retries: int = 2):
    for attempt in range(retries + 1):
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"⚠️ OTD '{ep}' HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ OTD '{ep}' fetch failed (attempt {attempt + 1}): {e!r}")
        if attempt < retries:
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


async def fetch_otd(target_date) -> dict:
    """Return {endpoint: [ {year, text, slug} ]} for the given date. Cached per (mm, dd)."""
    key = (target_date.month, target_date.day)
    if key in _cache:
        return _cache[key]

    mm, dd = f"{target_date.month:02d}", f"{target_date.day:02d}"
    out: dict = {ep: [] for ep in _ENDPOINTS}

    async with httpx.AsyncClient(headers={"User-Agent": WIKI_USER_AGENT}, timeout=15.0) as client:
        jsons = await asyncio.gather(*[
            _get_json(client, f"https://en.wikipedia.org/api/rest_v1/feed/onthisday/{ep}/{mm}/{dd}", ep)
            for ep in _ENDPOINTS
        ])

    for ep, data in zip(_ENDPOINTS, jsons):
        if not data:
            continue
        for entry in data.get(ep, []):
            pages = entry.get("pages", [])
            slug = ""
            if pages:
                p0 = pages[0]
                slug = (p0.get("titles", {}).get("canonical") or p0.get("title", "") or "")
            out[ep].append({
                "year": entry.get("year"),
                "text": entry.get("text", ""),
                "slug": slug.replace(" ", "_"),
            })

    total = sum(len(v) for v in out.values())
    logger.info(f"📅 OTD {mm}/{dd}: {total} entries "
                f"({', '.join(f'{ep}={len(out[ep])}' for ep in _ENDPOINTS)})")
    _cache[key] = out
    return out


def format_reference(otd: dict, limit: int = 120, types: tuple = _ENDPOINTS) -> str:
    """Compact, deduped reference block: `- YEAR [TYPE] Title — text`.

    Sorted by year so the model sees the full historical spread. Empty string when the
    feed returned nothing (discovery then falls back to pure recall)."""
    rows = []
    seen = set()
    for ep in types:
        for e in otd.get(ep, []):
            slug = e.get("slug") or ""
            year = e.get("year")
            if not slug or year is None or slug in seen:
                continue
            seen.add(slug)
            title = slug.replace("_", " ")
            text = (e.get("text") or "").strip()
            rows.append((year, f"- {year} [{_TYPE_LABEL.get(ep, 'EVENT')}] {title} — {text}"))

    if not rows:
        return ""
    rows.sort(key=lambda r: (r[0] is None, r[0]))
    return "\n".join(line for _, line in rows[:limit])


async def fetch_otd_reference(target_date, limit: int = 120, types: tuple = _ENDPOINTS) -> str:
    """Convenience: fetch + format in one call. Returns '' on total failure."""
    try:
        otd = await fetch_otd(target_date)
        return format_reference(otd, limit=limit, types=types)
    except Exception as e:
        logger.warning(f"⚠️ OTD reference unavailable: {e!r}")
        return ""
