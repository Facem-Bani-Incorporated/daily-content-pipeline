import asyncio
import httpx
from urllib.parse import quote
from datetime import datetime
from difflib import SequenceMatcher
from typing import Set
from core.logger import setup_logger

logger = setup_logger("WikiDateValidator")


class WikiDateValidator:
    """
    Two-tier date verification using Wikipedia.

    TIER 1 — "On this day" official feed (curated, fast, but incomplete):
      https://en.wikipedia.org/api/rest_v1/feed/onthisday/{type}/{mm}/{dd}
      → If slug appears here, it's INSTANTLY confirmed.

    TIER 2 — Article text scan (catches valid events not in OTD):
      For each candidate not in OTD, fetch its Wikipedia article extract
      and check if the target date phrase appears in the article body.

    TIER 3 — REJECT.
    """

    WIKI_USER_AGENT = (
        "DailyHistoryApp/2.0 (https://dailyhistory.app; contact@dailyhistory.app)"
    )

    def __init__(
        self,
        fuzzy_slug_threshold: float = 0.90,
        strict_threshold: float = None,  # backward compat
    ):
        # Accept both names; strict_threshold takes priority if provided
        self.fuzzy_slug_threshold = (
            strict_threshold if strict_threshold is not None else fuzzy_slug_threshold
        )
        self._otd_cache: dict = {}
        self._article_cache: dict = {}

    async def _fetch_otd(self, target_date: datetime) -> dict:
        cache_key = (target_date.month, target_date.day)
        if cache_key in self._otd_cache:
            return self._otd_cache[cache_key]

        mm = f"{target_date.month:02d}"
        dd = f"{target_date.day:02d}"

        endpoints = ["events", "births", "deaths", "selected"]
        all_entries = []
        headers = {"User-Agent": self.WIKI_USER_AGENT}

        async with httpx.AsyncClient(headers=headers, timeout=15.0) as client:
            tasks = [
                client.get(
                    f"https://en.wikipedia.org/api/rest_v1/feed/onthisday/{ep}/{mm}/{dd}"
                )
                for ep in endpoints
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for ep, resp in zip(endpoints, responses):
                if isinstance(resp, Exception):
                    logger.warning(f"⚠️ OTD '{ep}' fetch failed: {resp}")
                    continue
                if resp.status_code != 200:
                    continue
                try:
                    data = resp.json()
                    entries = data.get(ep, [])
                    for entry in entries:
                        year = entry.get("year")
                        text = entry.get("text", "")
                        pages = entry.get("pages", [])
                        slugs = []
                        for p in pages:
                            t = p.get("titles", {}).get("canonical") or p.get("title", "")
                            if t:
                                slugs.append(t.replace(" ", "_"))
                        all_entries.append({
                            "year": year, "text": text, "slugs": slugs,
                        })
                except Exception as e:
                    logger.warning(f"⚠️ OTD '{ep}' parse error: {e}")

        slug_set: Set[str] = set()
        for entry in all_entries:
            for slug in entry["slugs"]:
                slug_set.add(slug.lower())

        result = {"entries": all_entries, "slug_set": slug_set}
        logger.info(
            f"📅 Wikipedia OTD for {mm}/{dd}: "
            f"{len(all_entries)} entries, {len(slug_set)} slugs"
        )
        self._otd_cache[cache_key] = result
        return result

    async def _fetch_article_extract(self, slug: str) -> str:
        if slug in self._article_cache:
            return self._article_cache[slug]

        encoded_slug = quote(slug, safe="")
        url = (
            f"https://en.wikipedia.org/w/api.php"
            f"?action=query&titles={encoded_slug}"
            f"&prop=extracts&explaintext=true&exsectionformat=plain"
            f"&format=json"
        )
        headers = {"User-Agent": self.WIKI_USER_AGENT}

        try:
            async with httpx.AsyncClient(headers=headers, timeout=12.0) as client:
                res = await client.get(url)
                if res.status_code != 200:
                    self._article_cache[slug] = ""
                    return ""
                pages = res.json().get("query", {}).get("pages", {})
                for page_id, page in pages.items():
                    if page_id == "-1" or "missing" in page:
                        self._article_cache[slug] = ""
                        return ""
                    extract = page.get("extract", "") or ""
                    self._article_cache[slug] = extract
                    return extract
        except Exception as e:
            logger.warning(f"⚠️ Article fetch failed for {slug}: {e}")
            self._article_cache[slug] = ""
            return ""

        self._article_cache[slug] = ""
        return ""

    def _date_in_article(
        self, article_text: str, target_date: datetime, year: int
    ) -> tuple:
        if not article_text:
            return False, "no article text"

        text_lower = article_text.lower()
        month_name = target_date.strftime("%B").lower()
        day = target_date.day
        day_str = str(day)
        day_padded = f"{day:02d}"
        year_str = str(year)

        # Strong patterns: month + day + year together
        strong_patterns = [
            f"{month_name} {day_str}, {year_str}",
            f"{month_name} {day_str} {year_str}",
            f"{day_str} {month_name} {year_str}",
            f"{day_padded} {month_name} {year_str}",
            f"{month_name} {day_padded}, {year_str}",
            f"{month_name} {day_padded} {year_str}",
        ]
        for pat in strong_patterns:
            if pat in text_lower:
                return True, f"strong match: '{pat}'"

        # Medium: "Month day" appears AND year appears within 80 chars
        # Tighter window reduces false positives where the date appears
        # in the article for an unrelated event or section.
        date_phrases = [
            f"{month_name} {day_str}",
            f"{day_str} {month_name}",
            f"{month_name} {day_padded}",
        ]
        for date_phrase in date_phrases:
            idx = text_lower.find(date_phrase)
            while idx != -1:
                window_start = max(0, idx - 80)
                window_end = min(len(text_lower), idx + 80)
                window = text_lower[window_start:window_end]
                if year_str in window:
                    return True, f"proximity: '{date_phrase}' near {year_str}"
                idx = text_lower.find(date_phrase, idx + 1)

        return False, f"no date match for {month_name} {day} + {year_str}"

    async def validate_events(
        self, candidates: list, target_date: datetime, tier: str = "FREE"
    ) -> list:
        if not candidates:
            return []

        otd = await self._fetch_otd(target_date)
        slug_set = otd["slug_set"]

        otd_passed = []
        needs_article_check = []

        for cand in candidates:
            slug = (cand.get("slug") or "").replace(" ", "_")
            slug_lower = slug.lower()

            if not slug:
                continue

            if slug_lower in slug_set:
                otd_passed.append(cand)
                continue

            best_score = 0.0
            for off_slug in slug_set:
                score = SequenceMatcher(None, slug_lower, off_slug).ratio()
                if score > best_score:
                    best_score = score
                if score >= self.fuzzy_slug_threshold:
                    break

            if best_score >= self.fuzzy_slug_threshold:
                otd_passed.append(cand)
                continue

            needs_article_check.append(cand)

        logger.info(
            f"🔍 [{tier}] OTD instant pass: {len(otd_passed)}, "
            f"article check: {len(needs_article_check)}"
        )

        article_passed = []
        rejected_count = 0

        async def check_article(cand):
            slug = (cand.get("slug") or "").replace(" ", "_")
            year = cand.get("year")
            if not slug or year is None:
                return cand, False, "missing slug/year"

            try:
                year_int = int(year)
            except (ValueError, TypeError):
                return cand, False, f"invalid year"

            extract = await self._fetch_article_extract(slug)
            if not extract:
                return cand, False, "article not found"

            found, reason = self._date_in_article(extract, target_date, year_int)
            return cand, found, reason

        sem = asyncio.Semaphore(5)

        async def bounded(cand):
            async with sem:
                return await check_article(cand)

        results = await asyncio.gather(*[bounded(c) for c in needs_article_check])

        for cand, ok, reason in results:
            if ok:
                article_passed.append(cand)
                logger.info(
                    f"✓ [{tier}] Article confirmed: {cand.get('year')} "
                    f"{cand.get('slug')} — {reason}"
                )
            else:
                rejected_count += 1
                logger.warning(
                    f"🚫 [{tier}] Date REJECTED: {cand.get('year')} "
                    f"{cand.get('slug')} — {reason}"
                )

        verified = otd_passed + article_passed

        logger.info(
            f"✅ [{tier}] Date validation: {len(verified)} verified "
            f"({len(otd_passed)} OTD + {len(article_passed)} article scan), "
            f"{rejected_count} rejected (out of {len(candidates)})"
        )
        return verified