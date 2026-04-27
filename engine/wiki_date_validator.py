import asyncio
import httpx
from datetime import datetime
from difflib import SequenceMatcher
from typing import Set
from core.logger import setup_logger

logger = setup_logger("WikiDateValidator")


class WikiDateValidator:
    """
    Ground-truth date verification using Wikipedia's official
    "On this day" REST API:

      https://en.wikipedia.org/api/rest_v1/feed/onthisday/{type}/{mm}/{dd}

    This endpoint returns the curated list of events Wikipedia editors
    have confirmed happened on that calendar day — the most reliable
    source for date verification.

    A candidate passes if ANY of these is true:
      1. (year, slug) exact match against official entries
      2. Slug appears in any official entry for that day
      3. Slug fuzzy-matches an official slug (>= threshold)
      4. Year matches AND text fuzzy-matches an official entry's text
    """

    WIKI_USER_AGENT = (
        "DailyHistoryApp/2.0 (https://dailyhistory.app; contact@dailyhistory.app)"
    )

    def __init__(self, similarity_threshold: float = 0.80):
        self.threshold = similarity_threshold
        self._cache: dict = {}  # (month, day) -> validation set

    async def _fetch_official_events(self, target_date: datetime) -> dict:
        cache_key = (target_date.month, target_date.day)
        if cache_key in self._cache:
            return self._cache[cache_key]

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
                    logger.warning(f"⚠️ Wiki '{ep}' fetch failed: {resp}")
                    continue
                if resp.status_code != 200:
                    logger.warning(f"⚠️ Wiki '{ep}' HTTP {resp.status_code}")
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
                            "year": year,
                            "text": text,
                            "slugs": slugs,
                        })
                except Exception as e:
                    logger.warning(f"⚠️ Wiki '{ep}' parse error: {e}")

        slug_set: Set[str] = set()
        year_slug_pairs: Set[tuple] = set()
        for entry in all_entries:
            year = entry["year"]
            for slug in entry["slugs"]:
                slug_set.add(slug.lower())
                if year is not None:
                    try:
                        year_slug_pairs.add((int(year), slug.lower()))
                    except (ValueError, TypeError):
                        pass

        result = {
            "entries": all_entries,
            "slug_set": slug_set,
            "year_slug_pairs": year_slug_pairs,
        }

        logger.info(
            f"📅 Wikipedia 'On this day' for {mm}/{dd}: "
            f"{len(all_entries)} entries, {len(slug_set)} unique slugs"
        )

        self._cache[cache_key] = result
        return result

    def _fuzzy(self, a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, a.lower().strip()[:200], b.lower().strip()[:200]).ratio()

    async def validate_events(
        self, candidates: list, target_date: datetime, tier: str = "FREE"
    ) -> list:
        if not candidates:
            return []

        official = await self._fetch_official_events(target_date)
        slug_set = official["slug_set"]
        year_slug_pairs = official["year_slug_pairs"]
        entries = official["entries"]

        if not slug_set:
            logger.warning(
                f"⚠️ [{tier}] Wikipedia API returned no data — skipping date validation"
            )
            return candidates

        verified = []
        rejected_count = 0

        for cand in candidates:
            slug = (cand.get("slug") or "").replace(" ", "_")
            year = cand.get("year")
            text = cand.get("text") or ""
            slug_lower = slug.lower()

            if not slug or year is None:
                rejected_count += 1
                logger.warning(
                    f"🚫 [{tier}] Date REJECTED: missing slug/year — {cand.get('slug', '?')}"
                )
                continue

            try:
                year_int = int(year)
            except (ValueError, TypeError):
                rejected_count += 1
                logger.warning(f"🚫 [{tier}] Date REJECTED: invalid year {year}")
                continue

            # 1: Exact (year, slug) match
            if (year_int, slug_lower) in year_slug_pairs:
                verified.append(cand)
                continue

            # 2: Slug in official set (year may differ if AI got year wrong)
            if slug_lower in slug_set:
                verified.append(cand)
                continue

            # 3: Fuzzy slug match
            best_slug_score = 0.0
            for off_slug in slug_set:
                score = SequenceMatcher(None, slug_lower, off_slug).ratio()
                if score > best_slug_score:
                    best_slug_score = score
                if score >= self.threshold:
                    break

            if best_slug_score >= self.threshold:
                verified.append(cand)
                continue

            # 4: Fuzzy text match against entries with matching year
            best_text_score = 0.0
            for entry in entries:
                if entry["year"] is None:
                    continue
                try:
                    if int(entry["year"]) != year_int:
                        continue
                except (ValueError, TypeError):
                    continue
                score = self._fuzzy(text, entry["text"])
                if score > best_text_score:
                    best_text_score = score
                if score >= self.threshold:
                    break

            if best_text_score >= self.threshold:
                verified.append(cand)
                continue

            rejected_count += 1
            logger.warning(
                f"🚫 [{tier}] Date REJECTED: {year} {slug} — "
                f"not in official 'On this day' "
                f"(best slug {best_slug_score:.0%}, best text {best_text_score:.0%})"
            )

        logger.info(
            f"✅ [{tier}] Date validation: {len(verified)} verified, "
            f"{rejected_count} rejected (out of {len(candidates)})"
        )
        return verified