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

    STRICT MODE — a candidate passes ONLY if:
      1. Its slug appears in the official slug set for that day, OR
      2. Slug fuzzy-matches an official slug at >= strict_threshold (0.90)

    We deliberately DROPPED loose text-based fuzzy matching because it
    allowed events like "Chernobyl disaster" to pass on April 27 (the
    actual event was April 26 — Wikipedia mentions it on the 26th OTD,
    not the 27th).
    """

    WIKI_USER_AGENT = (
        "DailyHistoryApp/2.0 (https://dailyhistory.app; contact@dailyhistory.app)"
    )

    def __init__(self, strict_threshold: float = 0.90):
        # Threshold raised from 0.80 → 0.90: only minor naming variations
        # (e.g. underscore vs space) should pass the fuzzy check.
        self.strict_threshold = strict_threshold
        self._cache: dict = {}

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

    async def validate_events(
        self, candidates: list, target_date: datetime, tier: str = "FREE"
    ) -> list:
        if not candidates:
            return []

        official = await self._fetch_official_events(target_date)
        slug_set = official["slug_set"]
        year_slug_pairs = official["year_slug_pairs"]

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

            # CHECK 1: Exact (year, slug) match — strongest, instant pass
            if (year_int, slug_lower) in year_slug_pairs:
                verified.append(cand)
                continue

            # CHECK 2: Slug appears in official set for this day (any year)
            if slug_lower in slug_set:
                verified.append(cand)
                continue

            # CHECK 3: VERY strict fuzzy slug match (>= 90%)
            # Only allows things like "Battle_of_X" matching "Battle_of_X_(year)"
            best_slug_score = 0.0
            best_match = ""
            for off_slug in slug_set:
                score = SequenceMatcher(None, slug_lower, off_slug).ratio()
                if score > best_slug_score:
                    best_slug_score = score
                    best_match = off_slug
                if score >= self.strict_threshold:
                    break

            if best_slug_score >= self.strict_threshold:
                logger.info(
                    f"✓ [{tier}] Fuzzy slug pass: {slug} ≈ {best_match} ({best_slug_score:.0%})"
                )
                verified.append(cand)
                continue

            # If neither slug match works, REJECT.
            # We deliberately do NOT do text-based fuzzy matching anymore —
            # that's what let "Chernobyl disaster on April 27" slip through.
            rejected_count += 1
            logger.warning(
                f"🚫 [{tier}] Date REJECTED: {year} {slug} — "
                f"not in official 'On this day' (best slug match {best_slug_score:.0%})"
            )

        logger.info(
            f"✅ [{tier}] Date validation: {len(verified)} verified, "
            f"{rejected_count} rejected (out of {len(candidates)})"
        )
        return verified