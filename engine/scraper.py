import httpx
from datetime import datetime
from typing import List, Dict

from core.config import config
from core.logger import setup_logger
from engine.base_scraper import WikiScraper

logger = setup_logger("SmartScraper")


class SmartWikiScraper(WikiScraper):
    def __init__(self):
        super().__init__()

        self.strong_keywords = [
            "war", "battle", "revolution", "independence",
            "explosion", "assassinated", "killed",
            "founded", "discovered", "invented",
            "first", "launch", "moon", "space",
            "record", "breakthrough", "treaty",
            "attack", "bomb", "crash", "disaster"
        ]

        self.weak_keywords = [
            "appointed", "elected", "becomes", "publishes",
            "opens", "established", "born", "dies"
        ]

    async def fetch_today_events(self) -> List[Dict]:
        now = datetime.now()
        url = f"{config.WIKI_BASE_URL}/feed/onthisday/events/{now.month}/{now.day}"

        async with httpx.AsyncClient(headers=self.headers, timeout=20.0) as client:
            try:
                res = await client.get(url)
                res.raise_for_status()
                data = res.json()

                events = data.get("events", []) + data.get("selected", [])
                logger.info(f"🌐 Raw events fetched: {len(events)}")

                return events

            except Exception as e:
                logger.error(f"❌ Wiki fetch failed: {e}")
                return []

    def normalize_events(self, raw_events: List[Dict]) -> List[Dict]:
        clean = []

        for ev in raw_events:
            pages = ev.get("pages", [])
            if not pages:
                continue

            slug = pages[0].get("titles", {}).get("canonical")
            thumb = pages[0].get("thumbnail", {}).get("source")

            if not slug or not ev.get("text"):
                continue

            clean.append({
                "year": ev.get("year"),
                "text": ev.get("text"),
                "slug": slug,
                "wiki_thumb": thumb
            })

        logger.info(f"🧹 Normalized events: {len(clean)}")
        return clean

    def score_event(self, text: str, year: int) -> int:
        text_lower = text.lower()
        score = 0

        for k in self.strong_keywords:
            if k in text_lower:
                score += 30

        for k in self.weak_keywords:
            if k in text_lower:
                score -= 10

        if year:
            if year > 1900:
                score += 20
            elif year > 1800:
                score += 10

        if len(text) > 120:
            score += 10

        return score

    def filter_interesting(self, events: List[Dict]) -> List[Dict]:
        scored = []

        for ev in events:
            score = self.score_event(ev["text"], ev.get("year", 0))
            ev["interest_score"] = score

            if score >= 20:
                scored.append(ev)

        logger.info(f"🔥 Interesting events: {len(scored)}")
        return scored

    def rank_events(self, events: List[Dict], limit: int = 25) -> List[Dict]:
        events.sort(key=lambda x: x.get("interest_score", 0), reverse=True)
        return events[:limit]

    async def get_today_interesting_events(self, limit: int = 25) -> List[Dict]:
        raw = await self.fetch_today_events()
        normalized = self.normalize_events(raw)
        filtered = self.filter_interesting(normalized)
        ranked = self.rank_events(filtered, limit)

        logger.info(f"🏆 Final selected events: {len(ranked)}")
        return ranked