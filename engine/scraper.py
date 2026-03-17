import httpx
from datetime import datetime
from typing import List, Dict

from core.config import config
from core.logger import setup_logger

logger = setup_logger("SmartScraper")


class SmartWikiScraper:
    def __init__(self):
        self.headers = {"User-Agent": config.USER_AGENT}

        # 🔥 Keywords care indică evenimente interesante
        self.strong_keywords = [
            "war", "battle", "revolution", "independence",
            "explosion", "assassinated", "killed",
            "founded", "founded", "discovered", "invented",
            "first", "launch", "moon", "space",
            "record", "breakthrough", "treaty",
            "attack", "bomb", "crash", "disaster"
        ]

        # ❌ chestii boring
        self.weak_keywords = [
            "appointed", "elected", "becomes", "publishes",
            "opens", "established", "born", "dies"
        ]

    # ─────────────────────────────────────────────
    # STEP 1: Fetch real events for TODAY
    # ─────────────────────────────────────────────
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

    # ─────────────────────────────────────────────
    # STEP 2: Normalize
    # ─────────────────────────────────────────────
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

    # ─────────────────────────────────────────────
    # STEP 3: Interest scoring
    # ─────────────────────────────────────────────
    def score_event(self, text: str, year: int) -> int:
        text_lower = text.lower()
        score = 0

        # 🔥 Strong signals
        for k in self.strong_keywords:
            if k in text_lower:
                score += 30

        # ❌ Weak signals
        for k in self.weak_keywords:
            if k in text_lower:
                score -= 10

        # 🧠 Recency boost
        if year:
            if year > 1900:
                score += 20
            elif year > 1800:
                score += 10

        # 🎯 Length = usually more meaningful
        if len(text) > 120:
            score += 10

        return score

    # ─────────────────────────────────────────────
    # STEP 4: Filter interesting events
    # ─────────────────────────────────────────────
    def filter_interesting(self, events: List[Dict]) -> List[Dict]:
        scored = []

        for ev in events:
            score = self.score_event(ev["text"], ev.get("year", 0))
            ev["interest_score"] = score

            # 🔥 prag minim
            if score >= 20:
                scored.append(ev)

        logger.info(f"🔥 Interesting events: {len(scored)}")
        return scored

    # ─────────────────────────────────────────────
    # STEP 5: Sort + limit
    # ─────────────────────────────────────────────
    def rank_events(self, events: List[Dict], limit: int = 25) -> List[Dict]:
        events.sort(key=lambda x: x.get("interest_score", 0), reverse=True)
        return events[:limit]

    # ─────────────────────────────────────────────
    # FINAL PIPELINE
    # ─────────────────────────────────────────────
    async def get_today_interesting_events(self, limit: int = 25) -> List[Dict]:
        raw = await self.fetch_today_events()
        normalized = self.normalize_events(raw)
        filtered = self.filter_interesting(normalized)
        ranked = self.rank_events(filtered, limit)

        logger.info(f"🏆 Final selected events: {len(ranked)}")

        return ranked