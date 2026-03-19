import asyncio
import json
import re
from datetime import datetime
from groq import Groq
from core.config import config
from schema.models import EventCategory
from core.logger import setup_logger

logger = setup_logger("AIProcessor")


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model
        self.categories_list = [c.value for c in EventCategory]
        self.languages = ["en", "ro", "es", "de", "fr"]

    def _get_target_date_str(self, target_date: datetime) -> str:
        return target_date.strftime("%B %d")

    def _get_month_day(self, target_date: datetime) -> tuple:
        return target_date.month, target_date.day

    def _ensure_langs(self, data: dict, fallback_text: str = "Data pending") -> dict:
        if not isinstance(data, dict):
            data = {}
        return {lang: data.get(lang) or fallback_text for lang in self.languages}

    # ══════════════════════════════════════════════════════════════
    # PASS 1 — Discovery with strict date anchoring
    # ══════════════════════════════════════════════════════════════
    async def discover_events(self, target_date: datetime) -> list:
        date_str = self._get_target_date_str(target_date)
        month, day = self._get_month_day(target_date)

        prompt = f"""
You are a meticulous Senior Historian and Fact-Checker.
List historical events that occurred EXACTLY on {date_str} (month={month}, day={day}).

CRITICAL RULES — READ CAREFULLY:
1. DATE INTEGRITY: Every event MUST have occurred on EXACTLY {date_str}. 
   - If an event started on {date_str.replace(str(day), str(day-1 if day > 1 else day+1))}, it does NOT count.
   - If an event is commonly associated with this week but happened on a different day, EXCLUDE it.
2. WIKIPEDIA VERIFICATION: The "slug" MUST be the exact Wikipedia article title.
   - Example: "Battle_of_Gettysburg" not "battle of gettysburg"
   - The Wikipedia article must confirm the event happened on {date_str}.
3. YEAR ACCURACY: The year must be the exact year the event occurred.
4. NO HALLUCINATIONS: If you are not 100% certain this happened on {date_str}, EXCLUDE it.
5. QUANTITY: Aim for 50-60 events but NEVER sacrifice accuracy for quantity.
6. DIVERSITY: Include events from different centuries, regions, and categories.

SELF-CHECK before including each event:
- "Am I 100% sure this happened on {date_str}?"
- "Does the Wikipedia article for this slug confirm {date_str}?"
- "Am I confusing this with a similar event on a nearby date?"

STRICT JSON SCHEMA:
{{
  "events": [
    {{
      "year": 1945,
      "text": "Precise 1-2 sentence description clearly stating it happened on {date_str}.",
      "slug": "Exact_Wikipedia_Article_Title",
      "category": "one_from_allowed_list",
      "ai_score": 75,
      "date_confidence": "HIGH/MEDIUM",
      "date_source": "Brief note on why you're sure of this date"
    }}
  ]
}}

ALLOWED CATEGORIES: {self.categories_list}

ONLY include events where date_confidence is HIGH.
"""

        res = await self._safe_groq_call(prompt, f"Discovery ({date_str})", {"events": []})
        events = res.get("events", [])

        validated = []
        seen_slugs = set()
        for e in events:
            slug = e.get("slug")
            if not isinstance(e.get("year"), int) or not slug or slug in seen_slugs:
                continue
            if e.get("category") not in self.categories_list:
                e["category"] = EventCategory.CULTURE_ARTS.value
            # Only keep HIGH confidence events
            if e.get("date_confidence", "").upper() != "HIGH":
                logger.warning(f"⚠️ Skipping low-confidence: {slug}")
                continue

            seen_slugs.add(slug)
            validated.append(e)

        logger.info(f"✅ Found {len(validated)} HIGH-confidence events for {date_str}")
        return validated

    # ══════════════════════════════════════════════════════════════
    # PASS 1.5 — Integrity check with adversarial prompting
    # ══════════════════════════════════════════════════════════════
    async def verify_events_integrity(self, events: list, target_date: datetime) -> list:
        date_str = self._get_target_date_str(target_date)
        month, day = self._get_month_day(target_date)

        # Process in batches of 15 to avoid token limits
        batch_size = 15
        all_verified = []

        for batch_start in range(0, len(events), batch_size):
            batch = events[batch_start:batch_start + batch_size]
            check_list = [
                {
                    "id": i,
                    "year": e["year"],
                    "slug": e["slug"],
                    "summary": e["text"][:120],
                    "date_source": e.get("date_source", "none"),
                }
                for i, e in enumerate(batch)
            ]

            prompt = f"""
You are an ADVERSARIAL Fact-Checker whose job is to FIND ERRORS.
Your goal is to REJECT events that did not happen on exactly {date_str} (month={month}, day={day}).

COMMON MISTAKES TO CATCH:
- Events that happened on {date_str.replace(str(day), str(day-1 if day > 1 else day+1))} or {date_str.replace(str(day), str(day+1 if day < 28 else day-1))} instead of {date_str}
- Events where the DATE is actually when something was announced/signed vs when it happened
- Battles that STARTED on a different day but are commonly associated with this date
- Events from the wrong calendar system (Julian vs Gregorian confusion)
- Merging multiple events into one with wrong date

For EACH event, verify:
1. Does the Wikipedia article "{'{slug}'}" confirm this happened on {date_str}?
2. Is the year correct?
3. Could this be confused with a similar event on a nearby date?

EVENTS TO VERIFY:
{json.dumps(check_list)}

BE STRICT. When in doubt, mark as INVALID.

RETURN ONLY JSON:
{{
  "results": [
    {{
      "id": 0,
      "is_valid": true,
      "confidence": "HIGH/LOW",
      "reason": "Wikipedia confirms this battle began on {date_str}, {'{year}'}"
    }}
  ]
}}
"""

            res = await self._safe_groq_call(prompt, f"Integrity Batch {batch_start}", {"results": []})
            results_map = {r["id"]: r for r in res.get("results", [])}

            for i, event in enumerate(batch):
                v_info = results_map.get(i, {"is_valid": False, "confidence": "LOW"})
                if v_info.get("is_valid") and v_info.get("confidence", "").upper() == "HIGH":
                    all_verified.append(event)
                else:
                    logger.warning(
                        f"🚫 ELIMINATED: {event['year']} {event['slug']} — {v_info.get('reason', 'No reason')}"
                    )

        logger.info(f"✅ {len(all_verified)} events passed integrity check")
        return all_verified

    # ══════════════════════════════════════════════════════════════
    # PASS 2 — Deep ranking
    # ══════════════════════════════════════════════════════════════
    async def deep_rank_and_select(self, candidates: list, target_date: datetime) -> list:
        if not candidates:
            return []

        date_str = self._get_target_date_str(target_date)
        candidates_text = "\n".join(
            [f"ID_{i}: ({e['year']}) {e['text'][:200]}" for i, e in enumerate(candidates)]
        )

        prompt = f"""
Rank the 15 most globally impactful events for {date_str}.

RANKING CRITERIA (in order of importance):
1. Global reach — Did this affect millions of people across multiple countries?
2. Permanence — Is this still relevant/taught today?
3. Universal recognition — Would educated people worldwide know this?
4. Emotional power — Does this event evoke strong emotions?
5. Uniqueness — Is this a "first" or "only" event of its kind?

DIVERSITY REQUIREMENT: Include events from at least 3 different centuries and 3 different categories.

STRICT JSON SCHEMA:
{{
  "top15": [
    {{
      "original_id": "ID_0",
      "deep_score": 95,
      "score_breakdown": {{
        "global_reach": 30,
        "permanence": 25,
        "universal_recognition": 20,
        "emotional_power": 15,
        "uniqueness": 5
      }},
      "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
    }}
  ]
}}

CANDIDATES:
{candidates_text}
"""

        res = await self._safe_groq_call(prompt, "Deep Rank", {"top15": []})
        id_map = {f"ID_{i}": e for i, e in enumerate(candidates)}

        enriched = []
        for entry in res.get("top15", []):
            original_id = entry.get("original_id")
            if original_id in id_map:
                item = id_map[original_id]
                item.update(
                    {
                        "deep_score": entry.get("deep_score", 50),
                        "score_breakdown": entry.get("score_breakdown", {}),
                        "titles": self._ensure_langs(entry.get("titles", {})),
                    }
                )
                enriched.append(item)
        return enriched

    # ══════════════════════════════════════════════════════════════
    # PASS 2.5 — Final Wikipedia cross-reference (GROUND TRUTH)
    # ══════════════════════════════════════════════════════════════
    async def wikipedia_date_verify(self, events: list, target_date: datetime, scraper) -> list:
        """
        Cross-reference each event's date against actual Wikipedia content.
        Two verification methods:
        1. Check the Wikipedia article text for the target date
        2. Cross-reference against Wikipedia's "On this day" page
        """
        import httpx
        from urllib.parse import quote

        date_str = self._get_target_date_str(target_date)
        month, day = self._get_month_day(target_date)

        # Wikipedia requires a proper User-Agent or returns 403
        wiki_headers = {
            "User-Agent": "DailyHistoryApp/2.0 (https://dailyhistory.app; contact@dailyhistory.app)"
        }

        # Multiple date format patterns to search for
        month_name = target_date.strftime("%B")       # "March"
        month_name_short = target_date.strftime("%b")  # "Mar"
        day_str = str(day)
        day_padded = f"{day:02d}"

        date_patterns = [
            f"{month_name} {day_str}",           # "March 19"
            f"{month_name} {day_padded}",         # "March 09"
            f"{day_str} {month_name}",            # "19 March"
            f"{day_padded} {month_name}",         # "09 March"
            f"{month_name_short} {day_str}",      # "Mar 19"
            f"{month_name_short}. {day_str}",     # "Mar. 19"
            f"{day_str} {month_name_short}",      # "19 Mar"
        ]

        # ── Step A: Fetch Wikipedia's "On this day" page for ground truth slugs ──
        otd_slugs = set()
        otd_page_title = f"{month_name}_{day_str}"
        otd_url = (
            f"https://en.wikipedia.org/w/api.php"
            f"?action=query&titles={otd_page_title}"
            f"&prop=links&pllimit=500&format=json"
        )
        try:
            async with httpx.AsyncClient(headers=wiki_headers, timeout=15.0) as client:
                res = await client.get(otd_url)
                if res.status_code == 200:
                    pages = res.json().get("query", {}).get("pages", {})
                    for page in pages.values():
                        for link in page.get("links", []):
                            otd_slugs.add(link.get("title", "").replace(" ", "_"))
                    logger.info(f"📅 Wikipedia 'On this day' page has {len(otd_slugs)} linked articles")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch On-This-Day page: {e}")

        # ── Step B: Verify each event against Wikipedia article content ──
        async def verify_single(event: dict) -> tuple:
            slug = event.get("slug", "").replace(" ", "_")
            if not slug:
                return event, False, "No slug"

            # Quick check: is this slug linked from Wikipedia's "On this day" page?
            slug_clean = slug.replace("_", " ")
            otd_match = any(
                slug_clean.lower() in otd_slug.replace("_", " ").lower()
                or otd_slug.replace("_", " ").lower() in slug_clean.lower()
                for otd_slug in otd_slugs
            )
            if otd_match:
                return event, True, f"Found in Wikipedia 'On this day' ({month_name} {day_str})"

            # Full check: fetch article content and search for date
            encoded_slug = quote(slug, safe="")
            url = (
                f"https://en.wikipedia.org/w/api.php"
                f"?action=query&titles={encoded_slug}&prop=extracts"
                f"&explaintext=true&exsectionformat=plain&format=json"
            )

            try:
                async with httpx.AsyncClient(headers=wiki_headers, timeout=12.0) as client:
                    res = await client.get(url)
                    if res.status_code != 200:
                        return event, False, f"HTTP {res.status_code}"

                    pages = res.json().get("query", {}).get("pages", {})
                    for page_id, page in pages.items():
                        # Check for missing page
                        if page_id == "-1" or "missing" in page:
                            return event, False, "Article not found on Wikipedia"

                        extract = page.get("extract", "")
                        if not extract or len(extract) < 50:
                            return event, False, "No extract found"

                        extract_lower = extract.lower()

                        # Check if any date pattern appears in the article
                        for pattern in date_patterns:
                            if pattern.lower() in extract_lower:
                                return event, True, f"Found '{pattern}' in article text"

                        # Fuzzy check: year + month in same paragraph
                        year_str = str(event.get("year", ""))
                        paragraphs = extract_lower.split("\n")
                        for para in paragraphs:
                            if year_str in para and month_name.lower() in para:
                                return event, True, f"Found year {year_str} + {month_name} in same paragraph"

                        return event, False, f"Date {date_str} not found in article"

            except Exception as e:
                return event, False, f"Error: {e}"

            return event, False, "Unknown error"

        # Run all verifications in parallel
        results = await asyncio.gather(*[verify_single(e) for e in events])

        confirmed = []
        unconfirmed = []
        for event, is_valid, reason in results:
            if is_valid:
                logger.info(f"✅ WIKI CONFIRMED: {event['year']} {event['slug']} — {reason}")
                confirmed.append(event)
            else:
                logger.warning(f"⚠️ WIKI UNCONFIRMED: {event['year']} {event['slug']} — {reason}")
                event["_wiki_unconfirmed"] = True
                event["_wiki_reason"] = reason
                unconfirmed.append(event)

        logger.info(
            f"📊 Wikipedia verification: {len(confirmed)} confirmed, {len(unconfirmed)} unconfirmed"
        )

        # If we have enough confirmed events (>= 8), drop unconfirmed entirely
        if len(confirmed) >= 8:
            logger.info(f"✅ Enough confirmed events — dropping {len(unconfirmed)} unconfirmed")
            return confirmed

        # Otherwise keep unconfirmed but heavily penalize their scores
        for e in unconfirmed:
            e["deep_score"] = e.get("deep_score", 50) * 0.5  # 50% penalty
            e.pop("_wiki_unconfirmed", None)
            e.pop("_wiki_reason", None)

        for e in confirmed:
            e.pop("_wiki_unconfirmed", None)
            e.pop("_wiki_reason", None)

        return confirmed + unconfirmed

    # ══════════════════════════════════════════════════════════════
    # NARRATIVES — Longer, storytelling style
    # ══════════════════════════════════════════════════════════════
    async def generate_secondary_narratives(self, top_events: list, target_date: datetime):
        date_str = self._get_target_date_str(target_date)

        async def process_single(idx, item):
            tasks = [
                self._fetch_narrative_lang(idx, item, lang, date_str) for lang in self.languages
            ]
            return f"EVENT_{idx}", dict(await asyncio.gather(*tasks))

        return dict(
            await asyncio.gather(*[process_single(i, item) for i, item in enumerate(top_events)])
        )

    async def _fetch_narrative_lang(self, idx, item, lang, date_str):
        year = item.get("year", "")
        text = item.get("text", "")
        slug = item.get("slug", "")

        prompt = f"""
You are an award-winning historical storyteller writing for a premium mobile app.
Write a compelling narrative in {lang.upper()} about this event:

EVENT: {year} — {text}
WIKIPEDIA ARTICLE: {slug}
DATE: {date_str}

WRITING RULES:
1. LENGTH: Write 350-450 words. This must feel substantial, not like a blurb.
2. OPENING: Start with a vivid, cinematic scene-setting sentence that transports the reader to that moment.
   - BAD: "On {date_str}, {year}, an important event occurred."
   - GOOD: "The morning air in [city] was thick with tension as..."
3. STRUCTURE: Follow this arc:
   - Hook (1-2 sentences): Set the scene dramatically
   - Context (2-3 sentences): What led to this moment?
   - The Event (3-5 sentences): What exactly happened? Be specific with names, places, numbers.
   - Immediate Impact (2-3 sentences): What changed right after?
   - Legacy (2-3 sentences): Why does this still matter today?
4. TONE: Authoritative but engaging. Like a top documentary narrator — not dry, not sensational.
5. FACTS: Only include verified facts. Do not invent quotes or dialogue.
6. DATE ANCHOR: The narrative must clearly establish this happened on {date_str}, {year}.

Return JSON: {{ "content": "your narrative here" }}
"""

        res = await self._safe_groq_call(
            prompt, f"Narrative {idx}:{lang}", {"content": "Narrative pending..."}
        )
        content = res.get("content", "Narrative pending...")

        # Validate minimum length
        if len(content.split()) < 150:
            logger.warning(f"⚠️ Short narrative for event {idx} ({lang}): {len(content.split())} words")

        return lang, content

    # ══════════════════════════════════════════════════════════════
    # SAFE GROQ CALL
    # ══════════════════════════════════════════════════════════════
    async def _safe_groq_call(self, prompt: str, context: str, fallback: dict) -> dict:
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a strict History API. Output ONLY valid JSON. Never include markdown formatting.",
                    },
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
                max_completion_tokens=4096,
            )
            raw = completion.choices[0].message.content
            return json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(f"🚨 JSON Parse Error ({context}): {e}")
            return fallback
        except Exception as e:
            logger.error(f"🚨 AI Error ({context}): {e}")
            return fallback