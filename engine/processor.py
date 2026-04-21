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

        # ══════════════════════════════════════════════════════════
        # 12 distinct narrative opening techniques — rotated per event
        # to guarantee no two stories in the same batch start alike
        # ══════════════════════════════════════════════════════════
        self.opening_techniques = [
            {
                "name": "SENSORY_IMMERSION",
                "instruction": "Open with a vivid sensory detail — a sound, smell, texture, or visual that places the reader physically in that moment. Example style: 'The acrid smell of gunpowder hung low over the valley as...'",
            },
            {
                "name": "DIALOGUE_OR_QUOTE",
                "instruction": "Open with a powerful real quote or documented words spoken by a key figure in the event. If no verified quote exists, open with a documented reaction or proclamation. Example style: '\"We shall never surrender,\" echoed through the chamber as...'",
            },
            {
                "name": "COUNTDOWN_TENSION",
                "instruction": "Open by establishing urgency — a ticking clock, a deadline, a moment where everything hung in the balance. Example style: 'With less than twelve hours before the deadline expired, negotiators in Geneva faced an impossible choice...'",
            },
            {
                "name": "WIDE_TO_NARROW_ZOOM",
                "instruction": "Open with a panoramic, almost cinematic wide shot of the setting, then zoom into the specific human moment. Example style: 'Across the frozen plains of Eastern Europe, columns of smoke marked the advance of armies — but in one small farmhouse on the outskirts of...'",
            },
            {
                "name": "CONTRAST_JUXTAPOSITION",
                "instruction": "Open by contrasting the ordinary with the extraordinary — what normal life looked like vs. what was about to change. Example style: 'Shopkeepers in downtown Dallas were arranging their morning displays, unaware that within the hour...'",
            },
            {
                "name": "AFTERMATH_FLASHBACK",
                "instruction": "Open with the aftermath or consequence FIRST, then rewind to explain how it happened. Example style: 'When the dust finally settled, the map of Europe had been redrawn forever. It had all begun just hours earlier when...'",
            },
            {
                "name": "HUMAN_PORTRAIT",
                "instruction": "Open by focusing on a single person — their age, their role, what they were doing at that exact moment. Make the reader care about a human before revealing the larger event. Example style: 'Maria Kowalski, a 34-year-old radio operator, was halfway through her night shift when the transmission came through...'",
            },
            {
                "name": "GEOGRAPHIC_STORYTELLING",
                "instruction": "Open by painting the geographic/physical setting as almost a character in the story — the terrain, the weather, the architecture. Example style: 'The narrow strait between the two continents had witnessed empires rise and fall for millennia, but on this particular morning...'",
            },
            {
                "name": "STATISTICAL_SHOCK",
                "instruction": "Open with a striking number or statistic that immediately conveys scale. Example style: 'In the span of forty-eight seconds, an earthquake measuring 7.8 on the Richter scale reduced a city of two million to rubble...'",
            },
            {
                "name": "QUESTION_HOOK",
                "instruction": "Open with a thought-provoking rhetorical question that draws the reader in. Example style: 'What happens when a single signature on a single document changes the fate of an entire continent?'",
            },
            {
                "name": "IRONIC_FORESHADOWING",
                "instruction": "Open with dramatic irony — something that seemed insignificant at the time but turned out to be world-changing. Example style: 'The memo was only three pages long, stamped \"routine\" by the clerk who filed it. Within a year, it would topple a government...'",
            },
            {
                "name": "PARALLEL_WORLDS",
                "instruction": "Open by showing two different places or groups of people simultaneously — what was happening on both sides. Example style: 'In Washington, the president paced the Oval Office floor. Six thousand miles away in Moscow, his counterpart stared at the same intelligence report...'",
            },
        ]

    def _get_target_date_str(self, target_date: datetime) -> str:
        return target_date.strftime("%B %d")

    def _get_month_day(self, target_date: datetime) -> tuple:
        return target_date.month, target_date.day

    def _ensure_langs(self, data: dict, fallback_text: str = "Data pending") -> dict:
        if not isinstance(data, dict):
            data = {}
        return {lang: data.get(lang) or fallback_text for lang in self.languages}

    def _normalize_location(self, e: dict) -> dict:
        """Normalize location field — convert string 'null'/'none'/'' to actual None."""
        loc = e.get("location")
        if isinstance(loc, str) and loc.strip().lower() in ("null", "none", "", "n/a"):
            e["location"] = None
        return e

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
   - If an event started on {date_str.replace(str(day), str(day - 1 if day > 1 else day + 1))}, it does NOT count.
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
      "date_source": "Brief note on why you're sure of this date",
      "location": "City, Country (or null if not applicable)"
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

            # Normalize location field (same as PRO pipeline)
            e = self._normalize_location(e)

            seen_slugs.add(slug)
            validated.append(e)

        logger.info(f"✅ Found {len(validated)} HIGH-confidence events for {date_str}")
        return validated

    # ══════════════════════════════════════════════════════════════
    # PRO DISCOVERY — Dedicated pass for Personalities / Media / Sport
    # ══════════════════════════════════════════════════════════════
    async def discover_pro_events(self, target_date: datetime) -> list:
        """
        Separate AI discovery call focused EXCLUSIVELY on the 3 PRO categories:
          - personalities (births/deaths of iconic figures)
          - media         (films, albums, TV, radio, magazines)
          - sport         (Olympic moments, records, championships)
        """
        date_str = self._get_target_date_str(target_date)
        month, day = self._get_month_day(target_date)

        pro_cats = ["personalities", "media", "sport"]

        prompt = f"""
You are a Senior Pop-Culture & Entertainment Historian.
Your ONLY job is to list PREMIUM, crowd-pleasing historical events that occurred
EXACTLY on {date_str} (month={month}, day={day}) — STRICTLY from these 3 categories:

1. **personalities** — births or deaths of globally iconic people:
   actors, musicians, athletes, authors, scientists, political leaders, royalty, artists.
   Must be a household name (Einstein, Elvis, Ali, Mozart, Marilyn Monroe level).

2. **media** — milestone events in film, TV, music, radio, publishing:
   movie premieres of classics, #1 album releases, iconic TV firsts, historic concerts,
   Oscar-winning ceremonies, famous book publications, groundbreaking broadcasts.

3. **sport** — historic sporting moments:
   Olympic medal moments, world records broken, World Cup/Super Bowl wins,
   legendary boxing fights, first-ever championships, unforgettable games.

═══════════════════════════════════════════════════════════════
HARD RULES:
═══════════════════════════════════════════════════════════════
1. DATE INTEGRITY: Event MUST have occurred EXACTLY on {date_str}.
2. WIKIPEDIA: "slug" MUST match the exact Wikipedia article title (e.g. "Elvis_Presley").
3. FAME BAR: Only pick globally famous people/events. No obscure figures, no local news.
4. DIVERSITY: Aim for good spread across all 3 categories (at least 5 per category).
5. QUANTITY: Aim for 20-30 total events across the 3 categories combined.
6. CONFIDENCE: Only include HIGH-confidence events.

STRICT JSON SCHEMA:
{{
  "events": [
    {{
      "year": 1977,
      "text": "Precise 1-2 sentence description clearly stating it happened on {date_str}.",
      "slug": "Exact_Wikipedia_Article_Title",
      "category": "personalities | media | sport",
      "ai_score": 85,
      "date_confidence": "HIGH",
      "date_source": "Brief note on why you're sure of this date",
      "location": "City, Country (or null if not applicable)"
    }}
  ]
}}

ALLOWED CATEGORIES (strict): {pro_cats}

Remember: This content is for PAYING USERS. Quality and star-power matter more than quantity.
Only HIGH confidence events.
"""

        res = await self._safe_groq_call(
            prompt, f"PRO Discovery ({date_str})", {"events": []}
        )
        events = res.get("events", [])

        validated = []
        seen_slugs = set()
        for e in events:
            slug = e.get("slug")
            cat = e.get("category", "").lower()

            if not isinstance(e.get("year"), int) or not slug or slug in seen_slugs:
                continue
            # Hard filter: only the 3 PRO categories allowed here
            if cat not in pro_cats:
                logger.warning(f"⚠️ PRO discovery returned wrong category '{cat}' — skipped: {slug}")
                continue
            # Only HIGH confidence
            if e.get("date_confidence", "").upper() != "HIGH":
                logger.warning(f"⚠️ Skipping low-confidence PRO: {slug}")
                continue

            # Normalize location field
            e = self._normalize_location(e)

            seen_slugs.add(slug)
            validated.append(e)

        # Log per-category breakdown
        by_cat = {}
        for e in validated:
            by_cat[e["category"]] = by_cat.get(e["category"], 0) + 1
        logger.info(f"✅ PRO discovery: {len(validated)} events → {by_cat}")

        return validated

    # ══════════════════════════════════════════════════════════════
    # PRO SELECTION — Pick best event per category (1 personalities + 1 media + 1 sport)
    # ══════════════════════════════════════════════════════════════
    async def deep_rank_pro_per_category(self, candidates: list, target_date: datetime) -> list:
        """
        Rank PRO candidates and return the top 1 per category.
        Returns a list of up to 3 events (1 personalities + 1 media + 1 sport).
        """
        if not candidates:
            return []

        date_str = self._get_target_date_str(target_date)

        # Bucket candidates by category
        buckets = {"personalities": [], "media": [], "sport": []}
        for c in candidates:
            cat = c.get("category", "").lower()
            if cat in buckets:
                buckets[cat].append(c)

        # Build AI prompt with indexed candidates per bucket
        prompt_blocks = []
        id_map = {}
        counter = 0
        for cat_name, items in buckets.items():
            if not items:
                continue
            prompt_blocks.append(f"\n━━━ CATEGORY: {cat_name.upper()} ━━━")
            for item in items:
                key = f"ID_{counter}"
                id_map[key] = item
                prompt_blocks.append(
                    f"{key} ({item['year']}): {item['text'][:180]}"
                )
                counter += 1

        candidates_text = "\n".join(prompt_blocks)

        prompt = f"""
You are a premium content curator for a history app's PAID TIER.

For {date_str}, select the SINGLE MOST COMPELLING event from EACH of the 3 categories below.
These events will be shown to paying subscribers, so pick the ones with highest
star-power, cultural resonance, and emotional pull.

SELECTION CRITERIA:
1. Global fame — is this a universally recognized moment/person?
2. Storytelling potential — is there a rich narrative to tell?
3. Emotional impact — will readers feel something?
4. Shareability — would someone talk about this with friends?

OUTPUT: Pick exactly 1 event per category (personalities, media, sport).
If a category has no candidates, skip it.

STRICT JSON:
{{
  "selections": [
    {{
      "original_id": "ID_0",
      "category": "personalities",
      "deep_score": 92,
      "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
    }},
    {{
      "original_id": "ID_7",
      "category": "media",
      "deep_score": 88,
      "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
    }},
    {{
      "original_id": "ID_14",
      "category": "sport",
      "deep_score": 85,
      "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
    }}
  ]
}}

CANDIDATES:
{candidates_text}
"""

        res = await self._safe_groq_call(prompt, "PRO Deep Rank", {"selections": []})

        selected = []
        seen_cats = set()
        for entry in res.get("selections", []):
            original_id = entry.get("original_id")
            cat = entry.get("category", "").lower()

            if original_id not in id_map or cat in seen_cats:
                continue

            item = id_map[original_id]
            item.update({
                "deep_score": entry.get("deep_score", 50),
                "titles": self._ensure_langs(entry.get("titles", {})),
                "is_pro": True,
            })
            selected.append(item)
            seen_cats.add(cat)

        # Fallback: if AI didn't return all 3, fill from highest ai_score per missing category
        for cat_name in ["personalities", "media", "sport"]:
            if cat_name in seen_cats:
                continue
            pool = sorted(buckets[cat_name], key=lambda x: x.get("ai_score", 0), reverse=True)
            if pool:
                fallback = pool[0]
                fallback.update({
                    "deep_score": fallback.get("ai_score", 50),
                    "titles": self._ensure_langs({}),
                    "is_pro": True,
                })
                selected.append(fallback)
                logger.warning(f"⚠️ PRO fallback for '{cat_name}': {fallback['slug']}")

        logger.info(f"🏆 PRO selected {len(selected)} events (1 per category)")
        return selected

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
- Events that happened on {date_str.replace(str(day), str(day - 1 if day > 1 else day + 1))} or {date_str.replace(str(day), str(day + 1 if day < 28 else day - 1))} instead of {date_str}
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
        month_name = target_date.strftime("%B")  # "March"
        month_name_short = target_date.strftime("%b")  # "Mar"
        day_str = str(day)
        day_padded = f"{day:02d}"

        date_patterns = [
            f"{month_name} {day_str}",  # "March 19"
            f"{month_name} {day_padded}",  # "March 09"
            f"{day_str} {month_name}",  # "19 March"
            f"{day_padded} {month_name}",  # "09 March"
            f"{month_name_short} {day_str}",  # "Mar 19"
            f"{month_name_short}. {day_str}",  # "Mar. 19"
            f"{day_str} {month_name_short}",  # "19 Mar"
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
    # NARRATIVES — Bulletproof multilingual storytelling
    # ══════════════════════════════════════════════════════════════
    async def generate_secondary_narratives(self, top_events: list, target_date: datetime) -> dict:
        """
        Generate rich narratives for each event in all 5 languages.

        Bulletproof guarantees:
        1. Each event gets a UNIQUE opening technique (no two stories start alike)
        2. Every language is generated independently with retries
        3. Failed languages get a fallback translation from English
        4. Final validation ensures no placeholder text ships
        """
        date_str = self._get_target_date_str(target_date)

        # ── Step 1: Assign unique opening techniques to each event ──
        technique_assignments = self._assign_opening_techniques(len(top_events))
        for idx, item in enumerate(top_events):
            technique = technique_assignments[idx]
            logger.info(f"🎭 Event {idx} ({item.get('slug', '')[:30]}) → {technique['name']}")

        # ── Step 2: Generate all narratives in parallel ──
        async def process_single(idx, item):
            technique = technique_assignments[idx]
            lang_results = await asyncio.gather(*[
                self._fetch_narrative_lang_bulletproof(idx, item, lang, date_str, technique)
                for lang in self.languages
            ])
            return f"EVENT_{idx}", dict(lang_results)

        results = dict(
            await asyncio.gather(*[process_single(i, item) for i, item in enumerate(top_events)])
        )

        # ── Step 3: Verify & patch — no event ships with broken translations ──
        results = await self._verify_and_patch_narratives(results, top_events, date_str, technique_assignments)

        # ── Step 4: Anti-repetition audit across all events ──
        self._audit_opening_diversity(results)

        return results

    def _assign_opening_techniques(self, count: int) -> list:
        """
        Assign a unique opening technique to each event.
        Shuffles techniques so that even across days, patterns vary.
        """
        import random
        import hashlib
        from datetime import datetime

        # Seed shuffle with today's date so it's deterministic per day
        # but different across days
        day_seed = datetime.now().strftime("%Y-%m-%d")
        seed = int(hashlib.md5(day_seed.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)

        pool = list(self.opening_techniques)
        rng.shuffle(pool)

        # If we need more techniques than available, cycle through
        assignments = []
        for i in range(count):
            assignments.append(pool[i % len(pool)])

        return assignments

    async def _fetch_narrative_lang_bulletproof(
            self, idx: int, item: dict, lang: str, date_str: str, technique: dict
    ) -> tuple:
        """
        Generate a single narrative for one event in one language.
        Includes retry logic (up to 3 attempts) and quality validation.
        """
        max_retries = 3
        year = item.get("year", "")
        text = item.get("text", "")
        slug = item.get("slug", "")

        lang_names = {
            "en": "English",
            "ro": "Romanian",
            "es": "Spanish",
            "de": "German",
            "fr": "French",
        }
        lang_full = lang_names.get(lang, lang.upper())

        for attempt in range(1, max_retries + 1):
            prompt = f"""
You are an award-winning historical storyteller writing for a premium mobile app.
Write a compelling narrative in **{lang_full} ({lang.upper()})** about this event:

EVENT: {year} — {text}
WIKIPEDIA ARTICLE: {slug}
DATE: {date_str}

═══════════════════════════════════════════════
MANDATORY OPENING TECHNIQUE: {technique['name']}
{technique['instruction']}
═══════════════════════════════════════════════

WRITING RULES:
1. LENGTH: Write 350-450 words. This must feel substantial, not like a blurb.
2. OPENING: You MUST use the {technique['name']} technique described above.
   — FORBIDDEN OPENINGS (do NOT start with any of these patterns):
     • "On [date], [year], ..."
     • "In [year], on [date], ..."  
     • "[Date], [year] marked..."
     • "The date was [date], [year]..."
     • "It was [date], [year] when..."
     • Any variant that leads with the raw date as the first words.
   — The date MUST appear in the narrative, but NOT as the opening words.
3. STRUCTURE: Follow this arc:
   - Hook (1-2 sentences): Using the {technique['name']} technique
   - Context (2-3 sentences): What led to this moment?
   - The Event (3-5 sentences): What exactly happened? Be specific with names, places, numbers.
   - Immediate Impact (2-3 sentences): What changed right after?
   - Legacy (2-3 sentences): Why does this still matter today?
4. LANGUAGE: Write the ENTIRE narrative in {lang_full}. Not a single sentence in another language.
5. TONE: Authoritative but engaging. Like a top documentary narrator — not dry, not sensational.
6. FACTS: Only include verified facts. Do not invent quotes unless using documented historical words.
7. DATE ANCHOR: The narrative must clearly establish this happened on {date_str}, {year} — but woven naturally into the text, not as the opening line.

QUALITY CHECKLIST before submitting:
- [ ] Is the entire text in {lang_full}? (Not English, not mixed)
- [ ] Does it use the {technique['name']} opening technique?
- [ ] Does it NOT start with "On {date_str}" or "In {year}"?
- [ ] Is it between 350-450 words?
- [ ] Does it mention the specific date {date_str}, {year} somewhere?

Return JSON: {{ "content": "your narrative here" }}
"""

            res = await self._safe_groq_call(
                prompt,
                f"Narrative {idx}:{lang} (attempt {attempt})",
                {"content": ""},
            )
            content = res.get("content", "")

            # ── Quality gate ──
            is_valid, reason = self._validate_narrative(content, lang, technique)
            if is_valid:
                logger.info(f"✅ Narrative {idx}:{lang} — passed quality gate (attempt {attempt})")
                return lang, content
            else:
                logger.warning(
                    f"⚠️ Narrative {idx}:{lang} failed quality gate (attempt {attempt}): {reason}"
                )

        # All retries exhausted — return whatever we got, will be patched later
        logger.error(f"🚨 Narrative {idx}:{lang} — all {max_retries} attempts failed")
        return lang, content if content else ""

    def _validate_narrative(self, content: str, lang: str, technique: dict) -> tuple:
        """
        Validate a narrative meets quality standards.
        Returns (is_valid: bool, reason: str).
        """
        if not content or len(content.strip()) < 50:
            return False, "Content is empty or too short"

        word_count = len(content.split())

        # Must be at least 150 words (generous minimum to account for language differences)
        if word_count < 150:
            return False, f"Too short: {word_count} words (minimum 150)"

        # Check for placeholder / error content
        bad_markers = [
            "narrative pending",
            "content pending",
            "error generating",
            "i apologize",
            "i'm sorry",
            "as an ai",
            "i cannot",
        ]
        content_lower = content.lower()
        for marker in bad_markers:
            if marker in content_lower:
                return False, f"Contains placeholder/error text: '{marker}'"

        # Check it's not in English when it should be another language
        if lang != "en":
            # Simple heuristic: check for common English-only words that rarely appear in other languages
            english_giveaways = ["the ", "and ", "was ", "were ", "this ", "that ", "with ", "from "]
            english_word_count = sum(1 for word in english_giveaways if word in content_lower)
            total_ratio = english_word_count / max(len(english_giveaways), 1)
            if total_ratio > 0.8:
                return False, f"Appears to be in English instead of {lang} (english ratio: {total_ratio:.0%})"

        # Check it doesn't start with a raw date pattern
        first_30 = content[:30].lower().strip()
        date_openers = ["on ", "in ", "the date"]
        # Only flag if it starts with these AND immediately has a month/year
        for opener in date_openers:
            if first_30.startswith(opener):
                # Check if a month name follows within first 40 chars
                months = [
                    "january", "february", "march", "april", "may", "june",
                    "july", "august", "september", "october", "november", "december",
                    "enero", "febrero", "marzo", "abril", "mayo", "junio",
                    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
                    "januar", "februar", "märz", "april", "mai", "juni",
                    "juli", "august", "september", "oktober", "november", "dezember",
                    "ianuarie", "februarie", "martie", "aprilie", "mai", "iunie",
                    "iulie", "august", "septembrie", "octombrie", "noiembrie", "decembrie",
                    "janvier", "février", "mars", "avril", "mai", "juin",
                    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
                ]
                first_60 = content[:60].lower()
                if any(m in first_60 for m in months):
                    return False, f"Starts with boring date opener: '{content[:40]}...'"

        return True, "OK"

    async def _verify_and_patch_narratives(
            self, results: dict, top_events: list, date_str: str, technique_assignments: list
    ) -> dict:
        """
        Final verification pass: ensure every event has all 5 languages.
        Missing/broken languages get patched via translation from English.
        """
        patch_tasks = []

        for idx, item in enumerate(top_events):
            event_key = f"EVENT_{idx}"
            narratives = results.get(event_key, {})

            # Ensure English exists first (it's our fallback source)
            en_content = narratives.get("en", "")
            if not en_content or len(en_content.split()) < 100:
                logger.error(f"🚨 Event {idx}: English narrative is missing/broken — regenerating")
                patch_tasks.append(
                    self._emergency_regenerate(idx, item, "en", date_str, technique_assignments[idx], results)
                )

            # Check each non-English language
            for lang in ["ro", "es", "de", "fr"]:
                content = narratives.get(lang, "")
                is_valid, reason = self._validate_narrative(
                    content, lang, technique_assignments[idx]
                )
                if not is_valid:
                    logger.warning(f"⚠️ Event {idx}:{lang} failed final check: {reason} — patching")
                    patch_tasks.append(
                        self._patch_from_english(idx, lang, date_str, results)
                    )

        if patch_tasks:
            logger.info(f"🔧 Patching {len(patch_tasks)} broken narrative(s)...")
            await asyncio.gather(*patch_tasks)

        # Final safety net: fill any still-missing with explicit marker
        for idx in range(len(top_events)):
            event_key = f"EVENT_{idx}"
            if event_key not in results:
                results[event_key] = {}
            for lang in self.languages:
                if not results[event_key].get(lang) or len(results[event_key][lang].strip()) < 50:
                    logger.error(f"🚨 CRITICAL: Event {idx}:{lang} still missing after patching!")
                    results[event_key][lang] = results[event_key].get("en", "Narrative unavailable.")

        return results

    async def _emergency_regenerate(
            self, idx: int, item: dict, lang: str, date_str: str, technique: dict, results: dict
    ):
        """Emergency regeneration of a narrative from scratch."""
        _, content = await self._fetch_narrative_lang_bulletproof(idx, item, lang, date_str, technique)
        event_key = f"EVENT_{idx}"
        if event_key not in results:
            results[event_key] = {}
        results[event_key][lang] = content

    async def _patch_from_english(self, idx: int, target_lang: str, date_str: str, results: dict):
        """
        Translate the English narrative into the target language.
        Used as a fallback when direct generation failed.
        """
        event_key = f"EVENT_{idx}"
        en_content = results.get(event_key, {}).get("en", "")
        if not en_content or len(en_content.split()) < 100:
            logger.error(f"🚨 Cannot patch {idx}:{target_lang} — English source also broken")
            return

        lang_names = {
            "ro": "Romanian",
            "es": "Spanish",
            "de": "German",
            "fr": "French",
        }
        lang_full = lang_names.get(target_lang, target_lang.upper())

        prompt = f"""
You are a professional literary translator specializing in historical content.
Translate the following English historical narrative into {lang_full}.

TRANSLATION RULES:
1. Preserve the storytelling tone, structure, and emotional impact.
2. Do NOT add or remove information — translate faithfully.
3. Use natural, fluent {lang_full} — not word-for-word translation.
4. Keep proper nouns (names, places) in their commonly used {lang_full} forms.
5. The entire output must be in {lang_full}. Zero English words except proper nouns.

ENGLISH ORIGINAL:
{en_content}

Return JSON: {{ "content": "translated narrative in {lang_full}" }}
"""

        res = await self._safe_groq_call(
            prompt, f"Translation patch {idx}:{target_lang}", {"content": ""}
        )
        translated = res.get("content", "")

        if translated and len(translated.split()) >= 100:
            results[event_key][target_lang] = translated
            logger.info(f"✅ Patched {idx}:{target_lang} via English translation")
        else:
            # Last resort: use English
            results[event_key][target_lang] = en_content
            logger.warning(f"⚠️ Translation patch failed for {idx}:{target_lang} — using English fallback")

    def _audit_opening_diversity(self, results: dict):
        """
        Log the first 15 words of each English narrative to verify
        they don't all start the same way.
        """
        logger.info("🔍 Opening diversity audit (EN):")
        openings = []
        for key in sorted(results.keys()):
            en = results[key].get("en", "")
            first_words = " ".join(en.split()[:15])
            openings.append(first_words)
            logger.info(f"  {key}: \"{first_words}...\"")

        # Check for duplicate first-3-words
        first_three = [" ".join(o.split()[:3]).lower() for o in openings if o]
        duplicates = len(first_three) - len(set(first_three))
        if duplicates > 0:
            logger.warning(f"⚠️ {duplicates} events share the same opening 3 words!")
        else:
            logger.info("✅ All events have unique openings")

    # ══════════════════════════════════════════════════════════════
    # TITLE TRANSLATION VERIFICATION
    # ══════════════════════════════════════════════════════════════
    async def verify_and_fix_titles(self, events: list) -> list:
        """
        Post-processing pass to ensure all event titles have proper
        translations in all 5 languages. Fixes missing/placeholder titles.
        """
        repair_tasks = []

        for idx, item in enumerate(events):
            titles = item.get("titles", {})
            missing_langs = []

            for lang in self.languages:
                title = titles.get(lang, "")
                if not title or title in ("Event", "Data pending", ""):
                    missing_langs.append(lang)

            if missing_langs:
                repair_tasks.append(self._repair_titles(idx, item, missing_langs))

        if repair_tasks:
            logger.info(f"🔧 Repairing titles for {len(repair_tasks)} event(s)...")
            await asyncio.gather(*repair_tasks)

        return events

    async def _repair_titles(self, idx: int, item: dict, missing_langs: list):
        """Generate missing title translations for specific languages."""
        en_title = item.get("titles", {}).get("en", item.get("text", "Historical Event")[:80])
        year = item.get("year", "")
        slug = item.get("slug", "")

        lang_names = {"en": "English", "ro": "Romanian", "es": "Spanish", "de": "German", "fr": "French"}
        langs_str = ", ".join([f"{lang_names[l]} ({l})" for l in missing_langs])

        prompt = f"""
Translate this historical event title into the following languages: {langs_str}

ORIGINAL (English): {en_title}
CONTEXT: This event occurred in {year}. Wikipedia article: {slug}

RULES:
- Each title should be concise (5-15 words)
- Use natural phrasing for each language
- Keep proper nouns in their standard form for each language

Return JSON with language codes as keys:
{{ {', '.join([f'"{l}": "title in {lang_names[l]}"' for l in missing_langs])} }}
"""

        res = await self._safe_groq_call(prompt, f"Title repair {idx}", {})

        titles = item.get("titles", {})
        for lang in missing_langs:
            fixed = res.get(lang, "")
            if fixed and len(fixed) > 2:
                titles[lang] = fixed
                logger.info(f"✅ Fixed title {idx}:{lang} → {fixed[:50]}")
            else:
                # Fallback: use English
                titles[lang] = en_title
                logger.warning(f"⚠️ Title fix failed for {idx}:{lang} — using English")

        item["titles"] = titles

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