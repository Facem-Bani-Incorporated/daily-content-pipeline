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
        # 8 storytelling angles — all accessible, all anchor the
        # date+place clearly in the first 2 sentences. Then unfold
        # the story with simple, vivid language anyone can read.
        # ══════════════════════════════════════════════════════════
        self.storytelling_angles = [
            {
                "name": "SCENE_SETTING",
                "instruction": (
                    "Open by quickly placing the reader in the time and place. "
                    "Mention the date and city/country naturally in the first 2 sentences. "
                    "Then describe what life looked like that day — before everything changed. "
                    "Example feel: 'It was a quiet spring morning in Paris on April 14, 1912. "
                    "Across the Atlantic, a ship called the Titanic was about to make history — for all the wrong reasons.'"
                ),
            },
            {
                "name": "HUMAN_FOCUS",
                "instruction": (
                    "Open with a real person at the center of the story. Mention the date and place "
                    "naturally within the first 2-3 sentences. Make the reader care about this person "
                    "before showing what happened. Example feel: 'On July 20, 1969, a 38-year-old astronaut "
                    "named Neil Armstrong looked out a small window at a place no human had ever stood — the surface of the Moon.'"
                ),
            },
            {
                "name": "THE_BIG_MOMENT",
                "instruction": (
                    "Open by describing the exact moment the event happened — the date, the place, what was about to occur. "
                    "Use plain words but vivid imagery. Example feel: 'On the evening of November 9, 1989, in Berlin, "
                    "thousands of people gathered at a wall that had divided their city for 28 years. Tonight, that wall would come down.'"
                ),
            },
            {
                "name": "WHY_IT_MATTERED",
                "instruction": (
                    "Open by explaining what was at stake. Mention the date and location in the first 2 sentences. "
                    "Help the reader understand why this day was different. "
                    "Example feel: 'On August 6, 1945, the Japanese city of Hiroshima would change forever. "
                    "It was an ordinary Monday morning — until 8:15 AM.'"
                ),
            },
            {
                "name": "THE_CONTRAST",
                "instruction": (
                    "Open by showing the calm before the storm — life going on normally — then reveal what happened. "
                    "Mention the date and place clearly in the first 2 sentences. "
                    "Example feel: 'On the morning of April 18, 1906, the people of San Francisco were still asleep. "
                    "At 5:12 AM, the ground started shaking — and an entire city would be destroyed within hours.'"
                ),
            },
            {
                "name": "THE_NUMBERS_TELL",
                "instruction": (
                    "Open with a striking number that captures the scale of what happened. "
                    "Mention the date and place in the first 2 sentences. "
                    "Example feel: 'In just 47 seconds on July 28, 1976, in Tangshan, China, an earthquake killed "
                    "more than 240,000 people. It happened in the middle of the night, while everyone was sleeping.'"
                ),
            },
            {
                "name": "THE_WORLD_CHANGED",
                "instruction": (
                    "Open with the date and place, then immediately show how the world was different the next day. "
                    "Example feel: 'On December 17, 1903, on a windy beach in Kitty Hawk, North Carolina, two brothers "
                    "did something humans had dreamed about for thousands of years: they flew. The age of flight had begun.'"
                ),
            },
            {
                "name": "THE_STORY_BEHIND",
                "instruction": (
                    "Open by hinting at a backstory the reader probably doesn't know. Anchor the date and place clearly "
                    "within the first 2-3 sentences. Then unfold the surprise. "
                    "Example feel: 'On April 15, 1955, in San Bernardino, California, a 52-year-old milkshake-machine salesman "
                    "opened a small restaurant. He had no idea he was about to build the biggest fast-food chain in history.'"
                ),
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
        loc = e.get("location")
        if isinstance(loc, str) and loc.strip().lower() in ("null", "none", "", "n/a"):
            e["location"] = None
        return e

    # ══════════════════════════════════════════════════════════════
    # PASS 1 — Discovery
    # ══════════════════════════════════════════════════════════════
    async def discover_events(self, target_date: datetime) -> list:
        date_str = self._get_target_date_str(target_date)
        month, day = self._get_month_day(target_date)

        prompt = f"""
You are a meticulous Senior Historian and Fact-Checker.
List historical events that occurred EXACTLY on {date_str} (month={month}, day={day}).

CRITICAL RULES:
1. DATE INTEGRITY: Every event MUST have occurred on EXACTLY {date_str}.
   - If an event started on a different day, it does NOT count.
2. WIKIPEDIA: "slug" MUST be the exact Wikipedia article title.
3. YEAR ACCURACY: Exact year of the event.
4. NO HALLUCINATIONS: If unsure, EXCLUDE.
5. QUANTITY: 50-60 events, never sacrifice accuracy.
6. DIVERSITY: Different centuries, regions, categories.

STRICT JSON SCHEMA:
{{
  "events": [
    {{
      "year": 1945,
      "text": "Precise 1-2 sentence description.",
      "slug": "Exact_Wikipedia_Article_Title",
      "category": "one_from_allowed_list",
      "ai_score": 75,
      "date_confidence": "HIGH/MEDIUM",
      "date_source": "Brief note",
      "location": "City, Country (or null)"
    }}
  ]
}}

ALLOWED CATEGORIES: {self.categories_list}
ONLY HIGH confidence.
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
            if e.get("date_confidence", "").upper() != "HIGH":
                logger.warning(f"⚠️ Skipping low-confidence: {slug}")
                continue

            e = self._normalize_location(e)
            seen_slugs.add(slug)
            validated.append(e)

        logger.info(f"✅ Found {len(validated)} HIGH-confidence events for {date_str}")
        return validated

    # ══════════════════════════════════════════════════════════════
    # PRO DISCOVERY
    # ══════════════════════════════════════════════════════════════
    async def discover_pro_events(self, target_date: datetime) -> list:
        date_str = self._get_target_date_str(target_date)
        month, day = self._get_month_day(target_date)
        pro_cats = ["personalities", "media", "sport"]

        prompt = f"""
You are a Senior Pop-Culture & Entertainment Historian.
List PREMIUM historical events for {date_str} (month={month}, day={day}) — STRICTLY from:

1. **personalities** — births/deaths of globally iconic people
2. **media** — milestone events in film, TV, music, radio, publishing
3. **sport** — historic sporting moments

HARD RULES:
1. DATE: Must be EXACTLY {date_str}.
2. WIKIPEDIA: "slug" must match exact article title.
3. FAME: Only globally famous people/events.
4. 5+ per category, 20-30 total.
5. Only HIGH confidence.

STRICT JSON SCHEMA:
{{
  "events": [
    {{
      "year": 1977,
      "text": "Precise description.",
      "slug": "Exact_Wikipedia_Title",
      "category": "personalities | media | sport",
      "ai_score": 85,
      "date_confidence": "HIGH",
      "date_source": "Brief note",
      "location": "City, Country (or null)"
    }}
  ]
}}

ALLOWED: {pro_cats}
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
            if cat not in pro_cats:
                continue
            if e.get("date_confidence", "").upper() != "HIGH":
                continue

            e = self._normalize_location(e)
            seen_slugs.add(slug)
            validated.append(e)

        by_cat = {}
        for e in validated:
            by_cat[e["category"]] = by_cat.get(e["category"], 0) + 1
        logger.info(f"✅ PRO discovery: {len(validated)} events → {by_cat}")
        return validated

    # ══════════════════════════════════════════════════════════════
    # PRO RANK
    # ══════════════════════════════════════════════════════════════
    async def deep_rank_pro_per_category(self, candidates: list, target_date: datetime) -> list:
        if not candidates:
            return []

        date_str = self._get_target_date_str(target_date)
        buckets = {"personalities": [], "media": [], "sport": []}
        for c in candidates:
            cat = c.get("category", "").lower()
            if cat in buckets:
                buckets[cat].append(c)

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
                prompt_blocks.append(f"{key} ({item['year']}): {item['text'][:180]}")
                counter += 1

        candidates_text = "\n".join(prompt_blocks)

        prompt = f"""
You are a premium content curator for a history app's PAID TIER.
For {date_str}, select the SINGLE MOST COMPELLING event from EACH category.

CRITERIA: global fame, storytelling potential, emotional impact, shareability.
OUTPUT: 1 event per category (personalities, media, sport).

STRICT JSON:
{{
  "selections": [
    {{
      "original_id": "ID_0",
      "category": "personalities",
      "deep_score": 92,
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

        logger.info(f"🏆 PRO selected {len(selected)} events")
        return selected

    # ══════════════════════════════════════════════════════════════
    # PASS 2 — Deep ranking for FREE
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

CRITERIA: global reach, permanence, universal recognition, emotional power, uniqueness.
DIVERSITY: at least 3 different centuries, 3 different categories.

STRICT JSON:
{{
  "top15": [
    {{
      "original_id": "ID_0",
      "deep_score": 95,
      "score_breakdown": {{
        "global_reach": 30, "permanence": 25, "universal_recognition": 20,
        "emotional_power": 15, "uniqueness": 5
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
                item.update({
                    "deep_score": entry.get("deep_score", 50),
                    "score_breakdown": entry.get("score_breakdown", {}),
                    "titles": self._ensure_langs(entry.get("titles", {})),
                })
                enriched.append(item)
        return enriched

    # ══════════════════════════════════════════════════════════════
    # NARRATIVES — Accessible storytelling
    # Structure: Hook → What happened → WHY it happened → AFTERMATH → Legacy
    # ══════════════════════════════════════════════════════════════
    async def generate_secondary_narratives(self, top_events: list, target_date: datetime) -> dict:
        date_str = self._get_target_date_str(target_date)

        angle_assignments = self._assign_storytelling_angles(len(top_events))
        for idx, item in enumerate(top_events):
            angle = angle_assignments[idx]
            logger.info(f"📖 Event {idx} ({item.get('slug', '')[:30]}) → {angle['name']}")

        async def process_single(idx, item):
            angle = angle_assignments[idx]
            lang_results = await asyncio.gather(*[
                self._fetch_narrative_lang(idx, item, lang, date_str, angle)
                for lang in self.languages
            ])
            return f"EVENT_{idx}", dict(lang_results)

        results = dict(
            await asyncio.gather(*[process_single(i, item) for i, item in enumerate(top_events)])
        )

        results = await self._verify_and_patch_narratives(
            results, top_events, date_str, angle_assignments
        )
        self._audit_opening_diversity(results)
        return results

    def _assign_storytelling_angles(self, count: int) -> list:
        import random
        import hashlib

        day_seed = datetime.now().strftime("%Y-%m-%d")
        seed = int(hashlib.md5(day_seed.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)

        pool = list(self.storytelling_angles)
        rng.shuffle(pool)

        return [pool[i % len(pool)] for i in range(count)]

    async def _fetch_narrative_lang(
        self, idx: int, item: dict, lang: str, date_str: str, angle: dict
    ) -> tuple:
        """
        Generate ONE narrative in ONE language.

        Goal: ACCESSIBLE storytelling that anyone can read and enjoy.
        Structure:
          1. HOOK + when/where (using angle technique)
          2. WHAT happened (the event itself, plainly told)
          3. WHY it happened (the causes, the build-up)
          4. AFTERMATH (immediate consequences in days/weeks)
          5. LEGACY (why it still matters today)
        """
        max_retries = 3
        year = item.get("year", "")
        text = item.get("text", "")
        slug = item.get("slug", "")
        location = item.get("location") or "the location"

        lang_names = {
            "en": "English",
            "ro": "Romanian",
            "es": "Spanish",
            "de": "German",
            "fr": "French",
        }
        lang_full = lang_names.get(lang, lang.upper())

        last_content = ""

        for attempt in range(1, max_retries + 1):
            prompt = f"""
You are a great storyteller writing for a popular history app.
Your readers are curious people of all ages — not historians.
Write a narrative in **{lang_full} ({lang.upper()})** about this event.

EVENT: {year} — {text}
WIKIPEDIA: {slug}
DATE: {date_str}, {year}
LOCATION: {location}

═══════════════════════════════════════════════════════
STORYTELLING ANGLE: {angle['name']}
{angle['instruction']}
═══════════════════════════════════════════════════════

REQUIRED STRUCTURE — write all 5 parts in flowing prose (no headers, no bullet points):

PART 1 — THE HOOK (2-3 sentences):
Open using the {angle['name']} technique above. Make sure the date ({date_str}, {year})
and the location ({location}) are clearly mentioned in the first 2 sentences — naturally,
not awkwardly. The reader should instantly know WHEN and WHERE this happened.

PART 2 — WHAT HAPPENED (3-4 sentences):
Tell the event itself. Be concrete: who was there, what did they do, what did they see.
Use simple, vivid words. No jargon. No big abstract words.

PART 3 — WHY IT HAPPENED (3-4 sentences):
This is critical. Explain the CAUSES — the events, decisions, or pressures that led to this moment.
What was the world like before this? What tensions or dreams or mistakes built up to it?
Make the reader understand WHY this had to happen.

PART 4 — THE AFTERMATH (3-4 sentences):
What happened in the days, weeks, and months right after? Who was affected? How did people react?
What did governments, families, or societies do in response? Be specific.

PART 5 — THE LEGACY (2-3 sentences):
Why does this still matter today, in our world? How did it shape what came next?
End with something memorable — a thought that stays with the reader.

═══════════════════════════════════════════════════════
WRITING RULES:
1. LENGTH: 380-450 words total.
2. LANGUAGE: Write the ENTIRE text in {lang_full}. Not a single phrase in another language.
3. SIMPLICITY: Use everyday words. If a 14-year-old can't understand a sentence, rewrite it.
   AVOID: "subsequent," "ramifications," "paradigm," "unprecedented confluence," "geopolitical landscape."
   PREFER: "after that," "results," "way of thinking," "rare moment," "world politics."
4. SHOW, DON'T LIST: Don't write "the causes were A, B, and C." Tell it as a story.
5. CONCRETE DETAILS: Names, places, numbers, sensory details. NO vague abstractions.
6. EMOTION: Make the reader FEEL something. Curiosity, awe, sadness, hope.
7. FLOW: All 5 parts should blend seamlessly into one continuous narrative — no headings, no breaks.
8. NO AI VOICE: Don't say "I will tell you," "let me explain," or any phrase that sounds like an AI.
9. NO META: Don't reference "this article," "this event," or "today we learn about." Just tell the story.

QUALITY CHECKLIST before submitting:
- [ ] Does the FIRST sentence give a clear sense of WHEN and WHERE?
- [ ] Is the entire text in {lang_full}?
- [ ] Does it explain WHY this happened (causes/buildup)?
- [ ] Does it describe the AFTERMATH (what changed right after)?
- [ ] Could a 14-year-old read this and feel pulled in?
- [ ] Is it 380-450 words?

Return JSON: {{ "content": "your narrative here as one flowing piece of prose" }}
"""

            res = await self._safe_groq_call(
                prompt,
                f"Narrative {idx}:{lang} (attempt {attempt})",
                {"content": ""},
            )
            content = res.get("content", "")
            last_content = content

            is_valid, reason = self._validate_narrative(content, lang, angle)
            if is_valid:
                logger.info(f"✅ Narrative {idx}:{lang} — passed (attempt {attempt})")
                return lang, content
            else:
                logger.warning(
                    f"⚠️ Narrative {idx}:{lang} attempt {attempt}: {reason}"
                )

        logger.error(f"🚨 Narrative {idx}:{lang} — all {max_retries} attempts failed")
        return lang, last_content if last_content else ""

    def _validate_narrative(self, content: str, lang: str, angle: dict) -> tuple:
        if not content or len(content.strip()) < 50:
            return False, "Empty or too short"

        word_count = len(content.split())
        if word_count < 200:
            return False, f"Too short: {word_count} words (min 200)"
        if word_count > 600:
            return False, f"Too long: {word_count} words (max 600)"

        bad_markers = [
            "narrative pending", "content pending", "error generating",
            "i apologize", "i'm sorry", "as an ai", "i cannot",
            "let me tell you", "in this article", "in this story",
        ]
        content_lower = content.lower()
        for marker in bad_markers:
            if marker in content_lower:
                return False, f"Contains placeholder/AI text: '{marker}'"

        # Check it's actually in target language (rough heuristic)
        if lang != "en":
            english_giveaways = ["the ", "and ", "was ", "were ", "this ", "that ", "with ", "from "]
            count = sum(1 for w in english_giveaways if w in content_lower)
            ratio = count / len(english_giveaways)
            if ratio > 0.8:
                return False, f"Appears to be English instead of {lang}"

        return True, "OK"

    async def _verify_and_patch_narratives(
        self, results: dict, top_events: list, date_str: str, angle_assignments: list
    ) -> dict:
        patch_tasks = []

        for idx, item in enumerate(top_events):
            event_key = f"EVENT_{idx}"
            narratives = results.get(event_key, {})

            en_content = narratives.get("en", "")
            if not en_content or len(en_content.split()) < 100:
                logger.error(f"🚨 Event {idx}: English missing — regenerating")
                patch_tasks.append(
                    self._emergency_regenerate(
                        idx, item, "en", date_str, angle_assignments[idx], results
                    )
                )

            for lang in ["ro", "es", "de", "fr"]:
                content = narratives.get(lang, "")
                is_valid, reason = self._validate_narrative(
                    content, lang, angle_assignments[idx]
                )
                if not is_valid:
                    logger.warning(f"⚠️ Event {idx}:{lang} failed: {reason} — patching")
                    patch_tasks.append(self._patch_from_english(idx, lang, results))

        if patch_tasks:
            logger.info(f"🔧 Patching {len(patch_tasks)} narrative(s)...")
            await asyncio.gather(*patch_tasks)

        for idx in range(len(top_events)):
            event_key = f"EVENT_{idx}"
            if event_key not in results:
                results[event_key] = {}
            for lang in self.languages:
                if not results[event_key].get(lang) or len(results[event_key][lang].strip()) < 50:
                    logger.error(f"🚨 CRITICAL: {idx}:{lang} still missing")
                    results[event_key][lang] = results[event_key].get("en", "Narrative unavailable.")

        return results

    async def _emergency_regenerate(
        self, idx: int, item: dict, lang: str, date_str: str, angle: dict, results: dict
    ):
        _, content = await self._fetch_narrative_lang(idx, item, lang, date_str, angle)
        event_key = f"EVENT_{idx}"
        if event_key not in results:
            results[event_key] = {}
        results[event_key][lang] = content

    async def _patch_from_english(self, idx: int, target_lang: str, results: dict):
        event_key = f"EVENT_{idx}"
        en_content = results.get(event_key, {}).get("en", "")
        if not en_content or len(en_content.split()) < 100:
            logger.error(f"🚨 Cannot patch {idx}:{target_lang} — English broken too")
            return

        lang_names = {"ro": "Romanian", "es": "Spanish", "de": "German", "fr": "French"}
        lang_full = lang_names.get(target_lang, target_lang.upper())

        prompt = f"""
You are a literary translator. Translate this English historical narrative into {lang_full}.

RULES:
1. Preserve storytelling tone, structure, and emotional impact.
2. Translate faithfully — don't add or remove information.
3. Use natural, fluent {lang_full} — not word-for-word.
4. Keep proper nouns in their {lang_full} forms.
5. Entire output in {lang_full}. No English except proper nouns.
6. Keep the simple, accessible language style.

ENGLISH:
{en_content}

Return JSON: {{ "content": "translated narrative in {lang_full}" }}
"""

        res = await self._safe_groq_call(
            prompt, f"Translation {idx}:{target_lang}", {"content": ""}
        )
        translated = res.get("content", "")

        if translated and len(translated.split()) >= 100:
            results[event_key][target_lang] = translated
            logger.info(f"✅ Patched {idx}:{target_lang} via translation")
        else:
            results[event_key][target_lang] = en_content
            logger.warning(f"⚠️ Translation failed {idx}:{target_lang} — using English fallback")

    def _audit_opening_diversity(self, results: dict):
        logger.info("🔍 Opening diversity audit (EN):")
        openings = []
        for key in sorted(results.keys()):
            en = results[key].get("en", "")
            first_words = " ".join(en.split()[:15])
            openings.append(first_words)
            logger.info(f"  {key}: \"{first_words}...\"")

        first_three = [" ".join(o.split()[:3]).lower() for o in openings if o]
        duplicates = len(first_three) - len(set(first_three))
        if duplicates > 0:
            logger.warning(f"⚠️ {duplicates} events share opening 3 words!")
        else:
            logger.info("✅ All events have unique openings")

    # ══════════════════════════════════════════════════════════════
    # TITLE TRANSLATION VERIFICATION
    # ══════════════════════════════════════════════════════════════
    async def verify_and_fix_titles(self, events: list) -> list:
        repair_tasks = []
        for idx, item in enumerate(events):
            titles = item.get("titles", {})
            missing = [
                lang for lang in self.languages
                if not titles.get(lang) or titles.get(lang) in ("Event", "Data pending", "")
            ]
            if missing:
                repair_tasks.append(self._repair_titles(idx, item, missing))

        if repair_tasks:
            logger.info(f"🔧 Repairing titles for {len(repair_tasks)} event(s)...")
            await asyncio.gather(*repair_tasks)
        return events

    async def _repair_titles(self, idx: int, item: dict, missing_langs: list):
        en_title = item.get("titles", {}).get("en", item.get("text", "Historical Event")[:80])
        year = item.get("year", "")
        slug = item.get("slug", "")

        lang_names = {"en": "English", "ro": "Romanian", "es": "Spanish", "de": "German", "fr": "French"}
        langs_str = ", ".join([f"{lang_names[l]} ({l})" for l in missing_langs])

        prompt = f"""
Translate this historical event title into: {langs_str}

ORIGINAL (English): {en_title}
CONTEXT: Year {year}, Wikipedia: {slug}

RULES: Concise (5-15 words), natural phrasing, standard proper nouns.

Return JSON with language codes as keys:
{{ {', '.join([f'"{l}": "title in {lang_names[l]}"' for l in missing_langs])} }}
"""

        res = await self._safe_groq_call(prompt, f"Title repair {idx}", {})
        titles = item.get("titles", {})
        for lang in missing_langs:
            fixed = res.get(lang, "")
            if fixed and len(fixed) > 2:
                titles[lang] = fixed
            else:
                titles[lang] = en_title
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
                        "content": "You are a strict History API. Output ONLY valid JSON. Never include markdown.",
                    },
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.4,
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