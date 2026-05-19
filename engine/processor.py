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
        # 8 storytelling angles — each specifies WHAT TYPE of number
        # or stat to lead with. The first sentence is always a hook
        # built around a specific figure, duration, or count.
        # All angles anchor date+place within the first 2 sentences.
        # ══════════════════════════════════════════════════════════
        self.storytelling_angles = [
            {
                "name": "SCENE_SETTING",
                "instruction": (
                    "Lead with an EXACT DURATION — how long something had been building, waiting, or standing "
                    "before this moment. The first sentence must be a short punchy duration fact (e.g. '28 years. "
                    "That is how long the wall had divided Berlin.'). Then place the reader in the exact time and "
                    "location within the next sentence. Make the duration feel personal — not just historical."
                ),
            },
            {
                "name": "HUMAN_FOCUS",
                "instruction": (
                    "Lead with the person's AGE or a BIOGRAPHICAL NUMBER at the time of the event. "
                    "First sentence: short, punchy age or personal stat (e.g. '38 years old. That was Neil Armstrong "
                    "when he stepped onto the Moon.' or '52 years old, and Ray Kroc had never run a restaurant.'). "
                    "Anchor date and place in the next sentence. Make the reader feel the human scale of the story."
                ),
            },
            {
                "name": "THE_BIG_MOMENT",
                "instruction": (
                    "Lead with the EXACT TIME OF DAY, a precise countdown, or a sequence number — the smallest "
                    "unit of time that captures the moment (e.g. '8:15 AM.' or '12 seconds.' or 'The 3rd attempt.'). "
                    "First sentence: the time or count, nothing else — then immediately name the date and place. "
                    "Then zoom into what happened in that precise instant."
                ),
            },
            {
                "name": "WHY_IT_MATTERED",
                "instruction": (
                    "Lead with the SCALE NUMBER — how many people were affected, how much money was at stake, "
                    "how many countries changed course (e.g. '240,000 people.' or '$1 billion.' or '47 nations.'). "
                    "First sentence: just the number and what it represents — under 10 words. "
                    "Then anchor date and location. Then explain why that scale had to be understood immediately."
                ),
            },
            {
                "name": "THE_CONTRAST",
                "instruction": (
                    "Lead with the BEFORE/AFTER RATIO or percentage — what changed numerically from one day to the next "
                    "(e.g. '1 hour earlier, it was a normal Tuesday.' or 'In 47 seconds, a city of 1 million lost everything.'). "
                    "First sentence: the contrast stated as a number or time-gap — sharp and short. "
                    "Anchor date and place in sentence two. Then show the normal life that existed right before."
                ),
            },
            {
                "name": "THE_NUMBERS_TELL",
                "instruction": (
                    "Lead with THE SINGLE MOST STRIKING NUMBER of the entire event — the one that makes readers stop. "
                    "First sentence: the number alone, or with minimal context — under 10 words. "
                    "Then anchor date and location. Use numbers throughout this narrative more than any other angle — "
                    "each paragraph must contain at least one figure, stat, or measurement."
                ),
            },
            {
                "name": "THE_WORLD_CHANGED",
                "instruction": (
                    "Lead with HOW MANY YEARS or CENTURIES had passed since the last time this happened — or that it "
                    "was the FIRST TIME EVER in a number of years (e.g. '3,000 years of trying. Then it finally worked.' "
                    "or 'For 400 years, no human had done this.'). First sentence: the time-span — short and astonishing. "
                    "Then show what made this particular day the one that broke the record."
                ),
            },
            {
                "name": "THE_STORY_BEHIND",
                "instruction": (
                    "Lead with A LESSER-KNOWN NUMBER that reveals the hidden backstory — something that reframes "
                    "what most people think they know (e.g. '17 rejections.' or 'The patent cost $15.' or '3 days before "
                    "anyone noticed.'). First sentence: the surprising number — under 10 words. "
                    "Anchor date and place in sentence two. Then unfold the backstory that the number unlocks."
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
        """
        Selects 4 PRO events:
          - 1 event from each of the 3 categories (personalities, media, sport)
          - 1 EXTRA event (the next-best one across all categories)
        Total: 4 events, with at least 1 per category guaranteed.
        """
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
For {date_str}, select 4 events total:
  - 1 BEST event from EACH of the 3 categories (personalities, media, sport)
  - 1 EXTRA event (the next-best one, from any category)

That's 4 events total. The extra must be different from the 3 main picks.

CRITERIA: global fame, storytelling potential, emotional impact, shareability.

STRICT JSON:
{{
  "selections": [
    {{
      "original_id": "ID_0",
      "category": "personalities",
      "deep_score": 92,
      "is_extra": false,
      "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
    }},
    {{
      "original_id": "ID_3",
      "category": "media",
      "deep_score": 88,
      "is_extra": false,
      "titles": {{ ... }}
    }},
    {{
      "original_id": "ID_8",
      "category": "sport",
      "deep_score": 85,
      "is_extra": false,
      "titles": {{ ... }}
    }},
    {{
      "original_id": "ID_2",
      "category": "personalities",
      "deep_score": 90,
      "is_extra": true,
      "titles": {{ ... }}
    }}
  ]
}}

CANDIDATES:
{candidates_text}
"""

        res = await self._safe_groq_call(prompt, "PRO Deep Rank", {"selections": []})

        selected = []
        seen_ids = set()
        cats_filled = set()  # tracks which of the 3 main category slots are filled

        # First pass: fill the 3 main category slots (1 per category)
        for entry in res.get("selections", []):
            original_id = entry.get("original_id")
            cat = entry.get("category", "").lower()
            is_extra = entry.get("is_extra", False)

            if is_extra:
                continue  # handle extras after main slots
            if original_id not in id_map or original_id in seen_ids:
                continue
            if cat not in {"personalities", "media", "sport"}:
                continue
            if cat in cats_filled:
                continue

            item = id_map[original_id]
            item.update({
                "deep_score": entry.get("deep_score", 50),
                "titles": self._ensure_langs(entry.get("titles", {})),
                "is_pro": True,
            })
            selected.append(item)
            seen_ids.add(original_id)
            cats_filled.add(cat)

        # Fallback: fill any missing main category from highest ai_score in that bucket
        for cat_name in ["personalities", "media", "sport"]:
            if cat_name in cats_filled:
                continue
            pool = sorted(
                [c for c in buckets[cat_name] if f"ID_{list(id_map.values()).index(c)}" not in seen_ids]
                if buckets[cat_name] else [],
                key=lambda x: x.get("ai_score", 0),
                reverse=True,
            )
            # Simpler fallback — just take the top of the bucket if not already picked
            for cand in sorted(buckets[cat_name], key=lambda x: x.get("ai_score", 0), reverse=True):
                cand_id = next((k for k, v in id_map.items() if v is cand), None)
                if cand_id and cand_id not in seen_ids:
                    cand.update({
                        "deep_score": cand.get("ai_score", 50),
                        "titles": self._ensure_langs({}),
                        "is_pro": True,
                    })
                    selected.append(cand)
                    seen_ids.add(cand_id)
                    cats_filled.add(cat_name)
                    logger.warning(f"⚠️ PRO fallback for '{cat_name}': {cand['slug']}")
                    break

        # Second pass: add the EXTRA (4th event)
        extra_added = False
        for entry in res.get("selections", []):
            if not entry.get("is_extra", False):
                continue
            original_id = entry.get("original_id")
            if original_id not in id_map or original_id in seen_ids:
                continue

            item = id_map[original_id]
            item.update({
                "deep_score": entry.get("deep_score", 50),
                "titles": self._ensure_langs(entry.get("titles", {})),
                "is_pro": True,
            })
            selected.append(item)
            seen_ids.add(original_id)
            extra_added = True
            logger.info(f"⭐ PRO extra added: [{item.get('category')}] {item.get('slug')}")
            break  # only one extra

        # Fallback for extra: pick highest-score remaining candidate
        if not extra_added:
            remaining = []
            for cat_name in ["personalities", "media", "sport"]:
                for cand in buckets[cat_name]:
                    cand_id = next((k for k, v in id_map.items() if v is cand), None)
                    if cand_id and cand_id not in seen_ids:
                        remaining.append((cand, cand_id))

            if remaining:
                remaining.sort(key=lambda x: x[0].get("ai_score", 0), reverse=True)
                cand, cand_id = remaining[0]
                cand.update({
                    "deep_score": cand.get("ai_score", 50),
                    "titles": self._ensure_langs({}),
                    "is_pro": True,
                })
                selected.append(cand)
                seen_ids.add(cand_id)
                logger.warning(
                    f"⚠️ PRO extra fallback: [{cand.get('category')}] {cand['slug']}"
                )

        # Log summary
        by_cat = {}
        for ev in selected:
            c = ev.get("category", "?")
            by_cat[c] = by_cat.get(c, 0) + 1
        logger.info(f"🏆 PRO selected {len(selected)} events → {by_cat}")
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
You are a journalist writing for a history magazine. Your readers are curious 25-year-olds
who want to be surprised, not lectured. Write a narrative in **{lang_full} ({lang.upper()})**.

EVENT: {year} — {text}
WIKIPEDIA: {slug}
DATE: {date_str}, {year}
LOCATION: {location}

═══════════════════════════════════════════════════════
STORYTELLING ANGLE: {angle['name']}
{angle['instruction']}
═══════════════════════════════════════════════════════

MANDATORY 4-PARAGRAPH STRUCTURE (no headers, flowing prose):

PARAGRAPH 1 — THE HOOK:
Apply the {angle['name']} angle above. The VERY FIRST WORDS must be a NUMBER, STAT, or
shocking specific fact — this is non-negotiable. First sentence: MAX 10 WORDS.
Then anchor the date ({date_str}, {year}) and location ({location}) within 2 sentences.
BAD: "Anne Boleyn was an important queen of England."
GOOD: "1,000 days. That is all Anne Boleyn had beside Henry VIII."
GOOD: "19 years old. That was how old he was when everything changed."

PARAGRAPH 2 — THE FACTS (MAX 60 words):
What exactly happened. At least 1 specific number, duration, or quantity.
Mix short sentences (3–7 words) with medium ones. No filler phrases.
Include at least 1 detail that 95% of readers don't know.
FORBIDDEN: "it is known that" / "history tells us" / "he was a man who."

PARAGRAPH 3 — THE CONTEXT (MAX 60 words):
Why did this happen? What built up to it?
At least 1 specific number, date, or duration showing scale or timeline.
Use present tense for dramatic moments. Direct statements, zero hedging.

PARAGRAPH 4 — THE TWIST (MAX 60 words):
The unexpected consequence, the historical irony, or what happened the day/year AFTER.
A number or stat that recontextualizes everything that came before.
Connect to something the reader recognizes today. This is the paragraph that makes
readers say "I didn't know that." End here — no summary, no conclusion sentence.

═══════════════════════════════════════════════════════
ABSOLUTE RULES:
1. LENGTH: 180–250 words TOTAL. Every word earns its place.
2. LANGUAGE: Entire text in {lang_full}. Zero English except proper nouns.
3. FIRST SENTENCE: MAX 10 WORDS. MUST contain a number or stat.
4. PARAGRAPHS: Separate with a blank line. Each max 60 words.
5. NUMBERS: Minimum 2 specific numbers/statistics in the full narrative.
6. TONE: Journalist, not historian. Confident. No "it is said that."
7. NO AI VOICE: No "I will tell you," "let me explain," no meta-commentary.
8. PRESENT TENSE: Use it for dramatic moments — not throughout, just where it hits.
9. NO ACADEMIC WORDS: "subsequent" → "after"; "ramifications" → "consequences";
   "unprecedented" → cut it entirely; "geopolitical" → "political."

CHECKLIST before submitting:
- [ ] First sentence contains a NUMBER and is under 10 words?
- [ ] At least 2 numbers/stats across the narrative?
- [ ] Each paragraph under 60 words?
- [ ] Total 180–250 words?
- [ ] Paragraph 4 ends with a twist, irony, or surprising fact?
- [ ] Entire text in {lang_full}?

Return JSON: {{ "content": "your narrative here — 4 paragraphs separated by blank lines" }}
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
        if word_count < 150:
            return False, f"Too short: {word_count} words (min 150)"
        if word_count > 350:
            return False, f"Too long: {word_count} words (max 350)"

        bad_markers = [
            "narrative pending", "content pending", "error generating",
            "i apologize", "i'm sorry", "as an ai", "i cannot",
            "let me tell you", "in this article", "in this story",
        ]
        content_lower = content.lower()
        for marker in bad_markers:
            if marker in content_lower:
                return False, f"Contains placeholder/AI text: '{marker}'"

        # Require at least 2 numbers/statistics in the narrative
        numbers_found = re.findall(r'\b\d[\d.,]*\b', content)
        if len(numbers_found) < 2:
            return False, f"Too few numbers/stats: {len(numbers_found)} found (min 2 required)"

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
1. Preserve the short punchy sentence rhythm — do NOT smooth it into long flowing sentences.
2. Keep ALL numbers and statistics exactly as they appear (digits, not spelled out).
3. Translate faithfully — don't add or remove information.
4. Use natural, fluent {lang_full} — not word-for-word mechanical translation.
5. Keep proper nouns in their standard {lang_full} forms.
6. Entire output in {lang_full}. No English except proper nouns.
7. Preserve the 4-paragraph structure with blank lines between paragraphs.
8. The first sentence of the translated text must remain SHORT (under 10 words).

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