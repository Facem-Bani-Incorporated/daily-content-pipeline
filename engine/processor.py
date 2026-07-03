import asyncio
import json
import re
from datetime import datetime
import anthropic
from core.config import config
from schema.models import EventCategory
from core.logger import setup_logger

logger = setup_logger("AIProcessor")


def _parse_ai_json(message) -> dict:
    """Pull the text blocks out of an Anthropic message and parse JSON leniently.

    With extended thinking on, the response is [thinking_block, text_block]; we only
    want the text. Strips markdown fences and, as a last resort, the outermost braces.
    """
    text = "".join(
        b.text for b in message.content if getattr(b, "type", None) == "text"
    ).strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            return json.loads(text[start:end + 1])
        raise


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = anthropic.AsyncAnthropic(
            api_key=config.ANTHROPIC_API_KEY, timeout=600.0
        )
        self.model = model
        self.thinking_budget = config.AI_THINKING_BUDGET
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
                    "Emphasize the physical world just before this happened. "
                    "Put the reader in the place — the weather, the room, the street, what people could see and hear. "
                    "Show how ordinary the moment looked before it became historic. "
                    "Use duration and elapsed time as anchors: how long something had been building."
                ),
            },
            {
                "name": "HUMAN_FOCUS",
                "instruction": (
                    "Center the story on the people — their ages, backgrounds, what they wanted and feared. "
                    "Include the unknown figures present alongside the famous ones. "
                    "Make the reader feel the human scale: this was not 'history,' it was specific people "
                    "making specific decisions on a specific day."
                ),
            },
            {
                "name": "THE_BIG_MOMENT",
                "instruction": (
                    "Slow down on the exact moment itself. Use present tense. Be precise about sequence. "
                    "What happened first, what happened next, who moved, who spoke, what was the order of events. "
                    "If there's a known exact time, use it. The reader should feel like a witness."
                ),
            },
            {
                "name": "WHY_IT_MATTERED",
                "instruction": (
                    "Focus on consequences and scale: how many people were affected, what changed, for whom and how much. "
                    "Spend more time on the aftermath than the event itself. "
                    "Let the ripple effects — weeks, years, decades later — carry the weight of the story."
                ),
            },
            {
                "name": "THE_CONTRAST",
                "instruction": (
                    "Show the before and after. What did the world look like the day before this happened? "
                    "What was normal, expected, assumed to be permanent? "
                    "Then show how completely and quickly that changed. "
                    "The contrast is the story — not just the event."
                ),
            },
            {
                "name": "THE_NUMBERS_TELL",
                "instruction": (
                    "Let measurements, statistics, and figures carry the narrative. "
                    "Every paragraph should have at least one number that proves something. "
                    "Not decoration — evidence. Distances, costs, casualties, durations, temperatures, ages. "
                    "The numbers should make the scale visceral, not abstract."
                ),
            },
            {
                "name": "THE_WORLD_CHANGED",
                "instruction": (
                    "Frame it as a before/after in human understanding or capability. "
                    "What was impossible or unimaginable before this day? What became ordinary after? "
                    "Show the long arc: what took centuries to build, and what this event broke or created."
                ),
            },
            {
                "name": "THE_STORY_BEHIND",
                "instruction": (
                    "Lead with what most people don't know about this event. "
                    "The backstory, the hidden cause, the forgotten figure, the decision nobody remembers. "
                    "Reframe the familiar headline with the detail that changes its meaning entirely."
                ),
            },
        ]

        # ══════════════════════════════════════════════════════════
        # 6 narrative VOICES — a second axis of variation, orthogonal
        # to the angle above. The angle decides the STRUCTURE of the
        # piece; the voice decides its PERSONALITY. Each event gets a
        # unique (angle, voice) pair, so two articles can never read
        # the same way even on the same day.
        # ══════════════════════════════════════════════════════════
        self.narrative_voices = [
            {
                "name": "THE_RACONTEUR",
                "instruction": (
                    "Write like the best storyteller at the dinner table: warm, confident, "
                    "a little mischievous. Dry humor, perfect timing, the occasional aside that "
                    "makes the reader grin. You are not lecturing — you are letting them in on "
                    "a great story you can't wait to tell."
                ),
            },
            {
                "name": "THE_VIVID_EYE",
                "instruction": (
                    "Write like a cinematographer with a pen. Sensory, concrete, immediate — "
                    "what it looked like, sounded like, smelled like. The reader should see it, "
                    "not be told about it. Spare, sharp images over adjectives."
                ),
            },
            {
                "name": "THE_CURIOUS_MIND",
                "instruction": (
                    "Write like someone who just learned this and can't believe how cool it is. "
                    "Lead the reader through the 'wait, how did that even work?' moments. "
                    "Explain the mechanism, the trick, the science — and make the explanation "
                    "the most satisfying part of the piece."
                ),
            },
            {
                "name": "THE_WRY_OBSERVER",
                "instruction": (
                    "Write with a raised eyebrow. Find the irony, the absurd human detail, "
                    "the gap between what people intended and what actually happened. "
                    "Understated, never cruel, never forced — the humor comes from the truth, "
                    "not from jokes pasted on top."
                ),
            },
            {
                "name": "THE_LITERARY_HAND",
                "instruction": (
                    "Write like a novelist who happens to be telling something true. "
                    "Build a little tension, control the pace, let one image carry weight. "
                    "Lyrical but never purple — every sentence still earns its place and stays clear."
                ),
            },
            {
                "name": "THE_PLAINSPOKEN_GUIDE",
                "instruction": (
                    "Write crisp, direct, confident — but with a pulse. No throat-clearing, "
                    "no fluff, every sentence does work. Personality lives in the precision and "
                    "the well-chosen detail, not in decoration. Think great explainer, fast and clean."
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

    @staticmethod
    def _build_avoid_block(exclude_slugs: set = None) -> str:
        """Prompt block listing already-published slugs the AI must not repeat."""
        if not exclude_slugs:
            return ""
        listed = ", ".join(sorted(exclude_slugs))
        return (
            "\nALREADY PUBLISHED — do NOT return any of these Wikipedia articles. "
            "Find DIFFERENT events instead (go more niche if you must):\n"
            f"{listed}\n"
        )

    # ══════════════════════════════════════════════════════════════
    # PASS 1 — Discovery
    # ══════════════════════════════════════════════════════════════
    async def discover_events(self, target_date: datetime, exclude_slugs: set = None) -> list:
        date_str = self._get_target_date_str(target_date)
        month, day = self._get_month_day(target_date)
        avoid_block = self._build_avoid_block(exclude_slugs)

        prompt = f"""
You are a meticulous Senior Historian and Fact-Checker.
List historical events that occurred EXACTLY on {date_str} (month={month}, day={day}).

CRITICAL RULES:
1. DATE INTEGRITY: Every event MUST have occurred on EXACTLY {date_str}.
   - If an event started on a different day, it does NOT count.
2. WIKIPEDIA: "slug" MUST be the exact Wikipedia article title.
3. YEAR ACCURACY: Exact year of the event.
4. NO HALLUCINATIONS: If unsure, EXCLUDE. Accuracy always beats volume.
5. QUANTITY: Return as MANY accurate events as you can find — aim for 60+.
   Cast a wide net across all of world history for this exact day.
6. DIVERSITY: Different centuries, regions, categories.
7. DEPTH OVER FAME: if there aren't many globally famous events on this date,
   dig deeper instead of giving up — include well-documented but lesser-known
   ones (regional milestones, scientific/technical firsts, notable births &
   deaths, cultural or sporting curiosities). A thin or empty list is a failure:
   always come back with a rich set of real events. Lesser-known is welcome;
   invented or misdated is not.
{avoid_block}
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

        res = await self._safe_ai_call(prompt, f"Discovery ({date_str})", {"events": []})
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
    async def discover_pro_events(self, target_date: datetime, exclude_slugs: set = None) -> list:
        date_str = self._get_target_date_str(target_date)
        month, day = self._get_month_day(target_date)
        pro_cats = ["personalities", "media", "sport"]
        avoid_block = self._build_avoid_block(exclude_slugs)

        prompt = f"""
You are a Senior Pop-Culture & Entertainment Historian.
List PREMIUM historical events for {date_str} (month={month}, day={day}) — STRICTLY from:

1. **personalities** — births/deaths of globally iconic people
2. **media** — milestone events in film, TV, music, radio, publishing
3. **sport** — historic sporting moments

HARD RULES:
1. DATE: Must be EXACTLY {date_str}.
2. WIKIPEDIA: "slug" must match exact article title.
3. FAME: Prefer globally famous people/events — but if a category is thin for this
   date, go deeper and add strong regional or era-defining picks so every category
   is represented. Never leave a category empty. Never invent; accuracy over fame.
4. Aim for 6+ per category, 25-40 total. More is better.
5. Only HIGH confidence.
{avoid_block}
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

        res = await self._safe_ai_call(
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

        res = await self._safe_ai_call(prompt, "PRO Deep Rank", {"selections": []})

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
You are a rigorous historian curating a "Today in History" feed for {date_str}.
From the CANDIDATES below, select and rank the 15 most significant events.

Reason through each candidate before scoring it. Score on five components:
  • GLOBAL REACH (0–30) — did it affect the whole world, or just one region?
  • PERMANENCE (0–25) — are the consequences still felt today?
  • UNIVERSAL RECOGNITION (0–20) — would an educated person anywhere recognize it?
  • EMOTIONAL POWER (0–15) — human drama, stakes, a story worth telling.
  • UNIQUENESS (0–5) — a real "first" or turning point, not a routine occurrence.

deep_score = the sum of those five components (0–100). Rank by deep_score, highest first.

HARD RULES:
  1. World-changing beats locally-important. A famous-but-regional event still loses
     to a pivotal global one — significance, not mere name recognition.
  2. DIVERSITY: the top 15 must span at least 3 different centuries AND 3 different
     categories. Do not stack the list with one era or one theme.
  3. Only rank candidates that are given, by their exact ID. Never invent events.
  4. Titles are short, vivid, specific headlines — never "An event on {date_str}".

Return ONLY this JSON (score_breakdown MUST sum to deep_score):
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

        res = await self._safe_ai_call(prompt, "Deep Rank", {"top15": []})
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

        style_assignments = self._assign_narrative_styles(top_events)
        for idx, item in enumerate(top_events):
            style = style_assignments[idx]
            logger.info(
                f"📖 Event {idx} ({item.get('slug', '')[:30]}) → "
                f"{style['angle']['name']} / {style['voice']['name']}"
            )

        async def process_single(idx, item):
            style = style_assignments[idx]
            lang_results = await asyncio.gather(*[
                self._fetch_narrative_lang(idx, item, lang, date_str, style)
                for lang in self.languages
            ])
            return f"EVENT_{idx}", dict(lang_results)

        results = dict(
            await asyncio.gather(*[process_single(i, item) for i, item in enumerate(top_events)])
        )

        results = await self._verify_and_patch_narratives(
            results, top_events, date_str, style_assignments
        )
        self._audit_opening_diversity(results)
        return results

    def _assign_narrative_styles(self, items: list) -> list:
        """
        Assign each event a unique (angle, voice) pair.

        - The angle controls the STRUCTURE of the piece, the voice its PERSONALITY.
        - Seed = day + the batch's slugs, so the FREE batch and the PRO batch get
          different shuffles (no more "event 0 of both tiers gets the same angle").
        - Angles (8) and voices (6) are walked in lockstep with a uniqueness guard,
          so for any realistic batch size every event gets a distinct combination.
        """
        import random
        import hashlib

        count = len(items)
        day_seed = datetime.now().strftime("%Y-%m-%d")
        salt = "|".join(sorted((it.get("slug") or "") for it in items))
        seed = int(hashlib.md5(f"{day_seed}::{salt}".encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)

        angles = list(self.storytelling_angles)
        voices = list(self.narrative_voices)
        rng.shuffle(angles)
        rng.shuffle(voices)

        styles = []
        used_pairs = set()
        for i in range(count):
            angle = angles[i % len(angles)]
            voice = voices[i % len(voices)]
            # Guarantee the (angle, voice) pair is unique within this batch.
            guard = 0
            max_guard = len(angles) * len(voices)
            while (angle["name"], voice["name"]) in used_pairs and guard < max_guard:
                guard += 1
                voice = voices[(i + guard) % len(voices)]
                if (angle["name"], voice["name"]) in used_pairs:
                    angle = angles[(i + guard) % len(angles)]
            used_pairs.add((angle["name"], voice["name"]))
            styles.append({"angle": angle, "voice": voice})

        return styles

    async def _fetch_narrative_lang(
        self, idx: int, item: dict, lang: str, date_str: str, style: dict
    ) -> tuple:
        max_retries = 3
        year = item.get("year", "")
        text = item.get("text", "")
        slug = item.get("slug", "")
        location = item.get("location") or "the location"
        angle = style["angle"]
        voice = style["voice"]

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
You are an essayist and storyteller writing for a smart, curious reader — think of the
popular-history writers who can make you laugh, teach you something real, and keep you
reading to the very last line. One article, one event, around 600 words. Write in {lang_full}.

EVENT: {year} — {text}
WIKIPEDIA: {slug}
DATE: {date_str}, {year}
LOCATION: {location}

STRUCTURAL LENS — {angle['name']}:
{angle['instruction']}

VOICE — {voice['name']}:
{voice['instruction']}

This article must NOT sound like the other pieces published the same day. Its shape comes
from the lens; its personality comes from the voice. Make both unmistakable. If a reader saw
two of your articles side by side, they should feel written by two different, talented people.

TONE — the constant beneath every voice: be deeply informative first. The reader should
finish genuinely knowing more than they did — the facts, the how, the why, delivered with
real authority. Underneath that, run a thread of dry, intelligent irony: notice the absurd,
the gap between what people intended and what they actually got. Let a genuinely funny line
land RARELY — once, maybe twice in the whole piece — and only where the material earns it.
Wit is seasoning, not the meal: if every paragraph is winking, it turns smug and exhausting.
Most sentences do straight, substantial work; the humor is the occasional glint, never the
point. Never force it onto tragedy. The voice above only changes HOW this is delivered.

HOW TO WRITE IT:
- Open with the single most interesting thing you know about this event — a scene, a strange
  number, a line someone actually said. Never open with the date or with "On this day."
  The first sentence is a hook, not a header.
- Explain what happened so clearly a curious teenager would understand it — but never talk
  down. If there's science, engineering, or politics underneath, unpack it in plain language.
  The explanation should be the satisfying part, not a chore.
- Drop in one or two genuinely surprising facts — the kind of detail a reader repeats to
  someone else the same day. Delightful trivia, not filler. Make me go "huh, I didn't know that."
- Keep it mostly informative; let dry irony surface where the material invites it, and a
  genuinely funny observation only once or twice in the whole piece — a wry aside, never a
  joke pasted on. Read the room: never force humor onto tragedy, never be needlessly solemn.
- Use real numbers as evidence, woven into the sentences, never as a list. "Many people died"
  tells nothing; "Of the 300 who went in, 11 walked out" tells everything.
- Name real people. Quote them when you know the actual words.
- End on the detail that reframes the whole thing — an irony, a forgotten consequence, a twist.
  No summary, no moral, no "and that is why." Just land the last image and stop.

RHYTHM: Mix short punchy sentences with longer flowing ones. Present tense for the key moment,
past tense for everything else. Vary paragraph length. If a sentence is boring, cut it.

BANNED PHRASES (clichés that make every article sound the same):
"it is worth noting" / "history tells us" / "changed the course of history" /
"left an indelible mark" / "without a doubt" / "subsequently" / "in conclusion" /
"serves as a reminder" / "stands as a testament" / "it is no coincidence" /
"little did they know" / "on this day" / "fast forward" / "needless to say".

LENGTH: 500–800 words. Aim for 600. A tight 350 words beats a padded 700. No headers.
Paragraphs separated by blank lines.
LANGUAGE: Entire text in {lang_full}. Zero English except proper nouns.

Return JSON: {{ "content": "full article here — paragraphs separated by blank lines" }}
"""

            res = await self._safe_ai_call(
                prompt,
                f"Narrative {idx}:{lang} (attempt {attempt})",
                {"content": ""},
                temperature=0.8,
                max_tokens=4096,
            )
            content = res.get("content", "")
            last_content = content

            is_valid, reason = self._validate_narrative(content, lang, style)
            if is_valid:
                logger.info(f"✅ Narrative {idx}:{lang} — passed (attempt {attempt})")
                return lang, content
            else:
                logger.warning(
                    f"⚠️ Narrative {idx}:{lang} attempt {attempt}: {reason}"
                )

        logger.error(f"🚨 Narrative {idx}:{lang} — all {max_retries} attempts failed")
        return lang, last_content if last_content else ""

    def _validate_narrative(self, content: str, lang: str, style: dict) -> tuple:
        if not content or len(content.strip()) < 50:
            return False, "Empty or too short"

        word_count = len(content.split())

        # Hard floor: anything under 200 words is a broken/truncated response → retry
        if word_count < 200:
            return False, f"Too short: {word_count} words (hard min 200)"

        # Soft floor: 300–500 is acceptable, just logged as a warning (no retry)
        # Hard ceiling: above 1200 is unlikely to be tight prose → retry
        if word_count > 1200:
            return False, f"Too long: {word_count} words (max 1200)"

        # Broken/placeholder content → always retry
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

        # Numbers check: warn but don't retry — a short factual piece may naturally have fewer
        numbers_found = re.findall(r'\b\d[\d.,]*\b', content)
        if len(numbers_found) < 3:
            logger.warning(f"⚠️ Narrative [{lang}]: only {len(numbers_found)} numbers found (prefer ≥3)")

        return True, "OK"

    async def _verify_and_patch_narratives(
        self, results: dict, top_events: list, date_str: str, style_assignments: list
    ) -> dict:
        patch_tasks = []

        for idx, item in enumerate(top_events):
            event_key = f"EVENT_{idx}"
            narratives = results.get(event_key, {})

            en_content = narratives.get("en", "")
            if not en_content or len(en_content.split()) < 200:
                logger.error(f"🚨 Event {idx}: English missing or too short — regenerating")
                patch_tasks.append(
                    self._emergency_regenerate(
                        idx, item, "en", date_str, style_assignments[idx], results
                    )
                )

            for lang in ["ro", "es", "de", "fr"]:
                content = narratives.get(lang, "")
                is_valid, reason = self._validate_narrative(
                    content, lang, style_assignments[idx]
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
            # Use `or` so that an empty string ("") also triggers the fallback,
            # not just a missing key. Empty string means generation failed entirely.
            en_text = results[event_key].get("en") or ""
            fallback_text = en_text if len(en_text.strip()) >= 200 else "Narrative unavailable."
            for lang in self.languages:
                current = results[event_key].get(lang) or ""
                if not current or len(current.strip()) < 200:
                    logger.error(f"🚨 CRITICAL: {idx}:{lang} still missing or too short")
                    results[event_key][lang] = fallback_text

        return results

    async def _emergency_regenerate(
        self, idx: int, item: dict, lang: str, date_str: str, style: dict, results: dict
    ):
        _, content = await self._fetch_narrative_lang(idx, item, lang, date_str, style)
        event_key = f"EVENT_{idx}"
        if event_key not in results:
            results[event_key] = {}
        # Only update if we actually got something back
        if content and len(content.strip()) >= 200:
            results[event_key][lang] = content
        else:
            logger.error(f"🚨 Emergency regenerate for {idx}:{lang} also failed — keeping previous value")
            if not results[event_key].get(lang):
                results[event_key][lang] = content  # keep whatever we have, even if short

    async def _patch_from_english(self, idx: int, target_lang: str, results: dict):
        event_key = f"EVENT_{idx}"
        en_content = results.get(event_key, {}).get("en", "")
        if not en_content or len(en_content.split()) < 200:
            logger.error(f"🚨 Cannot patch {idx}:{target_lang} — English missing or too short")
            return

        lang_names = {"ro": "Romanian", "es": "Spanish", "de": "German", "fr": "French"}
        lang_full = lang_names.get(target_lang, target_lang.upper())

        prompt = f"""
Translate this historical narrative into {lang_full}.

Keep the voice — the short punchy sentences, the rhythm, the journalist tone.
Do not smooth it into academic prose. If the English has a fragment for impact, keep the fragment.
All numbers stay as digits. Proper nouns use their standard {lang_full} form.
Blank lines between paragraphs. First sentence stays under 10 words.
Output only in {lang_full} — no English except proper nouns.

ENGLISH:
{en_content}

Return JSON: {{ "content": "translated narrative in {lang_full}" }}
"""

        res = await self._safe_ai_call(
            prompt, f"Translation {idx}:{target_lang}", {"content": ""}, temperature=0.3, max_tokens=4096
        )
        translated = res.get("content", "")

        if translated and len(translated.split()) >= 200:
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

        res = await self._safe_ai_call(prompt, f"Title repair {idx}", {})
        titles = item.get("titles", {})
        for lang in missing_langs:
            fixed = res.get(lang, "")
            if fixed and len(fixed) > 2:
                titles[lang] = fixed
            else:
                titles[lang] = en_title
        item["titles"] = titles

    # ══════════════════════════════════════════════════════════════
    # SAFE AI CALL  (Claude Haiku + extended thinking)
    # ══════════════════════════════════════════════════════════════
    async def _safe_ai_call(
        self,
        prompt: str,
        context: str,
        fallback: dict,
        temperature: float = 0.4,   # kept for call-site compatibility; unused with thinking
        max_tokens: int = 4096,
        thinking_budget: int | None = None,
    ) -> dict:
        budget = self.thinking_budget if thinking_budget is None else thinking_budget
        try:
            kwargs = {
                "model": self.model,
                # thinking tokens count toward output, so give the answer its own headroom
                "max_tokens": (budget + max_tokens) if budget else max_tokens,
                "system": (
                    "You are a strict History API. Output ONLY valid JSON. "
                    "No markdown, no code fences, no commentary."
                ),
                "messages": [{"role": "user", "content": prompt}],
            }
            if budget:
                kwargs["thinking"] = {"type": "enabled", "budget_tokens": budget}
            message = await self.client.messages.create(**kwargs)
            return _parse_ai_json(message)
        except json.JSONDecodeError as e:
            logger.error(f"🚨 JSON Parse Error ({context}): {e}")
            return fallback
        except Exception as e:
            logger.error(f"🚨 AI Error ({context}): {e}")
            return fallback

