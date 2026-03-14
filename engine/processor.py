import asyncio
import json
from datetime import datetime
from groq import Groq
from core.config import config
from schema.models import EventCategory


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model  # "moonshotai/kimi-k2-instruct-0905"
        self.categories_list = [c.value for c in EventCategory]
        self.languages = ["en", "ro", "es", "de", "fr"]

    def _get_target_date_str(self, target_date: datetime) -> str:
        return target_date.strftime("%B %d")

    def _ensure_langs(self, data: dict, fallback_text: str = "Title pending") -> dict:
        if not isinstance(data, dict):
            data = {}
        return {lang: data.get(lang) or fallback_text for lang in self.languages}

    async def discover_events(self, target_date: datetime) -> list:
        """
        PASS 1 — Cast a wide net: AI returns 60 historical events for this date.
        No filtering yet — we want breadth across all eras and categories.
        """
        date_str = self._get_target_date_str(target_date)

        prompt = f"""
        You are a world-class historian. List exactly 60 historical events that occurred on {date_str} throughout all of recorded history.

        Cast the widest possible net:
        - Ancient history through modern times
        - All continents and civilizations
        - All domains: science, war, politics, exploration, culture, technology, religion, economics, natural disasters, sports milestones
        - Include both famous AND lesser-known but genuinely important events

        STRICT JSON SCHEMA — return ONLY this:
        {{
          "events": [
            {{
              "year": 1945,
              "text": "One clear sentence describing exactly what happened.",
              "slug": "Exact_Wikipedia_Article_Title",
              "category": "one_from_allowed_list",
              "ai_score": 75
            }}
          ]
        }}

        RULES:
        - "slug" = exact Wikipedia article title with underscores (e.g. "World_War_II", "Marie_Curie")
        - "category" must be one of: {self.categories_list}
        - "ai_score" = raw historical significance 0-100 (do NOT overthink this yet, just a first pass)
        - Return EXACTLY 60 events, no more, no less
        - NO duplicates
        """

        res = await self._safe_groq_call(prompt, "Event Discovery (60)", {"events": []})
        events = res.get("events", [])

        validated = []
        for e in events:
            if not isinstance(e.get("year"), int):
                continue
            if not e.get("text") or not e.get("slug"):
                continue
            if e.get("category") not in self.categories_list:
                e["category"] = EventCategory.CULTURE_ARTS.value
            e["ai_score"] = max(0, min(100, int(e.get("ai_score", 50))))
            validated.append(e)

        return validated

    async def deep_rank_and_select(self, candidates: list, target_date: datetime) -> list:
        """
        PASS 2 — Deep ranking: from 60 candidates, AI picks the 15 most globally important.

        Scoring criteria:
        - How many humans were affected (directly or indirectly)?
        - Did it change the course of history permanently?
        - Would someone from ANY country, culture, or background recognize this?
        - Does it have emotional resonance? (awe, fear, pride, curiosity)
        - Is it a "first ever" or "end of an era" moment?
        """
        date_str = self._get_target_date_str(target_date)
        candidates_text = "\n".join(
            [f"ID_{i}: ({e['year']}) {e['text'][:200]}" for i, e in enumerate(candidates)]
        )

        prompt = f"""
        You are ranking historical events for a global "Today in History" app used by people from every country.
        DATE: {date_str}

        From the list below, select and deeply score the 15 MOST GLOBALLY IMPACTFUL events.

        SCORING CRITERIA (apply all, no exceptions):
        1. GLOBAL REACH (0-30 pts): How many humans on Earth were affected? Billions > Millions > Thousands
        2. PERMANENCE (0-25 pts): Did this permanently alter the course of human history?
        3. UNIVERSAL RECOGNITION (0-20 pts): Would a person from Brazil, Japan, Nigeria, and Germany all know this?
        4. EMOTIONAL POWER (0-15 pts): Does it inspire awe, shock, pride, or wonder in any human?
        5. UNIQUENESS (0-10 pts): Was this a "first ever", "last ever", or "only time in history"?

        STRICT JSON SCHEMA:
        {{
          "top15": [
            {{
              "original_id": "ID_7",
              "deep_score": 94,
              "score_breakdown": {{
                "global_reach": 28,
                "permanence": 24,
                "universal_recognition": 19,
                "emotional_power": 14,
                "uniqueness": 9
              }},
              "titles": {{
                "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..."
              }}
            }}
          ]
        }}

        RULES:
        - Select EXACTLY 15 events from the list
        - Order by deep_score descending
        - Titles must be punchy, emotional, under 10 words each
        - Prefer events that would make someone stop scrolling and think "wow, this happened TODAY in history?"

        CANDIDATES:
        {candidates_text}
        """

        res = await self._safe_groq_call(prompt, "Deep Rank (15 from 60)", {"top15": []})
        top15_raw = res.get("top15", [])

        # Map back to original candidate data + enrich with deep scores
        id_to_candidate = {f"ID_{i}": e for i, e in enumerate(candidates)}
        enriched = []

        for entry in top15_raw:
            original_id = entry.get("original_id", "")
            candidate = id_to_candidate.get(original_id)
            if not candidate:
                continue

            breakdown = entry.get("score_breakdown", {})
            candidate["deep_score"] = max(0, min(100, int(entry.get("deep_score", 50))))
            candidate["score_breakdown"] = breakdown
            candidate["titles"] = self._ensure_langs(entry.get("titles", {}), "Historical Event")
            enriched.append(candidate)

        return enriched

    async def generate_secondary_narratives(self, top_events: list, target_date: datetime):
        """Generate short narratives for top 5 events in all 5 languages."""
        date_str = self._get_target_date_str(target_date)

        async def process_single(idx, item):
            event_tasks = [
                self._fetch_secondary_lang(idx, item, lang, date_str)
                for lang in self.languages
            ]
            lang_results = await asyncio.gather(*event_tasks)
            return f"EVENT_{idx}", dict(lang_results)

        tasks = [process_single(i, item) for i, item in enumerate(top_events)]
        final_results = await asyncio.gather(*tasks)
        return dict(final_results)

    async def _fetch_secondary_lang(self, idx, item, lang, date_str):
        event_info = f"{item['year']} — {item['text'][:300]}"
        prompt = f"""
        DATE IN HISTORY: {date_str}, {item['year']}.
        Write a 200-word engaging summary in {lang.upper()} about: {event_info}.

        RULES:
        - Present the event as occurring on {date_str}
        - Use vivid, journalistic language — write like a top newspaper journalist
        - Explain why this still matters to people alive today
        - Return ONLY JSON: {{ "content": "narrative text here" }}
        """
        res = await self._safe_groq_call(
            prompt, f"Narrative {idx} {lang}", {"content": "Historical data pending."}
        )
        return lang, res.get("content", "Historical data pending.")

    async def _safe_groq_call(self, prompt: str, context: str, fallback: dict) -> dict:
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a strict History API. "
                            "Output ONLY valid JSON matching the exact schema requested. "
                            "No markdown, no preamble, no extra keys."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.6,
                max_completion_tokens=4096,
                top_p=1,
                stream=False,
                stop=None,
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"🚨 AI Error ({context}): {e}")
            return fallback