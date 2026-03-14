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
        date_str = self._get_target_date_str(target_date)
        prompt = f"""
        Today is {date_str}. List the 15 most historically significant events that occurred on {date_str} throughout history.

        Selection criteria (in order of priority):
        1. Global impact — changed the world, affected millions of people
        2. Memorability — events people know and care about
        3. Diversity — mix of categories: science, politics, culture, war, exploration, technology
        4. Verifiability — events with a clear Wikipedia article

        STRICT JSON SCHEMA — return ONLY this, no extra text:
        {{
          "events": [
            {{
              "year": 1969,
              "text": "One sentence description of what happened.",
              "slug": "Apollo_11",
              "category": "science_technology",
              "ai_score": 92
            }}
          ]
        }}

        RULES:
        - "slug" must be the exact Wikipedia article title (use underscores, e.g. "French_Revolution")
        - "category" must be one of: {self.categories_list}
        - "ai_score" is your 0-100 confidence that this event is historically significant and engaging
        - Order events by ai_score descending
        - Return exactly 15 events
        """
        res = await self._safe_groq_call(prompt, "Event Discovery", {"events": []})
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

    async def batch_score_and_categorize(self, candidates: list, target_date: datetime):
        date_str = self._get_target_date_str(target_date)
        candidates_text = "\n".join(
            [f"ID_{i}: ({item['year']}) {item['text'][:250]}" for i, item in enumerate(candidates)]
        )
        prompt = f"""
        DATE: {date_str}
        These historical events occurred on {date_str}. For each, provide:
        1. A refined impact score (0-100) based on historical significance and modern relevance
        2. Engaging multilingual titles (short, punchy, under 10 words each)

        STRICT JSON SCHEMA:
        {{
          "results": {{
            "ID_0": {{
              "score": 88,
              "titles": {{
                "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..."
              }}
            }}
          }}
        }}

        EVENTS:
        {candidates_text}
        """
        res = await self._safe_groq_call(prompt, "Batch Scoring", {"results": {}})
        results = res.get("results", {})
        for vid, data in results.items():
            data["titles"] = self._ensure_langs(data.get("titles", {}), "Historical Event")
            data["score"] = max(0, min(100, int(data.get("score", 50))))
        return {"results": results}

    async def generate_secondary_narratives(self, top_events: list, target_date: datetime):
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
        - Use vivid, journalistic language
        - Focus on why this matters today
        - Return ONLY JSON: {{ "content": "narrative text here" }}
        """
        res = await self._safe_groq_call(prompt, f"Narrative {idx} {lang}", {"content": "Historical data pending."})
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