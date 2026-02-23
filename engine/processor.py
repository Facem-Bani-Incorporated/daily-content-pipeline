import json
from groq import Groq
from core.config import config
from schema.models import EventCategory


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model
        self.categories = [c.value for c in EventCategory]

    async def batch_score_and_categorize(self, candidates: list):
        candidates_text = "\n".join([
            f"ID_{i}: ({item['year']}) {item['text'][:200]}"
            for i, item in enumerate(candidates)
        ])

        prompt = f"""
        Return ONLY a JSON object. Allowed categories: {self.categories}.
        For each ID, provide:
        1. 'category': exactly one from the list.
        2. 'score': historical impact 0-100.
        3. 'titles': translations in en, ro, es, de, fr.

        JSON STRUCTURE:
        {{
            "results": {{
                "ID_0": {{
                    "category": "politics",
                    "score": 90,
                    "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
                }}
            }}
        }}
        INPUT:
        {candidates_text}
        """
        return await self._safe_groq_call(prompt, "Batch Analysis", {"results": {}})

    async def generate_multilingual_main_event(self, event_data: dict):
        # Am schimbat structura sa returneze 'narratives' direct pentru Translations(**narratives)
        prompt = f"""
        Write a professional history narrative of EXACTLY 400 words per language.
        Topic: {event_data.get('text')} ({event_data.get('year')}).
        Languages: en, ro, es, de, fr.

        CRITICAL: Each narrative must be at least 400 words. Do not summarize.

        JSON STRUCTURE:
        {{
            "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }},
            "narratives": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
        }}
        """
        return await self._safe_groq_call(prompt, "Main Event", {"titles": {}, "narratives": {}})

    async def generate_secondary_narratives(self, secondary_candidates: list):
        formatted_list = "\n".join([
            f"EVENT_{i}: ({item['year']}) {item['text']}"
            for i, item in enumerate(secondary_candidates)
        ])

        prompt = f"""
        For each EVENT_ID, write a 200-word narrative in: en, ro, es, de, fr.

        JSON STRUCTURE:
        {{
            "EVENT_0": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }},
            "EVENT_1": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
        }}
        """
        # Fallback-ul trebuie sa fie un dictionar gol pentru a evita erori de iterare
        return await self._safe_groq_call(prompt, "Secondary Narratives", {})

    async def _safe_groq_call(self, prompt, context, fallback):
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system",
                     "content": "You are a rigid historian API. You only output valid JSON. You never add prose outside the JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.1  # Scazut pentru maxima precizie
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"🚨 AI Error ({context}): {e}")
            return fallback