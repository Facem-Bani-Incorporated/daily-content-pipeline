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
        """Pasul 1: Clasificare și Scroring inițial."""
        candidates_text = "\n".join([
            f"ID_{i}: ({item['year']}) {item['text'][:200]}"
            for i, item in enumerate(candidates)
        ])

        prompt = f"""
        Return ONLY a JSON object. 
        List of allowed categories: {self.categories}.

        TASK:
        1. Categorize each ID using the list above.
        2. Score impact (0-100).
        3. Create engaging TITLES in: en, ro, es, de, fr.

        JSON STRUCTURE:
        {{
            "results": {{
                "ID_0": {{
                    "category": "science",
                    "score": 95,
                    "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
                }}
            }}
        }}
        INPUT:
        {candidates_text}
        """
        return await self._safe_groq_call(prompt, "Batch Analysis", {"results": {}})

    async def generate_multilingual_main_event(self, event_data: dict):
        """Pasul 2: Narațiune Premium 400 cuvinte."""
        prompt = f"""
        Write a professional history narrative of EXACTLY 400 words for EACH language: en, ro, es, de, fr.
        Topic: {event_data.get('text')} ({event_data.get('year')}).

        CRITICAL: All languages must have equal length and detail. Do not summarize.

        JSON STRUCTURE:
        {{
            "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }},
            "narratives": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
        }}
        """
        return await self._safe_groq_call(prompt, "Main Event", {"titles": {}, "narratives": {}})

    async def generate_secondary_narratives(self, secondary_candidates: list):
        """Pasul 3: Narațiuni 200 cuvinte pentru evenimentele secundare."""
        formatted_list = "\n".join([
            f"EVENT_{i}: ({item['year']}) {item['text']}"
            for i, item in enumerate(secondary_candidates)
        ])

        prompt = f"""
        Return ONLY JSON. For each EVENT_ID, write a 200-word narrative in: en, ro, es, de, fr.
        Minimum 150 words per language. Equal detail for Romanian and Spanish.

        INPUT:
        {formatted_list}

        JSON STRUCTURE:
        {{
            "EVENT_0": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
        }}
        """
        return await self._safe_groq_call(prompt, "Secondary Narratives", {})

    async def _safe_groq_call(self, prompt, context, fallback):
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system",
                     "content": "You are a rigid historian API. You only output valid, high-quality JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"🚨 AI Error ({context}): {e}")
            return fallback