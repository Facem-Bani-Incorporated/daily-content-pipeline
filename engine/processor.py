import json
from groq import Groq
from core.config import config
from enum import Enum


class EventCategory(str, Enum):
    WAR = "war"
    TECHNOLOGY = "technology"
    SCIENCE = "science"
    POLITICS = "politics"
    CULTURE = "culture"
    DISASTER = "disaster"
    DISCOVERY = "discovery"


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model
        self.categories = [c.value for c in EventCategory]

    async def batch_score_and_categorize(self, candidates: list):
        """Analizează candidații și generează titluri și rezumate scurte în 5 limbi."""
        candidates_text = "\n".join([
            f"ID_{i}: ({item['year']}) {item['text'][:150]}"
            for i, item in enumerate(candidates)
        ])

        prompt = f"""
        Return ONLY a JSON object. No prose.

        TASKS:
        1. Categorize each ID using ONLY: {self.categories}.
        2. Score impact (0-100).
        3. Create engaging TITLES in 5 languages (en, ro, es, de, fr).
        4. Create a SHORT SUMMARY (max 15 words) in 5 languages (en, ro, es, de, fr) for each event.

        STRUCTURE:
        {{
            "results": {{
                "ID_0": {{
                    "category": "science",
                    "score": 95,
                    "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }},
                    "summaries": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
                }}
            }}
        }}

        INPUT:
        {candidates_text}
        """

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system",
                     "content": "You are a professional historian and polyglot API. Output valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.15
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"AI Error: {e}")
            return {"results": {}}

    async def generate_multilingual_main_event(self, event_data: dict):
        """Generează narațiunea lungă (400 cuvinte) tradusă complet."""
        text = event_data.get('text', '')
        year = event_data.get('year', '')

        prompt = f"""
        Return ONLY JSON. Write a professional history narrative (approx 400 words) about: {text} ({year}).
        You must provide the narrative in 5 languages: English, Romanian, Spanish, German, and French.

        REQUIRED JSON STRUCTURE:
        {{
            "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }},
            "narratives": {{ 
                "en": "Full 400-word text...", 
                "ro": "Traducerea integrală de 400 cuvinte...", 
                "es": "Traducción completa...", 
                "de": "Vollständige Übersetzung...", 
                "fr": "Traduction complète..." 
            }}
        }}
        """

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system",
                     "content": "You are a world-class historian. Translate accurately and maintain the 400-word length for each language."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"Main Event AI Error: {e}")
            return {"titles": {}, "narratives": {}}