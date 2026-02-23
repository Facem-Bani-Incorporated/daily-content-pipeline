import json
import asyncio
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


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model
        # Extragem categoriile exact cum sunt în Enum-ul tău pentru consistență (lowercase)
        self.categories = [c.value for c in EventCategory]

    async def batch_score_and_categorize(self, candidates: list):
        """Analizează rapid candidații pentru ierarhizare și clasificare."""
        candidates_text = "\n".join([
            f"ID_{i}: ({item['year']}) {item['text'][:200]}"
            for i, item in enumerate(candidates)
        ])

        prompt = f"""
        Return ONLY a JSON object. No prose.
        TASKS:
        1. Categorize each ID using ONLY these exact lowercase values: {self.categories}.
        2. Score impact (0-100) based on global historical significance.
        3. Create engaging TITLES in 5 languages (en, ro, es, de, fr).

        STRUCTURE:
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

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system",
                     "content": "You are a professional historian API. Output valid JSON only. Use lowercase for categories."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.1
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"AI Batch Error: {e}")
            return {"results": {}}

    async def generate_multilingual_main_event(self, event_data: dict):
        """Generează narațiunea PREMIUM (400+ cuvinte) tradusă complet."""
        text = event_data.get('text', '')
        year = event_data.get('year', '')

        prompt = f"""
        Write a professional history narrative of EXACTLY 400 words for EACH language. 
        Topic: {text} ({year}).
        Languages: English, Romanian, Spanish, German, French.

        CRITICAL: The Romanian, Spanish, German, and French versions must be as LONG and DETAILED as the English one. Do not summarize.

        REQUIRED JSON STRUCTURE:
        {{
            "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }},
            "narratives": {{ 
                "en": "[400 words]", 
                "ro": "[400 words]", 
                "es": "[400 words]", 
                "de": "[400 words]", 
                "fr": "[400 words]" 
            }}
        }}
        """

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system",
                     "content": "You are a world-class polyglot historian. You never summarize translations; you provide equal depth in all 5 languages."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"Main Event AI Error: {e}")
            return {"titles": {}, "narratives": {}}

    async def generate_secondary_narratives(self, secondary_candidates: list):
        """Generează narațiuni de 150-250 cuvinte pentru evenimentele secundare."""
        formatted_list = "\n".join([
            f"EVENT_{i}: ({item['year']}) {item['text']}"
            for i, item in enumerate(secondary_candidates)
        ])

        prompt = f"""
        For each event, write a history narrative of 200 words in 5 languages (en, ro, es, de, fr).

        STRICT RULES:
        1. Each language must have roughly the same length (approx 200 words).
        2. Do NOT cut the Romanian or Spanish versions short.
        3. Maintain a professional, educational tone.

        INPUT:
        {formatted_list}

        REQUIRED JSON STRUCTURE:
        {{
            "EVENT_0": {{
                "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..."
            }}
        }}
        """

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system",
                     "content": "You are a detailed historian. You must provide extensive translations. Minimum 150 words per language per event."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"Secondary Events AI Error: {e}")
            return {}