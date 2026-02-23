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
    DISCOVERY = "discovery"

class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model
        self.categories = [c.value for c in EventCategory]

    async def batch_score_and_categorize(self, candidates: list):
        """
        Pasul 1: Analizează rapid toți candidații pentru a-i ierarhiza.
        Produce scoruri, categorii și titluri scurte.
        """
        candidates_text = "\n".join([
            f"ID_{i}: ({item['year']}) {item['text'][:150]}"
            for i, item in enumerate(candidates)
        ])

        prompt = f"""
        Return ONLY a JSON object. No prose.
        TASKS:
        1. Categorize each ID using ONLY: {self.categories}.
        2. Score impact (0-100) based on historical significance.
        3. Create engaging TITLES in 5 languages (en, ro, es, de, fr).
        4. Create a VERY SHORT SUMMARY (1 sentence) in 5 languages.

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
                    {"role": "system", "content": "You are a professional historian API. Output valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.15
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"AI Batch Error: {e}")
            return {"results": {}}

    async def generate_multilingual_main_event(self, event_data: dict):
        """
        Pasul 2: Generează narațiunea PREMIUM (400+ cuvinte) pentru evenimentul principal.
        """
        text = event_data.get('text', '')
        year = event_data.get('year', '')

        prompt = f"""
        Return ONLY JSON. Write a professional history narrative (approx 400 words) about: {text} ({year}).
        Provide the narrative in 5 languages: English, Romanian, Spanish, German, and French.
        Ensure historical accuracy and an engaging educational tone.

        REQUIRED JSON STRUCTURE:
        {{
            "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }},
            "narratives": {{ 
                "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." 
            }}
        }}
        """

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a world-class historian. Maintain 400 words per language."},
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
        """
        Pasul 3: Generează narațiuni medii (150-250 cuvinte) pentru evenimentele secundare.
        Trimitem toate evenimentele într-un singur apel pentru eficiență.
        """
        formatted_list = "\n".join([
            f"EVENT_{i}: ({item['year']}) {item['text']}"
            for i, item in enumerate(secondary_candidates)
        ])

        prompt = f"""
        Return ONLY a JSON object. 
        For each event provided, write a medium-length history narrative (150-250 words).
        You must provide these narratives in 5 languages: en, ro, es, de, fr.

        INPUT:
        {formatted_list}

        REQUIRED JSON STRUCTURE:
        {{
            "EVENT_0": {{
                "en": "Narrative...",
                "ro": "Narațiune...",
                "es": "...",
                "de": "...",
                "fr": "..."
            }},
            "EVENT_1": {{ ... }}
        }}
        """

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a concise but thorough historian. Write 150-250 words per narrative per language."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"Secondary Events AI Error: {e}")
            # Returnăm un dicționar gol pentru a fi gestionat de main.py
            return {}