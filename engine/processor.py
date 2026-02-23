import json
from groq import Groq
from core.config import config
from enum import Enum


# Definim categoriile la nivel de cod pentru consistență între Python și Java
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
        Analizează setul de candidați, le atribuie o categorie, un scor de impact
        și traduce titlurile. Include input despre popularitate (views).
        """
        # Construim un context bogat pentru AI
        candidates_text = "\n".join([
            f"ID {i}: ({item['year']}) {item['text'][:150]} | Monthly Views: {item.get('views', 0)}"
            for i, item in enumerate(candidates)
        ])

        prompt = f"""
        Act as an Elite Historian and Data Analyst. Analyze these historical events.

        TASKS:
        1. CATEGORY: Assign one of these categories: {self.categories}.
        2. IMPACT SCORE: Rate 0-100 based on global historical significance AND current relevance (Views).
        3. TRANSLATIONS: Provide short, engaging titles in EN, RO, ES, DE, FR.

        INPUT DATA:
        {candidates_text}

        CONSTRAINT: Return ONLY a valid JSON object.
        JSON STRUCTURE:
        {{
            "results": {{
                "ID_0": {{
                    "category": "technology",
                    "score": 85,
                    "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
                }}
            }}
        }}
        """

        completion = self.client.chat.completions.create(
            model="llama-3.3-70b-versatile",  # Modelul flagship pentru logică complexă
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)

    async def generate_multilingual_main_event(self, event_data: dict):
        """
        Generează conținutul premium pentru evenimentul principal al zilei.
        Folosește categoria pentru a adapta tonul narațiunii.
        """
        text = event_data.get('text', '')
        year = event_data.get('year', '')
        category = event_data.get('category', 'general')
        views = event_data.get('views', 0)

        prompt = f"""
        Deep Dive Analysis: {text} ({year}).
        Category: {category}.
        Monthly Interest: {views} page views.

        TASK:
        1. Create a captivating title.
        2. Write a 400-word narrative (storytelling style). Use a tone suitable for {category}.
        3. Translate both into: English, Romanian, Spanish, German, French.

        OUTPUT JSON:
        {{
            "titles": {{ "en": "..", "ro": "..", "es": "..", "de": "..", "fr": ".." }},
            "narratives": {{ "en": "..", "ro": "..", "es": "..", "de": "..", "fr": ".." }}
        }}
        """

        completion = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)