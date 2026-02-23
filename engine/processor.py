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
        """Analizează candidații și returnează un JSON strict."""
        candidates_text = "\n".join([
            f"ID_{i}: ({item['year']}) {item['text'][:150]} | Views: {item.get('views', 0)}"
            for i, item in enumerate(candidates)
        ])

        # Prompt ultra-strict cu One-Shot Example
        prompt = f"""
        Return ONLY a JSON object. No conversational text.

        TASKS:
        1. Categorize each ID using ONLY: {self.categories}.
        2. Impact Score (0-100) based on historical depth and monthly views.
        3. Multilingual titles (EN, RO, ES, DE, FR).

        INPUT:
        {candidates_text}

        EXAMPLE OUTPUT FORMAT:
        {{
            "results": {{
                "ID_0": {{
                    "category": "science",
                    "score": 92,
                    "titles": {{ "en": "Discovery of Penicillin", "ro": "Descoperirea penicilinei", "es": "Descubrimiento de la penicilina", "de": "Entdeckung des Penicillins", "fr": "Découverte de la pénicilline" }}
                }}
            }}
        }}
        """

        try:
            completion = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system",
                     "content": "You are a specialized API that only outputs valid JSON. Do not explain, do not comment."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.1  # Temperatura mică scade riscul de halucinații
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            # Fallback în caz de eroare majoră (rețea/limită rată)
            return {"results": {}, "error": str(e)}

    async def generate_multilingual_main_event(self, event_data: dict):
        """Generează narațiunea detaliată cu garanție de structură JSON."""
        text = event_data.get('text', '')
        year = event_data.get('year', '')
        category = event_data.get('category', 'general')

        prompt = f"""
        Return ONLY JSON. Write a professional history narrative (400 words) about: {text} ({year}).
        Tone: Academic yet engaging for category: {category}.

        REQUIRED STRUCTURE:
        {{
            "titles": {{ "en": "", "ro": "", "es": "", "de": "", "fr": "" }},
            "narratives": {{ "en": "", "ro": "", "es": "", "de": "", "fr": "" }}
        }}
        """

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system",
                     "content": "Output only the requested JSON. Ensure all fields are filled. Do not truncate the narratives."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            return {"titles": {}, "narratives": {}, "error": str(e)}