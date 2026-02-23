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
        """Analizează candidații și returnează un JSON strict, optimizat pentru virabilitate."""
        candidates_text = "\n".join([
            f"ID_{i}: ({item['year']}) {item['text'][:150]} | Views: {item.get('views', 0)}"
            for i, item in enumerate(candidates)
        ])

        # Prompt optimizat pentru engagement și diversitate geografică
        prompt = f"""
            Return ONLY a JSON object. No conversational text.

            TASKS & RULES:
            1. Categorize each ID using ONLY: {self.categories}.
            2. DIVERSITY RULE: If multiple events have similar impact, PRIORITIZE Technology, Science, and Discovery over War/Politics. 
               We want a balanced feed, not just a list of battles.
            3. Impact Score (0-100): 
               - High scores for breakthroughs (e.g., first satellite, DNA structure, internet).
               - Medium-high for major cultural shifts.
               - Lower for routine political appointments or minor battles.
        INPUT:
        {candidates_text}

        EXAMPLE OUTPUT FORMAT:
        {{
            "results": {{
                "ID_0": {{
                    "category": "science",
                    "score": 95,
                    "titles": {{ 
                        "en": "The Accidental Mold That Saved Millions: Penicillin", 
                        "ro": "Mucegaiul accidental care a salvat milioane de vieți: Penicilina", 
                        "es": "El moho accidental que salvó millones: La penicilina", 
                        "de": "Der zufällige Schimmelpilz, der Millionen rettete", 
                        "fr": "La moisissure accidentelle qui a sauvé des millions de vies" 
                    }}
                }}
            }}
        }}
        """

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a specialized API generating high-engagement historical content. Output valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.15 # Ușor crescută pentru creativitate la titluri, dar destul de mică pentru a menține JSON-ul stabil
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
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