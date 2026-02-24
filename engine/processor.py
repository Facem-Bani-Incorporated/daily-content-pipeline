import asyncio
import json
from groq import Groq
from core.config import config
from schema.models import EventCategory


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model
        # Extragem valorile din Enum pentru prompt
        self.categories_list = [c.value for c in EventCategory]
        self.languages = ["en", "ro", "es", "de", "fr"]

    async def batch_score_and_categorize(self, candidates: list):
        """Scoring inițial. Adăugăm rigoare pentru categorii."""
        candidates_text = "\n".join(
            [f"ID_{i}: ({item['year']}) {item['text'][:200]}" for i, item in enumerate(candidates)])

        prompt = f"""
        Analyze these historical events.
        ALLOWED CATEGORIES: {self.categories_list}

        For each ID, return:
        1. category (MUST be from the allowed list)
        2. score (0-100)
        3. titles: dict with keys {self.languages}

        INPUT: {candidates_text}
        """

        res = await self._safe_groq_call(prompt, "Batch Analysis", {"results": {}})

        # VALIDARE POST-AI: Ne asigurăm că AI-ul n-a inventat categorii
        results = res.get('results', {})
        for vid, data in results.items():
            if data.get('category') not in self.categories_list:
                data['category'] = EventCategory.CULTURE_ARTS.value  # Fallback sigur

        return {"results": results}

    async def generate_multilingual_main_event(self, event_data: dict):
        event_info = f"{event_data.get('year')} - {event_data.get('text')}"

        # 1. Titluri
        titles_prompt = f"Create 5 engaging titles for: {event_info} in {self.languages}. Root key: 'titles'."
        titles_res = await self._safe_groq_call(titles_prompt, "Main Titles",
                                                {"titles": {l: "History Event" for l in self.languages}})
        titles = titles_res.get('titles', {})

        # 2. Narațiuni în paralel (cheie fixă pentru parsare sigură)
        async def fetch_lang_narrative(lang):
            prompt = f"""
            Write a 400-word historical narrative in {lang.upper()} about: {event_info}.
            Focus on context and impact.
            RETURN JSON: {{ "content": "the_text" }}
            """
            # Folosim cheia 'content' ca să nu depindem de dinamica numelui limbii în JSON
            res = await self._safe_groq_call(prompt, f"Main {lang}", {"content": "Narrative missing."})
            return lang, res.get("content", "Narrative missing.")

        tasks = [fetch_lang_narrative(l) for l in self.languages]
        narrative_results = await asyncio.gather(*tasks)

        return {
            "titles": titles,
            "narratives": dict(narrative_results)
        }

    async def _safe_groq_call(self, prompt, context, fallback):
        """
        Metoda 'Fort Knox' pentru apeluri AI.
        """
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a specialized History API. You only output valid JSON. No conversational text, no markdown backticks."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0  # Esențial pentru predictibilitate
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            # Dacă JSON-ul e invalid, logăm eroarea și dăm fallback-ul să nu crape pipeline-ul
            print(f"🚨 AI Error ({context}): {e}")
            return fallback