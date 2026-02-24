import asyncio
import json
from datetime import datetime
from groq import Groq
from core.config import config
from schema.models import EventCategory


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model
        self.categories_list = [c.value for c in EventCategory]
        self.languages = ["en", "ro", "es", "de", "fr"]
        self.today_str = datetime.now().strftime("%d %B")

    def _ensure_langs(self, data: dict, fallback_text: str = "Title pending") -> dict:
        """
        DEFENSIVE PROGRAMMING: Garantează că toate cele 5 chei există.
        Dacă AI-ul uită 'fr', această funcție o va adăuga automat pentru a nu crăpa Pydantic-ul.
        """
        if not isinstance(data, dict):
            data = {}
        return {lang: data.get(lang) or fallback_text for lang in self.languages}

    async def batch_score_and_categorize(self, candidates: list):
        """Primul pas: Filtrare și clasificare rapidă cu schemă strictă."""
        candidates_text = "\n".join(
            [f"ID_{i}: ({item['year']}) {item['text'][:250]}" for i, item in enumerate(candidates)])

        prompt = f"""
        TODAY IS: {self.today_str}.
        Analyze these historical events occurred specifically on {self.today_str}.

        ALLOWED CATEGORIES: {self.categories_list}

        STRICT JSON SCHEMA REQUIRED:
        {{
          "results": {{
            "ID_0": {{
              "category": "one_from_allowed_list",
              "score": 85,
              "titles": {{
                "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..."
              }}
            }}
          }}
        }}

        INPUT: {candidates_text}
        """
        res = await self._safe_groq_call(prompt, "Batch Analysis", {"results": {}})

        # Validare Post-Procesare
        results = res.get('results', {})
        for vid, data in results.items():
            # 1. Reparăm categoria dacă a halucinat-o
            if data.get('category') not in self.categories_list:
                data['category'] = EventCategory.CULTURE_ARTS.value
            # 2. Reparăm titlurile dacă a uitat o limbă
            data['titles'] = self._ensure_langs(data.get('titles', {}), "Historical Event")

        return {"results": results}

    async def generate_multilingual_main_event(self, event_data: dict):
        """Generare narațiune și titluri finale pentru evenimentul principal."""
        event_info = f"Year: {event_data.get('year')} | Event: {event_data.get('text')}"

        # Pas 1: Titluri
        titles_prompt = f"""
        Create 5 highly engaging historical titles for: {event_info}.
        STRICT JSON SCHEMA:
        {{
            "titles": {{
                "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..."
            }}
        }}
        """
        titles_res = await self._safe_groq_call(titles_prompt, "Main Titles", {"titles": {}})
        # Asigurăm prezența tuturor limbilor
        safe_titles = self._ensure_langs(titles_res.get('titles', {}), "Major Historical Event")

        # Pas 2: Narațiuni paralele
        async def fetch_lang_narrative(lang):
            prompt = f"""
            TODAY IN HISTORY: {self.today_str}. EVENT: {event_info}.

            TASK: Write a 400-word historical narrative in {lang.upper()}.
            STRICT RULES:
            1. The event MUST be presented as occurring on {self.today_str}, {event_data.get('year')}.
            2. DO NOT mention dates from other months as the primary date.
            3. Return ONLY JSON: {{ "content": "narrative text here" }}
            """
            res = await self._safe_groq_call(prompt, f"Main {lang}", {"content": "Data pending generation."})
            return lang, res.get("content", "Data pending generation.")

        tasks = [fetch_lang_narrative(l) for l in self.languages]
        narrative_results = await asyncio.gather(*tasks)

        return {
            "titles": safe_titles,
            "narratives": dict(narrative_results)
        }

    async def generate_secondary_narratives(self, secondary_candidates: list):
        """Generare narațiuni scurte pentru evenimentele secundare."""

        async def process_single_secondary(idx, item):
            event_tasks = [self._fetch_secondary_lang(idx, item, lang) for lang in self.languages]
            lang_results = await asyncio.gather(*event_tasks)
            return f"EVENT_{idx}", dict(lang_results)

        main_tasks = [process_single_secondary(i, item) for i, item in enumerate(secondary_candidates)]
        final_results = await asyncio.gather(*main_tasks)

        return dict(final_results)

    async def _fetch_secondary_lang(self, idx, item, lang):
        event_info = f"{item['year']} - {item['text'][:300]}"
        prompt = f"""
        DATE: {self.today_str}.
        SUMMARY (200 words) in {lang.upper()} for: {event_info}.
        STRICT: Focus only on what happened on {self.today_str}.
        JSON SCHEMA: {{ "content": "summary text here" }}
        """
        res = await self._safe_groq_call(prompt, f"Sec {idx} {lang}", {"content": "Historical data pending."})
        return lang, res.get("content", "Historical data pending.")

    async def _safe_groq_call(self, prompt, context, fallback):
        """Nucleul de apelare securizată către LLM."""
        try:
            system_msg = f"You are a strict History API. Today is {self.today_str}. Output exactly the JSON schema requested, nothing else."

            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0  # Complet determinist
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"🚨 AI Error ({context}): {e}")
            return fallback