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
        # Ancora temporală: Azi
        self.today_str = datetime.now().strftime("%d %B")

    async def batch_score_and_categorize(self, candidates: list):
        """Primul pas: Filtrare și clasificare rapidă."""
        candidates_text = "\n".join(
            [f"ID_{i}: ({item['year']}) {item['text'][:250]}" for i, item in enumerate(candidates)])

        prompt = f"""
        TODAY IS: {self.today_str}.
        Analyze these historical events occurred specifically on {self.today_str}.

        ALLOWED CATEGORIES: {self.categories_list}

        For each ID, return JSON:
        {{
          "results": {{
            "ID_0": {{
              "category": "from_list",
              "score": 0-100,
              "titles": {{ "en": "...", "ro": "..." }}
            }}
          }}
        }}
        INPUT: {candidates_text}
        """
        res = await self._safe_groq_call(prompt, "Batch Analysis", {"results": {}})

        # Validare categorii
        results = res.get('results', {})
        for vid, data in results.items():
            if data.get('category') not in self.categories_list:
                data['category'] = EventCategory.CULTURE_ARTS.value

        return {"results": results}

    async def generate_multilingual_main_event(self, event_data: dict):
        """Modular: Generare narațiune extensivă pentru evenimentul principal."""
        event_info = f"Year: {event_data.get('year')} | Event: {event_data.get('text')}"

        # Pas 1: Titluri (asigurăm consistența)
        titles = event_data.get('titles', {l: "History" for l in self.languages})

        # Pas 2: Narațiuni paralele cu ancoră temporală strictă
        async def fetch_lang_narrative(lang):
            prompt = f"""
            TODAY IN HISTORY: {self.today_str}.
            EVENT: {event_info}.

            TASK: Write a 400-word historical narrative in {lang.upper()}.
            STRICT RULES:
            1. The event MUST be presented as occurring on {self.today_str}, {event_data.get('year')}.
            2. DO NOT mention other dates like March 5th or March 16th as the primary date.
            3. Focus on why THIS date ({self.today_str}) is important for this event.
            4. Return ONLY JSON: {{ "content": "..." }}
            """
            res = await self._safe_groq_call(prompt, f"Main {lang}", {"content": "Narrative pending..."})
            return lang, res.get("content", "Narrative pending...")

        tasks = [fetch_lang_narrative(l) for l in self.languages]
        narrative_results = await asyncio.gather(*tasks)

        return {
            "titles": titles,
            "narratives": dict(narrative_results)
        }

    async def generate_secondary_narratives(self, secondary_candidates: list):
        """Modular: Generare narațiuni scurte pentru evenimentele secundare."""

        async def process_single_secondary(idx, item):
            # Procesăm fiecare eveniment secundar independent pentru a evita mixarea datelor
            event_tasks = []
            for lang in self.languages:
                event_tasks.append(self._fetch_secondary_lang(idx, item, lang))

            lang_results = await asyncio.gather(*event_tasks)
            return f"EVENT_{idx}", dict(lang_results)

        # Executăm procesarea modulară pentru toate evenimentele secundare
        main_tasks = [process_single_secondary(i, item) for i, item in enumerate(secondary_candidates)]
        final_results = await asyncio.gather(*main_tasks)

        return dict(final_results)

    async def _fetch_secondary_lang(self, idx, item, lang):
        """Helper modular pentru o singură limbă per eveniment."""
        event_info = f"{item['year']} - {item['text'][:300]}"
        prompt = f"""
        DATE: {self.today_str}.
        SUMMARY (200 words) in {lang.upper()} for: {event_info}.
        STRICT: Focus only on what happened on {self.today_str}.
        JSON: {{ "content": "..." }}
        """
        res = await self._safe_groq_call(prompt, f"Sec {idx} {lang}", {"content": "Data pending..."})
        return lang, res.get("content", "Data pending...")

    async def _safe_groq_call(self, prompt, context, fallback):
        """Sistemul 'Fort Knox' pentru apeluri API."""
        try:
            # Injectăm contextul de sistem pentru a preveni halucinațiile de dată
            system_msg = f"You are a History Expert API. Today is {self.today_str}. You only output valid JSON."

            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0  # Zero creativitate, zero halucinații
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"🚨 AI Error ({context}): {e}")
            return fallback