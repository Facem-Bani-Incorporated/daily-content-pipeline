import asyncio
import json
from groq import Groq
from core.config import config
from schema.models import EventCategory


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model
        self.categories_list = [c.value for c in EventCategory]
        self.languages = ["en", "ro", "es", "de", "fr"]

    async def batch_score_and_categorize(self, candidates: list):
        """Scoring și clasificare inițială pentru tot batch-ul."""
        candidates_text = "\n".join(
            [f"ID_{i}: ({item['year']}) {item['text'][:200]}" for i, item in enumerate(candidates)])

        prompt = f"""
        Analyze these historical events. 
        ALLOWED CATEGORIES: {self.categories_list}

        For each ID, return a JSON object with:
        1. category (from the allowed list)
        2. score (0-100)
        3. titles (dict with keys {self.languages})

        INPUT: {candidates_text}
        """
        res = await self._safe_groq_call(prompt, "Batch Analysis", {"results": {}})

        # Validare categorii pentru a preveni erori în Java
        results = res.get('results', {})
        for vid, data in results.items():
            if data.get('category') not in self.categories_list:
                data['category'] = EventCategory.CULTURE_ARTS.value

        return {"results": results}

    async def generate_multilingual_main_event(self, event_data: dict):
        """Generare narațiune lungă (400 cuvinte) pentru evenimentul principal."""
        event_info = f"{event_data.get('year')} - {event_data.get('text')}"

        # 1. Titluri finale
        titles_prompt = f"Create 5 engaging historical titles for: {event_info} in {self.languages}. Root key: 'titles'."
        titles_res = await self._safe_groq_call(titles_prompt, "Main Titles",
                                                {"titles": {l: "History Event" for l in self.languages}})

        # 2. Narațiuni paralele
        async def fetch_lang_narrative(lang):
            prompt = f"Write a 400-word professional historical narrative in {lang.upper()} about: {event_info}. Return JSON: {{ 'content': 'text' }}"
            res = await self._safe_groq_call(prompt, f"Main {lang}", {"content": "Narrative pending..."})
            return lang, res.get("content", "Narrative pending...")

        tasks = [fetch_lang_narrative(l) for l in self.languages]
        narrative_results = await asyncio.gather(*tasks)

        return {
            "titles": titles_res.get('titles', {}),
            "narratives": dict(narrative_results)
        }

    async def generate_secondary_narratives(self, secondary_candidates: list):
        """
        METODA LIPSA: Generare narațiuni scurte (200 cuvinte) pentru evenimentele secundare.
        """
        final_map = {f"EVENT_{i}": {} for i in range(len(secondary_candidates))}

        async def fetch_sec_lang(idx, item, lang):
            event_info = f"{item['year']} - {item['text'][:300]}"
            prompt = f"Write a 200-word historical summary in {lang.upper()} for: {event_info}. Return JSON: {{ 'content': 'text' }}"
            res = await self._safe_groq_call(prompt, f"Sec {idx} {lang}", {"content": "Data pending..."})
            return idx, lang, res.get("content", "Data pending...")

        # Generăm toate limbile pentru toate evenimentele în paralel (High Performance)
        tasks = []
        for i, item in enumerate(secondary_candidates):
            for l in self.languages:
                tasks.append(fetch_sec_lang(i, item, l))

        results = await asyncio.gather(*tasks)

        # Reconstruim maparea pentru pipeline
        for idx, lang, text in results:
            final_map[f"EVENT_{idx}"][lang] = text

        return final_map

    async def _safe_groq_call(self, prompt, context, fallback):
        """Apel securizat către Groq cu formatare JSON forțată."""
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a rigid History API. Output ONLY valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"🚨 AI Error ({context}): {e}")
            return fallback