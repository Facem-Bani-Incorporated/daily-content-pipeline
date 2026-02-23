import asyncio
import json
from groq import Groq
from core.config import config
from schema.models import EventCategory


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model
        self.categories = [c.value for c in EventCategory]
        self.languages = ["en", "ro", "es", "de", "fr"]

    async def batch_score_and_categorize(self, candidates: list):
        """Păstrăm batch pentru scoring (e ieftin și scurt)."""
        candidates_text = "\n".join(
            [f"ID_{i}: ({item['year']}) {item['text'][:200]}" for i, item in enumerate(candidates)])

        prompt = f"""
        Return JSON. Allowed: {self.categories}.
        For each ID: category, impact score (0-100), and short engaging titles in {self.languages}.

        JSON: {{ "results": {{ "ID_0": {{ "category": "...", "score": 10, "titles": {{...}} }} }} }}
        INPUT: {candidates_text}
        """
        return await self._safe_groq_call(prompt, "Batch Analysis", {"results": {}})

    async def generate_multilingual_main_event(self, event_data: dict):
        """
        STRATEGIE NOUĂ: Generăm fiecare limbă separat în paralel.
        Astfel, fiecare apel are propriul context de 4000+ tokens.
        """
        event_info = f"{event_data.get('year')} - {event_data.get('text')}"

        # 1. Generăm titlurile (scurt)
        titles_prompt = f"Create 5 engaging historical titles for: {event_info} in {self.languages}. JSON: {{'en': '...', 'ro': '...'}}"
        titles = await self._safe_groq_call(titles_prompt, "Main Titles", {l: "History Event" for l in self.languages})

        # 2. Generăm narațiunile în paralel
        async def fetch_lang_narrative(lang):
            prompt = f"""
            Write a professional historical narrative of EXACTLY 400 words in {lang.upper()}.
            Topic: {event_info}.
            Focus on: Historical context, key figures, and long-term impact.
            CRITICAL: Return ONLY JSON: {{ "{lang}": "the_narrative_text..." }}
            """
            res = await self._safe_groq_call(prompt, f"Main Narrative {lang}", {lang: "Narrative missing."})
            return lang, res.get(lang, "Narrative missing.")

        tasks = [fetch_lang_narrative(l) for l in self.languages]
        narrative_results = await asyncio.gather(*tasks)

        return {
            "titles": titles,
            "narratives": dict(narrative_results)
        }

    async def generate_secondary_narratives(self, secondary_candidates: list):
        """
        Pentru secundare (200 cuvinte), putem grupa limbile câte 2 ca să nu pierdem timp,
        sau să le facem tot separat dacă vrem calitate maximă. Mergem pe separat pentru siguranță.
        """
        final_map = {f"EVENT_{i}": {} for i in range(len(secondary_candidates))}

        async def fetch_sec_lang(idx, item, lang):
            prompt = f"""
            Write a 200-word historical summary in {lang.upper()} for: {item['year']} - {item['text']}.
            Return ONLY JSON: {{ "text": "..." }}
            """
            res = await self._safe_groq_call(prompt, f"Sec {idx} {lang}", {"text": "Data pending..."})
            return idx, lang, res.get("text", "Data pending...")

        tasks = []
        for i, item in enumerate(secondary_candidates):
            for l in self.languages:
                tasks.append(fetch_sec_lang(i, item, l))

        results = await asyncio.gather(*tasks)

        for idx, lang, text in results:
            final_map[f"EVENT_{idx}"][lang] = text

        return final_map

    async def _safe_groq_call(self, prompt, context, fallback):
        try:
            # Reducem temperatura la 0 pentru zero 'fantezie' și maximă rigoare JSON
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a rigid historian API. Output strictly valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            content = completion.choices[0].message.content
            return json.loads(content)
        except Exception as e:
            print(f"🚨 AI Error ({context}): {e}")
            return fallback