import asyncio
import json
from datetime import datetime
from groq import Groq
from core.config import config
from schema.models import EventCategory
from core.logger import setup_logger

logger = setup_logger("AIProcessor")

class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model
        self.categories_list = [c.value for c in EventCategory]
        self.languages = ["en", "ro", "es", "de", "fr"]

    def _get_target_date_str(self, target_date: datetime) -> str:
        return target_date.strftime("%B %d")

    def _ensure_langs(self, data: dict, fallback_text: str = "Data pending") -> dict:
        if not isinstance(data, dict):
            data = {}
        return {lang: data.get(lang) or fallback_text for lang in self.languages}

    async def discover_events(self, target_date: datetime) -> list:
        """
        PASS 1 — Discovery cu rigoare istorică maximă.
        """
        date_str = self._get_target_date_str(target_date)

        prompt = f"""
        You are a meticulous Senior Historian and Fact-Checker. 
        List historical events that occurred EXACTLY on {date_str}.

        CRITICAL RULES:
        1. DATE INTEGRITY: Every event MUST have occurred on {date_str}. Do not include events from the 16th or 18th.
        2. VERIFICATION: Distinguish between "Ultimatums" and "Actual Invasions". If the invasion started on the 20th, it does NOT belong on the 17th.
        3. QUANTITY: Aim for 60 events, but PRIORITIZE accuracy. If only 40 are 100% verified, return only 40.
        4. NO HALLUCINATIONS: If you are not certain of the day, exclude it.

        STRICT JSON SCHEMA:
        {{
          "events": [
            {{
              "year": 1945,
              "text": "Precise historical description.",
              "slug": "Exact_Wikipedia_Article_Title",
              "category": "one_from_allowed_list",
              "ai_score": 75
            }}
          ]
        }}

        ALLOWED CATEGORIES: {self.categories_list}
        """

        res = await self._safe_groq_call(prompt, f"Discovery ({date_str})", {"events": []})
        events = res.get("events", [])

        validated = []
        seen_slugs = set()
        for e in events:
            slug = e.get("slug")
            if not isinstance(e.get("year"), int) or not slug or slug in seen_slugs:
                continue
            if e.get("category") not in self.categories_list:
                e["category"] = EventCategory.CULTURE_ARTS.value
            
            seen_slugs.add(slug)
            validated.append(e)

        logger.info(f"✅ Found {len(validated)} candidate events for {date_str}")
        return validated

    async def verify_events_integrity(self, events: list, target_date: datetime) -> list:
        """
        PASS 1.5 — The "Grand Inquisitor". 
        Elimină tot ce nu este 100% confirmat pentru ziua respectivă.
        """
        date_str = self._get_target_date_str(target_date)
        
        check_list = [
            {"id": i, "year": e['year'], "slug": e['slug'], "summary": e['text'][:100]} 
            for i, e in enumerate(events)
        ]

        prompt = f"""
        You are a brutal Fact-Checking Bot. Analyze these events for {date_str}.
        ELIMINATE any event that did not happen on exactly {date_str}.

        EXAMPLES OF ERRORS TO CATCH:
        - Iraq War: Started March 20, NOT March 17.
        - Battle of al-Qadisiyyah: Happened in November, NOT March.

        EVENTS TO CHECK:
        {json.dumps(check_list)}

        RETURN ONLY JSON:
        {{
          "results": [
            {{ "id": 0, "is_valid": true/false, "reason": "why" }}
          ]
        }}
        """

        res = await self._safe_groq_call(prompt, "Integrity Check", {"results": []})
        results_map = {r['id']: r for r in res.get("results", [])}

        verified = []
        for i, event in enumerate(events):
            v_info = results_map.get(i, {"is_valid": False})
            if v_info.get("is_valid"):
                verified.append(event)
            else:
                logger.warning(f"🚫 ELIMINATED: {event['year']} {event['slug']} - Reason: {v_info.get('reason')}")

        return verified

    async def deep_rank_and_select(self, candidates: list, target_date: datetime) -> list:
        """
        PASS 2 — Ranking pe baza impactului global.
        """
        if not candidates: return []
        
        date_str = self._get_target_date_str(target_date)
        candidates_text = "\n".join([f"ID_{i}: ({e['year']}) {e['text'][:200]}" for i, e in enumerate(candidates)])

        prompt = f"""
        Rank the 15 most globally impactful events for {date_str}.
        Criteria: Global reach, permanence, universal recognition, emotional power.

        STRICT JSON SCHEMA:
        {{
          "top15": [
            {{
              "original_id": "ID_0",
              "deep_score": 95,
              "score_breakdown": {{ "global_reach": 30, "permanence": 25, "universal_recognition": 20, "emotional_power": 15, "uniqueness": 5 }},
              "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
            }}
          ]
        }}

        CANDIDATES:
        {candidates_text}
        """

        res = await self._safe_groq_call(prompt, "Deep Rank", {"top15": []})
        id_map = {f"ID_{i}": e for i, e in enumerate(candidates)}
        
        enriched = []
        for entry in res.get("top15", []):
            original_id = entry.get("original_id")
            if original_id in id_map:
                item = id_map[original_id]
                item.update({
                    "deep_score": entry.get("deep_score", 50),
                    "score_breakdown": entry.get("score_breakdown", {}),
                    "titles": self._ensure_langs(entry.get("titles", {}))
                })
                enriched.append(item)
        return enriched

    async def generate_secondary_narratives(self, top_events: list, target_date: datetime):
        date_str = self._get_target_date_str(target_date)
        
        async def process_single(idx, item):
            # Verificare de siguranță suplimentară în promptul de narativ
            tasks = [self._fetch_secondary_lang(idx, item, lang, date_str) for lang in self.languages]
            return f"EVENT_{idx}", dict(await asyncio.gather(*tasks))

        return dict(await asyncio.gather(*[process_single(i, item) for i, item in enumerate(top_events)]))

    async def _fetch_secondary_lang(self, idx, item, lang, date_str):
        prompt = f"""
        Write a 200-word journalistic summary in {lang.upper()} for: {item['year']} - {item['text']}.
        Ensure the narrative explicitly confirms this happened on {date_str}.
        Return JSON: {{ "content": "..." }}
        """
        res = await self._safe_groq_call(prompt, f"Narrative {idx}:{lang}", {"content": "Data pending"})
        return lang, res.get("content", "Data pending")

    async def _safe_groq_call(self, prompt: str, context: str, fallback: dict) -> dict:
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a strict History API. Output ONLY valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.1, # Temperatură mică pentru precizie maximă
                max_completion_tokens=4096
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            logger.error(f"🚨 AI Error ({context}): {e}")
            return fallback