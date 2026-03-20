import asyncio
import json
import random
from groq import Groq
from core.config import config
from core.logger import setup_logger
from schema.models import QuizTranslations, QuizQuestion, QuizOption

logger = setup_logger("QuizGenerator")

LANGUAGES = ["en", "ro", "es", "de", "fr"]
VALID_IDS = {"a", "b", "c", "d"}
VALID_Q_IDS = {"q1", "q2", "q3", "q4"}


class QuizGenerator:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model

    # ══════════════════════════════════════════════════════════════════════
    #  PUBLIC — Generate quizzes for a list of events
    # ══════════════════════════════════════════════════════════════════════

    async def generate_quizzes(
        self,
        events: list,
        narratives_map: dict,
    ) -> list:
        """
        Generate quiz for each event in all 5 languages.
        
        Args:
            events: list of raw event dicts (with 'year', 'text', 'slug', 'titles')
            narratives_map: dict from processor, keyed "EVENT_0", "EVENT_1", etc.
                            each value is {"en": "...", "ro": "...", ...}
        
        Returns:
            list of QuizTranslations (one per event), same order as input.
            None entries for events where quiz generation failed.
        """
        tasks = [
            self._generate_single_quiz(idx, ev, narratives_map.get(f"EVENT_{idx}", {}))
            for idx, ev in enumerate(events)
        ]
        results = await asyncio.gather(*tasks)
        return results

    # ══════════════════════════════════════════════════════════════════════
    #  PRIVATE — Single event quiz (all 5 langs in one call)
    # ══════════════════════════════════════════════════════════════════════

    async def _generate_single_quiz(
        self,
        idx: int,
        event: dict,
        narratives: dict,
    ) -> QuizTranslations | None:
        """Generate quiz for one event in all 5 languages with a single AI call."""

        year = event.get("year", "Unknown")
        text = event.get("text", "")[:300]
        slug = event.get("slug", "")

        # Build narrative context per language
        narrative_block = ""
        for lang in LANGUAGES:
            narr = narratives.get(lang, "")
            if narr:
                narrative_block += f"\n[{lang.upper()}]: {narr[:400]}"

        prompt = f"""
You are creating a quiz for a history learning app. Generate exactly 4 multiple-choice questions about this historical event, in ALL 5 languages.

EVENT: ({year}) {text}
Wikipedia: {slug.replace('_', ' ')}

CONTEXT BY LANGUAGE:{narrative_block}

RULES:
- 4 questions per language, each with 4 options (a, b, c, d)
- Questions should test different aspects: date/year, person involved, location, key fact/consequence
- Wrong options must be PLAUSIBLE (real historical alternatives, not absurd)
- Distribute correct answers randomly across a/b/c/d (NOT always the same letter)
- Explanation = 1 short sentence confirming the correct answer
- Each language gets its OWN questions translated naturally (not word-for-word)
- Question text and options must be in the TARGET language
- Explanation must be in the TARGET language

STRICT JSON — return ONLY this:
{{
  "en": [
    {{
      "id": "q1",
      "question": "What year did this event occur?",
      "options": [
        {{ "id": "a", "text": "1943" }},
        {{ "id": "b", "text": "1945" }},
        {{ "id": "c", "text": "1947" }},
        {{ "id": "d", "text": "1941" }}
      ],
      "correctId": "b",
      "explanation": "This event took place in 1945."
    }},
    {{ "id": "q2", ... }},
    {{ "id": "q3", ... }},
    {{ "id": "q4", ... }}
  ],
  "ro": [ ... 4 questions in Romanian ... ],
  "es": [ ... 4 questions in Spanish ... ],
  "de": [ ... 4 questions in German ... ],
  "fr": [ ... 4 questions in French ... ]
}}
"""

        raw = await self._safe_call(prompt, f"Quiz Event #{idx}")
        if not raw:
            return None

        return self._validate_and_build(raw, idx)

    # ══════════════════════════════════════════════════════════════════════
    #  VALIDATION — Ensure quiz structure is correct
    # ══════════════════════════════════════════════════════════════════════

    def _validate_and_build(self, raw: dict, idx: int) -> QuizTranslations | None:
        """Validate raw AI output and build QuizTranslations model."""
        validated = {}

        for lang in LANGUAGES:
            questions_raw = raw.get(lang, [])
            if not isinstance(questions_raw, list):
                logger.warning(f"Quiz #{idx}: missing or invalid '{lang}' array")
                return None

            questions = []
            for qi, q in enumerate(questions_raw[:4]):
                try:
                    q_id = q.get("id", f"q{qi + 1}")
                    question_text = q.get("question", "")
                    correct_id = q.get("correctId", q.get("correct_id", ""))
                    explanation = q.get("explanation", "")
                    options_raw = q.get("options", [])

                    if not question_text or not correct_id or not options_raw:
                        logger.warning(f"Quiz #{idx} {lang} q{qi+1}: missing fields")
                        return None

                    if correct_id not in VALID_IDS:
                        logger.warning(f"Quiz #{idx} {lang} q{qi+1}: invalid correctId '{correct_id}'")
                        return None

                    # Build options
                    options = []
                    option_ids_seen = set()
                    for opt in options_raw[:4]:
                        opt_id = opt.get("id", "")
                        opt_text = opt.get("text", "")
                        if opt_id not in VALID_IDS or not opt_text:
                            continue
                        if opt_id in option_ids_seen:
                            continue
                        option_ids_seen.add(opt_id)
                        options.append(QuizOption(id=opt_id, text=str(opt_text)))

                    if len(options) != 4:
                        logger.warning(f"Quiz #{idx} {lang} q{qi+1}: expected 4 options, got {len(options)}")
                        return None

                    # Verify correct_id exists in options
                    if correct_id not in option_ids_seen:
                        logger.warning(f"Quiz #{idx} {lang} q{qi+1}: correctId '{correct_id}' not in options")
                        return None

                    questions.append(QuizQuestion(
                        id=q_id if q_id in VALID_Q_IDS else f"q{qi + 1}",
                        question=question_text,
                        options=options,
                        correct_id=correct_id,
                        explanation=explanation or "—",
                    ))
                except Exception as e:
                    logger.warning(f"Quiz #{idx} {lang} q{qi+1} parse error: {e}")
                    return None

            if len(questions) != 4:
                logger.warning(f"Quiz #{idx}: '{lang}' has {len(questions)} questions instead of 4")
                return None

            validated[lang] = questions

        if len(validated) != 5:
            logger.warning(f"Quiz #{idx}: only {len(validated)}/5 languages valid")
            return None

        try:
            return QuizTranslations(**validated)
        except Exception as e:
            logger.error(f"Quiz #{idx} model build failed: {e}")
            return None

    # ══════════════════════════════════════════════════════════════════════
    #  AI CALL — with retry
    # ══════════════════════════════════════════════════════════════════════

    async def _safe_call(self, prompt: str, context: str, retries: int = 2) -> dict | None:
        """Call Groq with retry logic. Returns parsed JSON or None."""
        for attempt in range(retries + 1):
            try:
                completion = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You are a quiz generator API for a history app. "
                                "Output ONLY valid JSON matching the exact schema requested. "
                                "No markdown, no preamble, no commentary. Just the JSON object."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.7,
                    max_completion_tokens=8192,
                    top_p=1,
                    stream=False,
                )
                result = json.loads(completion.choices[0].message.content)
                logger.info(f"✅ {context} — AI call OK (attempt {attempt + 1})")
                return result

            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ {context} — JSON parse error (attempt {attempt + 1}): {e}")
            except Exception as e:
                logger.warning(f"⚠️ {context} — AI error (attempt {attempt + 1}): {e}")

            if attempt < retries:
                wait = 2 * (attempt + 1)
                logger.info(f"⏳ Retrying {context} in {wait}s...")
                await asyncio.sleep(wait)

        logger.error(f"❌ {context} — All {retries + 1} attempts failed")
        return None