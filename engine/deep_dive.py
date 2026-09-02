"""Long-form PRO narratives ("The Long Read").

The free narrative sells the day; this sells the subscription. Every event gets a
second, longer piece — chapters, a timeline, the myth-correction, the aftermath and
real sources — that only PRO users ever receive in full.

Two rules shape the whole module:

1. **It must not repeat the free narrative.** A subscriber reading both should feel
   they got a second article, not a padded version of the first. `_overlap_ratio`
   enforces that mechanically; the prompt asks for it explicitly.

2. **English is generated, the rest is translated.** Generating 2,000 words natively
   in five languages would multiply the daily token bill by five and let the facts
   drift between languages. Instead English gets reasoning tokens (this is dense
   factual writing, unlike the creative short narrative) and the other four are
   translated from it — the same trade `AIProcessor._patch_from_english` already makes.
"""

import asyncio
import re

from core.llm import budget_allows
from core.logger import setup_logger

logger = setup_logger("DeepDive")

LANGUAGES = ["en", "ro", "es", "de", "fr"]
TRANSLATION_LANGS = ["ro", "es", "de", "fr"]

LANG_NAMES = {
    "en": "English",
    "ro": "Romanian",
    "es": "Spanish",
    "de": "German",
    "fr": "French",
}

# ── Validation thresholds ─────────────────────────────────────────────
MIN_WORDS = 1200          # below this the generation broke or truncated → retry
MAX_WORDS = 3200          # above this it is padding, not prose → retry
MIN_CHAPTERS = 4
MAX_CHAPTERS = 7
MIN_CHAPTER_WORDS = 110
MIN_SOURCES = 3
TEASER_WORDS = 70         # opening words shipped to free users as the pitch
MAX_OVERLAP = 0.12        # 8-gram overlap with the free narrative

BAD_MARKERS = [
    "narrative pending", "content pending", "error generating",
    "i apologize", "i'm sorry", "as an ai", "i cannot",
    "in this article", "in this chapter", "as mentioned above",
]


class DeepDiveGenerator:
    """Generates the PRO long read for a batch of events.

    Borrows the processor's client, model and `_safe_ai_call` rather than opening a
    second connection — provider config lives in exactly one place.
    """

    def __init__(self, processor):
        self.processor = processor
        self.thinking_budget = processor.thinking_budget

    # ══════════════════════════════════════════════════════════════════
    # ENTRY POINT
    # ══════════════════════════════════════════════════════════════════
    async def generate_deep_dives(
        self, top_events: list, narratives_map: dict, target_date
    ) -> dict:
        """Return {"EVENT_0": {"en": {...}, "ro": {...}, ...}, ...}.

        A failed event yields no key at all, so `_build_event_details` simply attaches
        nothing and the app shows no teaser. Partial output is never shipped.
        """
        date_str = target_date.strftime("%B %d")

        async def process_single(idx, item):
            # The long read is the richer half of an event, not the half that makes it
            # publishable. When the run is near its spend cap it is the first thing to
            # go, so the remaining dates still get narratives and translations.
            if not budget_allows(optional=True):
                logger.warning(
                    f"💸 DeepDive {idx} skipped — spend cap reached for optional stages"
                )
                return f"EVENT_{idx}", None
            short_narrative = narratives_map.get(f"EVENT_{idx}", {}).get("en", "")
            english = await self._generate_english(idx, item, date_str, short_narrative)
            if not english:
                logger.error(f"🚨 DeepDive {idx} — English failed, skipping this event")
                return f"EVENT_{idx}", None

            translations = await asyncio.gather(*[
                self._translate(idx, english, lang) for lang in TRANSLATION_LANGS
            ])

            out = {"en": english}
            for lang, payload in zip(TRANSLATION_LANGS, translations):
                # A failed translation falls back to English rather than leaving the
                # language empty — a readable English long read beats no long read.
                out[lang] = payload if payload else english
            return f"EVENT_{idx}", out

        results = dict(await asyncio.gather(*[
            process_single(i, item) for i, item in enumerate(top_events)
        ]))

        ok = sum(1 for v in results.values() if v)
        logger.info(f"📚 Deep dives: {ok}/{len(top_events)} events complete")
        return {k: v for k, v in results.items() if v}

    # ══════════════════════════════════════════════════════════════════
    # ENGLISH GENERATION
    # ══════════════════════════════════════════════════════════════════
    async def _generate_english(
        self, idx: int, item: dict, date_str: str, short_narrative: str
    ) -> dict | None:
        year = item.get("year", "")
        text = item.get("text", "")
        slug = item.get("slug", "")
        location = item.get("location") or "the location"

        for attempt in range(1, 4):
            prompt = self._build_prompt(
                year, text, slug, location, date_str, short_narrative
            )

            res = await self.processor._safe_ai_call(
                prompt,
                f"DeepDive {idx}:en (attempt {attempt})",
                {"chapters": []},
                temperature=0.7,
                max_tokens=8192,
                # Unlike the short narrative (creative, thinking_budget=0), this is
                # dense factual work — sequencing, causation, real sources. Reasoning
                # measurably reduces invented detail here.
                thinking_budget=self.thinking_budget,
            )

            payload = self._normalize(res)
            is_valid, reason = self._validate(payload, short_narrative)
            if is_valid:
                logger.info(
                    f"✅ DeepDive {idx}:en — {payload['word_count']} words, "
                    f"{len(payload['chapters'])} chapters (attempt {attempt})"
                )
                return payload

            logger.warning(f"⚠️ DeepDive {idx}:en attempt {attempt}: {reason}")

        return None

    def _build_prompt(
        self, year, text, slug, location, date_str, short_narrative
    ) -> str:
        # The free narrative is handed over purely as a "do not repeat this" reference.
        # Truncated because only its shape and angle matter, not its full text.
        avoid_block = ""
        if short_narrative:
            avoid_block = f"""
ALREADY PUBLISHED — DO NOT REPEAT THIS PIECE:
\"\"\"
{short_narrative[:1400]}
\"\"\"
The article above is what every reader already got for free. Yours is the second,
longer piece for paying subscribers. Open on a DIFFERENT hook. Take a DIFFERENT angle.
Do not reuse its opening image, its closing line, or its best fact. If the free piece
covered the moment itself, you cover the machinery behind it and what came after.
A subscriber reading both must feel they received two different articles.
"""

        return f"""
You are writing the long-form piece for subscribers of a history app — the kind of
article someone reads to the end on a Sunday morning and then tells someone about.
ONE event. {MIN_WORDS}-2400 words total across all chapters. Write in English.

EVENT: {year} — {text}
WIKIPEDIA: {slug}
DATE: {date_str}, {year}
LOCATION: {location}
{avoid_block}
WHAT TO PRODUCE:

1. CHAPTERS — {MIN_CHAPTERS} to {MAX_CHAPTERS} of them, each with a real title and at least
   {MIN_CHAPTER_WORDS} words. Titles are hooks, not labels: "The Order That Was Never Sent",
   not "Background". Each chapter does distinct work — set-up, mechanism, the moment,
   the people, the consequence. Never summarize the previous chapter.

2. TIMELINE — 5 to 12 entries, each "MARKER — what happened". The marker is a real time,
   date or year ("14:32", "3 March 1848", "Spring 1919"). Tight, factual, sequential.
   This is the spine of the event, not a repeat of the chapters.

3. MISCONCEPTION — 80-150 words on what most people get wrong about this event. The
   popular version, then what actually happened, and why the wrong version stuck.
   If there is genuinely no popular misconception, write instead about the detail that
   is consistently left out of the retellings.

4. AFTERMATH — 3 to 4 entries tracing consequences forward in time. Each begins with a
   time marker ("Within a decade", "By 1961", "Two centuries later"). Concrete effects
   on real people, institutions or places — not "it changed history".

5. SOURCES — {MIN_SOURCES} to 5 real references as "Author, Title (Year)". Books, papers,
   archives, published collections. NEVER invent a source. NEVER output a URL. If you
   are not confident a specific work exists, name the archive or the primary document
   type instead ("the Admiralty logs held at Kew").

HOW TO WRITE IT:
- Real numbers as evidence, woven into sentences. "Of the 300 who went in, 11 walked out."
- Name real people. Quote them only when you know the actual words.
- Explain mechanisms in plain language — the engineering, the law, the politics.
  The explanation should be the most satisfying part, never a chore.
- Dry, intelligent irony where the material earns it. A genuinely funny line once or
  twice in the whole piece, never forced onto tragedy.
- Vary paragraph and sentence length. If a sentence is boring, cut it.
- No headers inside chapter bodies. Paragraphs separated by blank lines.

BANNED PHRASES:
"it is worth noting" / "history tells us" / "changed the course of history" /
"left an indelible mark" / "without a doubt" / "subsequently" / "in conclusion" /
"serves as a reminder" / "stands as a testament" / "little did they know" /
"on this day" / "fast forward" / "needless to say" / "in this article".

Return JSON:
{{
  "chapters": [
    {{"title": "chapter title", "body": "chapter text, blank lines between paragraphs"}}
  ],
  "timeline": ["14:32 — the first signal reaches Lisbon", "..."],
  "misconception": "80-150 words",
  "aftermath": ["Within a decade — ...", "..."],
  "sources": ["Author, Title (Year)", "..."]
}}
"""

    # ══════════════════════════════════════════════════════════════════
    # TRANSLATION
    # ══════════════════════════════════════════════════════════════════
    async def _translate(self, idx: int, english: dict, lang: str) -> dict | None:
        lang_full = LANG_NAMES.get(lang, lang.upper())

        # Send the structure as JSON and ask for the same structure back. Translating
        # field by field would cost four times the requests for no gain in quality.
        payload = {
            "chapters": english["chapters"],
            "timeline": english["timeline"],
            "misconception": english["misconception"],
            "aftermath": english["aftermath"],
        }

        prompt = f"""
Translate this long-form historical article into {lang_full}.

Keep the voice: the rhythm, the short punchy sentences, the dry irony. Do not smooth it
into academic prose. If the English uses a fragment for impact, keep the fragment.
All numbers stay as digits. Proper nouns take their standard {lang_full} form.
Chapter titles stay hooks, not labels — translate their punch, not just their words.
Timeline and aftermath markers keep their format ("14:32 — ...", "By 1961 — ...").
Blank lines between paragraphs are preserved exactly.
Output only {lang_full} — no English except proper nouns.

SOURCES ARE NOT TRANSLATED — they are omitted from the input and re-attached afterwards.

ARTICLE JSON:
{payload}

Return the SAME JSON structure, with every string translated into {lang_full}:
{{
  "chapters": [{{"title": "...", "body": "..."}}],
  "timeline": ["..."],
  "misconception": "...",
  "aftermath": ["..."]
}}
"""

        res = await self.processor._safe_ai_call(
            prompt,
            f"DeepDive {idx}:{lang}",
            {"chapters": []},
            temperature=0.3,
            max_tokens=8192,
            thinking_budget=0,  # mechanical work — reasoning buys nothing
        )

        translated = self._normalize(res)
        # Sources are language-neutral bibliography; carry the English ones across.
        translated["sources"] = english["sources"]
        translated["teaser"] = self._extract_teaser(translated["chapters"])

        if translated["word_count"] < MIN_WORDS * 0.6:
            logger.warning(
                f"⚠️ DeepDive {idx}:{lang} — only {translated['word_count']} words, "
                f"falling back to English"
            )
            return None

        if not self._is_target_language(translated, lang):
            logger.warning(f"⚠️ DeepDive {idx}:{lang} — looks like English, falling back")
            return None

        logger.info(f"✅ DeepDive {idx}:{lang} — {translated['word_count']} words")
        return translated

    # ══════════════════════════════════════════════════════════════════
    # NORMALIZE / VALIDATE
    # ══════════════════════════════════════════════════════════════════
    def _normalize(self, res: dict) -> dict:
        """Coerce a raw model response into the DeepDive shape.

        The model occasionally returns a chapter as a bare string or a timeline entry
        as an object; normalising here keeps the validator and the serializer simple.
        """
        chapters = []
        for ch in (res.get("chapters") or []):
            if isinstance(ch, dict):
                title = str(ch.get("title") or "").strip()
                body = str(ch.get("body") or "").strip()
            else:
                title, body = "", str(ch).strip()
            if body:
                chapters.append({"title": title, "body": body})

        def _str_list(key: str) -> list:
            out = []
            for entry in (res.get(key) or []):
                if isinstance(entry, dict):
                    # e.g. {"marker": "14:32", "text": "..."} → "14:32 — ..."
                    marker = str(entry.get("marker") or entry.get("year") or "").strip()
                    body = str(entry.get("text") or entry.get("event") or "").strip()
                    entry = f"{marker} — {body}" if marker and body else (body or marker)
                entry = str(entry).strip()
                if entry:
                    out.append(entry)
            return out

        payload = {
            "chapters": chapters,
            "timeline": _str_list("timeline"),
            "misconception": str(res.get("misconception") or "").strip(),
            "aftermath": _str_list("aftermath"),
            "sources": _str_list("sources"),
        }
        payload["word_count"] = self._word_count(payload)
        payload["teaser"] = self._extract_teaser(chapters)
        return payload

    @staticmethod
    def _word_count(payload: dict) -> int:
        parts = [c["body"] for c in payload["chapters"]]
        parts.append(payload.get("misconception", ""))
        parts.extend(payload.get("aftermath", []))
        return sum(len(p.split()) for p in parts)

    @staticmethod
    def _extract_teaser(chapters: list) -> str:
        """First ~70 words of chapter one — the only body text a free user receives."""
        if not chapters:
            return ""
        words = chapters[0]["body"].split()
        if len(words) <= TEASER_WORDS:
            return chapters[0]["body"]
        return " ".join(words[:TEASER_WORDS]).rstrip(".,;:—-") + "…"

    def _validate(self, payload: dict, short_narrative: str) -> tuple:
        chapters = payload["chapters"]

        if not chapters:
            return False, "No chapters"
        if not (MIN_CHAPTERS <= len(chapters) <= MAX_CHAPTERS):
            return False, f"{len(chapters)} chapters (want {MIN_CHAPTERS}-{MAX_CHAPTERS})"

        for i, ch in enumerate(chapters):
            if not ch["title"]:
                return False, f"Chapter {i} has no title"
            if len(ch["body"].split()) < MIN_CHAPTER_WORDS:
                return False, f"Chapter {i} only {len(ch['body'].split())} words"

        wc = payload["word_count"]
        if wc < MIN_WORDS:
            return False, f"Too short: {wc} words (min {MIN_WORDS})"
        if wc > MAX_WORDS:
            return False, f"Too long: {wc} words (max {MAX_WORDS})"

        if len(payload["timeline"]) < 5:
            return False, f"Timeline has {len(payload['timeline'])} entries (min 5)"
        if len(payload["aftermath"]) < 3:
            return False, f"Aftermath has {len(payload['aftermath'])} entries (min 3)"
        if not payload["misconception"]:
            return False, "Missing misconception section"
        if len(payload["sources"]) < MIN_SOURCES:
            return False, f"Only {len(payload['sources'])} sources (min {MIN_SOURCES})"

        # An invented URL is worse than no source at all — it is a checkable lie.
        for src in payload["sources"]:
            if "http://" in src or "https://" in src or "www." in src:
                return False, f"Source contains a URL: {src[:60]}"

        blob = " ".join(c["body"] for c in chapters).lower()
        for marker in BAD_MARKERS:
            if marker in blob:
                return False, f"Contains placeholder/AI text: '{marker}'"

        if short_narrative:
            overlap = self._overlap_ratio(short_narrative, blob)
            if overlap > MAX_OVERLAP:
                return False, f"Repeats the free narrative ({overlap:.0%} 8-gram overlap)"

        return True, "OK"

    @staticmethod
    def _overlap_ratio(short_text: str, long_text: str) -> float:
        """Share of the free narrative's 8-grams that reappear in the long read.

        Eight words is long enough that a match means a recycled sentence rather than
        a shared proper noun or a common phrase.
        """
        def grams(text: str) -> set:
            words = re.findall(r"[a-z0-9']+", text.lower())
            return {tuple(words[i:i + 8]) for i in range(len(words) - 7)}

        short_grams = grams(short_text)
        if not short_grams:
            return 0.0
        return len(short_grams & grams(long_text)) / len(short_grams)

    @staticmethod
    def _is_target_language(payload: dict, lang: str) -> bool:
        """Rough guard against the model echoing English back — same heuristic the
        short-narrative validator uses."""
        if lang == "en":
            return True
        blob = " ".join(c["body"] for c in payload["chapters"]).lower()
        giveaways = ["the ", "and ", "was ", "were ", "this ", "that ", "with ", "from "]
        hits = sum(1 for w in giveaways if w in blob)
        return (hits / len(giveaways)) <= 0.8
