"""Parallel Universes — a branching what-if game built from a real event.

Not an article with alternatives. The player stands at a real hinge of history, makes
three consecutive decisions, and lands in one of eight endings, while four world meters
move against each other the whole way.

Three decisions that shape the design:

1. **The whole tree comes back in one call.** Fifteen nodes generated separately would
   contradict each other by the third level — branch B would forget what branch A had
   already established. One call means the model holds the whole shape at once, and it
   costs less than fifteen.

2. **Two choices per node, three levels deep.** 1 + 2 + 4 decision nodes and 8 endings.
   Three choices would mean forty nodes, which is both too much to generate coherently
   and too many endings for "3 of 8 discovered" to feel collectable.

3. **Pre-generated, not live.** A player who waits twelve seconds for an LLM is not
   playing a game. Everything here is on the device before they tap.

Generated only for the day's hero events — see TARGET in main.py. The cost is close to
a Long Read per event, and every event having one would multiply the daily bill for
content most people will never open.
"""

import asyncio
import re

from core.logger import setup_logger

logger = setup_logger("Parallel")

TRANSLATION_LANGS = ["ro", "es", "de", "fr"]
LANG_NAMES = {"ro": "Romanian", "es": "Spanish", "de": "German", "fr": "French"}

# Tree shape. Changing these means changing the prompt's id scheme too.
DEPTH = 3
BRANCH = 2
EXPECTED_NODES = 15          # 1 + 2 + 4 decision nodes, + 8 endings
EXPECTED_ENDINGS = 8

MIN_NODE_WORDS = 45
MAX_NODE_WORDS = 140
VALID_RARITY = {"common", "uncommon", "rare"}

BAD_MARKERS = [
    "as an ai", "i cannot", "i'm sorry", "error generating",
    "content pending", "lorem ipsum", "placeholder",
]


class ParallelGenerator:
    """Builds the decision tree for an event. Borrows the processor's client and
    `_safe_ai_call` so provider config stays in exactly one place."""

    def __init__(self, processor):
        self.processor = processor
        self.thinking_budget = processor.thinking_budget

    # ══════════════════════════════════════════════════════════════════
    # ENTRY POINT
    # ══════════════════════════════════════════════════════════════════
    async def generate(self, items: list, narratives_map: dict, target_date) -> dict:
        """Return {"EVENT_0": {"en": {...}, "ro": {...}}, ...} for the given items.

        A failed event yields no key, so the app simply shows no game rather than a
        broken one.
        """
        date_str = target_date.strftime("%B %d")

        async def one(idx: int, item: dict):
            context = narratives_map.get(f"EVENT_{idx}", {}).get("en", "")
            english = await self._generate_english(idx, item, date_str, context)
            if not english:
                logger.error(f"🚨 Parallel {idx} — English failed, skipping")
                return f"EVENT_{idx}", None

            out = {"en": english}
            translated = await asyncio.gather(*[
                self._translate(idx, english, lang) for lang in TRANSLATION_LANGS
            ])
            for lang, payload in zip(TRANSLATION_LANGS, translated):
                out[lang] = payload if payload else english
            return f"EVENT_{idx}", out

        results = dict(await asyncio.gather(*[one(i, it) for i, it in enumerate(items)]))
        ok = sum(1 for v in results.values() if v)
        logger.info(f"🌌 Parallel universes: {ok}/{len(items)} events complete")
        return {k: v for k, v in results.items() if v}

    # ══════════════════════════════════════════════════════════════════
    # ENGLISH
    # ══════════════════════════════════════════════════════════════════
    async def _generate_english(self, idx, item, date_str, context) -> dict | None:
        year = item.get("year", "")
        text = item.get("text", "")
        location = item.get("location") or "the location"

        for attempt in range(1, 4):
            res = await self.processor._safe_ai_call(
                self._prompt(year, text, location, date_str, context),
                f"Parallel {idx}:en (attempt {attempt})",
                {"nodes": []},
                temperature=0.85,   # this is fiction built on fact; it needs room
                max_tokens=8192,
                thinking_budget=self.thinking_budget,
            )
            payload = self._normalize(res)
            ok, why = self._validate(payload)
            if ok:
                logger.info(
                    f"✅ Parallel {idx}:en — {len(payload['nodes'])} nodes, "
                    f"{sum(1 for n in payload['nodes'] if not n['choices'])} endings "
                    f"(attempt {attempt})"
                )
                return payload
            logger.warning(f"⚠️ Parallel {idx}:en attempt {attempt}: {why}")
        return None

    def _prompt(self, year, text, location, date_str, context) -> str:
        ctx = f"\nWHAT ACTUALLY HAPPENED:\n{context[:1200]}\n" if context else ""
        return f"""
You are designing a short branching history game. The player stands at a real hinge in
the past and makes THREE consecutive decisions. Every path is grounded in what was
actually plausible at the time — no magic, no anachronism, no time travel.

EVENT: {year} — {text}
DATE: {date_str}, {year}
PLACE: {location}
{ctx}
BUILD EXACTLY THIS TREE — {EXPECTED_NODES} nodes, ids exactly as written:

  n0                                  (opening, 2 choices)
  n0a  n0b                            (2 nodes, 2 choices each)
  n0aa n0ab n0ba n0bb                 (4 nodes, 2 choices each)
  e1 e2 e3 e4 e5 e6 e7 e8             (8 endings, NO choices)

Wiring, exactly:
  n0   -> n0a, n0b
  n0a  -> n0aa, n0ab      n0b  -> n0ba, n0bb
  n0aa -> e1, e2          n0ab -> e3, e4
  n0ba -> e5, e6          n0bb -> e7, e8

WRITING THE NODES
- 60-110 words each. This is a game, not an essay. Concrete, present tense, tense.
- Each node opens on a situation, not a summary. Put the player in the room.
- Name real people who were actually there. Use real place names and real constraints —
  the technology, the distances, the politics of that exact year.
- Every node must read differently from its sibling. If two branches converge in tone,
  you have wasted a branch.
- `year` advances as the tree deepens: the opening at the event, level 2 within months
  or a few years, level 3 a decade or more out, endings further still.

WRITING THE CHOICES
- `label`: 2-5 words, an action. "Hold the line", "Send the second letter".
- `detail`: one line naming the real trade-off being accepted.
- The two choices at a node must be genuinely different strategies, not a good option
  and an obviously stupid one. Both must be defensible to a person alive at the time.

THE FOUR METERS — this is the heart of the game
Each choice carries `effects` with four integers from -30 to +30:
  stability  order, control, the state holding together
  lives      human cost — positive means fewer people die
  progress   science, industry, knowledge
  freedom    liberty, rights, self-determination

RULE: every choice must TRADE. At least one meter goes up and at least one goes down.
A choice where everything improves is a choice with nothing at stake — rewrite it.
Aim for totals in the -25..+25 range per choice; reserve anything past 20 for the
genuinely dramatic.

THE EIGHT ENDINGS
- 70-120 words, set well after the divergence. Show the world, do not grade it.
- `verdict`: one line — is this world better, worse, or simply unrecognisable?
- `epitaph`: one memorable sentence, the last thing the player reads. Make it land.
- `rarity`: mark 4 endings "common", 3 "uncommon", 1 "rare". The rare one is the most
  surprising or hardest-won world, not merely the happiest.
- The eight must be genuinely different worlds. Two endings that differ only in tone
  are one ending written twice.

BANNED: "little did they know", "changed the course of history", "the rest is history",
"only time will tell", "for better or worse", "a butterfly effect".

Return JSON, nothing else:
{{
  "pivot_year": "{year}",
  "pivot_title": "short name for the moment history forks",
  "premise": "two sentences on what actually happened, before we change it",
  "root": "n0",
  "nodes": [
    {{
      "id": "n0", "year": "{year}", "title": "short scene title",
      "text": "60-110 words",
      "choices": [
        {{"id": "c1", "label": "2-5 words", "detail": "the trade-off",
          "effects": {{"stability": 0, "lives": 0, "progress": 0, "freedom": 0}},
          "next": "n0a"}},
        {{"id": "c2", "label": "...", "detail": "...",
          "effects": {{"stability": 0, "lives": 0, "progress": 0, "freedom": 0}},
          "next": "n0b"}}
      ]
    }},
    {{
      "id": "e1", "year": "1935", "title": "ending title",
      "text": "70-120 words", "choices": [],
      "verdict": "one line", "epitaph": "one memorable sentence", "rarity": "common"
    }}
  ]
}}
"""

    # ══════════════════════════════════════════════════════════════════
    # TRANSLATION
    # ══════════════════════════════════════════════════════════════════
    async def _translate(self, idx: int, english: dict, lang: str) -> dict | None:
        lang_full = LANG_NAMES.get(lang, lang.upper())

        # Ids, wiring and effects are structure, not prose — they are stripped from the
        # request and reattached afterwards, so a translation can never rewire the tree.
        skeleton = [
            {
                "id": n["id"], "year": n["year"], "title": n["title"], "text": n["text"],
                "verdict": n.get("verdict", ""), "epitaph": n.get("epitaph", ""),
                "choices": [{"id": c["id"], "label": c["label"], "detail": c["detail"]}
                            for c in n["choices"]],
            }
            for n in english["nodes"]
        ]

        res = await self.processor._safe_ai_call(
            f"""
Translate this branching history game into {lang_full}.

Keep it playable: labels stay 2-5 words and stay actions, titles stay short, the prose
keeps its tension and present tense. Do not smooth it into a history lecture.
Proper nouns take their standard {lang_full} form. Years and numbers stay as digits.
Translate every "text", "title", "label", "detail", "verdict" and "epitaph".
Return the SAME array with the SAME ids in the SAME order — ids are structure, never
translate or reorder them.

{{"pivot_title": "...", "premise": "...", "nodes": {skeleton}}}

Return JSON with "pivot_title", "premise", and "nodes" as above, in {lang_full}.
""",
            f"Parallel {idx}:{lang}",
            {"nodes": []},
            temperature=0.3,
            max_tokens=8192,
            thinking_budget=0,
        )

        merged = self._merge_translation(english, res)
        if not merged:
            logger.warning(f"⚠️ Parallel {idx}:{lang} — unusable, falling back to English")
            return None
        logger.info(f"✅ Parallel {idx}:{lang}")
        return merged

    def _merge_translation(self, english: dict, res: dict) -> dict | None:
        """Graft translated prose onto the English structure.

        The tree's wiring is taken from the English original every time. A model that
        drops a node or renames an id can therefore break the wording of one screen, but
        never the shape of the game.
        """
        by_id = {}
        for n in (res.get("nodes") or []):
            if isinstance(n, dict) and n.get("id"):
                by_id[str(n["id"])] = n
        if len(by_id) < EXPECTED_NODES * 0.8:
            return None

        out_nodes = []
        for en_node in english["nodes"]:
            tr = by_id.get(en_node["id"], {})
            tr_choices = {str(c.get("id")): c for c in (tr.get("choices") or [])
                          if isinstance(c, dict)}
            out_nodes.append({
                **en_node,
                "title": str(tr.get("title") or en_node["title"]),
                "text": str(tr.get("text") or en_node["text"]),
                "verdict": str(tr.get("verdict") or en_node.get("verdict", "")),
                "epitaph": str(tr.get("epitaph") or en_node.get("epitaph", "")),
                "choices": [
                    {
                        **c,
                        "label": str((tr_choices.get(c["id"]) or {}).get("label") or c["label"]),
                        "detail": str((tr_choices.get(c["id"]) or {}).get("detail") or c["detail"]),
                    }
                    for c in en_node["choices"]
                ],
            })

        return {
            "pivot_year": english["pivot_year"],
            "pivot_title": str(res.get("pivot_title") or english["pivot_title"]),
            "premise": str(res.get("premise") or english["premise"]),
            "root": english["root"],
            "nodes": out_nodes,
        }

    # ══════════════════════════════════════════════════════════════════
    # NORMALIZE / VALIDATE
    # ══════════════════════════════════════════════════════════════════
    def _normalize(self, res: dict) -> dict:
        def eff(raw) -> dict:
            raw = raw if isinstance(raw, dict) else {}
            out = {}
            for k in ("stability", "lives", "progress", "freedom"):
                try:
                    out[k] = max(-40, min(40, int(raw.get(k, 0) or 0)))
                except (TypeError, ValueError):
                    out[k] = 0
            return out

        nodes = []
        for n in (res.get("nodes") or []):
            if not isinstance(n, dict) or not n.get("id"):
                continue
            choices = []
            for c in (n.get("choices") or []):
                if not isinstance(c, dict) or not c.get("next"):
                    continue
                choices.append({
                    "id": str(c.get("id") or f"c{len(choices) + 1}"),
                    "label": str(c.get("label") or "").strip(),
                    "detail": str(c.get("detail") or "").strip(),
                    "effects": eff(c.get("effects")),
                    "next": str(c["next"]).strip(),
                })
            nodes.append({
                "id": str(n["id"]).strip(),
                "year": str(n.get("year") or "").strip(),
                "title": str(n.get("title") or "").strip(),
                "text": str(n.get("text") or "").strip(),
                "choices": choices,
                "verdict": str(n.get("verdict") or "").strip(),
                "epitaph": str(n.get("epitaph") or "").strip(),
                "rarity": str(n.get("rarity") or "").strip().lower(),
            })

        return {
            "pivot_year": str(res.get("pivot_year") or "").strip(),
            "pivot_title": str(res.get("pivot_title") or "").strip(),
            "premise": str(res.get("premise") or "").strip(),
            "root": str(res.get("root") or "n0").strip(),
            "nodes": nodes,
        }

    def _validate(self, p: dict) -> tuple:
        nodes = p["nodes"]
        if len(nodes) != EXPECTED_NODES:
            return False, f"{len(nodes)} nodes, expected {EXPECTED_NODES}"
        if not p["premise"] or not p["pivot_title"]:
            return False, "Missing premise or pivot title"

        ids = {n["id"] for n in nodes}
        if len(ids) != len(nodes):
            return False, "Duplicate node ids"
        if p["root"] not in ids:
            return False, f"Root '{p['root']}' is not a node"

        endings = [n for n in nodes if not n["choices"]]
        if len(endings) != EXPECTED_ENDINGS:
            return False, f"{len(endings)} endings, expected {EXPECTED_ENDINGS}"

        # Every choice must land somewhere real — a dangling `next` is a dead end the
        # player would hit mid-game.
        for n in nodes:
            if n["choices"] and len(n["choices"]) != BRANCH:
                return False, f"Node {n['id']} has {len(n['choices'])} choices"
            for c in n["choices"]:
                if c["next"] not in ids:
                    return False, f"{n['id']} -> unknown node '{c['next']}'"
                if not c["label"] or not c["detail"]:
                    return False, f"{n['id']} has an unlabelled choice"
                # A choice with no cost is a choice with nothing at stake.
                vals = list(c["effects"].values())
                if all(v == 0 for v in vals):
                    return False, f"{n['id']}/{c['id']} has no effect on any meter"
                if not (any(v > 0 for v in vals) and any(v < 0 for v in vals)):
                    return False, f"{n['id']}/{c['id']} does not trade anything off"

        # Unreachable nodes mean the tree is mis-wired even if every id resolves.
        reachable, stack = set(), [p["root"]]
        by_id = {n["id"]: n for n in nodes}
        while stack:
            cur = stack.pop()
            if cur in reachable:
                continue
            reachable.add(cur)
            stack.extend(c["next"] for c in by_id[cur]["choices"])
        if len(reachable) != len(nodes):
            return False, f"{len(nodes) - len(reachable)} unreachable node(s)"

        for n in nodes:
            if not n["title"] or not n["text"]:
                return False, f"Node {n['id']} is missing title or text"
            wc = len(n["text"].split())
            if not (MIN_NODE_WORDS <= wc <= MAX_NODE_WORDS):
                return False, f"Node {n['id']}: {wc} words"
            low = n["text"].lower()
            for m in BAD_MARKERS:
                if m in low:
                    return False, f"Node {n['id']} contains '{m}'"

        for e in endings:
            if not e["verdict"] or not e["epitaph"]:
                return False, f"Ending {e['id']} missing verdict or epitaph"
            if e["rarity"] not in VALID_RARITY:
                return False, f"Ending {e['id']} has rarity '{e['rarity']}'"
        if sum(1 for e in endings if e["rarity"] == "rare") != 1:
            return False, "Expected exactly one rare ending"

        # Two endings with the same opening are the same ending written twice.
        openings = {" ".join(e["text"].lower().split()[:8]) for e in endings}
        if len(openings) < EXPECTED_ENDINGS:
            return False, "Endings repeat each other"

        return True, "OK"
