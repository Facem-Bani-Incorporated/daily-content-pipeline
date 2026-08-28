"""Parallel Universes — a branching what-if game built from a real event.

Not an article with alternatives. The player stands at a real hinge of history, makes
three consecutive decisions, and lands in one of twelve endings, while four world meters
and four named factions move against each other the whole way.

Three decisions that shape the design:

1. **The whole tree comes back in one call.** Twenty-two nodes generated separately would
   contradict each other by the third level — branch B would forget what branch A had
   already established. One call means the model holds the whole shape at once, and it
   costs less than twenty-two.

2. **Three choices at the opening, two after.** 1 + 3 + 6 decision nodes and 12 endings.
   The extra width sits where agency matters most; three-wide throughout would mean
   forty nodes, too many to keep coherent and too many endings to collect.

   Each node also carries hard facts, and each choice a concrete outcome, a risk score
   and deltas against four named factions. Prose is capped short on purpose — the first
   version was a paragraph and two buttons, which reads as a questionnaire, not a game.

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
# Three choices at the opening, two after. The extra width sits where agency matters
# most without the 40 nodes that three-wide-throughout would cost.
ROOT_BRANCH = 3
BRANCH = 2
EXPECTED_NODES = 22          # 1 + 3 + 6 decision nodes, + 12 endings
EXPECTED_ENDINGS = 12
EXPECTED_ACTORS = 4

# Short on purpose. The player should read two sentences and then look at numbers —
# a wall of prose between every tap is what made the first version feel like a quiz.
MIN_NODE_WORDS = 35
MAX_NODE_WORDS = 110
MIN_FACTS = 2
MIN_ENDING_STATS = 4
VALID_RARITY = {"common", "uncommon", "rare"}

_NL = chr(10)

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
                # 22 nodes with facts, outcomes and ending stats do not fit in 8192.
                max_tokens=16384,
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
        ctx = ""
        if context:
            ctx = _NL.join(["", "WHAT ACTUALLY HAPPENED:", context[:1200], ""])
        return f"""
You are designing a short branching history game. The player stands at a real hinge in
the past and makes THREE consecutive decisions. Every path must be grounded in what was
actually plausible at the time — no magic, no anachronism, no hindsight the people
involved could not have had.

EVENT: {year} — {text}
DATE: {date_str}, {year}
PLACE: {location}
{ctx}
BUILD EXACTLY THIS TREE — {EXPECTED_NODES} nodes, ids exactly as written:

  n0                                        (opening, THREE choices)
  na  nb  nc                                (3 nodes, TWO choices each)
  naa nab nba nbb nca ncb                   (6 nodes, TWO choices each)
  e1 ... e12                                (12 endings, NO choices)

Wiring, exactly:
  n0  -> na, nb, nc
  na  -> naa, nab      nb -> nba, nbb      nc -> nca, ncb
  naa -> e1, e2        nab -> e3, e4       nba -> e5, e6
  nbb -> e7, e8        nca -> e9, e10      ncb -> e11, e12

ACTORS — exactly {EXPECTED_ACTORS}
Real factions or people with a stake in this, alive and relevant in {year}. Give each an
`id` (short, lowercase), a `name`, and a `start` standing 0-100 reflecting their real
position at the moment of the fork. These are the political board the player is playing
on. Example ids: "habsburg", "estates", "papacy", "saxony".

WRITING THE NODES — SHORT
- {MIN_NODE_WORDS}-{MAX_NODE_WORDS} words. Two or three sentences. This is the single most
  important instruction: the player reads a moment, then looks at numbers. Long prose
  between every tap turns a game into a questionnaire.
- Open on a situation, not a summary. Present tense. Put the player in the room.
- Name real people who were actually there.
- Every node must read differently from its sibling.
- `year` advances as the tree deepens: opening at the event, level 2 within months or a
  few years, level 3 a decade out, endings further still.

FACTS — {MIN_FACTS} to 4 per decision node
Hard numbers about the situation right now, as {{label, value}}. These are what the
player actually decides on, so they must be real and specific to that exact moment:
  {{"label": "Bohemian army", "value": "15,000, unpaid since spring"}}
  {{"label": "Imperial treasury", "value": "empty — 2M florins owed"}}
  {{"label": "Distance to Prague", "value": "300 miles, 3 weeks by road"}}
Never vague. "Large army" is useless; "15,000 men" is the game.

WRITING THE CHOICES
- `label`: 2-5 words, an action. "March on Prague", "Send the second letter".
- `detail`: one line naming the trade-off being accepted.
- `outcome`: the one concrete thing this commits, as {{label, value}} —
  {{"label": "Troops committed", "value": "24,000"}}. This is previewed on the card.
- `risk`: 0-100, how likely this backfires given the facts above. Bold moves are high.
- Every choice at a node must be a genuinely different strategy. No option may be
  obviously correct — all must be defensible to someone alive at the time.

THE FOUR METERS
`effects` carries four integers, -30 to +30:
  stability  order, control, the state holding together
  lives      human cost — positive means fewer people die
  progress   science, industry, knowledge
  freedom    liberty, rights, self-determination

RULE: every choice must TRADE. At least one meter up AND at least one down. A choice
where everything improves has nothing at stake — rewrite it.

ACTOR EFFECTS
`actor_effects` maps actor ids to standing deltas, -30 to +30. Every choice must move at
least TWO actors, and at least one must go DOWN. Pleasing everyone is not politics.
  {{"habsburg": 12, "estates": -18}}

THE TWELVE ENDINGS
- {MIN_NODE_WORDS}-{MAX_NODE_WORDS} words, set well after the divergence. Show the world.
- `verdict`: one line — better, worse, or simply unrecognisable?
- `epitaph`: one memorable sentence. The last thing the player reads. Make it land.
- `rarity`: mark 6 "common", 5 "uncommon", exactly 1 "rare". The rare one is the most
  surprising or hardest-won world, not merely the happiest.
- `stats`: {MIN_ENDING_STATS} to 6 entries as {{label, real, alt}} comparing your world to the
  actual one. THIS IS THE PAYOFF — make the numbers concrete and checkable:
  {{"label": "Deaths in the German lands", "real": "8 million", "alt": "under 1 million"}}
  {{"label": "Holy Roman Empire dissolved", "real": "1806", "alt": "never"}}
  `real` must be the true historical figure. `alt` is your world's.
- The twelve must be genuinely different worlds, not one world in twelve moods.

BANNED: "little did they know", "changed the course of history", "the rest is history",
"only time will tell", "for better or worse", "a butterfly effect".

Return JSON, nothing else:
{{
  "pivot_year": "{year}",
  "pivot_title": "short name for the moment history forks",
  "premise": "two sentences on what actually happened, before we change it",
  "root": "n0",
  "actors": [{{"id": "habsburg", "name": "The Habsburgs", "start": 60}}],
  "nodes": [
    {{
      "id": "n0", "year": "{year}", "title": "short scene title",
      "text": "{MIN_NODE_WORDS}-{MAX_NODE_WORDS} words",
      "facts": [{{"label": "...", "value": "..."}}],
      "choices": [
        {{"id": "c1", "label": "2-5 words", "detail": "the trade-off",
          "effects": {{"stability": 0, "lives": 0, "progress": 0, "freedom": 0}},
          "actor_effects": {{"habsburg": 10, "estates": -15}},
          "risk": 70,
          "outcome": {{"label": "Troops committed", "value": "24,000"}},
          "next": "na"}}
      ]
    }},
    {{
      "id": "e1", "year": "1700", "title": "ending title",
      "text": "...", "facts": [], "choices": [],
      "verdict": "one line", "epitaph": "one memorable sentence", "rarity": "common",
      "stats": [{{"label": "...", "real": "...", "alt": "..."}}]
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
                "facts": [{"label": f["label"], "value": f["value"]} for f in n["facts"]],
                "stats": [{"label": st["label"], "real": st["real"], "alt": st["alt"]}
                          for st in n["stats"]],
                "choices": [
                    {
                        "id": c["id"], "label": c["label"], "detail": c["detail"],
                        "outcome": c["outcome"],
                    }
                    for c in n["choices"]
                ],
            }
            for n in english["nodes"]
        ]
        actor_names = [{"id": a["id"], "name": a["name"]} for a in english["actors"]]

        res = await self.processor._safe_ai_call(
            f"""
Translate this branching history game into {lang_full}.

Keep it playable: labels stay 2-5 words and stay actions, titles stay short, the prose
keeps its tension and present tense. Do not smooth it into a history lecture.
Proper nouns take their standard {lang_full} form. Years and numbers stay as digits —
translate the words around a figure, never the figure itself ("15,000, unpaid since
spring" keeps 15,000).
Translate every "text", "title", "label", "detail", "value", "real", "alt", "verdict",
"epitaph" and every actor "name".
Return the SAME arrays with the SAME ids in the SAME order — ids are structure, never
translate or reorder them.

ACTORS: {actor_names}

{{"pivot_title": "...", "premise": "...", "nodes": {skeleton}}}

Return JSON with "pivot_title", "premise", "actors" and "nodes" as above, in {lang_full}.
""",
            f"Parallel {idx}:{lang}",
            {"nodes": []},
            temperature=0.3,
            max_tokens=16384,
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

        def by_index(translated, original, keys):
            """Graft translated strings onto the original list, position by position.
            Anything missing or malformed falls back to English rather than blanking a
            field the UI expects to be there."""
            out = []
            for i, orig in enumerate(original):
                tr = translated[i] if isinstance(translated, list) and i < len(translated) else {}
                tr = tr if isinstance(tr, dict) else {}
                out.append({k: str(tr.get(k) or orig[k]) for k in keys})
            return out

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
                "facts": by_index(tr.get("facts"), en_node["facts"], ("label", "value")),
                "stats": by_index(tr.get("stats"), en_node["stats"], ("label", "real", "alt")),
                "choices": [
                    {
                        **c,
                        "label": str((tr_choices.get(c["id"]) or {}).get("label") or c["label"]),
                        "detail": str((tr_choices.get(c["id"]) or {}).get("detail") or c["detail"]),
                        "outcome": {
                            "label": str(((tr_choices.get(c["id"]) or {}).get("outcome") or {}).get("label")
                                         or c["outcome"]["label"]),
                            "value": str(((tr_choices.get(c["id"]) or {}).get("outcome") or {}).get("value")
                                         or c["outcome"]["value"]),
                        },
                    }
                    for c in en_node["choices"]
                ],
            })

        tr_actors = {str(a.get("id")): a for a in (res.get("actors") or [])
                     if isinstance(a, dict)}

        return {
            "pivot_year": english["pivot_year"],
            "pivot_title": str(res.get("pivot_title") or english["pivot_title"]),
            "premise": str(res.get("premise") or english["premise"]),
            "root": english["root"],
            "actors": [
                {**a, "name": str((tr_actors.get(a["id"]) or {}).get("name") or a["name"])}
                for a in english["actors"]
            ],
            "nodes": out_nodes,
        }

    # ══════════════════════════════════════════════════════════════════
    # NORMALIZE / VALIDATE
    # ══════════════════════════════════════════════════════════════════
    def _normalize(self, res: dict) -> dict:
        def clamp(v, lo=-40, hi=40):
            try:
                return max(lo, min(hi, int(v)))
            except (TypeError, ValueError):
                return 0

        def eff(raw) -> dict:
            raw = raw if isinstance(raw, dict) else {}
            return {k: clamp(raw.get(k, 0)) for k in
                    ("stability", "lives", "progress", "freedom")}

        def pairs(raw, keys) -> list:
            out = []
            for e in (raw or []):
                if not isinstance(e, dict):
                    continue
                row = {k: str(e.get(k) or "").strip() for k in keys}
                if all(row.values()):
                    out.append(row)
            return out

        actors = []
        for a in (res.get("actors") or []):
            if not isinstance(a, dict) or not a.get("id"):
                continue
            actors.append({
                "id": str(a["id"]).strip().lower(),
                "name": str(a.get("name") or "").strip(),
                "start": clamp(a.get("start", 50), 0, 100),
            })

        actor_ids = {a["id"] for a in actors}

        nodes = []
        for n in (res.get("nodes") or []):
            if not isinstance(n, dict) or not n.get("id"):
                continue
            choices = []
            for c in (n.get("choices") or []):
                if not isinstance(c, dict) or not c.get("next"):
                    continue
                raw_ae = c.get("actor_effects")
                raw_ae = raw_ae if isinstance(raw_ae, dict) else {}
                # Drop deltas aimed at actors that do not exist — a typo in an id would
                # otherwise move a bar the player cannot see.
                ae = {}
                for k, v in raw_ae.items():
                    key = str(k).strip().lower()
                    d = clamp(v)
                    if key in actor_ids and d != 0:
                        ae[key] = d
                oc = c.get("outcome") if isinstance(c.get("outcome"), dict) else {}
                choices.append({
                    "id": str(c.get("id") or ("c" + str(len(choices) + 1))),
                    "label": str(c.get("label") or "").strip(),
                    "detail": str(c.get("detail") or "").strip(),
                    "effects": eff(c.get("effects")),
                    "actor_effects": ae,
                    "risk": clamp(c.get("risk", 50), 0, 100),
                    "outcome": {
                        "label": str(oc.get("label") or "").strip(),
                        "value": str(oc.get("value") or "").strip(),
                    },
                    "next": str(c["next"]).strip(),
                })
            nodes.append({
                "id": str(n["id"]).strip(),
                "year": str(n.get("year") or "").strip(),
                "title": str(n.get("title") or "").strip(),
                "text": str(n.get("text") or "").strip(),
                "facts": pairs(n.get("facts"), ("label", "value")),
                "choices": choices,
                "verdict": str(n.get("verdict") or "").strip(),
                "epitaph": str(n.get("epitaph") or "").strip(),
                "rarity": str(n.get("rarity") or "").strip().lower(),
                "stats": pairs(n.get("stats"), ("label", "real", "alt")),
            })

        return {
            "pivot_year": str(res.get("pivot_year") or "").strip(),
            "pivot_title": str(res.get("pivot_title") or "").strip(),
            "premise": str(res.get("premise") or "").strip(),
            "root": str(res.get("root") or "n0").strip(),
            "actors": actors,
            "nodes": nodes,
        }

    def _validate(self, p: dict) -> tuple:
        nodes = p["nodes"]
        if len(nodes) != EXPECTED_NODES:
            return False, str(len(nodes)) + " nodes, expected " + str(EXPECTED_NODES)
        if not p["premise"] or not p["pivot_title"]:
            return False, "Missing premise or pivot title"
        if len(p["actors"]) != EXPECTED_ACTORS:
            return False, str(len(p["actors"])) + " actors, expected " + str(EXPECTED_ACTORS)
        if any(not a["name"] for a in p["actors"]):
            return False, "An actor has no name"

        ids = {n["id"] for n in nodes}
        if len(ids) != len(nodes):
            return False, "Duplicate node ids"
        if p["root"] not in ids:
            return False, "Root is not a node"

        endings = [n for n in nodes if not n["choices"]]
        if len(endings) != EXPECTED_ENDINGS:
            return False, str(len(endings)) + " endings, expected " + str(EXPECTED_ENDINGS)

        for n in nodes:
            if not n["choices"]:
                continue
            want = ROOT_BRANCH if n["id"] == p["root"] else BRANCH
            if len(n["choices"]) != want:
                return False, n["id"] + " has " + str(len(n["choices"])) + " choices, expected " + str(want)
            if len(n["facts"]) < MIN_FACTS:
                return False, n["id"] + " has " + str(len(n["facts"])) + " facts"

            for c in n["choices"]:
                if c["next"] not in ids:
                    return False, n["id"] + " -> unknown node " + c["next"]
                if not c["label"] or not c["detail"]:
                    return False, n["id"] + " has an unlabelled choice"
                if not c["outcome"]["label"] or not c["outcome"]["value"]:
                    return False, n["id"] + "/" + c["id"] + " has no outcome"

                vals = list(c["effects"].values())
                if all(v == 0 for v in vals):
                    return False, n["id"] + "/" + c["id"] + " has no effect on any meter"
                if not (any(v > 0 for v in vals) and any(v < 0 for v in vals)):
                    return False, n["id"] + "/" + c["id"] + " does not trade anything off"

                # Politics means someone loses. A choice everyone likes is not a choice.
                ae = c["actor_effects"]
                if len(ae) < 2:
                    return False, n["id"] + "/" + c["id"] + " moves " + str(len(ae)) + " actors"
                if not any(v < 0 for v in ae.values()):
                    return False, n["id"] + "/" + c["id"] + " pleases every actor"

        # Unreachable nodes mean the tree is mis-wired even if every id resolves.
        reachable = set()
        stack = [p["root"]]
        by_id = {n["id"]: n for n in nodes}
        while stack:
            cur = stack.pop()
            if cur in reachable:
                continue
            reachable.add(cur)
            stack.extend(c["next"] for c in by_id[cur]["choices"])
        if len(reachable) != len(nodes):
            return False, str(len(nodes) - len(reachable)) + " unreachable node(s)"

        for n in nodes:
            if not n["title"] or not n["text"]:
                return False, n["id"] + " is missing title or text"
            wc = len(n["text"].split())
            if not (MIN_NODE_WORDS <= wc <= MAX_NODE_WORDS):
                return False, n["id"] + ": " + str(wc) + " words"
            low = n["text"].lower()
            for mk in BAD_MARKERS:
                if mk in low:
                    return False, n["id"] + " contains " + mk

        for e in endings:
            if not e["verdict"] or not e["epitaph"]:
                return False, e["id"] + " missing verdict or epitaph"
            if e["rarity"] not in VALID_RARITY:
                return False, e["id"] + " has rarity " + e["rarity"]
            if len(e["stats"]) < MIN_ENDING_STATS:
                return False, e["id"] + " has " + str(len(e["stats"])) + " stats"
        if sum(1 for e in endings if e["rarity"] == "rare") != 1:
            return False, "Expected exactly one rare ending"

        # Two endings with the same opening are the same ending written twice.
        openings = {" ".join(e["text"].lower().split()[:8]) for e in endings}
        if len(openings) < EXPECTED_ENDINGS:
            return False, "Endings repeat each other"

        return True, "OK"
