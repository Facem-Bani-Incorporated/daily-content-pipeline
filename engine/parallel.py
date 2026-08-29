"""Parallel Universes — a branching what-if game built from a real event.

Not an article with alternatives. The player stands at a real hinge of history, makes
three consecutive decisions, and lands in one of eighteen endings, while four world
meters, four named factions and the mood of ordinary people all move against each other
the whole way.

Four decisions that shape the design:

1. **Two calls, not one and not twenty-two.** Generating each node separately would let
   branch B forget what branch A established by the third level. Generating all thirty-one
   at once no longer fits in a completion: the tree grew wider and every choice now carries
   the voices of people reacting to it. So it is built in two passes — the trunk (every
   decision node), then the endings, which are given the trunk and the exact path that
   reaches each one. Pass two sees the whole shape, so the endings stay coherent, and
   neither pass comes near the token cap.

2. **Three choices at the first two levels, two at the last.** 1 + 3 + 9 decision nodes
   and 18 endings. Width sits where agency matters most; three-wide throughout would mean
   40 endings, more than anyone will ever collect.

   Each node carries hard facts, and each choice a concrete outcome, a risk score and
   deltas against four named factions. Prose is capped short on purpose — the first
   version was a paragraph and two buttons, which reads as a questionnaire, not a game.

3. **Somebody always has an opinion.** Every choice carries two or three `reactions`:
   a named voice from that exact moment — a soldier, a widow, an ambassador, a pamphlet —
   with a `mood` drawn from a fixed vocabulary and one line of speech. Meters tell the
   player what changed; reactions tell them who is furious about it. The app aggregates
   the moods into a public-mood reading, which is why the vocabulary is closed and never
   translated: the words are structure, the quotes are prose.

   Endings carry `legacy` voices instead — the same idea, generations later.

4. **Pre-generated, not live.** A player who waits twelve seconds for an LLM is not
   playing a game. Everything here is on the device before they tap.

Generated for the top events of each tier — see PARALLEL_PER_TIER in main.py.
"""

import asyncio

from core.logger import setup_logger

logger = setup_logger("Parallel")

TRANSLATION_LANGS = ["ro", "es", "de", "fr"]
LANG_NAMES = {"ro": "Romanian", "es": "Spanish", "de": "German", "fr": "French"}

# ── Tree shape ───────────────────────────────────────────────────────────────
# TARGETS, not requirements. The prompt asks for this shape; the repair pass in
# `_prune` ships whatever subset of it actually came back. Asking for an exact node
# count is what made this feature cost money and produce nothing for a week: the model
# returned 12-16 nodes on every attempt against a demand for 22, so three full
# completions per event were thrown away and no game was ever generated.
ROOT_BRANCH = 3      # n0
MID_BRANCH = 3       # na, nb, nc
LEAF_BRANCH = 2      # naa … ncc
TARGET_DECISIONS = 13        # 1 + 3 + 9
TARGET_ENDINGS = 18          # 9 leaf nodes x 2
TARGET_ACTORS = 4
TARGET_RARE = 2

# ── Floors ───────────────────────────────────────────────────────────────────
# What a tree must still be after pruning to be worth shipping. Everything between a
# floor and its target is a smaller game, not a broken one.
MAX_DEPTH = 3        # decisions per run; the app reads "Decision 2 of 3" off this
MIN_CHOICES = 2      # a node offering one option is a corridor, not a decision
MIN_DEPTH = 2        # two decisions is still a game; one is a page with buttons
MIN_ENDINGS = 8      # below this the collection grid is not worth showing


def min_decisions(depth: int) -> int:
    """The fully-narrow tree at this depth: 1 + 2 + 4 + … Anything smaller has a branch
    missing rather than a branch that is merely narrow."""
    return sum(MIN_CHOICES ** i for i in range(depth))
MIN_ACTORS = 3

# The gap, on the summed-effects axis, between a node's best and worst option. Sixteen is
# two clear opposites (+8 and -8), which is what a three-decision run needs to be able to
# finish twenty points either side of history — the band the app draws the crowd on.
FLAT_NODE_SPREAD = 16

# How many events beyond `want` the generator may fall through to before giving up. The
# repair pass means the first candidate now usually works, so this is a safety net for
# genuinely hingeless days rather than the common path.
EXTRA_CANDIDATES = 2

MID_IDS = ["na", "nb", "nc"]
LEAF_IDS = ["naa", "nab", "nac", "nba", "nbb", "nbc", "nca", "ncb", "ncc"]
ENDING_IDS = ["e" + str(i) for i in range(1, TARGET_ENDINGS + 1)]

# leaf -> the two endings it opens onto.
LEAF_ENDINGS = {
    leaf: [ENDING_IDS[i * 2], ENDING_IDS[i * 2 + 1]]
    for i, leaf in enumerate(LEAF_IDS)
}

# Short on purpose. The player should read two sentences and then look at numbers —
# a wall of prose between every tap is what made the first version feel like a quiz.
MIN_NODE_WORDS = 35
MAX_NODE_WORDS = 110
MIN_FACTS = 2
MIN_ENDING_STATS = 4
MIN_REACTIONS = 2
MAX_REACTIONS = 3
MIN_LEGACY = 2
VALID_RARITY = {"common", "uncommon", "rare"}

# Closed vocabulary. The app maps each to a colour, an icon and a translated label, and
# sums their valences into the public-mood reading — so a mood outside this set would be
# a bar that does not move. Ordered roughly best to worst.
VALID_MOODS = [
    "elated", "hopeful", "relieved", "defiant",
    "uneasy", "resigned", "afraid", "angry", "betrayed", "grieving",
]
MOOD_SET = set(VALID_MOODS)

# ── Stances ──────────────────────────────────────────────────────────────────
# How boldly the fork may be chosen. Attempt 1 takes the safest reading of the event;
# each retry loosens it. Most days never leave `grounded` — the escalation exists because
# some events (a birth, a treaty signed without incident, a first performance) have no
# obvious hinge at all, and the app is supposed to have a game EVERY day, not on the days
# history happened to be dramatic.
STANCES = ["grounded", "contested", "speculative"]

STANCE_BRIEF = {
    "grounded": """STANCE — the obvious hinge.
Fork on the decision this event actually turned on, taken by the people who took it.""",

    "contested": """STANCE — the argument they were actually having.
This event has no single obvious hinge, so fork on what contemporaries genuinely FOUGHT
about at the time: the faction that lost the argument, the warning that was overruled, the
petition that was refused, the treaty clause that was struck out. Be pointed. Name who
wanted what and who was overruled. Controversy that was real then is the material —
present-day politics is not.""",

    "speculative": """STANCE — the near miss.
Fork on something that very nearly happened and did not: an order countermanded by hours,
a ship that sailed late, a man who lived when he should have died, an offer refused. State
the near miss plainly in the premise as what ALMOST happened, then run the game from the
moment it goes the other way. It must be documented as a real possibility of that week —
not an invention, and not a modern idea posted back in time. This is the boldest reading
allowed: everything after the fork is still bound by what was technologically, politically
and geographically possible in that year.""",
}

_NL = chr(10)

BAD_MARKERS = [
    "as an ai", "i cannot", "i'm sorry", "error generating",
    "content pending", "lorem ipsum", "placeholder",
]

# The prompts ban these; the validator enforces the ban, so the two cannot drift apart.
# They are the phrases that turn a scene back into a documentary voice-over, and one of
# them in the opening line undoes the present tense the rest of the node is written in.
BANNED_PHRASES = [
    "little did they know", "changed the course of history", "the rest is history",
    "only time will tell", "for better or worse", "butterfly effect",
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
    async def generate(self, items: list, narratives_map: dict, target_date,
                       want: int = 1) -> dict:
        """Return {"EVENT_0": {"en": {...}, "ro": {...}}, ...} — `want` games if it can.

        `items` is the whole tier, not a pre-sliced head. The generator tries the top
        `want` events first and, for each one that fails, falls down the list to the next
        candidate — because a day with no game at all is the one outcome worth spending
        an extra call to avoid. A failed event yields no key, so the app shows no game on
        that story rather than a broken one.

        Bounded by EXTRA_CANDIDATES so a genuinely unworkable day cannot run the bill up.
        """
        date_str = target_date.strftime("%B %d")

        async def one(idx: int, item: dict):
            context = narratives_map.get(f"EVENT_{idx}", {}).get("en", "")
            english = await self._generate_english(idx, item, date_str, context)
            if not english:
                logger.warning(f"⚠️ Parallel {idx} — no workable tree, trying the next event")
                return f"EVENT_{idx}", None

            out = {"en": english}
            translated = await asyncio.gather(*[
                self._translate(idx, english, lang) for lang in TRANSLATION_LANGS
            ])
            for lang, payload in zip(TRANSLATION_LANGS, translated):
                out[lang] = payload if payload else english
            return f"EVENT_{idx}", out

        want = max(0, min(want, len(items)))
        if not want:
            return {}

        results: dict = {}
        queue = list(range(len(items)))          # candidate indices, best first
        budget = want + EXTRA_CANDIDATES

        while queue and len(results) < want:
            # A day with NO game is the one outcome worth spending every candidate on, so
            # the budget only starts biting once the first tree has landed. After that the
            # extras are a nice-to-have and stay bounded.
            if results and budget <= 0:
                logger.info(f"🌌 Parallel universes: stopping at {len(results)}/{want} — budget spent")
                break
            take = min(want - len(results), len(queue))
            batch = [queue.pop(0) for _ in range(take)]
            budget -= len(batch)
            done = await asyncio.gather(*[one(i, items[i]) for i in batch])
            results.update({k: v for k, v in done if v})

        ok = len(results)
        tried = len(items) - len(queue)
        if not ok:
            logger.error(
                f"🚨 Parallel universes: NO game for this tier — all {tried} candidates "
                f"failed across every stance. This day will have no branching game."
            )
        elif ok < want:
            logger.warning(f"🌌 Parallel universes: {ok}/{want} after {tried} candidates")
        else:
            logger.info(f"🌌 Parallel universes: {ok}/{want} complete ({tried} candidates tried)")
        return results

    # ══════════════════════════════════════════════════════════════════
    # ENGLISH — two passes
    # ══════════════════════════════════════════════════════════════════
    async def _generate_english(self, idx, item, date_str, context) -> dict | None:
        built = await self._generate_trunk(idx, item, date_str, context)
        if not built:
            return None
        trunk, slots, depth = built

        endings = await self._generate_endings(idx, item, trunk, slots)
        if not endings:
            return None

        # Final prune, now that the endings are real: any leaf choice whose ending never
        # arrived is dropped, and any node that leaves too thin goes with it. This is the
        # seam between the two passes and the only place a dangling tap could survive.
        actor_ids = {a["id"] for a in trunk["actors"]}
        endings_by_id = {
            e["id"]: e for e in endings if not self._node_broken(e, ending=True)
        }
        nodes, _ = self._prune(trunk["nodes"], trunk["root"], endings_by_id, actor_ids, depth)
        if not nodes:
            logger.warning(f"⚠️ Parallel {idx} — nothing survived once the endings landed")
            return None

        reached = {c["next"] for n in nodes for c in n["choices"]} & set(endings_by_id)
        full = {**trunk, "nodes": nodes + [endings_by_id[e] for e in sorted(reached)]}

        ok, why = self._validate_full(full)
        if not ok:
            logger.warning(f"⚠️ Parallel {idx} — assembled tree unusable: {why}")
            return None

        logger.info(
            f"✅ Parallel {idx}:en — {len(full['nodes'])} nodes "
            f"({len(nodes)} decisions, {len(reached)} endings)"
        )
        return full

    # ── Pass 1: the trunk ─────────────────────────────────────────────
    async def _generate_trunk(self, idx, item, date_str, context):
        """Returns (trunk, ending slot ids) or None.

        One attempt per stance, and each attempt is PRUNED before it is judged — so a
        response with nine good nodes out of thirteen ships a nine-node game instead of
        being thrown away and paid for again.
        """
        year = item.get("year", "")
        text = item.get("text", "")
        location = item.get("location") or "the location"

        for attempt, stance in enumerate(STANCES, start=1):
            res = await self.processor._safe_ai_call(
                self._trunk_prompt(year, text, location, date_str, context, stance),
                f"Parallel {idx}:trunk ({stance}, attempt {attempt})",
                {"nodes": []},
                temperature=0.85,   # this is fiction built on fact; it needs room
                # Thirteen nodes with facts, thirty choices with outcomes and roughly
                # seventy-five quotes does not fit in 16k, and on Vertex the thinking
                # budget comes out of the same allowance. Being clipped here is the
                # worst possible spend: full price, unusable answer.
                max_tokens=32768,
                thinking_budget=self.thinking_budget,
            )
            payload = self._normalize(res)
            raw = len(payload["nodes"])
            actor_ids = {a["id"] for a in payload["actors"]}
            root = payload["root"] if payload["root"] in {n["id"] for n in payload["nodes"]} else "n0"

            # Full depth first; one shallower if nothing survives. A response that only
            # ever reached the second level still makes a two-decision game, and two
            # decisions beat the blank screen this feature has shown since launch.
            why = "no nodes"
            for depth in range(MAX_DEPTH, MIN_DEPTH - 1, -1):
                nodes, slots = self._prune(payload["nodes"], root, None, actor_ids, depth)
                cand = {**payload, "root": root, "nodes": nodes}
                ok, why = self._validate_trunk(cand, slots, depth)
                if ok:
                    decisions = [n for n in nodes if n.get("choices")]
                    moving = sum(1 for n in decisions if self._node_spread(n) >= FLAT_NODE_SPREAD)
                    short = "" if depth == MAX_DEPTH else f" — SHALLOW, {depth} decisions"
                    logger.info(
                        f"🌳 Parallel {idx}:trunk — {len(nodes)}/{TARGET_DECISIONS} decisions, "
                        f"{len(slots)} ending slots "
                        f"(kept {len(nodes)} of {raw} returned, {stance}{short}, "
                        f"{moving}/{len(decisions)} nodes move the world)"
                    )
                    return cand, slots, depth
            logger.warning(f"⚠️ Parallel {idx}:trunk {stance}: {why} (model returned {raw} nodes)")
        return None

    # ── Pass 2: the endings ───────────────────────────────────────────
    async def _generate_endings(self, idx, item, trunk: dict, slots: set) -> list | None:
        """Only the endings the pruned tree actually reaches. Generating the full
        eighteen when the trunk opens onto ten is ten worlds nobody can visit, paid for
        at the same rate as the ones they can."""
        wanted = sorted(slots)
        for attempt in range(1, 3):
            res = await self.processor._safe_ai_call(
                self._endings_prompt(item, trunk, wanted),
                f"Parallel {idx}:endings (attempt {attempt})",
                {"nodes": []},
                temperature=0.85,
                max_tokens=32768,
                thinking_budget=self.thinking_budget,
            )
            nodes = self._normalize({"nodes": res.get("nodes") if isinstance(res, dict) else []})["nodes"]
            endings = [n for n in nodes if n["id"] in slots and not n["choices"]]
            ok, why = self._validate_endings(endings, slots)
            if ok:
                logger.info(f"🌌 Parallel {idx}:endings — {len(endings)}/{len(wanted)} worlds")
                return endings
            logger.warning(f"⚠️ Parallel {idx}:endings attempt {attempt}: {why}")
        return None

    # ══════════════════════════════════════════════════════════════════
    # PROMPTS
    # ══════════════════════════════════════════════════════════════════
    def _trunk_prompt(self, year, text, location, date_str, context, stance="grounded") -> str:
        ctx = ""
        if context:
            ctx = _NL.join(["", "WHAT ACTUALLY HAPPENED:", context[:1200], ""])
        moods = ", ".join(VALID_MOODS)
        wiring = _NL.join(
            ["  n0  -> " + ", ".join(MID_IDS)] +
            [
                "  " + mid + " -> " + ", ".join(LEAF_IDS[i * 3:i * 3 + 3])
                for i, mid in enumerate(MID_IDS)
            ] +
            [
                "  " + leaf + " -> " + ", ".join(LEAF_ENDINGS[leaf])
                for leaf in LEAF_IDS
            ]
        )
        return f"""
You are designing a short branching history game. The player stands at a real hinge in
the past and makes THREE consecutive decisions. Every path must be grounded in what was
actually plausible at the time — no magic, no anachronism, no hindsight the people
involved could not have had.

EVENT: {year} — {text}
DATE: {date_str}, {year}
PLACE: {location}
{ctx}
{STANCE_BRIEF[stance]}

Whatever the stance, the fork must be a decision a named person could have taken that
week, and every branch must stay inside what was possible in {year}.

THIS PASS BUILDS THE DECISION NODES ONLY — {TARGET_DECISIONS} of them, ids as written.
The endings are written separately; you only wire choices to their ids.

  n0                              (opening, {ROOT_BRANCH} choices)
  {", ".join(MID_IDS)}                    ({MID_BRANCH} choices each)
  {", ".join(LEAF_IDS)}   ({LEAF_BRANCH} choices each)

Wiring, exactly:
{wiring}

If you cannot make three genuinely different options work at some node, give TWO good
ones rather than three where one is filler. A complete smaller tree is worth more than a
wide one with padding in it — every branch you write must go all the way down.

The ending ids ({ENDING_IDS[0]} … {ENDING_IDS[-1]}) are NOT nodes in this pass. They appear only as
the `next` value of a choice on a leaf node.

ACTORS — {TARGET_ACTORS} of them
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
- `year` advances as the tree deepens: n0 at the event, the second level within months or
  a few years, the third a decade out.

FACTS — {MIN_FACTS} to 4 per node
Hard numbers about the situation right now, as {{label, value}}. These are what the
player actually decides on, so they must be real and specific to that exact moment:
  {{"label": "Bohemian army", "value": "15,000, unpaid since spring"}}
  {{"label": "Imperial treasury", "value": "empty — 2M florins owed"}}
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

RULE: the four effects must not CANCEL OUT. Add them up — that sum is where the world
lands, and the screen draws it. Across the options at one node those sums must spread:
at least one option must leave the world clearly better (sum >= +8) and at least one
clearly worse (sum <= -8). Three options that all sum to roughly nothing are three ways
of changing nothing, and a whole run of them ends exactly where history did.

Aim so that a player who keeps choosing well finishes twenty or more points above
history, and one who keeps choosing badly twenty or more below.

A net-positive option is NOT the obviously correct one. It can cost you every actor's
standing and enrage the people — that is what `actor_effects` and `reactions` are for.
Good for the world and ruinous for you is the most interesting card in the game.

ACTOR EFFECTS
`actor_effects` maps actor ids to standing deltas, -30 to +30. Every choice must move at
least TWO actors, and at least one must go DOWN. Pleasing everyone is not politics.
  {{"habsburg": 12, "estates": -18}}

REACTIONS — {MIN_REACTIONS} or {MAX_REACTIONS} per choice. THIS IS THE HEART OF THE GAME.
How people alive at that moment take the news, as {{who, mood, quote}}:
- `who`: a specific person or group of that exact time and place, 2-6 words. Not "the
  people". "A Prague pewterer", "Tilly's unpaid sergeants", "The Venetian ambassador",
  "Widows of the Old Town".
- `mood`: EXACTLY ONE of: {moods}. Never invent a mood; these are a fixed vocabulary.
- `quote`: one sentence, 6-24 words, in that person's own voice, using only what they
  could plausibly know that week. No hindsight, no statistics they could not have.
    {{"who": "A Prague pewterer", "mood": "betrayed",
      "quote": "We threw them from the window for this, and now we kneel to Vienna anyway."}}
- Make the voices DISAGREE. A choice that leaves everyone in the same mood is a choice
  with no politics in it. At least one reaction on each choice must differ in mood from
  the others, and across a node's choices the moods must not all be the same.
- Ordinary people, not only princes. A soldier, a mother, a printer, a moneylender.

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
          "reactions": [
            {{"who": "A Prague pewterer", "mood": "betrayed", "quote": "..."}},
            {{"who": "Tilly's sergeants", "mood": "elated", "quote": "..."}}
          ],
          "next": "na"}}
      ]
    }}
  ]
}}
"""

    def _endings_prompt(self, item, trunk: dict, wanted: list) -> str:
        """Pass two. The model gets the finished trunk and, for each ending it must
        write, the exact chain of choices that reaches it plus the world they add up to —
        so an ending can never describe a war the player's path avoided."""
        by_id = {n["id"]: n for n in trunk["nodes"]}
        actor_names = {a["id"]: a["name"] for a in trunk["actors"]}
        moods = ", ".join(VALID_MOODS)
        want = set(wanted)

        # Walk every root-to-ending path once, carrying the running totals. Depth is
        # whatever survived pruning, so this recurses rather than assuming three levels.
        lines = []

        def walk(nid: str, labels: list, eff: dict, standing: dict):
            for c in by_id[nid]["choices"]:
                nxt = c["next"]
                eff2 = {k: eff[k] + c["effects"][k] for k in eff}
                st2 = dict(standing)
                for aid, d in c["actor_effects"].items():
                    if aid in st2:
                        st2[aid] = max(0, min(100, st2[aid] + d))
                if nxt in by_id:
                    walk(nxt, labels + [c["label"]], eff2, st2)
                elif nxt in want:
                    world = ", ".join(f"{k} {v:+d}" for k, v in eff2.items())
                    board = ", ".join(f"{actor_names.get(a, a)} {v}" for a, v in st2.items())
                    path = " → ".join(labels + [c["label"]])
                    lines.append(
                        f'  {nxt}: {path}{_NL}'
                        f'      world: {world}{_NL}'
                        f'      standing: {board}'
                    )

        walk(
            trunk["root"], [],
            {"stability": 0, "lives": 0, "progress": 0, "freedom": 0},
            {a["id"]: a["start"] for a in trunk["actors"]},
        )
        paths = _NL.join(lines)

        n_end = len(wanted)
        # Rarity is scaled to however many endings survived: one rare in a ten-ending
        # tree is the same treat as two in eighteen.
        n_rare = max(1, round(n_end * TARGET_RARE / TARGET_ENDINGS))
        n_uncommon = max(1, round(n_end * 7 / TARGET_ENDINGS))
        n_common = max(0, n_end - n_rare - n_uncommon)

        premise = trunk["premise"]
        pivot = trunk["pivot_title"]
        year = item.get("year", "")

        return f"""
You are finishing a branching history game. The decision tree is already written. Your
job is the {n_end} endings — the worlds those decisions produce.

THE FORK: {pivot} ({year})
WHAT REALLY HAPPENED: {premise}

Each ending below is listed with the choices that reach it, the sum of their effects on
the four world meters, and where the factions ended up. WRITE THE WORLD THOSE NUMBERS
DESCRIBE. An ending whose meters say `lives +40` cannot open on a massacre.

Write an ending for EVERY id listed and no others — these ids are the only ones the
tree can reach.

{paths}

FOR EACH OF THE {n_end} ENDINGS
- `id`: exactly as listed above. `choices`: an empty array. `facts`: an empty array.
- `year`: well after the divergence — a generation or a century later, as the world needs.
- `title`: short. Name the world, do not summarise it.
- `text`: {MIN_NODE_WORDS}-{MAX_NODE_WORDS} words. Show the world through one concrete scene or
  detail — a street, a ledger, a border, a language being spoken. Not a summary of events.
- `verdict`: one line — better, worse, or simply unrecognisable?
- `epitaph`: one memorable sentence. The last thing the player reads. Make it land.
- `rarity`: mark {n_rare} "rare", {n_uncommon} "uncommon" and {n_common} "common". A rare world is
  the most surprising or hardest-won, not merely the happiest.
- `stats`: {MIN_ENDING_STATS} to 6 entries as {{label, real, alt}} comparing your world to the
  actual one. THIS IS THE PAYOFF — make the numbers concrete and checkable:
  {{"label": "Deaths in the German lands", "real": "8 million", "alt": "under 1 million"}}
  {{"label": "Holy Roman Empire dissolved", "real": "1806", "alt": "never"}}
  `real` must be the true historical figure. `alt` is your world's.
- `legacy`: {MIN_LEGACY} or {MAX_REACTIONS} voices from INSIDE this world, generations after the
  fork, as {{who, mood, quote}} — how the people living in it talk about what was decided.
  `who` is specific and of that later time: "A schoolteacher in Brno, 1780", "A dockworker,
  1912". `mood` is EXACTLY ONE of: {moods}. `quote` is one sentence, 6-24 words, and must
  belong to this world alone — someone in a different ending could not have said it.
  Let them disagree with each other about whether it was worth it.

THE {n_end} MUST BE GENUINELY DIFFERENT WORLDS, not one world in {n_end} moods. Two endings
that hang off the same node are the pair a player sees back to back — those two above all
must diverge.

BANNED: "little did they know", "changed the course of history", "the rest is history",
"only time will tell", "for better or worse", "a butterfly effect".

Return JSON, nothing else:
{{
  "nodes": [
    {{
      "id": "{wanted[0] if wanted else 'e1'}", "year": "1789", "title": "ending title",
      "text": "{MIN_NODE_WORDS}-{MAX_NODE_WORDS} words", "facts": [], "choices": [],
      "verdict": "one line", "epitaph": "one memorable sentence", "rarity": "common",
      "stats": [{{"label": "...", "real": "...", "alt": "..."}}],
      "legacy": [{{"who": "A schoolteacher in Brno, 1780", "mood": "resigned", "quote": "..."}}]
    }}
  ]
}}
"""

    # ══════════════════════════════════════════════════════════════════
    # TRANSLATION
    # ══════════════════════════════════════════════════════════════════
    async def _translate(self, idx: int, english: dict, lang: str) -> dict | None:
        lang_full = LANG_NAMES.get(lang, lang.upper())

        # Ids, wiring, effects and moods are structure, not prose — they are stripped from
        # the request and reattached afterwards, so a translation can never rewire the tree
        # or invent a mood the app has no colour for.
        skeleton = [
            {
                "id": n["id"], "year": n["year"], "title": n["title"], "text": n["text"],
                "verdict": n.get("verdict", ""), "epitaph": n.get("epitaph", ""),
                "facts": [{"label": f["label"], "value": f["value"]} for f in n["facts"]],
                "stats": [{"label": st["label"], "real": st["real"], "alt": st["alt"]}
                          for st in n["stats"]],
                "legacy": [{"who": v["who"], "quote": v["quote"]} for v in n.get("legacy", [])],
                "choices": [
                    {
                        "id": c["id"], "label": c["label"], "detail": c["detail"],
                        "outcome": c["outcome"],
                        "reactions": [{"who": v["who"], "quote": v["quote"]}
                                      for v in c.get("reactions", [])],
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
"epitaph", "who", "quote" and every actor "name".
The quotes are ordinary people speaking. Keep them spoken, not written — a sergeant and a
widow do not sound like a chronicle. Keep each to one sentence.
Return the SAME arrays with the SAME ids in the SAME order — ids are structure, never
translate or reorder them.

ACTORS: {actor_names}

{{"pivot_title": "...", "premise": "...", "nodes": {skeleton}}}

Return JSON with "pivot_title", "premise", "actors" and "nodes" as above, in {lang_full}.
""",
            f"Parallel {idx}:{lang}",
            {"nodes": []},
            temperature=0.3,
            max_tokens=32768,
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
        if len(by_id) < len(english["nodes"]) * 0.8:
            return None

        def by_index(translated, original, keys):
            """Graft translated strings onto the original list, position by position.
            Anything missing or malformed falls back to English rather than blanking a
            field the UI expects to be there."""
            out = []
            for i, orig in enumerate(original):
                tr = translated[i] if isinstance(translated, list) and i < len(translated) else {}
                tr = tr if isinstance(tr, dict) else {}
                out.append({**orig, **{k: str(tr.get(k) or orig[k]) for k in keys}})
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
                # `mood` is carried over from the English node untouched by the **orig
                # spread in by_index — it is a key the app switches on, not prose.
                "legacy": by_index(tr.get("legacy"), en_node.get("legacy", []), ("who", "quote")),
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
                        "reactions": by_index(
                            (tr_choices.get(c["id"]) or {}).get("reactions"),
                            c.get("reactions", []),
                            ("who", "quote"),
                        ),
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
    # NORMALIZE
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

        def voices(raw) -> list:
            """Reactions and legacy voices. A voice whose mood is outside the vocabulary
            is dropped rather than kept: the app switches on `mood` for colour, icon and
            the public-mood total, so an unknown one is a silent hole in the UI."""
            out = []
            for v in (raw or []):
                if not isinstance(v, dict):
                    continue
                mood = str(v.get("mood") or "").strip().lower()
                who = str(v.get("who") or "").strip()
                quote = str(v.get("quote") or "").strip()
                if mood in MOOD_SET and who and quote:
                    out.append({"who": who, "mood": mood, "quote": quote})
            return out[:MAX_REACTIONS]

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
                    "reactions": voices(c.get("reactions")),
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
                # An unknown rarity is coerced rather than rejected: the app colours the
                # badge by this string and anything it does not recognise silently reads
                # as common, so making that explicit here keeps the data honest.
                "rarity": (lambda r: r if r in VALID_RARITY else "common")(
                    str(n.get("rarity") or "").strip().lower()
                ),
                "stats": pairs(n.get("stats"), ("label", "real", "alt")),
                "legacy": voices(n.get("legacy")),
            })

        return {
            "pivot_year": str(res.get("pivot_year") or "").strip(),
            "pivot_title": str(res.get("pivot_title") or "").strip(),
            "premise": str(res.get("premise") or "").strip(),
            "root": str(res.get("root") or "n0").strip(),
            "actors": actors,
            "nodes": nodes,
        }

    # ══════════════════════════════════════════════════════════════════
    # REPAIR — take what the model gave, not what it was asked for
    # ══════════════════════════════════════════════════════════════════
    #
    # The old contract was all-or-nothing: exactly 22 nodes or the tree was thrown away
    # and the call retried. In production the model returned 12-16 nodes every single
    # time, so every event burned three full completions and shipped nothing. The shape
    # was never the point — a playable tree is. So the tree is now PRUNED to the largest
    # thing that actually works, and only genuinely unplayable output is rejected.

    def _choice_flaws(self, c: dict, actor_ids: set) -> list:
        """Quality complaints about one choice. Empty means it is good.

        Split from the fatal checks on purpose: these describe a choice that is dull,
        not one that is broken, and a dull choice still beats no game.
        """
        flaws = []
        vals = list(c["effects"].values())
        if all(v == 0 for v in vals):
            flaws.append("no effect")
        elif not (any(v > 0 for v in vals) and any(v < 0 for v in vals)):
            flaws.append("trades nothing")
        ae = c["actor_effects"]
        if len(ae) < 2:
            flaws.append("moves <2 actors")
        elif not any(v < 0 for v in ae.values()):
            flaws.append("pleases everyone")
        rx = c["reactions"]
        if len(rx) < MIN_REACTIONS:
            flaws.append("too few voices")
        elif len({v["mood"] for v in rx}) < 2:
            flaws.append("one mood")
        if not c["outcome"]["label"] or not c["outcome"]["value"]:
            flaws.append("no outcome")
        return flaws

    @staticmethod
    def _node_spread(n: dict) -> int:
        """How far apart this node's options leave the world.

        Each choice's four effects sum to where it puts the world; the spread is the gap
        between the best and worst option on that axis. A node whose options all sum to
        about nothing is three ways of changing nothing, and a run of them lands exactly
        on history — which is what kept the crowd stuck between its three states for
        entire runs, since the screen reads that same sum.
        """
        nets = [sum(c["effects"].values()) for c in n.get("choices", []) if c.get("effects")]
        return max(nets) - min(nets) if len(nets) > 1 else 0

    @staticmethod
    def _choice_broken(c: dict) -> bool:
        """Fatal: the card cannot be drawn or the tap goes nowhere."""
        return not c["label"] or not c["detail"] or not c["next"]

    def _node_broken(self, n: dict, ending: bool) -> str | None:
        """Fatal problems with a node. Word count is deliberately NOT fatal — a node
        thirty words over is worth shipping; a node quoting a banned cliché is not,
        because that is the one thing that makes the whole screen read as filler."""
        if not n["title"] or not n["text"]:
            return "no title or text"
        low = n["text"].lower()
        for mk in BAD_MARKERS:
            if mk in low:
                return "contains " + mk
        for ph in BANNED_PHRASES:
            if ph in low:
                return 'banned phrase "' + ph + '"'
        if len(n["text"].split()) > MAX_NODE_WORDS * 2:
            return "runaway text"
        if ending and (not n["verdict"] or not n["epitaph"]):
            return "no verdict or epitaph"
        return None

    def _prune(self, nodes: list, root: str, endings_by_id: dict | None, actor_ids: set,
               depth: int = MAX_DEPTH):
        """The largest uniform-depth playable tree inside `nodes`.

        Walks down from the root keeping only choices that lead somewhere real, and drops
        any node left with fewer than MIN_CHOICES. Uniform depth matters: the app shows
        "Decision 2 of 3", so a branch that runs out early would lie to the player.

        `endings_by_id` is None during pass one, when the endings do not exist yet and any
        unknown target at the bottom level is simply an ending slot to be filled later.

        `depth` is how many decisions a run must take. Callers retry one shallower when
        nothing survives: two decisions and eight endings is a smaller game, but it is a
        game, and the app reads the depth off the tree rather than assuming three.

        Returns (kept nodes, ending slot ids).
        """
        by_id = {n["id"]: n for n in nodes}
        memo: dict = {}
        slots: set = set()

        def keep(nid: str, level: int, path: frozenset):
            if (nid, level) in memo:
                return memo[(nid, level)]
            if nid in path:          # a cycle: this branch is not a tree
                return None
            node = by_id.get(nid)
            if node is None or self._node_broken(node, ending=False):
                return None

            if level == depth:
                # Bottom decision node: its choices open onto endings.
                good = []
                for c in node["choices"]:
                    if self._choice_broken(c):
                        continue
                    if endings_by_id is None:
                        # Anything that is not a decision node we returned is a slot.
                        if c["next"] not in by_id:
                            good.append(c)
                    elif c["next"] in endings_by_id:
                        good.append(c)
                survivors = self._best(good, actor_ids)
            else:
                good = []
                for c in node["choices"]:
                    if self._choice_broken(c):
                        continue
                    if keep(c["next"], level + 1, path | {nid}) is not None:
                        good.append(c)
                survivors = self._best(good, actor_ids)

            if len(survivors) < MIN_CHOICES:
                memo[(nid, level)] = None
                return None
            out = {**node, "choices": survivors}
            memo[(nid, level)] = out
            return out

        kept_root = keep(root, 1, frozenset())
        if kept_root is None:
            return [], set()

        # Second walk collects the surviving nodes now that every branch is settled.
        kept: dict = {}
        stack = [(root, 1)]
        while stack:
            nid, level = stack.pop()
            node = memo.get((nid, level))
            if node is None or nid in kept:
                continue
            kept[nid] = node
            for c in node["choices"]:
                if level == depth:
                    slots.add(c["next"])
                else:
                    stack.append((c["next"], level + 1))

        return list(kept.values()), slots

    def _best(self, choices: list, actor_ids: set) -> list:
        """Keep every clean choice; if that leaves a node unplayable, top it up with the
        least-flawed of the rest. A node with two mediocre options is a game; a node with
        one is a corridor."""
        graded = [(len(self._choice_flaws(c, actor_ids)), i, c) for i, c in enumerate(choices)]
        clean = [c for n, _, c in graded if n == 0]
        if len(clean) >= MIN_CHOICES:
            return clean
        graded.sort(key=lambda g: (g[0], g[1]))
        return [c for _, _, c in graded[:max(MIN_CHOICES, len(clean))]]

    # ══════════════════════════════════════════════════════════════════
    # VALIDATE

    def _validate_trunk(self, p: dict, slots: set, depth: int = MAX_DEPTH) -> tuple:
        """What is left after pruning still has to be a game worth opening."""
        if not p["premise"] or not p["pivot_title"]:
            return False, "missing premise or pivot title"
        if len(p["actors"]) < MIN_ACTORS:
            return False, str(len(p["actors"])) + " actors, need " + str(MIN_ACTORS)
        if any(not a["name"] for a in p["actors"]):
            return False, "an actor has no name"
        floor = min_decisions(depth)
        if len(p["nodes"]) < floor:
            return False, (str(len(p["nodes"])) + " decision nodes survived at depth "
                           + str(depth) + ", need " + str(floor))
        if len(slots) < MIN_ENDINGS:
            return False, str(len(slots)) + " ending slots, need " + str(MIN_ENDINGS)

        decisions = [n for n in p["nodes"] if n.get("choices")]
        moving = [n for n in decisions if self._node_spread(n) >= FLAT_NODE_SPREAD]
        if decisions and not moving:
            return False, ("every decision leaves the world in the same place — "
                           "no node spreads its options by " + str(FLAT_NODE_SPREAD))
        return True, "OK"

    def _validate_endings(self, endings: list, slots: set) -> tuple:
        if len(endings) < MIN_ENDINGS:
            return False, str(len(endings)) + " endings, need " + str(MIN_ENDINGS)
        got = {e["id"] for e in endings}
        if not (got & slots):
            return False, "no ending matches a slot the tree actually reaches"
        return True, "OK"

    def _validate_full(self, p: dict) -> tuple:
        """Last gate before the tree ships: every tap must land somewhere real."""
        by_id = {n["id"]: n for n in p["nodes"]}
        if len(by_id) != len(p["nodes"]):
            return False, "duplicate node ids across passes"
        if p["root"] not in by_id:
            return False, "root is not a node"

        for n in p["nodes"]:
            for c in n["choices"]:
                if c["next"] not in by_id:
                    return False, n["id"] + " -> unknown node " + c["next"]

        reachable = set()
        stack = [p["root"]]
        while stack:
            cur = stack.pop()
            if cur in reachable:
                continue
            reachable.add(cur)
            stack.extend(c["next"] for c in by_id[cur]["choices"])
        if len(reachable) != len(p["nodes"]):
            return False, str(len(p["nodes"]) - len(reachable)) + " unreachable node(s)"

        endings = [n for n in p["nodes"] if not n["choices"]]
        if len(endings) < MIN_ENDINGS:
            return False, str(len(endings)) + " endings survived"
        return True, "OK"
