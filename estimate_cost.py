"""What a day of generation costs, without spending anything to find out.

Call counts here are STRUCTURAL -- they are read out of the pipeline's own constants,
so they are exactly what a run will do. Token sizes are ESTIMATES, based on the
lengths the prompts ask for (a ~600 word article, a ~2000 word long read) and on the
ceilings in the code. They will be wrong by some margin in both directions.

The real numbers come from core.llm.cost_report(), which meters an actual run. Use
this to decide whether a change is worth making; use the meter to find out if it was.

    py estimate_cost.py            # today's shape
    py estimate_cost.py --month    # the same, times 30
"""

import re
import sys
from pathlib import Path

# Groq, gpt-oss-120b. CHECK THESE against Groq's pricing page before trusting a
# number that comes out of here — they are the only hand-entered figures in the file.
# For reference, the Gemini Flash rates this replaced were 0.30 / 2.50, and the bulk
# of that bill arrived on a separate "Thinking On" output SKU that Groq does not have.
PRICE_IN = 0.15 / 1_000_000
PRICE_OUT = 0.75 / 1_000_000
BATCH_DISCOUNT = 0.5


def const(path: str, name: str, default: int) -> int:
    """Read a constant out of the source so this cannot drift from the pipeline."""
    try:
        src = Path(path).read_text(encoding="utf-8")
        m = re.search(rf"^{name}\s*=\s*(\d+)", src, re.M)
        return int(m.group(1)) if m else default
    except Exception:
        return default


FREE = const("main.py", "TARGET_FREE_COUNT", 4)
PRO = const("main.py", "TARGET_PRO_COUNT", 2)
PARALLEL_PER_TIER = const("main.py", "PARALLEL_PER_TIER", 1)
TARGET_ENDINGS = const("engine/parallel.py", "TARGET_ENDINGS", 8)
TARGET_DECISIONS = const("engine/parallel.py", "TARGET_DECISIONS", 7)

EVENTS = FREE + PRO
TRANSLATIONS = 4  # ro, es, de, fr -- English is generated, these are carried over

# (stage, calls, input tokens each, output tokens each)
# Sizes come from what each prompt asks for: ~600 words of article is ~900 tokens,
# a ~2000 word long read is ~2800, and a tree node carries a scene plus reactions.
TREE_OUT = int(320 * (TARGET_DECISIONS + TARGET_ENDINGS))  # scales with the shape
TREES = PARALLEL_PER_TIER * 2

STAGES = [
    ("narrative EN",        EVENTS,                    1500, 900),
    ("narrative translate", EVENTS * TRANSLATIONS,     1100, 900),
    ("long read EN",        PRO,                       2000, 2800),
    ("long read translate", PRO * TRANSLATIONS,        3000, 2800),
    ("quiz (5 langs, 1 call)", EVENTS,                 2500, 3500),
    ("parallel tree EN",    TREES,                     3000, TREE_OUT),
    ("parallel translate",  TREES * TRANSLATIONS,      TREE_OUT + 500, TREE_OUT),
    ("title repair",        EVENTS,                     400, 200),
    ("narrative verify",    EVENTS,                     900, 300),
]

# Discovery is what this pipeline stops doing if events arrive in a file. It is also
# the part these estimates are least able to model, because the call count depends on
# how many candidates a day throws up. Shown separately, and deliberately rough.
DISCOVERY = [
    ("rank candidates",     3, 12000, 2500),
    ("dedupe",              2,  8000, 1200),
    ("validate dates",      EVENTS * 2, 1200, 400),
]


def render(rows, title, times=1):
    print(f"\n{title}")
    print("-" * 76)
    print(f"{'stage':<26}{'calls':>7}{'in':>12}{'out':>12}{'USD':>10}{'%':>6}")
    print("-" * 76)
    total = 0.0
    priced = []
    for name, calls, tin, tout in rows:
        calls *= times
        cost = calls * (tin * PRICE_IN + tout * PRICE_OUT)
        total += cost
        priced.append((cost, name, calls, calls * tin, calls * tout))
    for cost, name, calls, tin, tout in sorted(priced, reverse=True):
        pct = cost / total * 100 if total else 0
        print(f"{name:<26}{calls:>7}{tin:>12,}{tout:>12,}{cost:>10.4f}{pct:>5.0f}%")
    print("-" * 76)
    print(f"{'subtotal':<26}{'':>7}{'':>12}{'':>12}{total:>10.4f}")
    return total


def main():
    month = "--month" in sys.argv
    days = 30 if month else 1
    label = "PER MONTH (30 days)" if month else "PER DAY"

    print("=" * 76)
    print(f"  COST ESTIMATE -- {label}")
    print(f"  {FREE} free + {PRO} pro = {EVENTS} events/day | {TREES} tree(s) of "
          f"{TARGET_DECISIONS} decisions + {TARGET_ENDINGS} endings")
    print("=" * 76)

    content = render(STAGES, "CONTENT -- kept even if you supply the events", days)
    discovery = render(DISCOVERY, "DISCOVERY -- disappears if you supply the events", days)

    print("\n" + "=" * 76)
    print(f"  {'content + discovery (today)':<46}{content + discovery:>10.4f}")
    print(f"  {'content only (you supply events)':<46}{content:>10.4f}")
    print(f"  {'content only, on Batch API (-50%)':<46}{content * BATCH_DISCOUNT:>10.4f}")
    print("=" * 76)
    print("\nToken sizes are estimates; call counts are read from the pipeline's own")
    print("constants. Run the pipeline once for the measured figure.\n")


if __name__ == "__main__":
    main()
