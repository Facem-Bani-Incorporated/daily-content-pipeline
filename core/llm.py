"""
Provider-agnostic LLM client for the whole pipeline.

Gemini, OpenAI and Groq all expose an OpenAI-compatible Chat Completions API, so we
talk to every one of them through the single `openai` SDK, differing only by base URL,
API key and model. Switch providers by changing env vars (AI_PROVIDER / AI_MODEL /
the matching *_API_KEY) — no code change.

Call sites keep passing the old Anthropic-style inputs (`system`, `prompt`,
`max_tokens`, `thinking_budget`); `build_params` translates them, and
`parse_json_response` reads `choices[0].message.content` and parses JSON leniently.
"""

import asyncio
import json
import re
import threading
import time
import traceback

from json_repair import repair_json
from openai import OpenAI, AsyncOpenAI

from core.config import config
from core.logger import setup_logger

logger = setup_logger("LLM")

# A model that is always valid on the default provider. If AI_MODEL is a typo or a
# retired name, the call 404s and we transparently retry on this, so the pipeline
# cannot be broken by one bad env value.
FALLBACK_MODEL = "openai/gpt-oss-20b"

# Groq is the only provider. The multi-provider table this replaced carried Gemini,
# OpenAI and Vertex, and every one of them was a way to accidentally keep billing
# Google after the project had moved off it.
#
# `reasoning`: gpt-oss accepts reasoning_effort. `idle_effort`: what the creative and
# mechanical calls get — writing an article or translating one gains nothing from
# deliberation, and reasoning tokens bill as output.
# `json_mode` is off: gpt-oss constrained decoding returns empty or invalid output,
# so a strict system prompt plus lenient parsing does the job instead.
_GROQ = {
    "base_url": "https://api.groq.com/openai/v1",
    "key_attr": "GROQ_API_KEY",
    "reasoning": True,
    "idle_effort": "low",
    "json_mode": False,
}

# The SDK retries 429s automatically, honouring the provider's retry-after header.
_MAX_RETRIES = 6

# Gemini drew thinking tokens out of max_tokens, so a reasoning call needed a floor
# under its ceiling or it truncated to empty JSON. Groq does not work that way, and
# the floor actively hurt there: Groq counts max_tokens against the tokens-per-minute
# budget, so reserving 12288 output tokens turned a ~4k discovery prompt into a 16.7k
# request and it was refused outright on an 8k TPM tier.
#
# Kept at 0 rather than deleted because the call sites still pass thinking_budget and
# the concept has to survive if a provider like that ever comes back.
_REASONING_TOKEN_FLOOR = 0

_async_client = None
_sync_client = None


def _provider() -> dict:
    return _GROQ


def log_config_diagnostics() -> None:
    """One-shot log of which LLM env vars actually reached the app, so a mangled
    variable name in the host shows up instead of silently defaulting."""
    key = "set" if getattr(config, "GROQ_API_KEY", None) else "MISSING"
    logger.info(
        f"LLM env -> provider=groq model={str(getattr(config, 'AI_MODEL', None))!r} "
        f"GROQ_API_KEY={key}"
    )


def _resolve_key() -> str:
    key = getattr(config, "GROQ_API_KEY", None)
    if not key:
        raise RuntimeError("GROQ_API_KEY is not set.")
    return str(key)


def get_async_client() -> AsyncOpenAI:
    global _async_client
    if _async_client is None:
        log_config_diagnostics()
        logger.info(f"LLM client -> groq model={config.AI_MODEL} base_url={_GROQ['base_url']}")
        _async_client = AsyncOpenAI(
            api_key=_resolve_key(), base_url=_GROQ["base_url"], timeout=600.0, max_retries=_MAX_RETRIES,
        )
    return _async_client


def get_sync_client() -> OpenAI:
    global _sync_client
    if _sync_client is None:
        log_config_diagnostics()
        logger.info(f"LLM client -> groq model={config.AI_MODEL} base_url={_GROQ['base_url']}")
        _sync_client = OpenAI(
            api_key=_resolve_key(), base_url=_GROQ["base_url"], timeout=600.0, max_retries=_MAX_RETRIES,
        )
    return _sync_client


def _reasoning_effort(prov: dict, thinking_budget: int):
    """Effort for this call, or None when the provider/model doesn't do reasoning.

    Discovery/ranking (budget > 0) get AI_REASONING_EFFORT so they reason about which
    events truly fall on a date; creative/mechanical calls (budget == 0) get the
    provider's idle effort ("none" on Gemini = no thinking = cheapest)."""
    if not prov["reasoning"]:
        return None
    if not thinking_budget or thinking_budget <= 0:
        return prov["idle_effort"]
    return str(getattr(config, "AI_REASONING_EFFORT", "medium")).lower()


def build_params(
    model: str,
    system: str,
    prompt: str,
    max_tokens: int,
    temperature: float | None = None,
    thinking_budget: int = 0,
    json_mode: bool | None = None,
) -> dict:
    """Assemble OpenAI-compatible chat.completions kwargs from Anthropic-style inputs.

    JSON mode defaults to the provider's capability (reliable on Gemini/OpenAI, off on
    Groq gpt-oss whose constrained decoding returns empty output). A strict system
    prompt + lenient parsing back it up either way.
    """
    prov = _provider()
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    model = str(model).strip()

    effort = _reasoning_effort(prov, thinking_budget)

    if _REASONING_TOKEN_FLOOR and effort in ("medium", "high"):
        max_tokens = max(max_tokens, _REASONING_TOKEN_FLOOR)

    cap = int(getattr(config, "AI_MAX_COMPLETION_TOKENS", 16384))
    params: dict = {
        "model": model,
        "messages": messages,
        "max_tokens": min(max_tokens, cap),
    }
    if temperature is not None:
        params["temperature"] = temperature

    if effort is not None:
        params["reasoning_effort"] = effort

    use_json = prov.get("json_mode", False) if json_mode is None else json_mode
    if use_json:
        params["response_format"] = {"type": "json_object"}
    return params


# Some models reject reasoning_effort="none" outright. Learn that once from the first
# rejection rather than sending a doomed parameter on every later call.
_EFFORT_FALLBACK = {"none": "low"}
_effort_rejected: set[str] = set()


def _is_effort_rejected(err: Exception) -> bool:
    msg = str(err).lower()
    return "reasoning_effort" in msg or ("thinking" in msg and "invalid" in msg)


def _downgrade_effort(params: dict) -> dict | None:
    """Same call with the next effort up, or None if there is nowhere to go."""
    effort = params.get("reasoning_effort")
    nxt = _EFFORT_FALLBACK.get(effort)
    if not nxt:
        return None
    _effort_rejected.add(str(params.get("model")))
    logger.warning(f"reasoning_effort={effort!r} rejected -> retrying with {nxt!r}")
    return {**params, "reasoning_effort": nxt}


def _is_model_missing(err: Exception) -> bool:
    """True if the error is a 'model not found / not supported' 404."""
    if getattr(err, "status_code", None) == 404:
        return True
    msg = str(err).lower()
    return "not found" in msg or "not supported" in msg or "model_not_found" in msg


def _fallback_model() -> str:
    return FALLBACK_MODEL


# ══════════════════════════════════════════════════════════════════════════════
# COST METER
# ══════════════════════════════════════════════════════════════════════════════
# Every call goes through achat/chat, so this is the one place that can answer
# "where does the daily bill actually go". Without it the only way to cut cost is
# to guess which stage is expensive, and guessing is how you spend a day cutting
# the wrong thing.
#
# Prices are per 1M tokens. Reasoning tokens bill as output, which is why effort
# settings show up on the invoice rather than just in latency.
# Groq, gpt-oss-120b. Verify against Groq's pricing page — these are the only
# hand-entered numbers in the meter.
_PRICE_PER_M = {"input": 0.15, "output": 0.75}
BATCH_DISCOUNT = 0.5

_usage: dict[str, dict] = {}


def _stage_from_stack() -> str:
    """Name the caller from the stack rather than from a global.

    The pipeline fans out with asyncio.gather, so a module-level "current stage"
    would only ever report whichever coroutine set it last. The stack is
    per-coroutine and always right: walk outwards past this module and report the
    first frame that belongs to the pipeline."""
    try:
        for fr in reversed(traceback.extract_stack()[:-2]):
            path = fr.filename.replace("\\", "/")
            if "/core/llm.py" in path:
                continue
            mod = path.rsplit("/", 1)[-1].replace(".py", "")
            return f"{mod}:{fr.name}"
    except Exception:
        pass
    return "unattributed"


def _record(resp) -> None:
    try:
        u = getattr(resp, "usage", None)
        if not u:
            return
        row = _usage.setdefault(_stage_from_stack(), {"calls": 0, "in": 0, "out": 0})
        row["calls"] += 1
        row["in"] += getattr(u, "prompt_tokens", 0) or 0
        row["out"] += getattr(u, "completion_tokens", 0) or 0
    except Exception:
        pass  # accounting must never break a run


def cost_report() -> str:
    """A per-stage breakdown, most expensive first."""
    if not _usage:
        return "no LLM usage recorded"
    rows = []
    total = 0.0
    for stage, r in _usage.items():
        cost = (r["in"] * _PRICE_PER_M["input"] + r["out"] * _PRICE_PER_M["output"]) / 1_000_000
        total += cost
        rows.append((cost, stage, r))
    rows.sort(reverse=True, key=lambda x: x[0])
    out = ["", "═" * 74, f"{'STAGE':<28}{'CALLS':>7}{'IN':>12}{'OUT':>12}{'USD':>10}{'%':>6}", "─" * 74]
    for cost, stage, r in rows:
        pct = (cost / total * 100) if total else 0
        out.append(f"{stage:<28}{r['calls']:>7}{r['in']:>12,}{r['out']:>12,}{cost:>10.4f}{pct:>5.0f}%")
    out.append("─" * 74)
    out.append(f"{'TOTAL':<28}{'':>7}{'':>12}{'':>12}{total:>10.4f}")
    out.append(f"{'same run on Batch API':<28}{'':>7}{'':>12}{'':>12}{total * BATCH_DISCOUNT:>10.4f}")
    out.append("═" * 74)
    return chr(10).join(out)


# ══════════════════════════════════════════════════════════════════════════════
# DAILY SPEND CAP
# ══════════════════════════════════════════════════════════════════════════════
# The meter above already knows what this run has billed. This turns that number into
# a brake, so a retry storm or a widened tree cannot quietly produce a $5 day the way
# Gemini did in Sept 2026 — the bill is found at the end of the month, and by then it
# has been running for a fortnight.
#
# Two ceilings, not one. Everything the app needs to have a day at all — discovery,
# narratives, their translations, the quiz — spends up to the full cap. The extras that
# make a day richer but not viable — the Parallel Universes tree, the Long Reads — stop
# at OPTIONAL_SHARE of it. So when the money runs low the run loses its game and keeps
# its content, rather than spending everything on one lavish date and leaving the next
# two blank. That is exactly the failure the 2026-09-02 run produced.
OPTIONAL_SHARE = 0.60


class BudgetExhausted(RuntimeError):
    """The run has spent its allowance. Not retryable — waiting does not refill it."""


def spend_usd() -> float:
    """What this run has billed so far, read off the same meter as cost_report()."""
    total = 0.0
    for r in _usage.values():
        total += (r["in"] * _PRICE_PER_M["input"] + r["out"] * _PRICE_PER_M["output"]) / 1_000_000
    return total


def _cap() -> float:
    return float(getattr(config, "AI_DAILY_BUDGET_USD", 0.0) or 0.0)


def budget_allows(optional: bool = False) -> bool:
    """True while there is room to make another call at this priority.

    `optional=True` is the richer-but-skippable work; it yields early so the essential
    stages keep their share. A cap of 0 disables the brake entirely."""
    cap = _cap()
    if not cap:
        return True
    ceiling = cap * OPTIONAL_SHARE if optional else cap
    return spend_usd() < ceiling


_budget_warned = False


def _check_budget() -> None:
    global _budget_warned
    cap = _cap()
    if not cap:
        return
    spent = spend_usd()
    if spent >= cap:
        if not _budget_warned:
            _budget_warned = True
            logger.error(
                f"🛑 Daily spend cap reached: ${spent:.4f} of ${cap:.2f}. "
                f"No further LLM calls this run."
            )
        raise BudgetExhausted(f"spent ${spent:.4f} of ${cap:.2f}")


# ══════════════════════════════════════════════════════════════════════════════
# TOKENS-PER-MINUTE BUDGET
# ══════════════════════════════════════════════════════════════════════════════
# Groq counts input + max_tokens against a per-minute allowance and refuses the whole
# request with a 413 when it would break it — it does not queue, and retrying an
# oversized request just fails again at the same size. The free tier allows 8000 TPM,
# which a single unpaced pipeline fan-out blows through instantly.
#
# So every call books its estimated cost before going out, and waits if the last
# sixty seconds are already spent. Raise AI_TPM_LIMIT when the account's tier does.
_TPM_WINDOW = 60.0
_spent: list[tuple[float, int]] = []   # (timestamp, tokens)
_tpm_lock = asyncio.Lock()
_tpm_lock_sync = threading.Lock()


# What the provider last told us the per-minute ceiling actually is. Groq states it in
# the body of every rate-limit error — "on tokens per minute (TPM): Limit 8000" — so the
# pacer does not have to be told: it starts unpaced and clamps itself the first time it
# is refused. That matters because AI_TPM_LIMIT is the one setting that is wrong by
# default after a tier change, and being wrong in the cautious direction is not harmless:
# an 8000 left over from the free tier makes a Dev-tier account sleep a minute between
# calls and trims generations that would have fit.
_learned_limit: int = 0

_TPM_LIMIT_RE = re.compile(r"tokens per minute \(TPM\):\s*Limit\s*(\d+)", re.I)


def note_rate_limit(err: Exception) -> None:
    """Read the real TPM ceiling out of a provider refusal and pace against it."""
    global _learned_limit
    m = _TPM_LIMIT_RE.search(str(err))
    if not m:
        return
    found = int(m.group(1))
    if found and found != _learned_limit:
        _learned_limit = found
        logger.warning(
            f"📉 Provider reports a {found} tokens-per-minute ceiling — pacing against "
            f"it from now on. Set AI_TPM_LIMIT={found} to skip this discovery."
        )


def _tpm_limit() -> int:
    """Configured ceiling, else whatever the provider has told us, else unpaced.

    0 means "do not pace" — the correct starting point on an unknown tier, because the
    spend cap already bounds the damage and the first refusal teaches us the real number.
    """
    configured = int(getattr(config, "AI_TPM_LIMIT", 0) or 0)
    return configured or _learned_limit


# Room left for the provider's own accounting to differ from our four-chars-a-token
# guess, and the smallest completion still worth asking for. A call trimmed below this
# would come back truncated, which costs full price for an unusable answer.
_TPM_HEADROOM = 256
_MIN_OUTPUT_TOKENS = 512

# How far a reservation may be trimmed before trimming stops being a rescue. A call cut
# to a third of what it asked for comes back truncated, gets repaired into valid-looking
# JSON, fails its size check and is regenerated — full price for an unusable answer. A
# 413, by contrast, is refused before the model runs and bills nothing. So below this
# fraction we send the request at its real size and let the provider decide: on a tier
# that allows it the call simply succeeds, and on one that does not it is refused for
# free. This is also the guard that keeps a stale AI_TPM_LIMIT from silently truncating
# every Parallel tree after a tier upgrade.
_MIN_TRIM_FRACTION = 0.6


def _trim_target(params: dict, limit: int) -> int | None:
    """Trimmed max_tokens that still fits the window, or None if trimming would cost
    more than the refusal it avoids."""
    room = limit - _input_tokens(params) - _TPM_HEADROOM
    asked = int(params.get("max_tokens", 0))
    if room < _MIN_OUTPUT_TOKENS or room < asked * _MIN_TRIM_FRACTION:
        return None
    return room


def _input_tokens(params: dict) -> int:
    chars = sum(len(str(m.get("content", ""))) for m in params.get("messages", []))
    return chars // 4


def _estimate_tokens(params: dict) -> int:
    """Input plus reserved output — the same sum the provider bills against.

    Four characters to the token is the usual rough conversion, and erring high is
    the safe direction: an overestimate slows the run, an underestimate gets a 413.
    """
    chars = sum(len(str(m.get("content", ""))) for m in params.get("messages", []))
    return chars // 4 + int(params.get("max_tokens", 0))


def _drop_expired(now: float) -> int:
    global _spent
    _spent = [(t, n) for t, n in _spent if now - t < _TPM_WINDOW]
    return sum(n for _, n in _spent)


def _wait_for(cost: int, used: int, limit: int, now: float) -> float:
    """Seconds until enough of the window has rolled off to fit this call."""
    if not _spent:
        return 0.0
    need = used + cost - limit
    freed = 0
    for t, n in sorted(_spent):
        freed += n
        if freed >= need:
            return max(0.0, _TPM_WINDOW - (now - t)) + 0.25
    return _TPM_WINDOW


async def _reserve_async(params: dict) -> None:
    limit = _tpm_limit()
    if not limit:
        return
    cost = _estimate_tokens(params)
    while True:
        async with _tpm_lock:
            now = time.monotonic()
            used = _drop_expired(now)
            if cost > limit:
                # Nothing will ever make room for this, so waiting is pointless — but
                # neither is sending it: the provider counts input + max_tokens and
                # refuses the whole thing with a 413 at any queue depth. Reserving more
                # output than the model will ever write is what caused that (a deep dive
                # asked for 8192 to produce ~1300), so trim the reservation to what the
                # window can actually admit and let the call through.
                room = _trim_target(params, limit)
                if room is not None:
                    logger.warning(
                        f"request needs ~{cost} tokens, over the {limit} TPM budget — "
                        f"trimming max_tokens {params.get('max_tokens')} → {room}"
                    )
                    params["max_tokens"] = room
                    _spent.append((now, limit))
                    return
                logger.warning(
                    f"request needs ~{cost} tokens against a {limit} TPM budget and "
                    f"cannot be trimmed without truncating the answer — sending at full "
                    f"size. If this 413s, raise AI_TPM_LIMIT to match your Groq tier."
                )
                return
            if used + cost <= limit:
                _spent.append((now, cost))
                return
            delay = _wait_for(cost, used, limit, now)
        logger.info(f"TPM budget: {used}/{limit} used, waiting {delay:.1f}s for ~{cost} tokens")
        await asyncio.sleep(delay)


def _reserve_sync(params: dict) -> None:
    limit = _tpm_limit()
    if not limit:
        return
    cost = _estimate_tokens(params)
    while True:
        with _tpm_lock_sync:
            now = time.monotonic()
            used = _drop_expired(now)
            if cost > limit:
                room = _trim_target(params, limit)
                if room is not None:
                    logger.warning(
                        f"request needs ~{cost} tokens, over the {limit} TPM budget — "
                        f"trimming max_tokens {params.get('max_tokens')} → {room}"
                    )
                    params["max_tokens"] = room
                _spent.append((now, min(cost, limit)))
                return
            if used + cost <= limit:
                _spent.append((now, cost))
                return
            delay = _wait_for(cost, used, limit, now)
        time.sleep(delay)


async def achat(params: dict):
    """Async chat completion with automatic fallback if the model name is invalid."""
    _check_budget()
    await _reserve_async(params)
    client = get_async_client()
    try:
        resp = await client.chat.completions.create(**params)
        _record(resp)
        return resp
    except Exception as e:
        note_rate_limit(e)
        if _is_effort_rejected(e):
            retry = _downgrade_effort(params)
            if retry:
                resp = await client.chat.completions.create(**retry)
                _record(resp)
                return resp
        fb = _fallback_model()
        if _is_model_missing(e) and params.get("model") != fb:
            logger.warning(f"⚠️ model {params.get('model')!r} unavailable → retrying with {fb}")
            resp = await client.chat.completions.create(**{**params, "model": fb})
            _record(resp)
            return resp
        raise


def chat(params: dict):
    """Sync chat completion with automatic fallback if the model name is invalid."""
    _check_budget()
    _reserve_sync(params)
    client = get_sync_client()
    try:
        resp = client.chat.completions.create(**params)
        _record(resp)
        return resp
    except Exception as e:
        note_rate_limit(e)
        if _is_effort_rejected(e):
            retry = _downgrade_effort(params)
            if retry:
                resp = client.chat.completions.create(**retry)
                _record(resp)
                return resp
        fb = _fallback_model()
        if _is_model_missing(e) and params.get("model") != fb:
            logger.warning(f"⚠️ model {params.get('model')!r} unavailable → retrying with {fb}")
            resp = client.chat.completions.create(**{**params, "model": fb})
            _record(resp)
            return resp
        raise


def parse_json_response(resp) -> dict:
    """Read the assistant text from a chat completion and parse JSON leniently.

    Strips markdown fences and, as a last resort, the outermost braces — same
    tolerance the old Anthropic `_parse_ai_json` had.
    """
    # A response cut off at the token ceiling still arrives as a well-formed object once
    # json_repair has closed the dangling braces, so nothing downstream can tell the
    # difference between "the model wrote a short list" and "we stopped paying halfway
    # through a long one". That ambiguity hid a truncation bug in the parallel-universe
    # generator for a week — every call was cut at 16k, repaired into a third of a tree,
    # and rejected as too small. Say it out loud instead.
    choice = resp.choices[0]
    if getattr(choice, "finish_reason", None) == "length":
        logger.warning(
            "✂️ Response hit the output token ceiling and was truncated — what follows "
            "is a repaired fragment, not the model's full answer. Raise max_tokens "
            "(or AI_MAX_COMPLETION_TOKENS) for this call."
        )
    text = (choice.message.content or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Narrow to the outermost object, then let json_repair fix the common LLM defects
    # (unescaped inner quotes, trailing commas, stray newlines) — cheaper than a retry,
    # which matters on rate-limited tiers.
    start, end = text.find("{"), text.rfind("}")
    candidate = text[start:end + 1] if (start != -1 and end > start) else text
    repaired = repair_json(candidate)
    return json.loads(repaired)
