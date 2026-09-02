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

import json
import re
import traceback

from json_repair import repair_json
from openai import OpenAI, AsyncOpenAI

from core.config import config
from core.logger import setup_logger

logger = setup_logger("LLM")

# A maintained Gemini alias that is always valid. If AI_MODEL is a typo (e.g.
# "gemini-3.5-flas") or a retired model, the call 404s and we transparently retry
# on this so the pipeline can't be broken by a bad env value.
FALLBACK_MODEL = "gemini-flash-latest"

# Per-provider wiring. `reasoning` = whether the model accepts `reasoning_effort`;
# `idle_effort` = the effort value for non-reasoning (creative) calls.
#
# This was "low", on the reasoning that low is accepted everywhere and keeps cost
# minimal. It does not: "low" still thinks, and thinking bills as output. Cloud
# Billing shows it on its own line — "Gemini 2.5 Flash GA Text Output (Thinking On)"
# — and that line was the single largest item on the invoice. Creative and mechanical
# calls (writing an article, translating one) gain nothing from deliberation, and
# there are far more of those than there are ranking calls.
#
# So: "none", with _EFFORT_FALLBACK below covering the models that reject it.
_PROVIDERS = {
    "gemini": {
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
        "key_attrs": ("GEMINI_API_KEY", "OPENAI_API_KEY"),
        "reasoning": True,
        "idle_effort": "none",
        "json_mode": True,  # Gemini's json_object mode is reliable → guaranteed-valid JSON
    },
    "openai": {
        "base_url": None,  # SDK default (api.openai.com)
        "key_attrs": ("OPENAI_API_KEY",),
        "reasoning": False,  # gpt-4o-mini etc. reject reasoning_effort
        "idle_effort": None,
        "json_mode": True,
    },
    "groq": {
        "base_url": "https://api.groq.com/openai/v1",
        "key_attrs": ("GROQ_API_KEY",),
        "reasoning": True,
        "idle_effort": "low",
        "json_mode": False,  # gpt-oss constrained decoding returns empty/invalid — use lenient parse
    },
    # Google Cloud Vertex AI — same Gemini models, billed via Cloud Billing (POSTPAID).
    # base_url is built from GCP_PROJECT/GCP_LOCATION and auth is a short-lived OAuth
    # token minted from the service account (not a static API key). Models are addressed
    # as "google/<model>".
    "vertex": {
        "base_url": None,  # built dynamically in _base_url()
        "key_attrs": (),   # token minted in _resolve_key()
        "reasoning": True,
        "idle_effort": "none",
        "json_mode": True,
        "vertex": True,
    },
}

# Provider-appropriate fallback model when AI_MODEL is invalid/retired (used by achat/chat).
_FALLBACK_MODEL = {
    "gemini": "gemini-flash-latest",
    "vertex": "google/gemini-2.5-flash",
    "openai": "gpt-4o-mini",
    # Must be a model the account can actually reach: the fallback fires exactly when
    # AI_MODEL is wrong, so pointing it at something retired turns a typo into a dead run.
    "groq": "openai/gpt-oss-20b",
}

# The SDK retries 429s automatically, honouring the provider's retry-after header.
_MAX_RETRIES = 6

# Minimum max_tokens for a medium/high reasoning call: on Gemini/Vertex the thinking
# tokens come out of max_tokens, so discovery (long list + reasoning) needs headroom or
# it truncates to empty/invalid JSON. Bounded by AI_MAX_COMPLETION_TOKENS.
_REASONING_TOKEN_FLOOR = 12288

_async_client = None
_sync_client = None


def _has_credentials(name: str) -> bool:
    prov = _PROVIDERS[name]
    if prov.get("vertex"):
        return bool(getattr(config, "GOOGLE_SERVICE_ACCOUNT_JSON", None) and getattr(config, "GCP_PROJECT", None))
    return any(getattr(config, a, None) for a in prov["key_attrs"])


def _autodetect_provider() -> str:
    """Pick a provider from whatever credentials are actually present."""
    if _has_credentials("vertex"):
        return "vertex"
    for name in ("gemini", "openai", "groq"):
        if _has_credentials(name):
            return name
    return "gemini"


def _provider_name() -> str:
    # Honour an explicit AI_PROVIDER (strip() guards a pasted "vertex " value) — but only
    # if that provider actually has credentials. Otherwise auto-detect from what's set.
    # This rescues the common case: AI_PROVIDER left on "gemini" after the gemini key was
    # deleted, with a Vertex service account present → use Vertex.
    name = str(getattr(config, "AI_PROVIDER", "") or "").strip().lower()
    if name in _PROVIDERS and _has_credentials(name):
        return name
    return _autodetect_provider()


def log_config_diagnostics() -> None:
    """One-shot log of which LLM-related env vars actually reached the app — so a
    mangled variable name in the host shows up instead of silently defaulting."""
    def present(attr):
        return "set" if getattr(config, attr, None) else "MISSING"
    logger.info(
        "🔎 LLM env → "
        f"AI_PROVIDER={str(getattr(config, 'AI_PROVIDER', None))!r} "
        f"GCP_PROJECT={str(getattr(config, 'GCP_PROJECT', None))!r} "
        f"SERVICE_ACCOUNT={present('GOOGLE_SERVICE_ACCOUNT_JSON')} "
        f"GEMINI_KEY={present('GEMINI_API_KEY')} "
        f"→ resolved provider={_provider_name()}"
    )


def _provider() -> dict:
    return _PROVIDERS.get(_provider_name(), _PROVIDERS["gemini"])


def _load_service_account_info() -> dict:
    """Parse the service account credentials from GOOGLE_SERVICE_ACCOUNT_JSON.

    Accepts either the raw JSON or a base64-encoded JSON. base64 is the safe way to set
    it in a hosting UI (Railway): a single line with no quotes/newlines/`=` that would
    otherwise be mis-split into stray, empty-named variables.
    """
    raw = getattr(config, "GOOGLE_SERVICE_ACCOUNT_JSON", None)
    if not raw:
        raise RuntimeError("AI_PROVIDER=vertex needs GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON or base64).")
    raw = raw.strip()
    if not raw.startswith("{"):
        import base64
        raw = base64.b64decode(raw).decode("utf-8")
    return json.loads(raw)


def _vertex_token() -> str:
    """Mint a short-lived OAuth token from the service account JSON for Vertex AI."""
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request as GAuthRequest

    info = _load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(GAuthRequest())
    return creds.token


def _resolve_key(prov: dict) -> str:
    if prov.get("vertex"):
        return _vertex_token()
    for attr in prov["key_attrs"]:
        key = getattr(config, attr, None)
        if key:
            return key
    raise RuntimeError(
        f"No API key set for provider '{config.AI_PROVIDER}'. "
        f"Set one of: {', '.join(prov['key_attrs'])}."
    )


def _base_url(prov: dict):
    override = getattr(config, "AI_BASE_URL", None)
    if override:
        return override
    if prov.get("vertex"):
        project = getattr(config, "GCP_PROJECT", None)
        location = getattr(config, "GCP_LOCATION", None) or "global"
        if not project:
            raise RuntimeError("AI_PROVIDER=vertex needs GCP_PROJECT.")
        host = "aiplatform.googleapis.com" if location == "global" else f"{location}-aiplatform.googleapis.com"
        return f"https://{host}/v1beta1/projects/{project}/locations/{location}/endpoints/openapi"
    return prov["base_url"]


def get_async_client() -> AsyncOpenAI:
    global _async_client
    if _async_client is None:
        log_config_diagnostics()
        prov = _provider()
        base = _base_url(prov)
        logger.info(f"🤖 LLM client → provider={_provider_name()} model={config.AI_MODEL} base_url={base}")
        _async_client = AsyncOpenAI(
            api_key=_resolve_key(prov), base_url=base, timeout=600.0, max_retries=_MAX_RETRIES,
        )
    return _async_client


def get_sync_client() -> OpenAI:
    global _sync_client
    if _sync_client is None:
        log_config_diagnostics()
        prov = _provider()
        base = _base_url(prov)
        logger.info(f"🤖 LLM client → provider={_provider_name()} model={config.AI_MODEL} base_url={base}")
        _sync_client = OpenAI(
            api_key=_resolve_key(prov), base_url=base, timeout=600.0, max_retries=_MAX_RETRIES,
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
    # Vertex addresses Gemini models as "google/<model>".
    if prov.get("vertex") and not model.startswith("google/"):
        model = f"google/{model}"

    effort = _reasoning_effort(prov, thinking_budget)

    # On Gemini/Vertex, thinking tokens are drawn from max_tokens, so a reasoning call
    # with a small budget truncates (empty/invalid output). Give medium/high effort a
    # floor so the answer survives the thinking. Bounded by the hard cap below.
    if effort in ("medium", "high"):
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
    return _FALLBACK_MODEL.get(_provider_name(), FALLBACK_MODEL)


# ══════════════════════════════════════════════════════════════════════════════
# COST METER
# ══════════════════════════════════════════════════════════════════════════════
# Every call goes through achat/chat, so this is the one place that can answer
# "where does the daily bill actually go". Without it the only way to cut cost is
# to guess which stage is expensive, and guessing is how you spend a day cutting
# the wrong thing.
#
# Prices are per 1M tokens, Gemini Flash pay-as-you-go. Thinking tokens bill as
# output, which is why a 2000-token reasoning budget on a 16k-token generation is
# not a rounding error. Batch submissions are half price — see BATCH_DISCOUNT.
_PRICE_PER_M = {"input": 0.30, "output": 2.50}
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


async def achat(params: dict):
    """Async chat completion with automatic fallback if the model name is invalid."""
    client = get_async_client()
    try:
        resp = await client.chat.completions.create(**params)
        _record(resp)
        return resp
    except Exception as e:
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
    client = get_sync_client()
    try:
        resp = client.chat.completions.create(**params)
        _record(resp)
        return resp
    except Exception as e:
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
