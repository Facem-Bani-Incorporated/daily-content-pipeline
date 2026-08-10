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
# `idle_effort` = the effort value for non-reasoning (creative) calls. We use "low"
# rather than "none": "low" is accepted by every Gemini flash model (some newer ones
# reject "none") and still keeps thinking — and cost — minimal.
_PROVIDERS = {
    "gemini": {
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
        "key_attrs": ("GEMINI_API_KEY", "OPENAI_API_KEY"),
        "reasoning": True,
        "idle_effort": "low",
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
        "idle_effort": "low",
        "json_mode": True,
        "vertex": True,
    },
}

# Provider-appropriate fallback model when AI_MODEL is invalid/retired (used by achat/chat).
_FALLBACK_MODEL = {
    "gemini": "gemini-flash-latest",
    "vertex": "google/gemini-2.5-flash",
    "openai": "gpt-4o-mini",
    "groq": "llama-3.3-70b-versatile",
}

# The SDK retries 429s automatically, honouring the provider's retry-after header.
_MAX_RETRIES = 6

# Minimum max_tokens for a medium/high reasoning call: on Gemini/Vertex the thinking
# tokens come out of max_tokens, so discovery (long list + reasoning) needs headroom or
# it truncates to empty/invalid JSON. Bounded by AI_MAX_COMPLETION_TOKENS.
_REASONING_TOKEN_FLOOR = 12288

_async_client = None
_sync_client = None


def _provider_name() -> str:
    return str(getattr(config, "AI_PROVIDER", "gemini")).lower()


def _provider() -> dict:
    return _PROVIDERS.get(_provider_name(), _PROVIDERS["gemini"])


def _vertex_token() -> str:
    """Mint a short-lived OAuth token from the service account JSON for Vertex AI."""
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request as GAuthRequest

    raw = getattr(config, "GOOGLE_SERVICE_ACCOUNT_JSON", None)
    if not raw:
        raise RuntimeError("AI_PROVIDER=vertex needs GOOGLE_SERVICE_ACCOUNT_JSON (the service account JSON).")
    info = json.loads(raw)
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
        prov = _provider()
        _async_client = AsyncOpenAI(
            api_key=_resolve_key(prov),
            base_url=_base_url(prov),
            timeout=600.0,
            max_retries=_MAX_RETRIES,
        )
    return _async_client


def get_sync_client() -> OpenAI:
    global _sync_client
    if _sync_client is None:
        prov = _provider()
        _sync_client = OpenAI(
            api_key=_resolve_key(prov),
            base_url=_base_url(prov),
            timeout=600.0,
            max_retries=_MAX_RETRIES,
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


def _is_model_missing(err: Exception) -> bool:
    """True if the error is a 'model not found / not supported' 404."""
    if getattr(err, "status_code", None) == 404:
        return True
    msg = str(err).lower()
    return "not found" in msg or "not supported" in msg or "model_not_found" in msg


def _fallback_model() -> str:
    return _FALLBACK_MODEL.get(_provider_name(), FALLBACK_MODEL)


async def achat(params: dict):
    """Async chat completion with automatic fallback if the model name is invalid."""
    client = get_async_client()
    try:
        return await client.chat.completions.create(**params)
    except Exception as e:
        fb = _fallback_model()
        if _is_model_missing(e) and params.get("model") != fb:
            logger.warning(f"⚠️ model {params.get('model')!r} unavailable → retrying with {fb}")
            return await client.chat.completions.create(**{**params, "model": fb})
        raise


def chat(params: dict):
    """Sync chat completion with automatic fallback if the model name is invalid."""
    client = get_sync_client()
    try:
        return client.chat.completions.create(**params)
    except Exception as e:
        fb = _fallback_model()
        if _is_model_missing(e) and params.get("model") != fb:
            logger.warning(f"⚠️ model {params.get('model')!r} unavailable → retrying with {fb}")
            return client.chat.completions.create(**{**params, "model": fb})
        raise


def parse_json_response(resp) -> dict:
    """Read the assistant text from a chat completion and parse JSON leniently.

    Strips markdown fences and, as a last resort, the outermost braces — same
    tolerance the old Anthropic `_parse_ai_json` had.
    """
    text = (resp.choices[0].message.content or "").strip()
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
