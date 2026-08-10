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

from openai import OpenAI, AsyncOpenAI

from core.config import config

# Per-provider wiring. `reasoning` = whether the model accepts `reasoning_effort`;
# `idle_effort` = the effort value for non-reasoning (creative) calls ("none" disables
# thinking on Gemini for the cheapest calls; Groq has no "none", so it uses "low").
_PROVIDERS = {
    "gemini": {
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
        "key_attrs": ("GEMINI_API_KEY", "OPENAI_API_KEY"),
        "reasoning": True,
        "idle_effort": "none",
    },
    "openai": {
        "base_url": None,  # SDK default (api.openai.com)
        "key_attrs": ("OPENAI_API_KEY",),
        "reasoning": False,  # gpt-4o-mini etc. reject reasoning_effort
        "idle_effort": None,
    },
    "groq": {
        "base_url": "https://api.groq.com/openai/v1",
        "key_attrs": ("GROQ_API_KEY",),
        "reasoning": True,
        "idle_effort": "low",
    },
}

# The SDK retries 429s automatically, honouring the provider's retry-after header.
_MAX_RETRIES = 6

_async_client = None
_sync_client = None


def _provider() -> dict:
    name = str(getattr(config, "AI_PROVIDER", "gemini")).lower()
    return _PROVIDERS.get(name, _PROVIDERS["gemini"])


def _resolve_key(prov: dict) -> str:
    for attr in prov["key_attrs"]:
        key = getattr(config, attr, None)
        if key:
            return key
    raise RuntimeError(
        f"No API key set for provider '{config.AI_PROVIDER}'. "
        f"Set one of: {', '.join(prov['key_attrs'])}."
    )


def _base_url(prov: dict):
    return getattr(config, "AI_BASE_URL", None) or prov["base_url"]


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
    json_mode: bool = False,
) -> dict:
    """Assemble OpenAI-compatible chat.completions kwargs from Anthropic-style inputs.

    We rely on a strict system prompt + lenient parsing rather than a provider JSON
    mode, so behaviour is identical across Gemini/OpenAI/Groq.
    """
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    cap = int(getattr(config, "AI_MAX_COMPLETION_TOKENS", 8192))
    params: dict = {
        "model": model,
        "messages": messages,
        "max_tokens": min(max_tokens, cap),
    }
    if temperature is not None:
        params["temperature"] = temperature

    effort = _reasoning_effort(_provider(), thinking_budget)
    if effort is not None:
        params["reasoning_effort"] = effort

    if json_mode:
        params["response_format"] = {"type": "json_object"}
    return params


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
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            return json.loads(text[start:end + 1])
        raise
