"""
Groq LLM helper — shared client + request/response glue for the whole pipeline.

Groq exposes an OpenAI-compatible Chat Completions API, so this module hides the
few differences from the old Anthropic call sites:

  * `system` is passed as a normal message with role="system" (not a top-level arg).
  * `max_tokens` becomes `max_completion_tokens`.
  * There is no "extended thinking" budget. For reasoning models (e.g. gpt-oss) we
    map the old thinking budget onto Groq's `reasoning_effort` knob instead:
    budget > 0  -> "medium"  (discovery / ranking benefit from reasoning)
    budget == 0 -> "low"     (creative / mechanical calls — cheapest)
  * Every prompt in this pipeline demands a single JSON object, so we request
    `response_format={"type": "json_object"}` (the word "json" is present in every
    system prompt, which Groq requires for JSON mode).

The response text lives at `resp.choices[0].message.content`; `parse_json_response`
reads it and parses leniently, mirroring the old `_parse_ai_json` behaviour so the
call sites stay unchanged.
"""

import json
import re

from groq import Groq, AsyncGroq

from core.config import config

_async_client = None
_sync_client = None


# The SDK retries 429s automatically, honouring Groq's `retry-after` header. Groq's
# free tier caps tokens-per-minute low, so parallel calls burst past it; a generous
# retry count lets them serialise into the rolling window instead of hard-failing.
_MAX_RETRIES = 10

# Reasoning tokens on gpt-oss count toward the completion budget, so we pad
# max_completion_tokens per effort level to keep the JSON answer from being truncated
# (a truncated answer = invalid JSON).
_REASONING_HEADROOM = {"low": 1024, "medium": 4096, "high": 8192}


def get_async_client() -> AsyncGroq:
    global _async_client
    if _async_client is None:
        _async_client = AsyncGroq(
            api_key=config.GROQ_API_KEY, timeout=600.0, max_retries=_MAX_RETRIES
        )
    return _async_client


def get_sync_client() -> Groq:
    global _sync_client
    if _sync_client is None:
        _sync_client = Groq(
            api_key=config.GROQ_API_KEY, timeout=600.0, max_retries=_MAX_RETRIES
        )
    return _sync_client


def _reasoning_effort(thinking_budget: int) -> str:
    """Per-call reasoning effort for gpt-oss.

    Discovery/ranking (the calls that pass a budget > 0) MUST reason about which
    events genuinely fall on a given date — at "low" the model just lists famous
    names with fabricated dates and the Wikipedia validator rejects them all, so
    they use AI_REASONING_EFFORT (default "high"). Creative/mechanical calls
    (budget == 0) get nothing from reasoning tokens and stay at "low".
    """
    if not thinking_budget or thinking_budget <= 0:
        return "low"
    effort = str(getattr(config, "AI_REASONING_EFFORT", "high")).lower()
    return effort if effort in _REASONING_HEADROOM else "high"


def build_params(
    model: str,
    system: str,
    prompt: str,
    max_tokens: int,
    temperature: float | None = None,
    thinking_budget: int = 0,  # accepted for call-site compatibility; effort is global now
    json_mode: bool = False,
) -> dict:
    """Assemble Groq chat.completions kwargs from Anthropic-style inputs.

    We deliberately do NOT use Groq's json_object response_format: with gpt-oss its
    constrained decoding intermittently returns an empty/invalid generation
    (`json_validate_failed`). The system prompts already demand pure JSON and
    `parse_json_response` extracts it leniently — the same approach that worked on
    Anthropic.
    """
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    effort = _reasoning_effort(thinking_budget)
    params: dict = {
        "model": model,
        "messages": messages,
        "max_completion_tokens": max_tokens + _REASONING_HEADROOM[effort],
        "reasoning_effort": effort,
    }
    if temperature is not None:
        params["temperature"] = temperature
    if json_mode:
        params["response_format"] = {"type": "json_object"}
    return params


def parse_json_response(resp) -> dict:
    """Read the assistant text from a Groq response and parse JSON leniently.

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
