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


def get_async_client() -> AsyncGroq:
    global _async_client
    if _async_client is None:
        _async_client = AsyncGroq(api_key=config.GROQ_API_KEY, timeout=600.0)
    return _async_client


def get_sync_client() -> Groq:
    global _sync_client
    if _sync_client is None:
        _sync_client = Groq(api_key=config.GROQ_API_KEY, timeout=600.0)
    return _sync_client


def _reasoning_effort(thinking_budget: int) -> str:
    """Map the legacy Anthropic thinking budget onto Groq's reasoning_effort."""
    return "medium" if thinking_budget and thinking_budget > 0 else "low"


def build_params(
    model: str,
    system: str,
    prompt: str,
    max_tokens: int,
    temperature: float | None = None,
    thinking_budget: int = 0,
    json_mode: bool = True,
) -> dict:
    """Assemble Groq chat.completions kwargs from Anthropic-style inputs."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    params: dict = {
        "model": model,
        "messages": messages,
        "max_completion_tokens": max_tokens,
        "reasoning_effort": _reasoning_effort(thinking_budget),
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
