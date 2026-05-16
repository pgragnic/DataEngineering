import json

from anthropic import Anthropic

from app.config import settings

_client = Anthropic(api_key=settings.anthropic_api_key)


def call_claude_json(
    system: str,
    user_content: list[dict],
    max_tokens: int = 2000,
) -> dict:
    """Call Claude Sonnet 4.6, parse JSON from the response robustly."""
    resp = _client.messages.create(
        model=settings.claude_model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user_content}],
    )
    text = resp.content[0].text
    # tolerate ```json fences
    text = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    return json.loads(text)
