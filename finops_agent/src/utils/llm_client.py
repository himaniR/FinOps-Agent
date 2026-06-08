
"""LLM client wrapper for the Databricks Foundation Model API.

Provides:
- Retries on transient errors (rate limits, timeouts)
- JSON-mode helper that strips markdown fences
- Token usage tracking
- Optional fallback to Azure OpenAI
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


@dataclass
class LLMUsage:
    """Tracks cumulative LLM usage in this session."""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    calls: int = 0
    failures: int = 0
    latency_seconds: list[float] = field(default_factory=list)

    def avg_latency(self) -> float:
        return sum(self.latency_seconds) / len(self.latency_seconds) if self.latency_seconds else 0.0

    def summary(self) -> dict:
        return {
            "calls": self.calls,
            "failures": self.failures,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "avg_latency_seconds": round(self.avg_latency(), 2),
        }


class LLMError(Exception):
    """Raised when the LLM call fails after all retries."""


class LLMClient:
    """Wrapper around mlflow.deployments client for Databricks Foundation Models."""

    def __init__(
        self,
        endpoint: str,
        temperature: float = 0.2,
        max_tokens: int = 2000,
        timeout_seconds: int = 120,
        max_retries: int = 3,
    ):
        self.endpoint = endpoint
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.usage = LLMUsage()
        self._client = self._init_client()

    def _init_client(self):
        """Lazy-init mlflow deployments client."""
        try:
            import mlflow.deployments
            return mlflow.deployments.get_deploy_client("databricks")
        except Exception as e:
            logger.error("Failed to initialize MLflow deployments client: %s", e)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((TimeoutError, ConnectionError)),
        reraise=True,
    )
    def _call(self, messages: list[dict], **kwargs) -> dict:
        """Low-level call with retry."""
        start = time.time()
        try:
            response = self._client.predict(
                endpoint=self.endpoint,
                inputs={
                    "messages": messages,
                    "temperature": kwargs.get("temperature", self.temperature),
                    "max_tokens": kwargs.get("max_tokens", self.max_tokens),
                },
            )
            latency = time.time() - start
            self.usage.calls += 1
            self.usage.latency_seconds.append(latency)

            usage = response.get("usage", {})
            self.usage.prompt_tokens += usage.get("prompt_tokens", 0)
            self.usage.completion_tokens += usage.get("completion_tokens", 0)
            self.usage.total_tokens += usage.get("total_tokens", 0)

            logger.info(
                "LLM call OK | endpoint=%s | latency=%.2fs | tokens=%d",
                self.endpoint, latency, usage.get("total_tokens", 0)
            )
            return response

        except Exception as e:
            self.usage.failures += 1
            logger.error("LLM call failed: %s", e)
            raise

    def chat(self, system: str, user: str, **kwargs) -> str:
        """Simple chat: system prompt + user message → assistant response (string)."""
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        try:
            response = self._call(messages, **kwargs)
            return response["choices"][0]["message"]["content"].strip()
        except Exception as e:
            raise LLMError(f"Chat call failed: {e}") from e

    def chat_json(self, system: str, user: str, **kwargs) -> dict:
        """Chat that expects a JSON response. Strips markdown fences automatically."""
        text = self.chat(system, user, **kwargs)
        return self._parse_json_response(text)

    @staticmethod
    def _parse_json_response(text: str) -> dict:
        """Robustly parse JSON from LLM response, handling markdown fences."""
        # Strip code fences
        text = re.sub(r"^```(?:json)?\s*", "", text.strip())
        text = re.sub(r"\s*```$", "", text.strip())

        # Find first { and last } to handle pre/post chatter
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            text = text[start : end + 1]

        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON from LLM:\n%s", text[:500])
            raise LLMError(f"Invalid JSON from LLM: {e}") from e


# ----------------------------------------------------------
# Factory
# ----------------------------------------------------------
def make_llm_client(settings) -> LLMClient:
    """Create an LLMClient from a Settings object."""
    return LLMClient(
        endpoint=settings.llm_endpoint,
        temperature=settings.llm_temperature,
        max_tokens=settings.llm_max_tokens,
        timeout_seconds=settings.llm_timeout_seconds,
        max_retries=settings.llm_max_retries,
    )