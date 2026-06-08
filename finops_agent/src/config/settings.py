"""Centralized settings using Pydantic.

Settings are loaded from (in priority order):
1. Explicit parameters passed to get_settings(...)
2. Environment variables (e.g., FINOPS_CATALOG, FINOPS_LLM_ENDPOINT)
3. Defaults defined in this file

In a Databricks notebook, settings are typically passed via job parameters
(dbutils.widgets) and forwarded to get_settings().
"""
from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the FinOps agent."""

    model_config = SettingsConfigDict(
        env_prefix="FINOPS_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Unity Catalog ---
    catalog: str = Field(default="finops", description="UC catalog where finops data lives")
    bronze_schema: str = Field(default="bronze", description="Raw data schema")
    silver_schema: str = Field(default="silver", description="Cleaned data schema")
    gold_schema: str = Field(default="gold", description="Curated data schema")
    agent_schema: str = Field(default="agent", description="Agent state/audit schema")

    # --- LLM ---
    llm_endpoint: str = Field(
        default="databricks-meta-llama-3-3-70b-instruct",
        description="Foundation Model serving endpoint",
    )
    llm_temperature: float = Field(default=0.2, description="LLM temperature (lower = more deterministic)")
    llm_max_tokens: int = Field(default=2000, description="Max tokens per LLM response")
    llm_timeout_seconds: int = Field(default=120, description="LLM call timeout")
    llm_max_retries: int = Field(default=3, description="Retries on transient LLM errors")

    # --- Azure ---
    azure_subscription_id: Optional[str] = Field(default=None, description="Azure subscription ID")

    # --- Behavior ---
    dry_run: bool = Field(default=False, description="If True, no writes happen — only logs")
    lookback_days: int = Field(default=7, description="Default analysis window in days")

    # --- Budgets ---
    budget_monthly_usd: float = Field(default=1000.0, description="Monthly budget in USD")

    # --- Convenience ---
    @property
    def bronze(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}"

    @property
    def silver(self) -> str:
        return f"{self.catalog}.{self.silver_schema}"

    @property
    def gold(self) -> str:
        return f"{self.catalog}.{self.gold_schema}"

    @property
    def agent(self) -> str:
        return f"{self.catalog}.{self.agent_schema}"


@lru_cache(maxsize=1)
def get_settings(**overrides) -> Settings:
    """Get the settings singleton, with optional runtime overrides.

    Example:
        settings = get_settings(catalog="finops_dev", dry_run=True)
    """
    if overrides:
        # When overrides are passed, bypass the cache and create fresh
        return Settings(**overrides)
    return Settings()