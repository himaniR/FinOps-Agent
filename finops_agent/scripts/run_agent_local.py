"""Local smoke test — run the agent end-to-end via Databricks Connect."""
from src.agent.runner import run_agent

if __name__ == "__main__":
    state = run_agent(triggered_by="local-test",
                      config_overrides={"lookback_days": 14})
    print("\n" + "=" * 70)
    print("REPORT")
    print("=" * 70)
    print(state.get("report_markdown", "_no report_"))