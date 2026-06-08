# FinOps Agent

Autonomous agent for Databricks cost analysis, anomaly detection, and optimization recommendations.

## Capabilities

- 📊 **Cost Analysis** — DBU, compute, storage by user/team/project
- 🚨 **Anomaly Detection** — Spike detection, runaway clusters, idle resources
- 💡 **Optimization** — Right-sizing, autoscale, Photon, serverless recommendations
- 🔮 **Forecasting** — Month-end projections, budget burn-rate alerts
- 💰 **Chargeback** — Tag-based cost allocation, untagged resource detection
- 🛡️ **Policy Enforcement** — Tag compliance, expensive node-type flagging
- 📄 **Reporting** — LLM-generated executive summaries, dashboards, Genie space

## Architecture

- **Bronze** → Raw billing/usage data from `system.billing.usage` + Azure Cost API
- **Silver** → Cleaned, joined, enriched cost data
- **Gold** → Aggregated metrics for dashboards
- **Agent** → State, audit logs, recommendations, run history

## Tech Stack

- Databricks Asset Bundles
- LangGraph (agent orchestration)
- Foundation Model API (Llama 3.3 70B)
- Unity Catalog
- AI/BI Lakeview Dashboards

## Quick Start

```bash
# 1. Activate venv
.venv\Scripts\Activate.ps1   # Windows
source .venv/bin/activate    # Mac/Linux

# 2. Install dependencies
pip install -r requirements-dev.txt

# 3. Validate the bundle
databricks bundle validate --target dev

# 4. Deploy
databricks bundle deploy --target dev

# 5. Run setup (one-time)
databricks bundle run finops_setup --target dev

# 6. Run the agent
databricks bundle run finops_agent_daily --target dev
```

## Project Structure

```
finops_agent/
├── databricks.yml        # Bundle definition
├── resources/            # Job & dashboard resources
├── notebooks/            # Entry points run by jobs
├── src/                  # Python package
│   ├── tools/            # Atomic tools (cost analysis, anomaly, etc.)
│   ├── agent/            # LangGraph agent (state, nodes, graph)
│   ├── utils/            # SQL helpers, LLM client, prompts
│   └── config/           # Settings & thresholds
├── tests/                # Unit tests
└── dashboards/           # Lakeview JSON exports
```

## Development

```bash
# Run tests
pytest

# Format code
black src/ tests/

# Lint
ruff check src/ tests/
```

## License

Internal POC — not for distribution.