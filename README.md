# 📈 Stock Market Analytics Platform on Snowflake

## Overview
An end-to-end ELT pipeline that ingests stock market data from the Snowflake 
Marketplace (Cybersyn), transforms it through a layered dbt architecture, 
and serves analytics through a dimensional data mart.

## Architecture
![Architecture Diagram](docs/architecture_diagram.png)

## Tech Stack
- **Snowflake** — Cloud data warehouse
- **dbt Core** — Data transformation & modeling
- **Cybersyn (Snowflake Marketplace)** — Stock market data source
- **GitHub Actions** — CI/CD for automated testing
- **Streamlit** — Analytics dashboard (coming Week 8)

## Project Structure
\`\`\`
stock-analytics-snowflake/
├── dbt_project/        # All dbt models, tests, macros
├── streamlit_app/      # Dashboard application
├── .github/workflows/  # CI/CD pipeline
└── docs/               # Architecture diagrams
\`\`\`

## Data Layers
| Layer | Purpose |
|---|---|
| Staging | Clean & rename raw Cybersyn data |
| Intermediate | Business logic, financial calculations |
| Marts | Fact & dimension tables for analytics |

## Lineage Diagram


## Setup Instructions
_(To be filled as project progresses)_

## Key Insights
_(To be filled after data exploration)_
