# onusreport-ai v5.0

Hệ thống Agentic Kiểm soát Giao dịch — Phát hiện, Học hỏi, và Mở rộng.

## Kiến trúc

```
5 Agents → Aurora MySQL (Hot) + S3 Parquet (Warm) → LLM → Learning Loop 3 lớp → Telegram Alert
```

## Cấu trúc repo

```
onusreport-ai/
├── agents/           ← base_agent.py + 5 agent + orchestrator
├── datalake/         ← parquet_writer, schema_registry, duckdb_reader
├── risk/             ← risk modules per agent + risk_scorer + context_checker
├── learning/         ← feedback_handler + rule_analyzer
├── alerts/           ← telegram, slack, email
├── config/           ← settings.yaml + schemas/ + report_map_ai
└── requirements.txt
```

## Dependencies

- **onuslibs** (G:\Learn\OnusLibs) — fetch_json, DB pool, bulk_upsert
- **onusreport** (G:\Learn\OnusReport) — ETL transform, report_etl_meta

## Quick start

```bash
pip install -r requirements.txt
python -m agents.onchain_agent --start 2026-03-18 --end 2026-03-18
```
