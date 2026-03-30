# onusreport-ai

> Multi-Agent ETL Pipeline + Real-time Risk AI cho [goonus.io](https://goonus.io)

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![Aurora](https://img.shields.io/badge/Aurora-MySQL-orange)](https://aws.amazon.com/rds/aurora/)
[![S3](https://img.shields.io/badge/S3-Parquet%20%2B%20DuckDB-yellow)](https://aws.amazon.com/s3/)
[![LLM](https://img.shields.io/badge/LLM-Claude%20Sonnet%20%2B%20Gemini%20Flash-purple)](https://anthropic.com)

---

## Tổng quan

**onusreport-ai** là hệ thống multi-agent chạy hàng đêm, bao gồm:

- **ETL pipeline**: 5 agents độc lập, `asyncio.gather`, ghi Aurora + S3 Parquet
- **Real-time risk**: phát hiện bất thường per batch, per userid — trong lúc ETL đang chạy
- **End-of-day risk**: cross-agent, cross-market detection sau khi finalize
- **LLM layer**: Claude Sonnet (alert narrative, daily briefing) + Gemini Flash (borderline scoring)
- **OpenClaw Skills**: scheduler, Telegram bot, `/confirm`, query answering

| Chỉ số | Giá trị |
|---|---|
| Phases | 7 |
| Thời gian build | ~45 ngày |
| Agents | 5 (Onchain, Pro, BuySell, Exchange, Spot) |
| Risk layers | 2 (real-time + end-of-day) |
| Chi phí ops | ~$18/tháng |
| LLM stack | Claude Sonnet + Gemini Flash (không Ollama) |

---

## Confirmed Facts — v6 (thay đổi so với v5)

| # | Điểm | Trạng thái |
|---|---|---|
| 1 | Exchange agent fetch API → ghi **chỉ `vu_exchange`**, không liên quan `spot_diary` | ✅ Confirmed |
| 2 | `spot_diary` = order-level, ETL onuslibs đã chạy → Phase 6 chỉ **wrap**, không tạo table mới | ✅ Confirmed |
| 3 | **Bỏ Ollama hoàn toàn** — Claude Sonnet + Gemini Flash là đủ | ✅ Confirmed |
| 4 | Risk AI tables (`user_context`, `ai_daily_summary`...) → **defer sau EDA Phase 5**. Chỉ tạo `risk_events` ngay từ đầu | ✅ Confirmed |

---

## Kiến trúc Multi-Agent

```
Layer 4 │ OpenClaw (Telegram bot · cron 03/05/06h · 7 Skills · APScheduler fallback)
─────────┼──────────────────────────────────────────────────────────────────────────
Layer 3 │ 5 Agents (asyncio.gather · fail 1 không ảnh hưởng 4 còn lại)
         │  ├── OnchainAgent  → onchain_diary   + Parquet + real-time risk
         │  ├── ProAgent      → pro_diary        + Parquet + real-time risk
         │  ├── BuySellAgent  → buy_sell_diary   + Parquet + real-time risk
         │  ├── ExchangeAgent → vu_exchange only + Parquet + real-time risk
         │  └── SpotAgent     → spot_diary (wrap)+ Parquet + real-time risk
─────────┼──────────────────────────────────────────────────────────────────────────
Layer 2 │ Risk Engine (real-time rules + end-of-day cross-agent/cross-market)
─────────┼──────────────────────────────────────────────────────────────────────────
Layer 1 │ Aurora MySQL  +  S3 Parquet  +  DuckDB in-process
```

### agent_runner_skill — Progressive 1 → 5 agents

| Phase | asyncio.gather |
|---|---|
| Phase 2 | `onchain` |
| Phase 3 | `onchain` · `pro` · `buysell` |
| Phase 5 | `onchain` · `pro` · `buysell` · `exchange` |
| Phase 6 | `onchain` · `pro` · `buysell` · `exchange` · `spot` |

### Daily Flow — 03h → 06h sáng

```
03:00  etl_runner_skill.py    → asyncio.gather 5 agents (ETL + Parquet + real-time risk)
05:00  finalize_skill.py      → /tmp/*.jsonl → .parquet → S3 raw/{group}/Y/M/D/
                               → end_of_day_rules.py (cross-agent, cross-market)
06:00  daily_briefing_skill.py → DuckDB summary → Claude Sonnet → Telegram
```

---

## Repo Structure

```
onusreport-ai/
├── agents/
│   ├── base_agent.py          # Abstract: fetch() → asyncio.gather 3 handlers
│   ├── onchain_agent.py
│   ├── pro_agent.py
│   ├── buysell_agent.py
│   ├── exchange_agent.py
│   └── spot_agent.py
├── etl/                       # Copy từ onusreport — không sửa logic
│   ├── transform.py
│   ├── etl_meta.py
│   ├── report_map.py
│   └── buysell_filters.py
├── risk/
│   ├── onchain_risk.py
│   ├── pro_risk.py
│   ├── buysell_risk.py
│   ├── exchange_risk.py
│   ├── spot_risk.py
│   ├── risk_scorer.py         # Score 0–10, anomaly_x multiplier DuckDB
│   ├── end_of_day_rules.py    # Cross-agent, cross-market (sau finalize)
│   └── llm_enricher.py        # Claude Sonnet + Gemini Flash async
├── datalake/
│   ├── parquet_writer.py      # append_batch() + finalize_day() → S3
│   ├── duckdb_reader.py       # get_user_history(userid, group, days=30)
│   └── schema_registry.py
├── learning/
│   ├── feedback_handler.py    # /confirm → update risk_events.verdict
│   └── rule_analyzer.py       # SQL phân tích FP rate (Phase 5+)
├── openclaw_skills/
│   ├── etl_runner_skill.py    # cron 03:00
│   ├── finalize_skill.py      # cron 05:00
│   ├── confirm_handler_skill.py
│   ├── daily_briefing_skill.py
│   ├── query_user_skill.py
│   ├── risk_query_skill.py
│   └── rule_analyzer_skill.py # cron monthly (Phase 5+)
├── alerts/
│   ├── telegram_bot.py
│   ├── slack_webhook.py
│   └── email_sender.py
├── scheduler/
│   └── job_scheduler.py       # APScheduler fallback
├── config/
│   ├── settings.yaml          # .gitignore ngay
│   └── schemas/
├── tests/
└── .env                       # .gitignore ngay
```

---

## BaseAgent — Interface chuẩn

```python
# agents/base_agent.py
import asyncio
from abc import ABC, abstractmethod

class BaseAgent(ABC):
    def __init__(self, date, config):
        self.date = date
        self.config = config
        self.session_buffer = {}          # in-memory per-run risk buffer

    @abstractmethod
    async def fetch(self) -> list[dict]:
        """Fetch raw data từ onuslibs — implement trong mỗi agent"""
        ...

    async def run(self):
        rows = await self.fetch()
        await asyncio.gather(
            self._etl_handler(rows),      # → Aurora diary table
            self._parquet_handler(rows),  # → /tmp/{group}_{date}.jsonl
            self._risk_handler(rows),     # → risk_events nếu score ≥ threshold
        )

    async def _process_batch(self, userid, user_rows):
        score, rules = risk_rules.check(userid, user_rows, self.session_buffer)
        if score >= THRESHOLD_MEDIUM:
            case_id = risk_events.create(
                userid=userid, agent_type=self.group_name,
                score=score, rules_triggered=rules,
                raw_evidence=user_rows, detection_layer="realtime"
            )
            if score >= THRESHOLD_CRITICAL:
                await telegram.critical(case_id, userid, score, rules)
                await slack.alert(case_id, score, rules)
            elif score >= THRESHOLD_HIGH:
                await telegram.alert(case_id, userid, score, rules)
```

```python
# openclaw_skills/etl_runner_skill.py
async def run_all_agents(date):
    results = await asyncio.gather(
        OnchainAgent(date, config).run(),
        ProAgent(date, config).run(),
        BuySellAgent(date, config).run(),
        ExchangeAgent(date, config).run(),   # API → chỉ vu_exchange
        SpotAgent(date, config).run(),       # onuslibs → spot_diary (wrap)
        return_exceptions=True               # fail 1 không dừng 4 còn lại
    )
    ok   = sum(1 for r in results if not isinstance(r, Exception))
    fail = [r for r in results if isinstance(r, Exception)]
    await telegram.send(f"✅ {ok}/5 agents OK · ⚠️ {len(fail)} failed: {fail}")
```

---

## Database Tables

| Table | Agent | Status |
|---|---|---|
| `onchain_diary` | Onchain | ✅ Có sẵn |
| `pro_diary` | Pro | ✅ Có sẵn |
| `buy_sell_diary` | BuySell | ✅ Có sẵn |
| `vu_exchange` | Exchange | ✅ Có sẵn (nguồn đổi CSV → API, schema giữ nguyên) |
| `spot_diary` | Spot | ✅ Có sẵn — order-level, ETL đã chạy |
| `vu_onus_users` | Users | ✅ Có sẵn |
| `risk_events` | Risk MVP | ⚠️ **Tạo mới — Phase 1** |
| `user_context`, `ai_daily_summary`... | Risk AI | ⏳ Defer Phase 5+ sau EDA |

### risk_events — Table duy nhất cần CREATE ngay

```sql
CREATE TABLE risk_events (
    id              BIGINT       AUTO_INCREMENT PRIMARY KEY,
    case_id         VARCHAR(36)  NOT NULL UNIQUE,       -- UUID, ref trong /confirm
    userid          VARCHAR(64)  NOT NULL,
    agent_type      ENUM('onchain','pro','buysell','exchange','spot') NOT NULL,
    date_tx         DATE         NOT NULL,
    risk_score      DECIMAL(4,2) NOT NULL,              -- 0.00–10.00
    risk_type       VARCHAR(64)  NOT NULL,              -- STRUCTURING, VELOCITY...
    rules_triggered JSON         NOT NULL,
    raw_evidence    JSON         NOT NULL,
    llm_summary     TEXT         DEFAULT NULL,          -- điền async sau LLM enrich
    detection_layer ENUM('realtime','endofday') NOT NULL,
    verdict         ENUM('pending','TRUE_POSITIVE','FALSE_POSITIVE','snooze') DEFAULT 'pending',
    confirmed_by    VARCHAR(64)  DEFAULT NULL,
    confirmed_at    DATETIME     DEFAULT NULL,
    alert_sent_at   DATETIME     DEFAULT NULL,
    created_at      DATETIME     DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_userid     (userid),
    INDEX idx_date_score (date_tx, risk_score DESC),
    INDEX idx_agent_date (agent_type, date_tx),
    INDEX idx_verdict    (verdict)
);
```

---

## Risk Rules

### Real-time (per batch, in-memory)

| Rule | Logic | Score | Groups |
|---|---|---|---|
| `velocity` | ≥ N tx trong 60 phút | +2.0 + 0.5/tx thêm | All |
| `structuring` | 3+ tx ~cùng mức, tổng vượt threshold, trong 120 phút | +3.0 | onchain, pro, buysell |
| `off_hours` | tx lớn lúc 1h–4h (UTC+7) | +1.5 | All |
| `new_large` | user < 30 ngày, amount > P95 | +2.5 | All |
| `wash_trading` | BUY→SELL ≤30 phút, amount ≈ (±5%) | +3.5 | buysell |
| `rapid_swap` | BUY→SELL cùng coin ≤5 phút, trace DuckDB | +3.0 | exchange |
| `order_layering` | ≥5 orders cùng coin, giá ±0.5%, cancel >80% trong <60s | +4.0 | spot |
| `quote_stuffing` | >20 orders/phút vs DuckDB 30-day avg | +3.0 | spot |
| `abnormal_fill_rate` | fill_rate <10% vs DuckDB avg 30 ngày | +2.0 | spot |

> ⚠️ Tất cả threshold (N, P95, amount) để trống trong `settings.yaml`. Set sau EDA Phase 5.

### End-of-day (sau finalize, query risk_events)

| Rule | Logic | Score |
|---|---|---|
| `cross_agent_layering` | Cùng userid có risk_events ≥2 agent_types cùng ngày | +3.0 |
| `cross_market_pump` | onchain + spot risk_event cùng userid cùng coin cùng ngày | +4.0 |
| `rp1_repeat` | rp1 source lặp >3 lần trong 30 ngày (DuckDB) | +2.0 |

### Score bands & Alert routing

| Score | Level | Channel | SLA |
|---|---|---|---|
| ≥ 9.0 | CRITICAL | Telegram immediate + Slack | 30 phút |
| 7.5–8.9 | HIGH | Telegram + Claude Sonnet summary | 2 giờ |
| 5.0–7.4 | MEDIUM | risk_events + Gemini Flash enrich + Email digest | Cuối ngày |
| 0–4.9 | Normal | Parquet only | — |

---

## LLM Role Mapping

| Model | Task | Trigger | Freq |
|---|---|---|---|
| Claude Sonnet | Alert narrative | score ≥ 7.5, async | ~5–15x/ngày |
| Claude Sonnet | Daily briefing | cron 06:00 sau finalize | 1x/ngày |
| Claude Sonnet | Telegram query ("user X hôm nay") | on-demand | — |
| Gemini Flash | Borderline scoring enrichment | score 5.0–7.4, async | — |
| Gemini Flash | Rule analyzer monthly | cron ngày 1/tháng | 1x/tháng |

---

## OpenClaw Skills

| Skill | Trigger | Phase |
|---|---|---|
| `etl_runner_skill.py` | cron 03:00 | Phase 1 |
| `finalize_skill.py` | cron 05:00 | Phase 1 |
| `confirm_handler_skill.py` | Telegram: `/confirm {id} {verdict}` | Phase 2 |
| `daily_briefing_skill.py` | cron 06:00 | Phase 4 |
| `query_user_skill.py` | Telegram: "user X hôm nay" | Phase 4 |
| `risk_query_skill.py` | Telegram: "top risk hôm nay" | Phase 4 |
| `rule_analyzer_skill.py` | cron ngày 1/tháng | Phase 5+ |

**Telegram commands:** `/confirm` `/detail` `/snooze` `/status` `/summary` `/cases` `/apply`

---

## Setup

### Dependencies

```bash
python3.11 -m venv venv && source venv/bin/activate

pip install boto3 pandas pyarrow duckdb \
            anthropic google-generativeai \
            python-telegram-bot requests \
            pymysql sqlalchemy pyyaml \
            apscheduler python-dotenv

# Không cài Ollama

# OpenClaw
git clone https://github.com/steipete/openclaw
cd openclaw && pip install -e .

# Verify
python -c "import boto3, duckdb, anthropic; print('✅ All deps OK')"
```

### config/settings.yaml

```yaml
# .gitignore NGAY — không commit file này
api:
  onus_base_url: "https://wallet.vndc.io"
  onus_api_key:  "${ONUS_API_KEY}"
  claude_api_key: "${ANTHROPIC_API_KEY}"
  gemini_api_key: "${GEMINI_API_KEY}"

database:
  aurora_host: "your-aurora-endpoint.rds.amazonaws.com"
  aurora_db:   "onusreport"
  aurora_user: "${DB_USER}"
  aurora_password: "${DB_PASSWORD}"
  pool_size: 5                  # 5 agents × 1 conn

s3:
  bucket: "your-existing-bucket"
  prefix_raw: "raw/"
  region: "ap-southeast-1"
  lifecycle_days: 730

alerts:
  telegram_token:   "${TELEGRAM_BOT_TOKEN}"
  telegram_chat_id: "${TELEGRAM_CHAT_ID}"
  slack_webhook:    "${SLACK_WEBHOOK_URL}"
  email_smtp: "smtp.gmail.com"

risk_rules:                     # Set sau EDA Phase 5
  critical_threshold: 9.0
  high_threshold: 7.5
  medium_threshold: 5.0
  off_hours: [1, 2, 3, 4]
  velocity_per_hour: null       # TODO: set sau EDA
  structuring_count: null       # TODO: set sau EDA
  new_large_p95: null           # TODO: set sau EDA

schedule:
  agents_start: "03:00"
  finalize_day: "05:00"
  orchestrator:  "06:00"
```

### .gitignore

```
.env
config/settings.yaml
venv/
__pycache__/
*.pyc
/tmp/
```

---

## Kế Hoạch Triển Khai — 7 Phases

### Phase 1 · Ngày 1–3 — Nền Tảng

> BaseAgent + Parquet/DuckDB + OpenClaw + `risk_events` table

**Ngày 1 — Repo Setup**
- [ ] Tạo repo, init cấu trúc đầy đủ
- [ ] Copy ETL modules từ onusreport: `cp onusreport/etl/*.py etl/` → test import 0 errors
- [ ] Tạo `config/settings.yaml` + `.env` → `.gitignore` ngay
- [ ] Verify S3 boto3 `list_objects` → OK · Kiểm tra Lifecycle 730 ngày
- [ ] Chạy SQL tạo `risk_events` table (staging trước, production sau verify)

**Ngày 2 — BaseAgent + Data Lake**
- [ ] Viết `agents/base_agent.py`: abstract, `run()` → `asyncio.gather` 3 handlers
- [ ] Viết `datalake/parquet_writer.py`: `append_batch()` + `finalize_day()` → S3 `raw/{group}/Y/M/D/`
- [ ] Viết `datalake/duckdb_reader.py`: `get_user_history(userid, group, days=30)` + `get_daily_summary(date)`
- [ ] Test: 10 rows dummy → append → finalize → S3 upload → DuckDB query <500ms ✓

**Ngày 3 — OpenClaw + Alert Skeleton**
- [ ] Cài OpenClaw, cấu hình Telegram Bot · Test `/ping` → reply <3 giây
- [ ] Viết `etl_runner_skill.py`: cron `"0 3 * * *"`, placeholder 5 agents, báo Telegram
- [ ] Viết `finalize_skill.py`: cron `"0 5 * * *"`
- [ ] Viết `scheduler/job_scheduler.py`: APScheduler fallback
- [ ] Viết `alerts/telegram_bot.py`: format alert với case_id + 6 commands

**Acceptance Criteria:**
| Hạng mục | Pass |
|---|---|
| S3 Parquet upload | `raw/{group}/Y/M/D/data.parquet` đúng path |
| DuckDB query | < 500ms |
| ETL import | 0 errors |
| OpenClaw /ping | Round-trip < 3 giây |

---

### Phase 2 · Ngày 4–10 — Onchain Agent + Risk Engine

> Agent đầu tiên end-to-end · Template cho 4 agents còn lại

- [ ] Viết `agents/onchain_agent.py` kế thừa BaseAgent, `fetch()` dùng `onuslibs.fetch_json`
- [ ] ETL handler: enrich → normalize → aggregate → `bulk_upsert(onchain_diary)` (tái dụng 100%)
- [ ] Viết `risk/onchain_risk.py`: 4 rules — `structuring`, `velocity`, `off_hours`, `new_large` (threshold để YAML)
- [ ] Viết `risk/risk_scorer.py`: score 0–10, `anomaly_x` **tắt 7 ngày đầu** (chưa có baseline DuckDB)
- [ ] Viết `learning/feedback_handler.py`: `/confirm {case_id} {verdict}` → update `risk_events.verdict`
- [ ] Viết `openclaw_skills/confirm_handler_skill.py`
- [ ] Test: inject score ≥9.0 → Telegram nhận alert → `/confirm` → verify `risk_events.verdict`

> ⚠️ Ngày 1–7: `anomaly_x = 1.0` (disabled). Bật lại sau khi có 7 ngày Parquet data. Ghi `# TODO: enable after 7 days` trong code.

---

### Phase 3 · Ngày 11–17 — Pro + BuySell + Cross-agent

- [ ] Viết `agents/pro_agent.py`: 4 sub-reports offchain → `pro_diary` → Parquet
- [ ] Viết `risk/pro_risk.py`: volume spike + cross-agent check (query `risk_events` cùng userid → +3 score)
- [ ] Viết `agents/buysell_agent.py`: buy/sell × system/partner → `buy_sell_diary` + rp1~rp4 → Parquet
- [ ] Viết `risk/buysell_risk.py`: wash_trading (BUY→SELL ≤30 phút), rp1 lặp >3 lần/tháng (DuckDB)
- [ ] Cập nhật `etl_runner_skill.py`: `asyncio.gather` 3 agents
- [ ] Viết `learning/rule_analyzer.py`: SQL phân tích FP rate từ `risk_events.verdict`
- [ ] Test stress: 3 agents song song, verify DB pool không leak

**Acceptance Criteria:**

| Hạng mục | Pass |
|---|---|
| 3 agents song song | asyncio OK, không conflict DB pool |
| Cross-agent | Pro thấy Onchain risk_event cùng userid → +3 score |
| Wash trading | BUY→SELL 10 phút → flag đúng |
| Parquet S3 | 3 files/ngày sau 5AM |

---

### Phase 4 · Ngày 18–24 — Exchange Agent + Orchestrator + LLM

> ⚠️ **Ngày 18 FIRST**: Fetch thử Exchange API, print raw JSON, xác nhận schema khớp `vu_exchange` trước khi code agent.

- [ ] **Ngày 18**: Fetch endpoint `vndcacc.exchange` + `usdtacc.exchange` → print raw response → confirm schema
- [ ] Viết `agents/exchange_agent.py`: fetch API → ETL `vu_exchange` **(không spot_diary)** → Parquet
- [ ] Viết `risk/exchange_risk.py`: rapid_swap (BUY→SELL ≤5 phút, trace DuckDB), off_hours_large
- [ ] Viết `risk/llm_enricher.py`: Claude Sonnet async (score ≥7.5) + Gemini Flash async (5.0–7.4)
- [ ] Viết `risk/end_of_day_rules.py`: `cross_agent_layering`, `cross_market_pump`
- [ ] Viết `openclaw_skills/daily_briefing_skill.py`: cron 06:00, DuckDB → Claude Sonnet → Telegram + OpenClaw memory
- [ ] Viết `openclaw_skills/query_user_skill.py`: userid → diary + risk_events → Claude Sonnet → tiếng Việt
- [ ] Viết `openclaw_skills/risk_query_skill.py`: "top risk hôm nay" → risk_events → format → Telegram
- [ ] Viết `alerts/slack_webhook.py` (HIGH) + `alerts/email_sender.py` (MEDIUM daily digest)
- [ ] Cập nhật `etl_runner_skill.py`: 4 agents

---

### Phase 5 · Ngày 25–31 — Rule Analyzer + Scheduler

- [ ] Viết `openclaw_skills/rule_analyzer_skill.py`: cron ngày 1/tháng → phân tích FP rate → gợi ý threshold → Telegram
- [ ] Implement `/apply {rule_name}`: update `settings.yaml` → hot-reload (risk_scorer load mỗi lần, không cache global)
- [ ] Test `/apply velocity` → verify log lần chạy tiếp dùng threshold mới
- [ ] Test finalize_day crash recovery: kill giữa chừng → restart → temp file vẫn còn → finalize OK
- [ ] Verify S3 folder structure: `raw/{agent}/year=Y/month=M/day=D/data.parquet`

---

### Phase 6 · Ngày 32–38 — Spot Agent + Cross-Market · 5 agents hoàn chỉnh

> `spot_diary` đã tồn tại, order-level, ETL onuslibs đã chạy → **chỉ wrap vào BaseAgent**, ~2 ngày

- [ ] Viết `agents/spot_agent.py`: wrap ETL spot đã có, thêm `_parquet_handler` + `_risk_handler`
- [ ] Viết `risk/spot_risk.py`: 4 rules — `order_layering`, `quote_stuffing`, `cross_market_pump`, `abnormal_fill_rate`
- [ ] Cập nhật `etl_runner_skill.py`: `asyncio.gather` đủ 5 agents
- [ ] Cập nhật `finalize_skill.py`: thêm `'spot'` vào loop
- [ ] Test stress: 5 agents song song 3–5AM, verify DB pool 5 connections không deadlock

---

### Phase 7 · Ngày 39–45 — Shadow Mode → Looker → Go-Live

**Shadow Mode (5 đêm bắt buộc)**
- [ ] Tạo Aurora schema staging `onusreport_ai_staging`
- [ ] `SHADOW_MODE=true` → agents ghi staging, không production
- [ ] Chạy 5 nights. Mỗi sáng: comparison script → divergence phải <5% vs onusreport cũ
- [ ] Calibrate threshold dựa trên alert volume thực tế → update `settings.yaml`

**Looker Studio (ngày 3–5)**
- [ ] Dashboard 1: Diary tables overview — 5 groups, daily volume, date gaps
- [ ] Dashboard 2: Risk events — score distribution, risk_type, daily trend, verdict split
- [ ] Dashboard 3: Learning Loop — FP rate từ `risk_events.verdict`, threshold change log

**Go-Live (ngày 6–7)**
- [ ] 5 đêm shadow OK + divergence <5% → tắt `SHADOW_MODE`
- [ ] Monitor 48h đầu, verify Telegram briefing 6AM
- [ ] Tắt Prefect/onusreport cũ **chỉ sau 5 ngày production ổn định**

---

## Testing Strategy

| Cấp | Phạm vi | Tool | Khi nào | Pass |
|---|---|---|---|---|
| Unit | Risk rules, score calc, parquet writer | pytest | Mỗi phase, trước push | 100%, 0 exception |
| Integration | 1 agent full pipeline (DB staging) | Manual | Cuối mỗi phase | Parquet OK, diary update, alert gửi |
| Stress | 5 agents song song + DB pool + S3 | Manual | Phase 6 sau Spot | Không deadlock |
| Shadow | Toàn hệ thống vs onusreport cũ | Python script | Phase 7, 5 đêm | Divergence <5% |

```python
# tests/test_risk_rules.py

def test_structuring_detected():
    txs = [make_tx(amount=990), make_tx(amount=995), make_tx(amount=1000),
           make_tx(amount=1005), make_tx(amount=998)]
    result = onchain_risk.check_structuring(txs)
    assert result.flagged == True
    assert result.risk_type == "STRUCTURING"

def test_wash_trading_detected():
    buy  = make_tx(direction="BUY",  amount=1000, ts=0)
    sell = make_tx(direction="SELL", amount=1000, ts=1200)  # 20 phút sau
    assert buysell_risk.check_wash_trading([buy, sell]).flagged == True

def test_spot_order_layering():
    orders = make_layering_orders(count=6, cancel_rate=0.83, window_seconds=45)
    assert spot_risk.check_order_layering(orders).flagged == True

def test_finalize_crash_recovery():
    writer.append_batch("onchain", make_batch(10))
    writer2 = ParquetWriter()  # simulate crash
    assert writer2.finalize_day("onchain")["uploaded"] == True

def test_confirm_updates_verdict():
    case_id = create_risk_event(score=8.5)
    feedback_handler.handle(case_id=case_id, verdict="FALSE_POSITIVE")
    event = db.get_risk_event(case_id)
    assert event.verdict == "FALSE_POSITIVE"
    assert event.confirmed_at is not None
```

---

## Go-Live Checklist

### 🔴 Critical — Bắt buộc 100%

- [ ] 5 agents chạy 3AM không lỗi ít nhất 5 đêm liên tiếp trong shadow
- [ ] `finalize_day` upload đủ 5 Parquet files mỗi ngày
- [ ] Telegram CRITICAL alert hoạt động — test score giả ≥9.0
- [ ] `/confirm` lưu đúng verdict + `confirmed_at` vào `risk_events`
- [ ] DB connection pool không leak sau 1 đêm 5 agents
- [ ] `settings.yaml` và `.env` không có trong git history
- [ ] S3 bucket không public · IAM policy đúng · Aurora backup enabled
- [ ] Divergence diary tables vs onusreport cũ <5%

### 🟡 Important — Nên có

- [ ] Daily briefing 6AM chạy đúng ≥3 ngày
- [ ] Slack HIGH alert hoạt động — test score 8.0
- [ ] DuckDB có data ≥7 ngày → `anomaly_x` tính được
- [ ] Looker Studio dashboard load <1 ngày
- [ ] Cross-agent detection (Pro + Onchain) test OK
- [ ] Spot order_layering detect đúng test data
- [ ] Exchange API schema xác nhận khớp `vu_exchange`

---

## Monitoring & Ops

### Daily — 6:00 AM
1. Đọc OpenClaw daily briefing Telegram
2. Verify 5 Parquet files trên S3
3. Check không có ERROR trong log
4. Review CRITICAL cases nếu có — `/confirm` hoặc `/snooze`

### Weekly — Thứ 2
1. Looker dashboard 7 ngày qua
2. FP rate từ `risk_events.verdict`
3. OpenClaw memory tích lũy
4. S3 usage growth
5. `anomaly_x` baseline đủ chưa (≥7 ngày)

### Monthly — Ngày 1
1. `rule_analyzer_skill` tự chạy → review gợi ý → `/apply` nếu hợp lý
2. S3 Parquet size check
3. Alert response SLA review

### Health Check SQL

```sql
-- 1. Agents có chạy đêm qua không?
SELECT agent_type, COUNT(*) AS records, MAX(created_at) AS last_run
FROM risk_events
WHERE DATE(created_at) = CURDATE() - INTERVAL 1 DAY
GROUP BY agent_type;

-- 2. False positive rate 7 ngày
SELECT risk_type,
       COUNT(*) AS total,
       ROUND(SUM(verdict='FALSE_POSITIVE')/COUNT(*)*100, 1) AS fp_pct
FROM risk_events
WHERE confirmed_at >= NOW() - INTERVAL 7 DAY AND verdict != 'pending'
GROUP BY risk_type ORDER BY fp_pct DESC;

-- 3. Top users risk score tuần này
SELECT userid, COUNT(*) AS events,
       ROUND(AVG(risk_score),2) AS avg_score, MAX(risk_score) AS max_score
FROM risk_events
WHERE date_tx >= CURDATE() - INTERVAL 7 DAY
GROUP BY userid ORDER BY avg_score DESC LIMIT 20;

-- 4. Cases pending quá lâu (>24h)
SELECT case_id, userid, agent_type, risk_score, created_at
FROM risk_events
WHERE verdict = 'pending' AND created_at < NOW() - INTERVAL 24 HOUR
ORDER BY risk_score DESC LIMIT 10;
```

---

## Rollback Plan

### L1 — 1 Agent Crash 🟡
- `return_exceptions=True` trong `asyncio.gather` — 4 agents còn lại chạy bình thường
- OpenClaw tự nhắn Telegram: `"⚠️ exchange_agent failed: {error_msg}"`
- `finalize_skill` bỏ qua group lỗi nếu không có `/tmp` file
- Sáng: xem log → fix → chạy lại thủ công với `--date yesterday`
- Data loss: 1 ngày × 1 agent → backfill từ API Onus

### L2 — Toàn hệ thống crash 🔴
- onusreport cũ vẫn chạy → KPI báo cáo không bị gián đoạn
- APScheduler fallback đã kích hoạt chưa? Check log
- Xác định nguyên nhân: DB down? S3 outage? API Onus thay đổi schema?
- Nếu API thay đổi: update onuslibs → test → deploy trong 24h

### L3 — Data Corruption 🟣
- Stop tất cả agents ngay (kill cron + APScheduler)
- Restore Aurora từ automated snapshot (daily, giữ 7 ngày)
- S3 Parquet append-only — không bị ảnh hưởng, dùng để rebuild nếu cần
- Replay với `SHADOW_MODE=true`, verify trước khi restore production

> **Nguyên tắc bất biến**: onusreport cũ không tắt cho đến khi onusreport-ai ổn định ≥30 ngày liên tiếp không có sự cố nghiêm trọng.

---

## Môi trường

| Thành phần | Trạng thái |
|---|---|
| Aurora MySQL | ✅ Có sẵn |
| AWS S3 + IAM | ✅ Có sẵn |
| onusreport ETL modules | ✅ Copy trực tiếp, không sửa |
| onuslibs v3.1 | ✅ Có sẵn |
| Telegram Bot | ⚠️ Tạo mới |
| Slack Webhook | ⚠️ Tạo mới |
| OpenClaw | ⚠️ Cài mới |
| Ollama | ❌ Không dùng |

---

*onusreport-ai v6 · build trên dữ liệu thực, confirmed từng điểm*
