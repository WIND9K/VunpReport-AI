"""
base_agent.py — Interface chuẩn cho 5 specialist agents.

Mỗi agent kế thừa BaseAgent và implement:
- get_report_keys() → list report_key ("onchain/vndc_send", ...)
- get_risk_rules()  → list risk rule functions
- (optional) custom ETL logic

Pipeline mỗi agent:
  fetch_json(on_batch=callback) → enrich → 3 nhánh:
    1. ETL Handler  → normalize → aggregate → bulk_upsert Aurora diary
    2. Raw Handler  → append enriched .jsonl (ParquetWriter convert cuối ngày)
    3. Risk Handler → accumulate enriched data (scoring chạy sau khi fetch xong)
"""

from __future__ import annotations

import logging
import time
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger(__name__)


class AgentResult:
    """Kết quả chạy 1 agent."""

    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self.status: str = "pending"
        self.total_records: int = 0
        self.etl_rows: int = 0
        self.raw_rows: int = 0
        self.risk_events: int = 0
        self.risk_scored: List[Dict] = []  # scored events cho Orchestrator
        self.errors: List[str] = []
        self.duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_name": self.agent_name,
            "status": self.status,
            "total_records": self.total_records,
            "etl_rows": self.etl_rows,
            "raw_rows": self.raw_rows,
            "risk_events": self.risk_events,
            "duration_seconds": round(self.duration_seconds, 2),
            "errors": self.errors,
        }

    def summary_line(self) -> str:
        icon = "✅" if self.status == "success" else "❌"
        return f"{icon} {self.agent_name}: {self.total_records} txn, {self.risk_events} risk events"


class BaseAgent(ABC):
    """
    Interface chuẩn cho mọi specialist agent.

    Subclass cần implement:
    - get_report_keys()   → ["onchain/vndc_send", ...]
    - get_risk_rules()    → [rule_func1, rule_func2, ...]
    - get_schema_name()   → "onchain" (cho schema_registry)
    """

    def __init__(
        self,
        name: str,
        *,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        timeout_seconds: int = 300,
        temp_dir: str = "/tmp",
    ):
        self.name = name
        self.date_from = date_from
        self.date_to = date_to
        self.timeout_seconds = timeout_seconds
        self.temp_dir = temp_dir

        # Handlers — inject bởi runner
        self.etl_handler: Optional[Callable] = None
        self.raw_handler: Optional[Callable] = None  # ParquetWriter.append_batch

        self._result = AgentResult(name)
        self._batch_count = 0
        self._all_enriched: List[Dict] = []  # tích lũy enriched records cho risk scoring
        self._current_report_key: str = ""

    # =====================
    # Abstract methods
    # =====================

    @abstractmethod
    def get_report_keys(self) -> List[str]:
        ...

    @abstractmethod
    def get_risk_rules(self) -> List[Callable]:
        """Trả về list risk rule functions. Mỗi function nhận enriched records, trả về list risk events."""
        ...

    @abstractmethod
    def get_schema_name(self) -> str:
        """Trả về tên agent cho schema_registry enricher."""
        ...

    # =====================
    # Pipeline
    # =====================

    def _dispatch(self, batch: List[Dict[str, Any]]) -> None:
        """on_batch callback — enrich rồi gọi handlers.

        Flow:
        1. Enrich raw batch → flat fields (schema_registry)
        2. Raw Handler: append enriched → .jsonl (cho Parquet cuối ngày)
        3. Tích lũy enriched records (risk scoring chạy SAU khi tất cả batches xong)
        """
        if not batch:
            return

        self._batch_count += 1
        self._result.total_records += len(batch)

        # 1. Enrich raw → flat fields
        enriched = batch  # default: không enrich
        try:
            from datalake.schema_registry import enrich_batch
            enriched = enrich_batch(self.get_schema_name(), batch, self._current_report_key)
        except Exception as e:
            log.warning("[%s] Enrich failed batch #%d: %s — using raw", self.name, self._batch_count, e)

        # 2. Raw Handler — ghi enriched vào .jsonl
        if self.raw_handler:
            try:
                raw_count = self.raw_handler(self.name, enriched)
                self._result.raw_rows += raw_count or 0
            except Exception as e:
                log.warning("[%s] Raw handler error batch #%d: %s", self.name, self._batch_count, e)
                self._result.errors.append(f"Raw batch#{self._batch_count}: {e}")

        # 3. Tích lũy enriched records cho risk scoring
        self._all_enriched.extend(enriched)

    def _run_risk_scoring(self, event_date: str) -> None:
        """Chạy risk scoring SAU KHI tất cả batches đã fetch xong.

        Flow:
        1. Chạy từng risk rule trên toàn bộ enriched records
        2. Gom raw risk events
        3. Gọi risk_scorer.score_risk_events() → tính final score + ghi DB
        """
        if not self._all_enriched:
            return

        rules = self.get_risk_rules()
        if not rules:
            return

        all_raw_events = []

        # 1. Chạy từng rule
        for rule_func in rules:
            try:
                events = rule_func(self._all_enriched)
                all_raw_events.extend(events)
                if events:
                    log.info("[%s] Rule %s → %d events", self.name, rule_func.__name__, len(events))
            except Exception as e:
                log.warning("[%s] Rule %s failed: %s", self.name, rule_func.__name__, e)
                self._result.errors.append(f"Rule {rule_func.__name__}: {e}")

        # 2. Score + ghi DB
        if all_raw_events:
            try:
                from risk.risk_scorer import score_risk_events
                scored = score_risk_events(all_raw_events, self.name, event_date)
                self._result.risk_events = len(scored)
                self._result.risk_scored = scored
                log.info("[%s] Risk scoring done: %d events scored", self.name, len(scored))
            except Exception as e:
                log.error("[%s] Risk scoring failed: %s", self.name, e)
                self._result.errors.append(f"Risk scoring: {e}")

    def run(self, settings=None) -> AgentResult:
        """Chạy agent: fetch → enrich → parquet → risk scoring.

        Args:
            settings: OnusSettings instance (None → tạo mới)
        """
        self._result.started_at = datetime.now()
        self._result.status = "running"
        start_time = time.time()

        try:
            from onuslibs.unified.api import fetch_json
            from onuslibs.config.settings import OnusSettings
            from onuslibs.utils.date_utils import build_date_period
            from config.report_map_ai import REPORT_MAP_AI

            if settings is None:
                settings = OnusSettings()

            date_from = self.date_from or datetime.now().strftime("%Y-%m-%d")
            date_to = self.date_to or date_from

            for report_key in self.get_report_keys():
                self._current_report_key = report_key
                spec = REPORT_MAP_AI.get(report_key)
                if not spec:
                    log.warning("[%s] No spec for %s", self.name, report_key)
                    continue

                params = {}
                if spec.get("filter_key") and spec.get("type"):
                    params[spec["filter_key"]] = spec["type"]
                if spec.get("extra_params"):
                    params.update(spec["extra_params"])
                params["datePeriod"] = build_date_period(date_from, date_to)

                try:
                    records = fetch_json(
                        endpoint=spec["endpoint"],
                        params=params,
                        fields=spec.get("fields"),
                        paginate=True,
                        order_by="date asc",
                        on_batch=self._dispatch,
                        settings=settings,
                    )
                    log.info("[%s] %s: %d records", self.name, report_key, len(records) if records else 0)
                except Exception as e:
                    log.error("[%s] Fetch %s failed: %s", self.name, report_key, e)
                    self._result.errors.append(f"fetch {report_key}: {e}")

            # Risk scoring — chạy SAU KHI tất cả reports đã fetch xong
            self._run_risk_scoring(date_from)

            self._result.status = "success"

        except Exception as e:
            log.error("[%s] Agent failed: %s", self.name, e)
            self._result.status = "failed"
            self._result.errors.append(str(e))

        finally:
            self._result.finished_at = datetime.now()
            self._result.duration_seconds = time.time() - start_time

        return self._result
