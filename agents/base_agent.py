"""
base_agent.py — Interface chuẩn cho 5 specialist agents.

Mỗi agent kế thừa BaseAgent và implement:
- get_report_keys() → list report_key ("onchain/vndc_send", ...)
- build_risk_rules() → risk rules cho agent này
- (optional) custom ETL logic

Pipeline chuẩn cho mỗi agent:
  fetch_json(on_batch=_dispatch) → _dispatch gọi 3 nhánh song song:
    1. ETL Handler  → enrich → normalize → aggregate → bulk_upsert Aurora diary
    2. Raw Handler  → append .jsonl (ParquetWriter sẽ convert cuối ngày)
    3. Risk Handler → scoring rules + DuckDB lịch sử → risk_events

Tái sử dụng 100% từ onusreport:
- report_map.py (REPORT_MAP)
- report_etl_meta.py (get_report_meta)
- transform.py (enrich_records, normalize_date_field_to_epoch_midnight, group_and_aggregate)
- db.py (write_to_db)

Tái sử dụng từ onuslibs:
- fetch_json (auto-pagination, on_batch callback)
- DB class (connection pooling, bulk_upsert, transaction)
"""

from __future__ import annotations

import asyncio
import logging
import time
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from pathlib import Path

log = logging.getLogger(__name__)


class AgentResult:
    """Kết quả chạy 1 agent."""

    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self.status: str = "pending"  # pending | running | success | failed | timeout
        self.total_records: int = 0
        self.etl_rows: int = 0
        self.raw_rows: int = 0
        self.risk_events: int = 0
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
        parts = [
            f"{icon} {self.agent_name}: {self.total_records} txn",
            f"{self.risk_events} risk events",
        ]
        if self.errors:
            parts.append(f"{len(self.errors)} errors")
        return ", ".join(parts)


class BaseAgent(ABC):
    """
    Interface chuẩn cho mọi specialist agent.

    Subclass cần implement:
    - get_report_keys()   → ["onchain/vndc_send", "onchain/vndc_receive", ...]
    - build_risk_rules()  → dict cấu hình risk rules
    - (optional) custom_etl(), custom_risk()
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

        # Sẽ được inject bởi agent_runner
        self.etl_handler: Optional[Callable] = None
        self.raw_handler: Optional[Callable] = None
        self.risk_handler: Optional[Callable] = None

        self._result = AgentResult(name)
        self._batch_count = 0

    # =====================
    # Abstract methods
    # =====================

    @abstractmethod
    def get_report_keys(self) -> List[str]:
        """Trả về list report_key dạng "group/kind".

        Ví dụ: ["onchain/vndc_send", "onchain/vndc_receive", ...]
        Agent sẽ fetch lần lượt từng report_key.
        """
        ...

    @abstractmethod
    def build_risk_rules(self) -> Dict[str, Any]:
        """Trả về cấu hình risk rules cho agent này.

        Ví dụ:
        {
            "structuring": {"min_txn": 5, "amount_tolerance_pct": 10, "time_window_hours": 2},
            "velocity": {"threshold_per_hour": 10, "lookback_days": 30},
            "off_hours": {"time_window": "01:00-04:00", "min_amount_vs_avg": 2.0},
        }
        """
        ...

    # =====================
    # Pipeline chính
    # =====================

    def _dispatch(self, batch: List[Dict[str, Any]]) -> None:
        """on_batch callback — gọi 3 handlers song song.

        Được truyền vào fetch_json(on_batch=self._dispatch).
        Mỗi batch từ API sẽ đồng thời:
        1. ETL → diary table (Aurora)
        2. Raw → append .jsonl (temp local)
        3. Risk → scoring (risk_events)
        """
        if not batch:
            return

        self._batch_count += 1
        self._result.total_records += len(batch)

        # Nhánh 1: ETL Handler
        if self.etl_handler:
            try:
                rows_written = self.etl_handler(self.name, batch)
                self._result.etl_rows += rows_written or 0
            except Exception as e:
                log.warning("[%s] ETL handler error batch #%d: %s", self.name, self._batch_count, e)
                self._result.errors.append(f"ETL batch#{self._batch_count}: {e}")

        # Nhánh 2: Raw Handler (append .jsonl)
        if self.raw_handler:
            try:
                raw_count = self.raw_handler(self.name, batch)
                self._result.raw_rows += raw_count or 0
            except Exception as e:
                log.warning("[%s] Raw handler error batch #%d: %s", self.name, self._batch_count, e)
                self._result.errors.append(f"Raw batch#{self._batch_count}: {e}")

        # Nhánh 3: Risk Handler
        if self.risk_handler:
            try:
                risk_count = self.risk_handler(self.name, batch, self.build_risk_rules())
                self._result.risk_events += risk_count or 0
            except Exception as e:
                log.warning("[%s] Risk handler error batch #%d: %s", self.name, self._batch_count, e)
                self._result.errors.append(f"Risk batch#{self._batch_count}: {e}")

    def run(self) -> AgentResult:
        """Chạy agent: fetch tất cả report_keys, dispatch qua 3 nhánh.

        Returns:
            AgentResult với status, counts, errors.
        """
        self._result.started_at = datetime.now()
        self._result.status = "running"
        start_time = time.time()

        try:
            # Import tại đây để tránh circular import
            from core.onus_client import fetch_report_raw
            from config.report_map import REPORT_MAP

            for report_key in self.get_report_keys():
                log.info("[%s] Fetching %s ...", self.name, report_key)

                try:
                    records = fetch_report_raw(
                        report_key=report_key,
                        report_map=REPORT_MAP,
                        date_from=self.date_from,
                        date_to=self.date_to,
                        paginate=True,
                        order_by="date asc",
                        on_batch=self._dispatch,
                    )
                    log.info(
                        "[%s] %s done: %d records total",
                        self.name,
                        report_key,
                        len(records) if records else 0,
                    )

                except Exception as e:
                    log.error("[%s] Failed to fetch %s: %s", self.name, report_key, e)
                    self._result.errors.append(f"fetch {report_key}: {e}")

            self._result.status = "success"

        except Exception as e:
            log.error("[%s] Agent failed: %s", self.name, e)
            self._result.status = "failed"
            self._result.errors.append(str(e))

        finally:
            self._result.finished_at = datetime.now()
            self._result.duration_seconds = time.time() - start_time

        return self._result

    # =====================
    # Utility
    # =====================

    def get_temp_jsonl_path(self, date_str: Optional[str] = None) -> str:
        """Trả về path file .jsonl tạm cho agent này."""
        d = date_str or (self.date_from or datetime.now().strftime("%Y-%m-%d"))
        return os.path.join(self.temp_dir, f"{self.name}_{d}.jsonl")
