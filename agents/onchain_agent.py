"""
agents/onchain_agent.py — Onchain Agent cho onusreport-ai v5.0.

Phát hiện: structuring, velocity, off-hours, new+large
Nguồn: 4 sub-reports (vndc_send/receive, usdt_send/receive)
Pipeline: fetch → enrich → Parquet S3 → Risk scoring (dùng enriched fields)

QUAN TRỌNG: Risk rules nhận ENRICHED records (flat fields):
  from_user_id, to_user_id, amount, date, transfer_type, currency, direction, agent_type
  KHÔNG dùng raw JSON nested (from.user.id)
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Dict, List

from agents.base_agent import BaseAgent

log = logging.getLogger(__name__)


class OnchainAgent(BaseAgent):
    """
    Agent phân tích giao dịch onchain (VNDC + USDT, send + receive).

    Risk rules đọc từ risk_rules/onchain.md:
    - Structuring: ≥5 GD ~cùng mức trong <2h
    - Velocity: ≥10 GD/giờ → DuckDB so avg 30 ngày
    - Off-hours: 1AM-4AM + amount > avg lịch sử
    - New+Large: không có lịch sử 90 ngày + amount lớn
    """

    def __init__(self, **kwargs):
        super().__init__(name="onchain", **kwargs)

    def get_report_keys(self) -> List[str]:
        return [
            "onchain/vndc_send",
            "onchain/vndc_receive",
            "onchain/usdt_send",
            "onchain/usdt_receive",
        ]

    def get_etl_meta(self, report_key: str) -> Dict[str, Any]:
        """Build ETL meta — dùng enriched field names."""
        return {
            "projection": [
                {"name": "date_tx", "path": "date"},
                {"name": "userid", "path": "from_user_id"},
                {"name": "direction", "path": "direction"},
                {"name": "base_currency", "path": "currency"},
                {"name": "amount", "path": "amount"},
            ],
            "group_keys": ["date_tx", "userid", "base_currency", "direction"],
            "amount_field": "amount",
            "out_amount_field": "total_amount",
        }

    def get_diary_table(self, report_key: str) -> str:
        return "onchain_diary"

    def get_schema_name(self) -> str:
        return "onchain"

    def get_risk_rules(self) -> List[Callable]:
        """Trả về list risk rule functions cho onchain."""
        return [
            self._rule_structuring,
            self._rule_velocity,
            self._rule_off_hours,
            self._rule_new_large,
        ]

    # ========== Risk Rules — nhận ENRICHED records ==========

    def _rule_structuring(self, records: List[Dict]) -> List[Dict]:
        """
        Structuring: ≥5 GD ~cùng mức (±10%) trong <2h cho cùng user.
        Đọc enriched fields: from_user_id, amount, date
        """
        from collections import defaultdict
        from datetime import datetime, timedelta

        events = []
        settings = self._load_rule_settings("structuring")
        if not settings.get("enabled", True):
            return events

        min_tx = settings.get("min_transactions", 5)
        tolerance = settings.get("amount_tolerance_pct", 10) / 100
        window_hours = settings.get("time_window_hours", 2)
        base_score = settings.get("score", 7.5)

        # Group by userid — enriched field
        by_user = defaultdict(list)
        for r in records:
            uid = r.get("from_user_id") or r.get("to_user_id")
            if uid:
                by_user[uid].append(r)

        for userid, txns in by_user.items():
            if len(txns) < min_tx:
                continue

            txns_sorted = sorted(txns, key=lambda x: x.get("date", ""))

            for i in range(len(txns_sorted)):
                try:
                    t_start = datetime.fromisoformat(txns_sorted[i]["date"].replace("Z", "+00:00"))
                    amount_base = float(txns_sorted[i].get("amount", 0))
                except (ValueError, TypeError):
                    continue

                if amount_base <= 0:
                    continue

                cluster = [txns_sorted[i]]
                for j in range(i + 1, len(txns_sorted)):
                    try:
                        t_j = datetime.fromisoformat(txns_sorted[j]["date"].replace("Z", "+00:00"))
                        amt_j = float(txns_sorted[j].get("amount", 0))
                    except (ValueError, TypeError):
                        continue

                    if (t_j - t_start) > timedelta(hours=window_hours):
                        break

                    if abs(amt_j - amount_base) / amount_base <= tolerance:
                        cluster.append(txns_sorted[j])

                if len(cluster) >= min_tx:
                    events.append({
                        "userid": userid,
                        "agent_type": "onchain",
                        "risk_type": "structuring",
                        "base_score": base_score,
                        "evidence": {
                            "tx_count": len(cluster),
                            "amount_range": f"{amount_base:.0f} ±{tolerance*100}%",
                            "time_window": f"{window_hours}h",
                            "transactions": [c.get("transactionNumber") for c in cluster[:5]],
                        },
                    })
                    break  # 1 event per user per run

        return events

    def _rule_velocity(self, records: List[Dict]) -> List[Dict]:
        """
        Velocity: ≥10 GD/giờ cho cùng user → so với DuckDB avg 30 ngày.
        Đọc enriched fields: from_user_id, date
        """
        from collections import defaultdict, Counter
        from datetime import datetime

        events = []
        settings = self._load_rule_settings("velocity")
        if not settings.get("enabled", True):
            return events

        threshold_per_hour = settings.get("threshold_per_hour", 10)
        anomaly_threshold = settings.get("anomaly_x_threshold", 5.0)
        base_score = settings.get("score", 7.0)

        user_hourly = defaultdict(Counter)
        for r in records:
            uid = r.get("from_user_id") or r.get("to_user_id")
            try:
                dt = datetime.fromisoformat(r["date"].replace("Z", "+00:00"))
                hour_key = dt.strftime("%Y-%m-%d %H:00")
            except (ValueError, KeyError):
                continue
            if uid:
                user_hourly[uid][hour_key] += 1

        for userid, hours in user_hourly.items():
            max_hour = max(hours.values()) if hours else 0
            if max_hour < threshold_per_hour:
                continue

            # DuckDB baseline
            anomaly_x = 0
            avg_daily = 0
            try:
                from datalake.duckdb_reader import DuckDBReader
                reader = DuckDBReader()
                stats = reader.get_user_avg_stats("onchain", userid, days=30)
                avg_daily = stats.get("avg_daily_tx", 0)
                today_count = sum(hours.values())
                if avg_daily > 0:
                    anomaly_x = round(today_count / avg_daily, 2)
                reader.close()
            except Exception as e:
                log.warning("[velocity] DuckDB failed for %s: %s", userid, e)

            if anomaly_x >= anomaly_threshold or max_hour >= threshold_per_hour * 2:
                events.append({
                    "userid": userid,
                    "agent_type": "onchain",
                    "risk_type": "velocity",
                    "base_score": base_score,
                    "anomaly_x": anomaly_x,
                    "evidence": {
                        "max_txn_per_hour": max_hour,
                        "total_today": sum(hours.values()),
                        "anomaly_x": anomaly_x,
                        "baseline_avg_daily": avg_daily,
                    },
                })

        return events

    def _rule_off_hours(self, records: List[Dict]) -> List[Dict]:
        """
        Off-hours: GD trong 1AM-4AM (UTC+7) + amount > 2× avg lịch sử.
        Đọc enriched fields: from_user_id, date, amount
        """
        from collections import defaultdict
        from datetime import datetime, timezone, timedelta

        events = []
        settings = self._load_rule_settings("off_hours")
        if not settings.get("enabled", True):
            return events

        time_window = settings.get("time_window", "01:00-04:00")
        min_amount_vs_avg = settings.get("min_amount_vs_avg", 2.0)
        base_score = settings.get("score", 6.5)

        start_h, end_h = [int(t.split(":")[0]) for t in time_window.split("-")]
        tz_vn = timezone(timedelta(hours=7))

        off_hour_txns = defaultdict(list)
        for r in records:
            uid = r.get("from_user_id") or r.get("to_user_id")
            try:
                dt = datetime.fromisoformat(r["date"].replace("Z", "+00:00")).astimezone(tz_vn)
                if start_h <= dt.hour < end_h:
                    off_hour_txns[uid].append(r)
            except (ValueError, KeyError):
                continue

        for userid, txns in off_hour_txns.items():
            total_amount = sum(float(t.get("amount", 0)) for t in txns)

            avg = 0
            try:
                from datalake.duckdb_reader import DuckDBReader
                reader = DuckDBReader()
                stats = reader.get_user_avg_stats("onchain", userid, days=30)
                avg = stats.get("avg_daily_amount", 0)
                reader.close()
            except Exception:
                pass

            if avg > 0 and total_amount >= avg * min_amount_vs_avg:
                events.append({
                    "userid": userid,
                    "agent_type": "onchain",
                    "risk_type": "off_hours",
                    "base_score": base_score,
                    "evidence": {
                        "time_window": time_window,
                        "tx_count": len(txns),
                        "total_amount": total_amount,
                        "avg_30d": avg,
                        "ratio": round(total_amount / avg, 2) if avg else "N/A",
                    },
                })

        return events

    def _rule_new_large(self, records: List[Dict]) -> List[Dict]:
        """
        New+Large: user không có lịch sử 90 ngày + amount ≥ 500M VND.
        Đọc enriched fields: from_user_id, amount
        """
        from collections import defaultdict

        events = []
        settings = self._load_rule_settings("new_large")
        if not settings.get("enabled", True):
            return events

        no_history_days = settings.get("no_history_days", 90)
        min_amount = settings.get("min_amount_vndc", 500_000_000)
        base_score = settings.get("score", 8.0)

        by_user = defaultdict(float)
        for r in records:
            uid = r.get("from_user_id") or r.get("to_user_id")
            if uid:
                by_user[uid] += float(r.get("amount", 0))

        for userid, total in by_user.items():
            if total < min_amount:
                continue

            has_history = False
            try:
                from datalake.duckdb_reader import DuckDBReader
                reader = DuckDBReader()
                stats = reader.get_user_avg_stats("onchain", userid, days=no_history_days)
                has_history = stats.get("has_history", False)
                reader.close()
            except Exception:
                pass

            if not has_history:
                events.append({
                    "userid": userid,
                    "agent_type": "onchain",
                    "risk_type": "new_large",
                    "base_score": base_score,
                    "evidence": {
                        "total_amount": total,
                        "threshold": min_amount,
                        "history_days_checked": no_history_days,
                        "has_history": False,
                    },
                })

        return events

    # ========== Helpers ==========

    def _load_rule_settings(self, rule_name: str) -> Dict[str, Any]:
        """Load settings cho 1 rule từ settings.yaml."""
        try:
            import yaml
            settings_path = os.path.join(
                os.path.dirname(__file__), "..", "config", "settings.yaml"
            )
            with open(settings_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            return cfg.get("risk_rules", {}).get(rule_name, {})
        except Exception:
            return {}
