"""
duckdb_reader.py — Query lịch sử S3 Parquet (enriched) bằng DuckDB in-memory.

Dùng trong Risk Handler để so sánh hôm nay vs baseline 30/90 ngày.
DuckDB đọc thẳng S3 Parquet → in-memory → cực nhanh, không cần download.

Enriched schema dùng fields chuẩn: from_user_id, date, amount, direction, etc.

Ví dụ:
    reader = DuckDBReader(s3_bucket="onus-datalake")
    history = reader.get_user_history("onchain", userid="12345", days=30)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


class DuckDBReader:
    """Query S3 Parquet enriched data cho Risk Handler."""

    def __init__(
        self,
        s3_bucket: str = "onus-datalake",
        s3_region: str = "ap-southeast-1",
    ):
        self.s3_bucket = s3_bucket
        self.s3_region = s3_region
        self._conn = None

    def _get_conn(self):
        """Lazy init DuckDB connection với S3 credentials."""
        if self._conn is not None:
            return self._conn

        import duckdb
        import os

        self._conn = duckdb.connect(":memory:")
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")
        self._conn.execute(f"SET s3_region = '{self.s3_region}';")

        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        if aws_key and aws_secret:
            self._conn.execute(f"SET s3_access_key_id = '{aws_key}';")
            self._conn.execute(f"SET s3_secret_access_key = '{aws_secret}';")

        return self._conn

    def _parquet_glob(self, agent_type: str) -> str:
        """S3 parquet glob path — enriched data."""
        return f"s3://{self.s3_bucket}/enriched/{agent_type}/*/*/*/*/data.parquet"

    # ====================
    # Query methods
    # ====================

    def get_user_history(
        self,
        agent_type: str,
        userid: str,
        days: int = 30,
    ) -> List[Dict[str, Any]]:
        """Lấy lịch sử giao dịch user theo ngày trong N ngày gần nhất.

        Enriched schema dùng from_user_id làm userid chính.

        Returns:
            list[dict]: day, tx_count, total, avg_amount
        """
        conn = self._get_conn()
        glob = self._parquet_glob(agent_type)

        try:
            result = conn.execute(f"""
                SELECT date_trunc('day', CAST(date AS TIMESTAMP)) AS day,
                       COUNT(*) AS tx_count,
                       SUM(amount) AS total,
                       AVG(amount) AS avg_amount
                FROM read_parquet('{glob}', hive_partitioning=true)
                WHERE from_user_id = ?
                  AND CAST(date AS TIMESTAMP) >= current_date - INTERVAL ? DAY
                GROUP BY 1
                ORDER BY 1
            """, [str(userid), days]).fetchall()

            columns = ["day", "tx_count", "total", "avg_amount"]
            return [dict(zip(columns, row)) for row in result]

        except Exception as e:
            log.warning("[DuckDB] get_user_history failed for %s/%s: %s", agent_type, userid, e)
            return []

    def get_user_avg_stats(
        self,
        agent_type: str,
        userid: str,
        days: int = 30,
    ) -> Dict[str, Any]:
        """Thống kê trung bình user: avg tx/ngày, avg amount/ngày.

        Dùng để tính anomaly_x = today_count / avg_daily_count.
        """
        history = self.get_user_history(agent_type, userid, days)

        if not history:
            return {
                "avg_daily_tx": 0,
                "avg_daily_amount": 0,
                "total_days": 0,
                "total_tx": 0,
                "has_history": False,
            }

        total_tx = sum(h["tx_count"] for h in history)
        total_days = len(history)
        total_amount = sum(h["total"] or 0 for h in history)

        return {
            "avg_daily_tx": round(total_tx / total_days, 2) if total_days > 0 else 0,
            "avg_daily_amount": round(total_amount / total_days, 2) if total_days > 0 else 0,
            "total_days": total_days,
            "total_tx": total_tx,
            "has_history": True,
        }

    def calculate_anomaly_x(
        self,
        agent_type: str,
        userid: str,
        today_count: int,
        days: int = 30,
    ) -> float:
        """Tính anomaly_x = today_count / avg_daily_count.

        Returns:
            anomaly_x (float). 0.0 nếu không có lịch sử.
        """
        stats = self.get_user_avg_stats(agent_type, userid, days)
        if not stats["has_history"] or stats["avg_daily_tx"] == 0:
            return 0.0
        return round(today_count / stats["avg_daily_tx"], 2)

    def close(self):
        """Đóng DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
