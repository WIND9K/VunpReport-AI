"""
duckdb_reader.py — Query lịch sử S3 Parquet bằng DuckDB in-memory.

Dùng trong Risk Handler để so sánh hôm nay vs baseline 30/90 ngày.
DuckDB đọc thẳng S3 Parquet → in-memory → cực nhanh, không cần download.

Ví dụ:
    reader = DuckDBReader(s3_bucket="onus-datalake")
    history = reader.get_user_history("onchain", userid="12345", days=30)
    # → DataFrame: day | tx_count | total | avg_amount
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

_duckdb = None


def _ensure_duckdb():
    global _duckdb
    if _duckdb is None:
        import duckdb as _duckdb  # type: ignore
    return _duckdb


class DuckDBReader:
    """Query S3 Parquet lịch sử cho Risk Handler."""

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

        duckdb = _ensure_duckdb()
        self._conn = duckdb.connect(":memory:")

        # Cài httpfs extension cho S3 access
        self._conn.execute("INSTALL httpfs;")
        self._conn.execute("LOAD httpfs;")
        self._conn.execute(f"SET s3_region = '{self.s3_region}';")

        # AWS credentials từ environment (IAM role hoặc .env)
        import os
        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        if aws_key and aws_secret:
            self._conn.execute(f"SET s3_access_key_id = '{aws_key}';")
            self._conn.execute(f"SET s3_secret_access_key = '{aws_secret}';")

        return self._conn

    def _parquet_glob(self, agent_type: str) -> str:
        """S3 parquet glob path cho agent."""
        return f"s3://{self.s3_bucket}/raw/{agent_type}/*/*/*/*/data.parquet"

    # ====================
    # Query methods
    # ====================

    def get_user_history(
        self,
        agent_type: str,
        userid: str,
        days: int = 30,
        userid_field: str = "userid",
        timestamp_field: str = "date",
        amount_field: str = "amount",
    ) -> List[Dict[str, Any]]:
        """Lấy lịch sử giao dịch user theo ngày trong N ngày gần nhất.

        Returns:
            list[dict] với columns: day, tx_count, total, avg_amount
        """
        conn = self._get_conn()
        glob = self._parquet_glob(agent_type)

        try:
            result = conn.execute(f"""
                SELECT date_trunc('day', CAST("{timestamp_field}" AS TIMESTAMP)) AS day,
                       COUNT(*)    AS tx_count,
                       SUM(CAST("{amount_field}" AS DOUBLE))  AS total,
                       AVG(CAST("{amount_field}" AS DOUBLE))  AS avg_amount
                FROM read_parquet('{glob}')
                WHERE CAST("{userid_field}" AS VARCHAR) = ?
                  AND CAST("{timestamp_field}" AS TIMESTAMP) >= current_date - INTERVAL ? DAY
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
        userid_field: str = "userid",
        timestamp_field: str = "date",
        amount_field: str = "amount",
    ) -> Dict[str, Any]:
        """Lấy thống kê trung bình user: avg tx/ngày, avg amount/ngày.

        Dùng để tính anomaly_x = today_count / avg_daily_count.

        Returns:
            dict: avg_daily_tx, avg_daily_amount, total_days, total_tx
        """
        history = self.get_user_history(
            agent_type, userid, days,
            userid_field=userid_field,
            timestamp_field=timestamp_field,
            amount_field=amount_field,
        )

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
            "avg_daily_tx": total_tx / total_days if total_days > 0 else 0,
            "avg_daily_amount": total_amount / total_days if total_days > 0 else 0,
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
        **kwargs,
    ) -> float:
        """Tính anomaly_x = today_count / avg_daily_count.

        Returns:
            anomaly_x (float). 0.0 nếu không có lịch sử.
        """
        stats = self.get_user_avg_stats(agent_type, userid, days, **kwargs)

        if not stats["has_history"] or stats["avg_daily_tx"] == 0:
            return 0.0

        return round(today_count / stats["avg_daily_tx"], 2)

    def close(self):
        """Đóng DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
