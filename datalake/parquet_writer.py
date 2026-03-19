"""
parquet_writer.py — Ghi raw data vào temp .jsonl, convert Parquet, upload S3.

Workflow:
1. Trong lúc agents chạy (3-5 AM): append_batch() ghi từng batch vào .jsonl
2. Sau khi 5 agents xong (~5 AM): finalize_day() convert .jsonl → Parquet → upload S3

Tại sao không stream thẳng S3:
- Stream → hàng nghìn file nhỏ → phân mảnh → DuckDB scan chậm + PUT request tốn tiền
- Ghi temp local → 1 file Parquet/agent/ngày → sạch, hiệu quả
- Nếu crash: temp file vẫn còn, không mất data
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# Lazy imports — chỉ import khi cần (agents chạy trên server có thể chưa cài)
_pd = None
_pa = None
_pq = None


def _ensure_pandas():
    global _pd
    if _pd is None:
        import pandas as _pd  # type: ignore
    return _pd


def _ensure_pyarrow():
    global _pa, _pq
    if _pa is None:
        import pyarrow as _pa  # type: ignore
        import pyarrow.parquet as _pq  # type: ignore
    return _pa, _pq


class ParquetWriter:
    """Quản lý ghi raw data → .jsonl → Parquet → S3."""

    def __init__(
        self,
        temp_dir: str = "/tmp",
        s3_bucket: str = "onus-datalake",
        date_str: Optional[str] = None,
    ):
        self.temp_dir = temp_dir
        self.s3_bucket = s3_bucket
        self.today = date_str or datetime.now().strftime("%Y-%m-%d")

        # Đảm bảo temp_dir tồn tại
        os.makedirs(self.temp_dir, exist_ok=True)

    def _jsonl_path(self, agent_type: str) -> str:
        return os.path.join(self.temp_dir, f"{agent_type}_{self.today}.jsonl")

    def _parquet_path(self, agent_type: str) -> str:
        return os.path.join(self.temp_dir, f"{agent_type}_{self.today}.parquet")

    # ====================
    # Nhánh 2: Raw Handler
    # ====================

    def append_batch(self, agent_type: str, batch: List[Dict[str, Any]]) -> int:
        """Append 1 batch vào .jsonl file (gọi từ on_batch callback).

        Args:
            agent_type: "onchain", "buysell", "exchange", "pro", "spot"
            batch: list[dict] raw records từ API

        Returns:
            Số records đã ghi
        """
        if not batch:
            return 0

        path = self._jsonl_path(agent_type)
        try:
            with open(path, "a", encoding="utf-8") as f:
                for record in batch:
                    f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            return len(batch)
        except Exception as e:
            log.error("[ParquetWriter] Failed to append %s: %s", agent_type, e)
            return 0

    def as_raw_handler(self):
        """Trả về callable phù hợp với BaseAgent.raw_handler signature.

        Signature: (agent_name: str, batch: list[dict]) -> int
        """
        def handler(agent_name: str, batch: List[Dict[str, Any]]) -> int:
            return self.append_batch(agent_name, batch)
        return handler

    # ====================
    # Finalize: .jsonl → Parquet → S3
    # ====================

    def _apply_fixed_schema(self, df, agent_type: str):
        """Áp dụng schema cố định: field thiếu → NULL.

        Đọc schema từ config/schemas/{agent_type}.json.
        Nếu không có schema file → giữ nguyên df.
        """
        schema_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "config", "schemas", f"{agent_type}.json",
        )
        if not os.path.exists(schema_path):
            log.debug("[ParquetWriter] No schema file for %s, using raw columns", agent_type)
            return df

        try:
            with open(schema_path, "r") as f:
                schema = json.load(f)

            expected_cols = schema.get("columns", [])
            if not expected_cols:
                return df

            pd = _ensure_pandas()
            for col in expected_cols:
                col_name = col["name"]
                if col_name not in df.columns:
                    df[col_name] = None  # Field thiếu → NULL

            # Giữ thứ tự cột theo schema + cột extra từ API
            schema_cols = [c["name"] for c in expected_cols]
            extra_cols = [c for c in df.columns if c not in schema_cols]
            df = df[schema_cols + extra_cols]

            return df

        except Exception as e:
            log.warning("[ParquetWriter] Schema apply failed for %s: %s", agent_type, e)
            return df

    def _s3_key(self, agent_type: str) -> str:
        """Build S3 key: raw/{agent}/year=YYYY/month=MM/day=DD/data.parquet"""
        d = datetime.strptime(self.today, "%Y-%m-%d")
        return (
            f"raw/{agent_type}/"
            f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
            f"data.parquet"
        )

    def _upload_s3(self, local_path: str, s3_key: str, max_retries: int = 3) -> bool:
        """Upload file lên S3 với exponential backoff retry.

        Returns:
            True nếu upload thành công
        """
        import time

        try:
            import boto3
        except ImportError:
            log.error("[ParquetWriter] boto3 not installed, cannot upload to S3")
            return False

        s3 = boto3.client("s3")

        for attempt in range(max_retries):
            try:
                s3.upload_file(local_path, self.s3_bucket, s3_key)
                log.info("[ParquetWriter] Uploaded %s → s3://%s/%s", local_path, self.s3_bucket, s3_key)
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    log.warning(
                        "[ParquetWriter] S3 upload attempt %d/%d failed: %s. Retry in %ds...",
                        attempt + 1, max_retries, e, wait,
                    )
                    time.sleep(wait)
                else:
                    log.error(
                        "[ParquetWriter] S3 upload failed after %d attempts: %s",
                        max_retries, e,
                    )
                    return False

        return False

    def finalize_agent(self, agent_type: str) -> Dict[str, Any]:
        """Convert .jsonl → Parquet → upload S3 cho 1 agent.

        Returns:
            dict với status, rows, file_size, s3_key
        """
        jsonl_path = self._jsonl_path(agent_type)

        if not os.path.exists(jsonl_path):
            return {"agent": agent_type, "status": "no_data", "rows": 0}

        result = {"agent": agent_type, "status": "pending", "rows": 0}

        try:
            pd = _ensure_pandas()
            df = pd.read_json(jsonl_path, lines=True)
            result["rows"] = len(df)

            if df.empty:
                os.remove(jsonl_path)
                result["status"] = "empty"
                return result

            # Apply fixed schema
            df = self._apply_fixed_schema(df, agent_type)

            # Convert → Parquet
            parquet_path = self._parquet_path(agent_type)
            df.to_parquet(parquet_path, compression="snappy", index=False)

            file_size = os.path.getsize(parquet_path)
            result["file_size_bytes"] = file_size
            result["file_size_mb"] = round(file_size / (1024 * 1024), 2)

            # Upload S3
            s3_key = self._s3_key(agent_type)
            result["s3_key"] = s3_key

            uploaded = self._upload_s3(parquet_path, s3_key)
            result["status"] = "success" if uploaded else "s3_failed"

            # Cleanup temp files
            if uploaded:
                os.remove(jsonl_path)
                os.remove(parquet_path)
                log.info("[ParquetWriter] Cleaned up temp files for %s", agent_type)
            else:
                log.warning(
                    "[ParquetWriter] Keeping temp files for %s (S3 upload failed)",
                    agent_type,
                )

        except Exception as e:
            log.error("[ParquetWriter] finalize_agent %s failed: %s", agent_type, e)
            result["status"] = "failed"
            result["error"] = str(e)

        return result

    def finalize_day(self) -> List[Dict[str, Any]]:
        """Convert + upload tất cả 5 agents. Gọi sau khi agents xong (~5 AM).

        Returns:
            list[dict] kết quả từng agent
        """
        agents = ["onchain", "buysell", "exchange", "pro", "spot"]
        results = []

        for agent_type in agents:
            r = self.finalize_agent(agent_type)
            results.append(r)
            log.info("[ParquetWriter] %s: %s (%d rows)", agent_type, r["status"], r["rows"])

        return results
