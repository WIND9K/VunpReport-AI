"""
parquet_writer.py — Ghi enriched data vào temp .jsonl, convert Parquet, upload S3.

Dữ liệu lưu S3 là ENRICHED (đã enrich từ raw JSON, chỉ giữ fields cần cho risk).
Mỗi agent có enricher riêng trong schema_registry.py.

Workflow:
1. Agents chạy: fetch API → enrich → append_batch() ghi .jsonl
2. Agents xong: finalize_day() convert .jsonl → Parquet → upload S3
3. Cleanup temp files

S3 path: s3://{bucket}/enriched/{agent}/year=YYYY/month=MM/day=DD/data.parquet
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


class ParquetWriter:
    """Quản lý ghi enriched data → .jsonl → Parquet → S3."""

    def __init__(
        self,
        temp_dir: str = "/tmp",
        s3_bucket: str = "onus-datalake",
        date_str: Optional[str] = None,
    ):
        self.temp_dir = temp_dir
        self.s3_bucket = s3_bucket
        self.today = date_str or datetime.now().strftime("%Y-%m-%d")
        os.makedirs(self.temp_dir, exist_ok=True)

    def _jsonl_path(self, agent_type: str) -> str:
        return os.path.join(self.temp_dir, f"{agent_type}_{self.today}.jsonl")

    def _parquet_path(self, agent_type: str) -> str:
        return os.path.join(self.temp_dir, f"{agent_type}_{self.today}.parquet")

    # ====================
    # Append enriched data
    # ====================

    def append_batch(self, agent_type: str, batch: List[Dict[str, Any]]) -> int:
        """Append enriched records vào .jsonl file.

        Args:
            agent_type: "onchain", "buysell", "exchange", "pro", "spot"
            batch: list[dict] ENRICHED records (flat, core fields cho risk)
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

    # ====================
    # Finalize: .jsonl → Parquet → S3
    # ====================

    def _s3_key(self, agent_type: str) -> str:
        """Build S3 key: enriched/{agent}/year=YYYY/month=MM/day=DD/data.parquet"""
        d = datetime.strptime(self.today, "%Y-%m-%d")
        return (
            f"enriched/{agent_type}/"
            f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
            f"data.parquet"
        )

    def _upload_s3(self, local_path: str, s3_key: str, max_retries: int = 3) -> bool:
        """Upload file lên S3 với exponential backoff retry."""
        import time

        try:
            import boto3
        except ImportError:
            log.error("[ParquetWriter] boto3 not installed")
            return False

        s3 = boto3.client("s3")
        for attempt in range(max_retries):
            try:
                s3.upload_file(local_path, self.s3_bucket, s3_key)
                log.info("[ParquetWriter] Uploaded → s3://%s/%s", self.s3_bucket, s3_key)
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    log.warning("[ParquetWriter] S3 retry %d/%d: %s", attempt + 1, max_retries, e)
                    time.sleep(wait)
                else:
                    log.error("[ParquetWriter] S3 failed after %d retries: %s", max_retries, e)
                    return False
        return False

    def finalize_agent(self, agent_type: str) -> Dict[str, Any]:
        """Convert .jsonl → Parquet → upload S3 cho 1 agent."""
        jsonl_path = self._jsonl_path(agent_type)

        if not os.path.exists(jsonl_path):
            return {"agent": agent_type, "status": "no_data", "rows": 0}

        result = {"agent": agent_type, "status": "pending", "rows": 0}

        try:
            import pandas as pd
            from datalake.schema_registry import apply_schema_to_df

            df = pd.read_json(jsonl_path, lines=True)
            result["rows"] = len(df)

            if df.empty:
                os.remove(jsonl_path)
                result["status"] = "empty"
                return result

            df = apply_schema_to_df(df, agent_type)

            parquet_path = self._parquet_path(agent_type)
            df.to_parquet(parquet_path, compression="snappy", index=False)

            file_size = os.path.getsize(parquet_path)
            result["file_size_bytes"] = file_size
            result["file_size_mb"] = round(file_size / (1024 * 1024), 2)

            s3_key = self._s3_key(agent_type)
            result["s3_key"] = s3_key
            uploaded = self._upload_s3(parquet_path, s3_key)
            result["status"] = "success" if uploaded else "s3_failed"

            if uploaded:
                os.remove(jsonl_path)
                os.remove(parquet_path)
            else:
                log.warning("[ParquetWriter] Keeping temp files for %s", agent_type)

        except Exception as e:
            log.error("[ParquetWriter] finalize %s failed: %s", agent_type, e)
            result["status"] = "failed"
            result["error"] = str(e)

        return result

    def finalize_day(self) -> List[Dict[str, Any]]:
        """Convert + upload tất cả 5 agents."""
        agents = ["onchain", "buysell", "exchange", "pro", "spot"]
        return [self.finalize_agent(a) for a in agents]
