"""
datalake/schema_registry.py — Schema cố định cho Parquet.

Mỗi agent có schema riêng. Khi API thêm field mới → thêm vào schema,
file Parquet cũ tự NULL. DuckDB xử lý hoàn toàn ổn.
"""

from __future__ import annotations

from typing import Dict, List

import logging

log = logging.getLogger(__name__)

# ======== Schema definitions ========
# Mỗi field: (name, dtype)
# dtype: "str", "float", "int", "datetime"

SCHEMAS: Dict[str, List[tuple]] = {
    "onchain": [
        ("transactionNumber", "str"),
        ("date", "str"),
        ("amount", "float"),
        ("from_user_id", "str"),
        ("from_user_display", "str"),
        ("to_user_id", "str"),
        ("to_user_display", "str"),
        ("type_internalName", "str"),
    ],
    "pro": [
        ("transactionNumber", "str"),
        ("date", "str"),
        ("amount", "float"),
        ("from_user_id", "str"),
        ("from_user_display", "str"),
        ("to_user_id", "str"),
        ("to_user_display", "str"),
        ("type_internalName", "str"),
    ],
    "buysell": [
        ("transactionNumber", "str"),
        ("date", "str"),
        ("amount", "float"),
        ("from_user_id", "str"),
        ("from_user_display", "str"),
        ("to_user_id", "str"),
        ("to_user_display", "str"),
        ("type_internalName", "str"),
    ],
    "exchange": [
        ("transactionNumber", "str"),
        ("date", "str"),
        ("amount", "float"),
        ("from_user_id", "str"),
        ("from_user_display", "str"),
        ("to_user_id", "str"),
        ("to_user_display", "str"),
        ("type_internalName", "str"),
    ],
    "spot": [
        ("transactionNumber", "str"),
        ("date", "str"),
        ("amount", "float"),
        ("related_user_id", "str"),
        ("related_user_display", "str"),
        ("type_name", "str"),
        ("currency", "str"),
        ("description", "str"),
        ("authorizationStatus", "str"),
    ],
}

# Mapping dtype → pandas dtype
_DTYPE_MAP = {
    "str": "object",
    "float": "float64",
    "int": "Int64",  # nullable int
    "datetime": "object",
}


def _flatten_record(record: dict, prefix: str = "") -> dict:
    """
    Flatten nested dict: {"from": {"user": {"id": "123"}}} → {"from_user_id": "123"}
    """
    flat = {}
    for k, v in record.items():
        key = f"{prefix}_{k}" if prefix else k
        # Thay dấu . bằng _
        key = key.replace(".", "_")
        if isinstance(v, dict):
            flat.update(_flatten_record(v, key))
        else:
            flat[key] = v
    return flat


def apply_fixed_schema(df, agent_type: str):
    """
    Apply schema cố định cho DataFrame.
    
    - Flatten nested columns (from.user.id → from_user_id)
    - Field thiếu → NULL
    - Field thừa → giữ nguyên (không drop, để linh hoạt)
    - Cast dtype theo schema
    """
    import pandas as pd

    schema = SCHEMAS.get(agent_type)
    if not schema:
        log.warning(f"[schema_registry] No schema for agent_type={agent_type}")
        return df

    # Flatten nếu có nested columns
    if any("." in str(c) for c in df.columns):
        records = df.to_dict("records")
        flat_records = [_flatten_record(r) for r in records]
        df = pd.DataFrame(flat_records)

    # Đảm bảo tất cả schema columns tồn tại
    for col_name, col_dtype in schema:
        if col_name not in df.columns:
            df[col_name] = None

    # Cast dtype
    for col_name, col_dtype in schema:
        target = _DTYPE_MAP.get(col_dtype, "object")
        try:
            if col_dtype == "float":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            elif col_dtype == "int":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
            else:
                df[col_name] = df[col_name].astype(target)
        except Exception:
            pass  # giữ nguyên nếu cast fail

    return df


def get_schema_columns(agent_type: str) -> List[str]:
    """Trả về list tên cột cho agent."""
    schema = SCHEMAS.get(agent_type, [])
    return [col_name for col_name, _ in schema]
