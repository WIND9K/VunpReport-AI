"""
datalake/schema_registry.py — Enriched schema cho Parquet S3.

Chỉ giữ fields cần cho risk analysis. Raw JSON từ API có nhiều nested fields thừa
(account type IDs, currency metadata, display names...) — bỏ hết.

Nguyên tắc:
- transactionNumber: trace GD cụ thể
- date: timestamp chính xác (velocity, off-hours)
- amount: so sánh structuring, threshold
- from_user_id / to_user_id: ai gửi, ai nhận
- transfer_type: type.internalName gốc — xác định kind + direction
- currency: VNDC/USDT
- direction: SEND/RECEIVE/BUY/SELL/DEPOSIT/WITHDRAW
- agent_type: onchain/pro/buysell/exchange/spot

Tương lai: thêm field mới → thêm vào schema, file cũ tự NULL.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# ======== Enriched Schema per Agent ========
# Tất cả agent dùng chung 9 core fields

CORE_FIELDS = [
    ("transactionNumber", "str"),
    ("date", "str"),
    ("amount", "float"),
    ("from_user_id", "str"),
    ("to_user_id", "str"),
    ("transfer_type", "str"),    # type.internalName gốc
    ("currency", "str"),          # VNDC / USDT
    ("direction", "str"),         # SEND / RECEIVE / BUY / SELL / DEPOSIT / WITHDRAW
    ("agent_type", "str"),        # onchain / pro / buysell / exchange / spot
]

# Agent-specific extra fields (nếu cần thêm sau này)
EXTRA_FIELDS = {
    "onchain": [],
    "pro": [],
    "buysell": [
        ("source", "str"),        # SYSTEM / PARTNER
    ],
    "exchange": [],
    "spot": [
        ("coin", "str"),          # BTC / ETH / ...
        ("order_type", "str"),    # type.name gốc (VNDC SPOT Deposit, etc.)
    ],
}


def get_schema(agent_type: str) -> List[tuple]:
    """Trả về schema (list of (name, dtype)) cho agent."""
    extra = EXTRA_FIELDS.get(agent_type, [])
    return CORE_FIELDS + extra


def get_column_names(agent_type: str) -> List[str]:
    """Trả về list tên cột."""
    return [name for name, _ in get_schema(agent_type)]


# ======== Enrich Functions — từ raw JSON → flat dict ========

def enrich_onchain(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    """Enrich 1 raw record onchain → flat dict 9 fields."""
    kind = report_key.split("/")[1]  # vndc_send, usdt_receive, etc.
    currency = "VNDC" if kind.startswith("vndc") else "USDT"
    direction = "SEND" if "send" in kind else "RECEIVE"

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "from_user_id": _nested(record, "from.user.id"),
        "to_user_id": _nested(record, "to.user.id"),
        "transfer_type": _nested(record, "type.internalName"),
        "currency": currency,
        "direction": direction,
        "agent_type": "onchain",
    }


def enrich_pro(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    """Enrich 1 raw record pro → flat dict 9 fields."""
    kind = report_key.split("/")[1]
    currency = "VNDC" if kind.startswith("vndc") else "USDT"
    direction = "SEND" if "send" in kind else "RECEIVE"

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "from_user_id": _nested(record, "from.user.id"),
        "to_user_id": _nested(record, "to.user.id"),
        "transfer_type": _nested(record, "type.internalName"),
        "currency": currency,
        "direction": direction,
        "agent_type": "pro",
    }


def enrich_buysell(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    """Enrich 1 raw record buysell → flat dict 10 fields (thêm source)."""
    kind = report_key.split("/")[1]  # buy_system, sell_partner, etc.
    direction = "BUY" if kind.startswith("buy") else "SELL"
    source = "PARTNER" if kind.endswith("partner") else "SYSTEM"

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "from_user_id": _nested(record, "from.user.id"),
        "to_user_id": _nested(record, "to.user.id"),
        "transfer_type": _nested(record, "type.internalName"),
        "currency": "VNDC",  # buysell luôn VNDC
        "direction": direction,
        "agent_type": "buysell",
        "source": source,
    }


def enrich_exchange(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    """Enrich 1 raw record exchange → flat dict 9 fields."""
    kind = report_key.split("/")[1]  # vndcacc, usdtacc
    currency = "VNDC" if kind == "vndcacc" else "USDT"

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "from_user_id": _nested(record, "from.user.id"),
        "to_user_id": _nested(record, "to.user.id"),
        "transfer_type": _nested(record, "type.internalName"),
        "currency": currency,
        "direction": "EXCHANGE",
        "agent_type": "exchange",
    }


def enrich_spot(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    """Enrich 1 raw record spot → flat dict 11 fields (thêm coin, order_type)."""
    type_name = _nested(record, "type.name") or ""
    if "Deposit" in type_name:
        direction = "DEPOSIT"
    elif "Withdraw" in type_name:
        direction = "WITHDRAW"
    else:
        direction = "OTHER"

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "from_user_id": _nested(record, "related.user.id"),  # spot dùng related
        "to_user_id": None,  # spot không có to
        "transfer_type": _nested(record, "type.internalName"),
        "currency": record.get("currency", ""),
        "direction": direction,
        "agent_type": "spot",
        "coin": record.get("currency", ""),
        "order_type": type_name,
    }


# ======== Dispatcher ========

ENRICHERS = {
    "onchain": enrich_onchain,
    "pro": enrich_pro,
    "buysell": enrich_buysell,
    "exchange": enrich_exchange,
    "spot": enrich_spot,
}


def enrich_batch(
    agent_type: str,
    records: List[Dict[str, Any]],
    report_key: str,
) -> List[Dict[str, Any]]:
    """Enrich toàn bộ batch raw records → list flat dicts."""
    enricher = ENRICHERS.get(agent_type)
    if not enricher:
        log.warning("No enricher for agent_type=%s, returning empty", agent_type)
        return []
    return [enricher(r, report_key) for r in records]


# ======== Apply Schema cho DataFrame (Parquet) ========

def apply_schema_to_df(df, agent_type: str):
    """Đảm bảo DataFrame có đúng columns theo schema. Field thiếu → NULL."""
    try:
        import pandas as pd
    except ImportError:
        return df

    schema = get_schema(agent_type)
    for col_name, col_dtype in schema:
        if col_name not in df.columns:
            df[col_name] = None

    # Cast types
    for col_name, col_dtype in schema:
        try:
            if col_dtype == "float":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            elif col_dtype == "int":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
        except Exception:
            pass

    # Sắp xếp cột theo schema trước, extra sau
    schema_cols = [c for c, _ in schema]
    extra_cols = [c for c in df.columns if c not in schema_cols]
    return df[schema_cols + extra_cols]


# ======== Helpers ========

def _nested(record: dict, path: str) -> Any:
    """Extract nested value: 'from.user.id' → record['from']['user']['id']"""
    cur = record
    for key in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        if cur is None:
            return None
    return cur


def _safe_float(val) -> Optional[float]:
    """Convert to float safely."""
    if val is None:
        return None
    try:
        return float(str(val).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None
