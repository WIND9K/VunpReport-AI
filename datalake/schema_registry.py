"""
datalake/schema_registry.py — Enriched schema cho Parquet S3.

QUAN TRỌNG — Quy tắc userid theo direction (từ OnusReport report_etl_meta.py):
  Onchain/Pro:  SEND → userid = from.user.id | RECEIVE → userid = to.user.id
  BuySell:      BUY  → userid = to.user.id   | SELL    → userid = from.user.id
  Spot:         Luôn → userid = related.user.id

Field `userid` = ĐỐI TƯỢNG ĐANG ĐƯỢC PHÂN TÍCH RỦI RO.
Không cần from_user_id / to_user_id vì:
  userid + direction + agent_type → xác định được ai gửi ai nhận.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# ======== Enriched Schema ========

CORE_FIELDS = [
    ("transactionNumber", "str"),
    ("date", "str"),
    ("amount", "float"),
    ("userid", "str"),           # ĐỐI TƯỢNG PHÂN TÍCH (xác định theo direction)
    ("transfer_type", "str"),    # type.internalName gốc
    ("currency", "str"),         # VNDC / USDT
    ("direction", "str"),        # SEND / RECEIVE / BUY / SELL / DEPOSIT / WITHDRAW
    ("agent_type", "str"),       # onchain / pro / buysell / exchange / spot
]

EXTRA_FIELDS = {
    "onchain": [],
    "pro": [],
    "buysell": [
        ("source", "str"),       # SYSTEM / PARTNER
    ],
    "exchange": [],
    "spot": [
        ("coin", "str"),
        ("order_type", "str"),
    ],
}


def get_schema(agent_type: str) -> List[tuple]:
    extra = EXTRA_FIELDS.get(agent_type, [])
    return CORE_FIELDS + extra


def get_column_names(agent_type: str) -> List[str]:
    return [name for name, _ in get_schema(agent_type)]


# ======== Enrich Functions ========

def enrich_onchain(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    """
    Quy tắc userid (từ report_etl_meta):
      SEND    → userid = from.user.id
      RECEIVE → userid = to.user.id
    """
    kind = report_key.split("/")[1]
    currency = "VNDC" if kind.startswith("vndc") else "USDT"
    direction = "SEND" if "send" in kind else "RECEIVE"
    userid = _nested(record, "from.user.id") if direction == "SEND" else _nested(record, "to.user.id")

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "userid": userid,
        "transfer_type": _nested(record, "type.internalName"),
        "currency": currency,
        "direction": direction,
        "agent_type": "onchain",
    }


def enrich_pro(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    """Cùng logic userid như onchain."""
    kind = report_key.split("/")[1]
    currency = "VNDC" if kind.startswith("vndc") else "USDT"
    direction = "SEND" if "send" in kind else "RECEIVE"
    userid = _nested(record, "from.user.id") if direction == "SEND" else _nested(record, "to.user.id")

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "userid": userid,
        "transfer_type": _nested(record, "type.internalName"),
        "currency": currency,
        "direction": direction,
        "agent_type": "pro",
    }


def enrich_buysell(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    """
    Quy tắc userid (từ report_etl_meta):
      BUY  → userid = to.user.id
      SELL → userid = from.user.id
    """
    kind = report_key.split("/")[1]
    direction = "BUY" if kind.startswith("buy") else "SELL"
    source = "PARTNER" if kind.endswith("partner") else "SYSTEM"
    userid = _nested(record, "to.user.id") if direction == "BUY" else _nested(record, "from.user.id")

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "userid": userid,
        "transfer_type": _nested(record, "type.internalName"),
        "currency": "VNDC",
        "direction": direction,
        "agent_type": "buysell",
        "source": source,
    }


def enrich_exchange(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    """Exchange: userid = from.user.id (người gửi)."""
    kind = report_key.split("/")[1]
    currency = "VNDC" if kind == "vndcacc" else "USDT"

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "userid": _nested(record, "from.user.id"),
        "transfer_type": _nested(record, "type.internalName"),
        "currency": currency,
        "direction": "EXCHANGE",
        "agent_type": "exchange",
    }


def enrich_spot(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    """Spot: userid = related.user.id."""
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
        "userid": _nested(record, "related.user.id"),
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


def enrich_batch(agent_type, records, report_key):
    enricher = ENRICHERS.get(agent_type)
    if not enricher:
        log.warning("No enricher for agent_type=%s", agent_type)
        return []
    return [enricher(r, report_key) for r in records]


# ======== Apply Schema cho DataFrame ========

def apply_schema_to_df(df, agent_type: str):
    try:
        import pandas as pd
    except ImportError:
        return df

    schema = get_schema(agent_type)
    for col_name, col_dtype in schema:
        if col_name not in df.columns:
            df[col_name] = None
    for col_name, col_dtype in schema:
        try:
            if col_dtype == "float":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        except Exception:
            pass

    schema_cols = [c for c, _ in schema]
    extra_cols = [c for c in df.columns if c not in schema_cols]
    return df[schema_cols + extra_cols]


# ======== Helpers ========

def _nested(record: dict, path: str) -> Any:
    cur = record
    for key in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        if cur is None:
            return None
    return cur


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(str(val).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None
