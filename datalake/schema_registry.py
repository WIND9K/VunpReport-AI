"""
datalake/schema_registry.py — Enriched schema cho Parquet S3.

Enriched schema chỉ giữ fields CẦN CHO RISK ANALYSIS.
Không lưu thông tin dư thừa (transfer_type, from_user_id, to_user_id).

Onchain 7 fields:
  transactionNumber — trace GD cụ thể
  date              — timestamp (velocity, off-hours)
  amount            — số tiền (structuring, threshold)
  userid            — ĐỐI TƯỢNG phân tích (xác định theo direction)
  currency          — từ API currency.internalName (VNDC / USDT)
  direction         — parse từ API type.internalName (SEND / RECEIVE / etc.)
  agent_type        — onchain / pro / buysell / exchange / spot

Quy tắc userid (từ OnusReport report_etl_meta.py):
  Onchain/Pro:  SEND → from.user.id | RECEIVE → to.user.id
  BuySell:      BUY  → to.user.id   | SELL    → from.user.id
  Spot:         Luôn → related.user.id
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
    ("userid", "str"),           # ĐỐI TƯỢNG PHÂN TÍCH
    ("currency", "str"),         # currency.internalName từ API
    ("direction", "str"),        # parse từ type.internalName
    ("agent_type", "str"),       # onchain / pro / buysell / exchange / spot
]

EXTRA_FIELDS = {
    "onchain": [],
    "pro": [],
    "buysell": [
        ("source", "str"),       # SYSTEM / PARTNER (parse từ type.internalName)
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


# ======== Direction parser ========

def _parse_direction(transfer_type: str) -> str:
    """Parse direction từ type.internalName.

    vndcacc.vndc_onchain_send       → SEND
    usdtacc.usdt_onchain_receive    → RECEIVE
    vndcacc.buy_via_system          → BUY
    vndcacc.sell_via_agency         → SELL
    vndcacc.exchange                → EXCHANGE
    """
    t = (transfer_type or "").lower()
    if "send" in t:
        return "SEND"
    if "receive" in t:
        return "RECEIVE"
    if "buy" in t:
        return "BUY"
    if "sell" in t:
        return "SELL"
    if "exchange" in t:
        return "EXCHANGE"
    return "OTHER"


# ======== Enrich Functions ========

def enrich_onchain(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    transfer_type = _nested(record, "type.internalName") or ""
    currency = (_nested(record, "currency.internalName") or "").upper()
    direction = _parse_direction(transfer_type)
    userid = _nested(record, "from.user.id") if direction == "SEND" else _nested(record, "to.user.id")

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "userid": userid,
        "currency": currency,
        "direction": direction,
        "agent_type": "onchain",
    }


def enrich_pro(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    transfer_type = _nested(record, "type.internalName") or ""
    currency = (_nested(record, "currency.internalName") or "").upper()
    direction = _parse_direction(transfer_type)
    userid = _nested(record, "from.user.id") if direction == "SEND" else _nested(record, "to.user.id")

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "userid": userid,
        "currency": currency,
        "direction": direction,
        "agent_type": "pro",
    }


def enrich_buysell(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    transfer_type = _nested(record, "type.internalName") or ""
    currency = (_nested(record, "currency.internalName") or "").upper()
    direction = _parse_direction(transfer_type)
    userid = _nested(record, "to.user.id") if direction == "BUY" else _nested(record, "from.user.id")

    t_lower = transfer_type.lower()
    source = "PARTNER" if "agency" in t_lower else "SYSTEM"

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "userid": userid,
        "currency": currency,
        "direction": direction,
        "agent_type": "buysell",
        "source": source,
    }


def enrich_exchange(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
    transfer_type = _nested(record, "type.internalName") or ""
    currency = (_nested(record, "currency.internalName") or "").upper()

    return {
        "transactionNumber": record.get("transactionNumber"),
        "date": record.get("date"),
        "amount": _safe_float(record.get("amount")),
        "userid": _nested(record, "from.user.id"),
        "currency": currency,
        "direction": "EXCHANGE",
        "agent_type": "exchange",
    }


def enrich_spot(record: Dict[str, Any], report_key: str) -> Dict[str, Any]:
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
        "currency": (record.get("currency") or "").upper(),
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
