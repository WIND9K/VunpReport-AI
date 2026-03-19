"""
risk/context_checker.py — "Bộ nhớ" của hệ thống.

Kiểm tra user đã có feedback (FALSE_POSITIVE / TRUE_POSITIVE) trước khi chấm điểm.
Đọc từ bảng user_context trong Aurora MySQL.

Flow:
    userid + risk_type → query user_context → trả modifier
    - FALSE_POSITIVE → 0.2 (giảm 80%)
    - TRUE_POSITIVE  → 1.5 (tăng 50%)
    - UNDER_REVIEW   → 1.0 (giữ nguyên, nhưng flag suppress_alert)
    - Không có        → 1.0 (bình thường)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)


@dataclass
class ContextResult:
    """Kết quả kiểm tra context cho 1 userid + risk_type."""
    has_context: bool = False
    verdict: Optional[str] = None        # FALSE_POSITIVE / TRUE_POSITIVE / UNDER_REVIEW
    reason: Optional[str] = None         # Lý do Vũ ghi
    modifier: float = 1.0                # Nhân với base_score
    suppress_alert: bool = False         # True nếu UNDER_REVIEW → không alert
    valid_until: Optional[str] = None    # Ngày hết hạn context


# Modifier theo verdict — đọc từ _common.md
VERDICT_MODIFIERS = {
    "FALSE_POSITIVE": 0.2,
    "TRUE_POSITIVE": 1.5,
    "UNDER_REVIEW": 1.0,
}


def check_context(
    userid: str,
    agent_type: str,
    risk_type: str,
    db=None,
) -> ContextResult:
    """
    Kiểm tra user_context cho 1 userid.

    Args:
        userid: ID user cần check
        agent_type: "onchain", "pro", "buysell", "exchange", "spot"
        risk_type: "structuring", "velocity", "off_hours", etc.
        db: DB instance (OnusLibs). None → tạo mới từ settings.

    Returns:
        ContextResult với modifier và thông tin context
    """
    result = ContextResult()

    try:
        if db is None:
            from onuslibs.db import query as db_query
        else:
            db_query = db.query

        rows = db_query(
            """
            SELECT verdict, reason, valid_until
            FROM user_context
            WHERE userid = %s
              AND agent_type = %s
              AND risk_type = %s
              AND valid_until >= NOW()
            ORDER BY confirmed_at DESC
            LIMIT 1
            """,
            (userid, agent_type, risk_type),
        )

        if not rows:
            return result

        row = rows[0]
        verdict = row.get("verdict", "").upper()

        result.has_context = True
        result.verdict = verdict
        result.reason = row.get("reason")
        result.valid_until = str(row.get("valid_until", ""))
        result.modifier = VERDICT_MODIFIERS.get(verdict, 1.0)
        result.suppress_alert = verdict == "UNDER_REVIEW"

        log.info(
            "[context_checker] %s/%s/%s → %s (modifier=%.1f, reason=%s)",
            userid, agent_type, risk_type, verdict, result.modifier, result.reason,
        )

    except Exception as e:
        log.warning("[context_checker] Query failed for %s: %s", userid, e)
        # Nếu DB lỗi → trả default (không block scoring)

    return result


def check_context_batch(
    userids_risk: list[tuple[str, str, str]],
    db=None,
) -> Dict[str, ContextResult]:
    """
    Batch check context cho nhiều (userid, agent_type, risk_type).

    Args:
        userids_risk: list of (userid, agent_type, risk_type)

    Returns:
        dict key="userid:agent_type:risk_type" → ContextResult
    """
    results = {}
    for userid, agent_type, risk_type in userids_risk:
        key = f"{userid}:{agent_type}:{risk_type}"
        results[key] = check_context(userid, agent_type, risk_type, db)
    return results
