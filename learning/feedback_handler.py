"""
learning/feedback_handler.py — "Tiếp nhận phản hồi" từ Vũ.

Xử lý /confirm, /reject từ Telegram → ghi user_context DB + log.

Flow:
    /confirm 42 FALSE_POSITIVE "Đối tác thanh toán hàng tuần"
        → parse → lookup risk_events id=42 → ghi user_context → reply

Commands:
    /confirm CASE_ID FALSE_POSITIVE "reason"
    /confirm CASE_ID TRUE_POSITIVE "reason"
    /reject CASE_ID "reason"                    → ghi UNDER_REVIEW
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

# Context valid 90 ngày (theo _common.md)
DEFAULT_VALID_DAYS = 90


@dataclass
class FeedbackResult:
    """Kết quả xử lý 1 feedback command."""
    success: bool = False
    message: str = ""
    userid: Optional[str] = None
    agent_type: Optional[str] = None
    risk_type: Optional[str] = None
    verdict: Optional[str] = None
    reason: Optional[str] = None
    case_id: Optional[int] = None


def parse_confirm(message: str) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Parse /confirm message.

    Formats:
        /confirm 42 FALSE_POSITIVE "reason here"
        /confirm 42 FALSE_POSITIVE reason here
        /confirm 42 TRUE_POSITIVE "reason"

    Returns:
        (case_id, verdict, reason) or (None, None, None)
    """
    message = message.strip()

    # /confirm CASE_ID VERDICT "reason" hoặc reason
    match = re.match(
        r'/confirm\s+(\d+)\s+(FALSE_POSITIVE|TRUE_POSITIVE|UNDER_REVIEW)\s+["\']?(.+?)["\']?\s*$',
        message,
        re.IGNORECASE,
    )
    if match:
        return int(match.group(1)), match.group(2).upper(), match.group(3)

    return None, None, None


def parse_reject(message: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Parse /reject message.

    Format:
        /reject 42 "reason"
        /reject 42 reason here

    Returns:
        (case_id, reason) or (None, None)
    """
    message = message.strip()

    match = re.match(
        r'/reject\s+(\d+)\s+["\']?(.+?)["\']?\s*$',
        message,
        re.IGNORECASE,
    )
    if match:
        return int(match.group(1)), match.group(2)

    return None, None


def handle_confirm(message: str, confirmed_by: str = "Vũ", db=None) -> FeedbackResult:
    """
    Xử lý /confirm command.

    Args:
        message: raw Telegram message
        confirmed_by: ai confirm (default "Vũ")
        db: DB instance

    Returns:
        FeedbackResult
    """
    case_id, verdict, reason = parse_confirm(message)

    if case_id is None:
        return FeedbackResult(
            success=False,
            message='❌ Format sai. Dùng: /confirm CASE_ID FALSE_POSITIVE "lý do"',
        )

    return _process_feedback(case_id, verdict, reason, confirmed_by, db)


def handle_reject(message: str, confirmed_by: str = "Vũ", db=None) -> FeedbackResult:
    """
    Xử lý /reject command → ghi UNDER_REVIEW.
    """
    case_id, reason = parse_reject(message)

    if case_id is None:
        return FeedbackResult(
            success=False,
            message='❌ Format sai. Dùng: /reject CASE_ID "lý do"',
        )

    return _process_feedback(case_id, "UNDER_REVIEW", reason, confirmed_by, db)


def _process_feedback(
    case_id: int,
    verdict: str,
    reason: str,
    confirmed_by: str,
    db=None,
) -> FeedbackResult:
    """Core logic: lookup case → ghi user_context → return result."""

    result = FeedbackResult(case_id=case_id, verdict=verdict, reason=reason)

    try:
        if db is None:
            from onuslibs.db import query as db_query, execute as db_execute
        else:
            db_query = db.query
            db_execute = db.execute

        # 1. Lookup risk_events
        rows = db_query(
            "SELECT userid, agent_type, risk_type, risk_score FROM risk_events WHERE id = %s",
            (case_id,),
        )

        if not rows:
            result.message = f"❌ Không tìm thấy case #{case_id}"
            return result

        row = rows[0]
        result.userid = row["userid"]
        result.agent_type = row["agent_type"]
        result.risk_type = row["risk_type"]

        # 2. Ghi user_context
        db_execute(
            """
            INSERT INTO user_context
                (userid, agent_type, risk_type, verdict, reason,
                 action_taken, confirmed_by, confirmed_at, valid_until)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(),
                    DATE_ADD(NOW(), INTERVAL %s DAY))
            """,
            (
                result.userid,
                result.agent_type,
                result.risk_type,
                verdict,
                reason,
                _action_from_verdict(verdict),
                confirmed_by,
                DEFAULT_VALID_DAYS,
            ),
        )

        # 3. Success message
        modifier = {
            "FALSE_POSITIVE": "giảm 80%",
            "TRUE_POSITIVE": "tăng 50%",
            "UNDER_REVIEW": "giữ nguyên (tạm ẩn alert)",
        }.get(verdict, "")

        result.success = True
        result.message = (
            f"✅ Đã ghi feedback cho case #{case_id}\n"
            f"👤 User: {result.userid}\n"
            f"🔍 {result.agent_type}/{result.risk_type} → {verdict}\n"
            f"📝 Lý do: {reason}\n"
            f"⚡ Score sẽ {modifier} cho user này ({DEFAULT_VALID_DAYS} ngày)"
        )

        log.info(
            "[feedback] case=%d userid=%s %s/%s → %s: %s",
            case_id, result.userid, result.agent_type, result.risk_type, verdict, reason,
        )

    except Exception as e:
        log.error("[feedback] Failed to process case %d: %s", case_id, e)
        result.message = f"❌ Lỗi xử lý: {e}"

    return result


def _action_from_verdict(verdict: str) -> str:
    """Map verdict → action_taken mô tả."""
    return {
        "FALSE_POSITIVE": "Bỏ qua — không phải rủi ro",
        "TRUE_POSITIVE": "Xác nhận rủi ro — theo dõi",
        "UNDER_REVIEW": "Đang điều tra — tạm ẩn alert",
    }.get(verdict, "Khác")
