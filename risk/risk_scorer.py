"""
risk/risk_scorer.py — "Người chấm điểm" của hệ thống.

Nhận risk events từ agent → áp dụng context + anomaly_x → tính final score → ghi DB.

Flow:
    1. Agent rules trả về raw risk events (base_score)
    2. context_checker: user có feedback cũ? → modifier
    3. anomaly_x > 5 → +1.0, > 10 → +2.0
    4. final_score = min(base_score + anomaly_bonus, 10) × context_modifier
    5. Ghi risk_events vào Aurora MySQL

Quan trọng: Đây là PER-AGENT score. Orchestrator sẽ tổng hợp cross-agent sau.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from risk.context_checker import check_context, ContextResult

log = logging.getLogger(__name__)


def score_risk_events(
    raw_events: List[Dict[str, Any]],
    agent_type: str,
    event_date: Optional[str] = None,
    db=None,
) -> List[Dict[str, Any]]:
    """
    Nhận raw risk events từ agent rules → tính final score → ghi DB.

    Args:
        raw_events: list risk events từ agent (có base_score, userid, risk_type, evidence)
        agent_type: "onchain", "pro", etc.
        event_date: YYYY-MM-DD (default: today)
        db: DB instance (None → dùng onuslibs default)

    Returns:
        list scored events (thêm final_score, context_verdict, suppressed)
    """
    if not raw_events:
        return []

    event_date = event_date or datetime.now().strftime("%Y-%m-%d")
    scored_events = []

    for event in raw_events:
        scored = _score_one_event(event, agent_type, event_date, db)
        scored_events.append(scored)

        # Ghi vào DB (nếu score > 0 và không suppress)
        if scored["final_score"] > 0 and not scored.get("suppressed", False):
            _write_risk_event(scored, db)

    # Log summary
    total = len(scored_events)
    critical = sum(1 for e in scored_events if e["final_score"] >= 9.0)
    high = sum(1 for e in scored_events if 7.5 <= e["final_score"] < 9.0)
    suppressed = sum(1 for e in scored_events if e.get("suppressed", False))

    log.info(
        "[risk_scorer] %s: %d events scored (%d CRITICAL, %d HIGH, %d suppressed)",
        agent_type, total, critical, high, suppressed,
    )

    return scored_events


def _score_one_event(
    event: Dict[str, Any],
    agent_type: str,
    event_date: str,
    db=None,
) -> Dict[str, Any]:
    """Tính score cho 1 risk event."""

    userid = event.get("userid", "")
    risk_type = event.get("risk_type", "")
    base_score = float(event.get("risk_score", 0) or event.get("base_score", 0))
    anomaly_x = float(event.get("anomaly_x", 0))
    evidence = event.get("evidence", {})

    # 1. Context check — user đã có feedback?
    ctx = check_context(userid, agent_type, risk_type, db)

    # 2. Anomaly bonus (theo _common.md)
    anomaly_bonus = 0
    if anomaly_x > 10:
        anomaly_bonus = 2.0
    elif anomaly_x > 5:
        anomaly_bonus = 1.0

    # 3. Final score = min(base + bonus, 10) × modifier
    adjusted_score = min(base_score + anomaly_bonus, 10.0)
    final_score = round(adjusted_score * ctx.modifier, 2)
    final_score = min(final_score, 10.0)
    final_score = max(final_score, 0.0)

    # 4. Build scored event
    scored = {
        "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "event_date": event_date,
        "userid": userid,
        "agent_type": agent_type,
        "risk_type": risk_type,
        "base_score": base_score,
        "anomaly_x": anomaly_x,
        "anomaly_bonus": anomaly_bonus,
        "context_verdict": ctx.verdict,
        "context_modifier": ctx.modifier,
        "final_score": final_score,
        "evidence": evidence,
        "suppressed": ctx.suppress_alert,  # UNDER_REVIEW → không alert
    }

    if ctx.has_context:
        log.info(
            "[risk_scorer] %s %s: base=%.1f anomaly_x=%.1f +bonus=%.1f ×modifier=%.1f → final=%.2f (%s: %s)",
            userid, risk_type, base_score, anomaly_x, anomaly_bonus,
            ctx.modifier, final_score, ctx.verdict, ctx.reason,
        )

    return scored


def _write_risk_event(scored: Dict[str, Any], db=None):
    """Ghi 1 scored risk event vào Aurora risk_events table."""
    import json

    try:
        if db is None:
            from onuslibs.db import execute as db_execute
        else:
            db_execute = db.execute

        evidence_json = json.dumps(scored.get("evidence", {}), ensure_ascii=False, default=str)

        db_execute(
            """
            INSERT INTO risk_events
                (detected_at, event_date, userid, agent_type, risk_type,
                 risk_score, evidence, anomaly_x)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                scored["detected_at"],
                scored["event_date"],
                scored["userid"],
                scored["agent_type"],
                scored["risk_type"],
                scored["final_score"],
                evidence_json,
                scored.get("anomaly_x", 0),
            ),
        )

    except Exception as e:
        log.warning("[risk_scorer] Failed to write risk_event: %s", e)


def get_today_events(
    agent_type: Optional[str] = None,
    event_date: Optional[str] = None,
    min_score: float = 0,
    db=None,
) -> List[Dict[str, Any]]:
    """
    Đọc risk_events hôm nay từ DB. Dùng bởi Orchestrator.

    Args:
        agent_type: filter theo agent (None = tất cả)
        event_date: YYYY-MM-DD (default: today)
        min_score: chỉ lấy events có score >= min_score

    Returns:
        list[dict] risk events
    """
    import json

    event_date = event_date or datetime.now().strftime("%Y-%m-%d")

    try:
        if db is None:
            from onuslibs.db import query as db_query
        else:
            db_query = db.query

        if agent_type:
            rows = db_query(
                """
                SELECT * FROM risk_events
                WHERE event_date = %s AND agent_type = %s AND risk_score >= %s
                ORDER BY risk_score DESC
                """,
                (event_date, agent_type, min_score),
            )
        else:
            rows = db_query(
                """
                SELECT * FROM risk_events
                WHERE event_date = %s AND risk_score >= %s
                ORDER BY risk_score DESC
                """,
                (event_date, min_score),
            )

        # Parse evidence JSON
        for row in rows:
            if isinstance(row.get("evidence"), str):
                try:
                    row["evidence"] = json.loads(row["evidence"])
                except Exception:
                    pass

        return list(rows)

    except Exception as e:
        log.warning("[risk_scorer] get_today_events failed: %s", e)
        return []
