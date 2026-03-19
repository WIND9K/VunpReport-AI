"""
scripts/get_risk_report.py — Orchestrator dùng để lấy risk report từ DB.

Usage:
  python3 get_risk_report.py --date 2026-03-19
  python3 get_risk_report.py --date today
  python3 get_risk_report.py --date 2026-03-19 --agent onchain
  python3 get_risk_report.py --date 2026-03-19 --min-score 5.0

Output: text format cho LLM đọc và phân tích.
"""
import sys, os, json, argparse
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, '/home/node/learn/OnusLibs')
os.chdir(os.path.join(os.path.dirname(__file__), '..'))
from dotenv import load_dotenv; load_dotenv('.env')

import pymysql

SYSTEM_USERS = {"6277729709484240798"}  # ONUS Gateway

def get_db():
    return pymysql.connect(
        host=os.environ['ONUSLIBS_DB_HOST'],
        user=os.environ['ONUSLIBS_DB_USER'],
        password=os.environ['ONUSLIBS_DB_PASSWORD'],
        database=os.environ['ONUSLIBS_DB_NAME'],
        port=int(os.environ.get('ONUSLIBS_DB_PORT', 3306)),
        cursorclass=pymysql.cursors.DictCursor,
    )

def get_risk_events(date_str, agent_type=None, min_score=0):
    conn = get_db()
    cur = conn.cursor()
    if agent_type:
        cur.execute(
            "SELECT * FROM risk_events WHERE event_date=%s AND agent_type=%s AND risk_score>=%s ORDER BY risk_score DESC",
            (date_str, agent_type, min_score),
        )
    else:
        cur.execute(
            "SELECT * FROM risk_events WHERE event_date=%s AND risk_score>=%s ORDER BY risk_score DESC",
            (date_str, min_score),
        )
    rows = list(cur.fetchall())
    conn.close()
    return rows

def get_user_contexts(userids):
    if not userids:
        return {}
    conn = get_db()
    cur = conn.cursor()
    placeholders = ','.join(['%s'] * len(userids))
    cur.execute(
        f"SELECT * FROM user_context WHERE userid IN ({placeholders}) AND valid_until >= NOW()",
        tuple(userids),
    )
    rows = cur.fetchall()
    conn.close()
    ctx = {}
    for r in rows:
        key = r['userid']
        if key not in ctx:
            ctx[key] = []
        ctx[key].append(r)
    return ctx

def format_report(events, contexts, date_str):
    # Filter system users
    events = [e for e in events if e['userid'] not in SYSTEM_USERS]

    if not events:
        return f"📊 Risk Report — {date_str}\n\nKhông có risk events (đã exclude system users)."

    # Group by userid
    by_user = defaultdict(list)
    for e in events:
        by_user[e['userid']].append(e)

    # Sort by max score
    sorted_users = sorted(
        by_user.items(),
        key=lambda x: max(float(e['risk_score']) for e in x[1]),
        reverse=True,
    )

    lines = []
    lines.append(f"📊 Risk Report — {date_str}")
    lines.append(f"Total events: {len(events)} | Unique users: {len(by_user)} (excluded {len(SYSTEM_USERS)} system users)")
    lines.append("")

    critical = []
    high = []
    medium = []

    for userid, user_events in sorted_users:
        max_score = max(float(e['risk_score']) for e in user_events)
        agents = set(e['agent_type'] for e in user_events)
        types = [e['risk_type'] for e in user_events]

        # Context
        user_ctx = contexts.get(userid, [])
        ctx_str = ""
        if user_ctx:
            verdicts = [c['verdict'] for c in user_ctx]
            reasons = [c.get('reason', '') for c in user_ctx if c.get('reason')]
            ctx_str = f" | Context: {', '.join(verdicts)}"
            if reasons:
                ctx_str += f" ({'; '.join(reasons)})"

        cross_agent = " [CROSS-AGENT]" if len(agents) > 1 else ""
        entry = f"  User {userid} — score={max_score:.1f} agents={list(agents)} types={types}{cross_agent}{ctx_str}"

        for e in user_events:
            evidence = e.get('evidence', '')
            if isinstance(evidence, str):
                try:
                    evidence = json.loads(evidence)
                except Exception:
                    pass
            if isinstance(evidence, dict):
                ev_short = ', '.join(f"{k}={v}" for k, v in list(evidence.items())[:4])
            else:
                ev_short = str(evidence)[:100]
            entry += f"\n    #{e['id']} {e['agent_type']}/{e['risk_type']} score={float(e['risk_score']):.1f} anomaly_x={float(e.get('anomaly_x',0)):.1f} evidence=[{ev_short}]"

        if max_score >= 9.0:
            critical.append(entry)
        elif max_score >= 7.5:
            high.append(entry)
        elif max_score >= 5.0:
            medium.append(entry)

    if critical:
        lines.append(f"🔴 CRITICAL ({len(critical)} users):")
        lines.extend(critical)
        lines.append("")
    if high:
        lines.append(f"🟡 HIGH ({len(high)} users):")
        lines.extend(high)
        lines.append("")
    if medium:
        lines.append(f"🟠 MEDIUM ({len(medium)} users):")
        lines.extend(medium)
        lines.append("")

    if not critical:
        lines.append("✅ Không có CRITICAL alerts hôm nay.")

    lines.append("")
    lines.append("Reply /confirm CASE_ID VERDICT \"lý do\" để xử lý.")
    return "\n".join(lines)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default='today')
    parser.add_argument('--agent', default=None)
    parser.add_argument('--min-score', type=float, default=0)
    args = parser.parse_args()

    if args.date == 'today':
        date_str = datetime.now().strftime('%Y-%m-%d')
    elif args.date == 'yesterday':
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        date_str = args.date

    events = get_risk_events(date_str, args.agent, args.min_score)
    userids = list(set(e['userid'] for e in events if e['userid'] not in SYSTEM_USERS))
    contexts = get_user_contexts(userids)

    report = format_report(events, contexts, date_str)
    print(report)

if __name__ == '__main__':
    main()
