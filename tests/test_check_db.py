"""Check current risk_events and user_context in DB"""
import sys, os, pymysql
os.chdir('/home/node/learn/OnusReport-AI')
from dotenv import load_dotenv; load_dotenv('.env')
conn = pymysql.connect(
    host=os.environ['ONUSLIBS_DB_HOST'], user=os.environ['ONUSLIBS_DB_USER'],
    password=os.environ['ONUSLIBS_DB_PASSWORD'], database=os.environ['ONUSLIBS_DB_NAME'],
    port=3306, cursorclass=pymysql.cursors.DictCursor
)
cur = conn.cursor()

cur.execute('SELECT id, event_date, userid, agent_type, risk_type, risk_score, anomaly_x FROM risk_events ORDER BY risk_score DESC')
rows = cur.fetchall()
print(f"=== risk_events: {len(rows)} rows ===")
for r in rows:
    uid = r['userid'][:20]
    score = float(r['risk_score'])
    print(f"  id={r['id']} date={r['event_date']} userid={uid:22s} {r['agent_type']:8s} {r['risk_type']:15s} score={score:.2f}")

# Group by userid
from collections import defaultdict
by_user = defaultdict(list)
for r in rows:
    by_user[r['userid']].append(r)

print(f"\n=== Unique users with risk events: {len(by_user)} ===")
for uid, events in sorted(by_user.items(), key=lambda x: -max(float(e['risk_score']) for e in x[1])):
    agents = set(e['agent_type'] for e in events)
    max_score = max(float(e['risk_score']) for e in events)
    types = [e['risk_type'] for e in events]
    multi = "CROSS-AGENT" if len(agents) > 1 else ""
    print(f"  {uid[:20]:22s} agents={list(agents)} max={max_score:.2f} types={types} {multi}")

cur.execute('SELECT COUNT(*) as c FROM user_context')
print(f"\nuser_context: {cur.fetchone()['c']} rows")
conn.close()
