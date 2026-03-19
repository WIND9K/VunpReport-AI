"""Test enriched fields — confirm 8 fields, no from_user_id/to_user_id"""
import sys, os, json
sys.path.insert(0, '/home/node/learn/OnusLibs')
sys.path.insert(0, '/home/node/learn/OnusReport-AI')
os.chdir('/home/node/learn/OnusReport-AI')
from dotenv import load_dotenv; load_dotenv('.env')
from onuslibs.unified.api import fetch_json
from onuslibs.config.settings import OnusSettings
from onuslibs.utils.date_utils import build_date_period
from config.report_map_ai import REPORT_MAP_AI
from datalake.schema_registry import enrich_batch, get_column_names

st = OnusSettings()
dp = build_date_period('2026-03-19', '2026-03-19')

print("=== ENRICHED SCHEMA ===")
print("Columns:", get_column_names("onchain"))
print()

for rk in ['onchain/vndc_send', 'onchain/usdt_receive']:
    spec = REPORT_MAP_AI[rk]
    params = {}
    if spec.get('filter_key') and spec.get('type'):
        params[spec['filter_key']] = spec['type']
    params['datePeriod'] = dp
    recs = fetch_json(endpoint=spec['endpoint'], params=params, fields=spec.get('fields'), paginate=True, settings=st)
    if not recs:
        continue
    enriched = enrich_batch('onchain', recs[:1], rk)
    e = enriched[0]
    print(f"=== {rk} ===")
    for k, v in e.items():
        print(f"  {k:25s} = {v}")
    has_from = 'from_user_id' in e
    has_to = 'to_user_id' in e
    print(f"  from_user_id present: {has_from} (should be False)")
    print(f"  to_user_id present:   {has_to} (should be False)")
    print()
