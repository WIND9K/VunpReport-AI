"""Phân tích tất cả transfer_type unique cho onchain — dữ liệu thực tế"""
import sys, os, json
sys.path.insert(0, '/home/node/learn/OnusLibs')
sys.path.insert(0, '/home/node/learn/OnusReport-AI')
os.chdir('/home/node/learn/OnusReport-AI')
from dotenv import load_dotenv; load_dotenv('.env')
from onuslibs.unified.api import fetch_json
from onuslibs.config.settings import OnusSettings
from onuslibs.utils.date_utils import build_date_period
from config.report_map_ai import REPORT_MAP_AI
from collections import Counter

st = OnusSettings()
dp = build_date_period('2026-03-19', '2026-03-19')

all_types = Counter()

for rk in ['onchain/vndc_send', 'onchain/vndc_receive', 'onchain/usdt_send', 'onchain/usdt_receive']:
    spec = REPORT_MAP_AI[rk]
    params = {}
    if spec.get('filter_key') and spec.get('type'):
        params[spec['filter_key']] = spec['type']
    params['datePeriod'] = dp
    recs = fetch_json(endpoint=spec['endpoint'], params=params, fields=spec.get('fields'), paginate=True, settings=st)
    
    types = Counter()
    for r in recs:
        tt = r.get('type', {}).get('internalName', '?') if isinstance(r.get('type'), dict) else '?'
        types[tt] += 1
        all_types[tt] += 1
    
    print(f"=== {rk} ({len(recs)} records) ===")
    for tt, count in types.most_common():
        # Parse direction and currency from transfer_type
        t = tt.lower()
        direction = 'SEND' if 'send' in t else 'RECEIVE' if 'receive' in t else '?'
        currency = 'VNDC' if 'vndc' in t else 'USDT' if 'usdt' in t else '?'
        has_trc20 = 'trc20' in t
        print(f"  {tt:45s} count={count:5d}  → direction={direction:8s} currency={currency:4s} trc20={has_trc20}")
    print()

print("=== ALL UNIQUE transfer_types ===")
for tt, count in all_types.most_common():
    print(f"  {tt:45s} count={count}")
