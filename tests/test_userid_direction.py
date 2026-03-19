"""Test userid enrichment theo direction"""
import sys, os, json
sys.path.insert(0, '/home/node/learn/OnusLibs')
sys.path.insert(0, '/home/node/learn/OnusReport-AI')
os.chdir('/home/node/learn/OnusReport-AI')
from dotenv import load_dotenv; load_dotenv('.env')
from onuslibs.unified.api import fetch_json
from onuslibs.config.settings import OnusSettings
from onuslibs.utils.date_utils import build_date_period
from config.report_map_ai import REPORT_MAP_AI
from datalake.schema_registry import enrich_batch

st = OnusSettings()
dp = build_date_period('2026-03-19', '2026-03-19')

for rk in ['onchain/vndc_send', 'onchain/vndc_receive', 'onchain/usdt_send', 'onchain/usdt_receive']:
    spec = REPORT_MAP_AI[rk]
    params = {}
    if spec.get('filter_key') and spec.get('type'):
        params[spec['filter_key']] = spec['type']
    params['datePeriod'] = dp
    recs = fetch_json(endpoint=spec['endpoint'], params=params, fields=spec.get('fields'), paginate=True, settings=st)
    if not recs:
        print(f"{rk}: 0 records")
        continue
    enriched = enrich_batch('onchain', recs[:1], rk)
    e = enriched[0]
    direction = e['direction']
    userid = e['userid']
    from_uid = e['from_user_id']
    to_uid = e['to_user_id']

    if direction == 'SEND':
        correct = (userid == from_uid)
        expect = 'from_user_id'
    else:
        correct = (userid == to_uid)
        expect = 'to_user_id'

    status = 'OK' if correct else 'WRONG'
    print(f"{rk}: direction={direction} userid should be {expect} -> {status}")
    print(f"  userid={userid}")
    print(f"  from={from_uid}  to={to_uid}")
    print()
