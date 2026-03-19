"""Test enriched pipeline: fetch → enrich → .jsonl → Parquet → S3 (enriched/)"""
import sys, os, json
sys.path.insert(0, '/home/node/learn/OnusLibs')
sys.path.insert(0, '/home/node/learn/OnusReport')
sys.path.insert(0, '/home/node/learn/OnusReport-AI')
os.chdir('/home/node/learn/OnusReport-AI')
from dotenv import load_dotenv; load_dotenv('.env')
from onuslibs.unified.api import fetch_json
from onuslibs.config.settings import OnusSettings
from onuslibs.utils.date_utils import build_date_period
from config.report_map_ai import REPORT_MAP_AI
from datalake.parquet_writer import ParquetWriter
from datalake.schema_registry import enrich_batch

st = OnusSettings()
writer = ParquetWriter(temp_dir='/tmp', s3_bucket='onus-datalake', date_str='2026-03-18')

total = 0
for rk in ['onchain/vndc_send','onchain/vndc_receive','onchain/usdt_send','onchain/usdt_receive']:
    spec = REPORT_MAP_AI[rk]
    params = {}
    if spec.get('filter_key') and spec.get('type'):
        params[spec['filter_key']] = spec['type']
    params['datePeriod'] = build_date_period('2026-03-18', '2026-03-18')

    def on_batch(batch, _rk=rk):
        enriched = enrich_batch('onchain', batch, _rk)
        writer.append_batch('onchain', enriched)

    recs = fetch_json(
        endpoint=spec['endpoint'], params=params,
        fields=spec.get('fields'), paginate=True,
        on_batch=on_batch, settings=st,
    )
    print(f"  {rk}: {len(recs)} records")
    total += len(recs)

print(f"\nTotal fetched: {total}")

# Finalize
result = writer.finalize_agent('onchain')
print(f"Finalize: {json.dumps(result, default=str)}")

# Verify S3
import boto3
s3 = boto3.client('s3')
r = s3.list_objects_v2(Bucket='onus-datalake', Prefix='enriched/')
for o in r.get('Contents', []):
    print(f"  S3: {o['Key']}  size={o['Size']}")
