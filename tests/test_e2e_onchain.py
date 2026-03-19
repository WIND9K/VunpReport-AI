"""Test end-to-end: OnchainAgent full pipeline"""
import sys, os, json
sys.path.insert(0, '/home/node/learn/OnusLibs')
sys.path.insert(0, '/home/node/learn/OnusReport')
sys.path.insert(0, '/home/node/learn/OnusReport-AI')
os.chdir('/home/node/learn/OnusReport-AI')
from dotenv import load_dotenv; load_dotenv('.env')
import logging; logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

from agents.onchain_agent import OnchainAgent
from datalake.parquet_writer import ParquetWriter

# Setup
writer = ParquetWriter(temp_dir='/tmp', s3_bucket='onus-datalake', date_str='2026-03-19')
agent = OnchainAgent(date_from='2026-03-19', date_to='2026-03-19')
agent.raw_handler = lambda name, batch: writer.append_batch(name, batch)

# Run full pipeline
print("=== Running OnchainAgent full pipeline ===")
result = agent.run()

# Summary
print("\n=== Agent Result ===")
print(json.dumps(result.to_dict(), indent=2, default=str))

# Risk events
print(f"\n=== Risk Events: {len(result.risk_scored)} ===")
for e in result.risk_scored[:10]:
    uid = e.get("userid", "?")[:20]
    rt = e.get("risk_type", "?")
    bs = e.get("base_score", 0)
    fs = e.get("final_score", 0)
    ax = e.get("anomaly_x", 0)
    cv = e.get("context_verdict", "-")
    print(f"  {uid:22s} {rt:15s} base={bs:5.1f} final={fs:5.2f} anomaly_x={ax} context={cv}")

# Finalize Parquet → S3
print("\n=== Finalizing Parquet → S3 ===")
s3_result = writer.finalize_agent('onchain')
print(json.dumps(s3_result, indent=2, default=str))
