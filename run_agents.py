"""
run_agents.py — Entry point cho OnusReport-AI v5.0.

Usage:
  python3 run_agents.py --test-fetch onchain --date 2026-03-19
  python3 run_agents.py --agent onchain --date 2026-03-19
  python3 run_agents.py --all --date 2026-03-19
"""

import sys
import os
import argparse
import logging
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "OnusLibs"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "OnusReport"))
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("run_agents")


def test_fetch(agent_name, date_from, date_to):
    """Test fetch data cho 1 agent — chỉ lấy data, không ghi DB/Parquet."""
    from onuslibs.unified.api import fetch_json
    from onuslibs.config.settings import OnusSettings
    from onuslibs.utils.date_utils import build_date_period
    from config.report_map_ai import REPORT_MAP_AI

    AGENT_KEYS = {
        "onchain": ["onchain/vndc_send", "onchain/vndc_receive", "onchain/usdt_send", "onchain/usdt_receive"],
        "pro": ["pro/vndc_send", "pro/vndc_receive", "pro/usdt_send", "pro/usdt_receive"],
        "buysell": ["buysell/buy_system", "buysell/buy_partner", "buysell/sell_system", "buysell/sell_partner"],
        "exchange": ["exchange/vndcacc", "exchange/usdtacc"],
        "spot": ["spot/daily"],
    }

    keys = AGENT_KEYS.get(agent_name)
    if not keys:
        print(f"Unknown agent: {agent_name}. Available: {list(AGENT_KEYS.keys())}")
        return

    st = OnusSettings()
    total = 0
    results = []

    for rk in keys:
        spec = REPORT_MAP_AI.get(rk)
        if not spec:
            continue

        params = {}
        if spec.get("base_params"):
            params.update(spec["base_params"])
        if spec.get("filter_key") and spec.get("type"):
            params[spec["filter_key"]] = spec["type"]
        if spec.get("extra_params"):
            params.update(spec["extra_params"])
        params["datePeriod"] = build_date_period(date_from, date_to)

        try:
            records = fetch_json(
                endpoint=spec["endpoint"], params=params,
                fields=spec.get("fields"), paginate=True,
                order_by="date asc", settings=st,
            )
            count = len(records)
            total += count
            results.append((rk, count, "OK"))
            print(f"  {rk}: {count} records")
            if records:
                r = records[0]
                print(f"    sample: txn={r.get('transactionNumber','?')} date={r.get('date','?')[:19]} amount={r.get('amount','?')}")
        except Exception as e:
            results.append((rk, 0, f"ERROR: {e}"))
            print(f"  {rk}: ERROR - {e}")

    print(f"\n{'='*50}")
    print(f"Agent: {agent_name}")
    print(f"Date: {date_from} -> {date_to}")
    print(f"Total: {total} records")
    for rk, count, status in results:
        icon = "OK" if status == "OK" else "FAIL"
        print(f"  [{icon}] {rk}: {count} records")


def run_agent(agent_name, date_from, date_to):
    """Chạy 1 agent full pipeline: fetch → enrich → Parquet S3 → Risk scoring."""
    from datalake.parquet_writer import ParquetWriter

    writer = ParquetWriter(temp_dir="/tmp", s3_bucket="onus-datalake", date_str=date_from)

    if agent_name == "onchain":
        from agents.onchain_agent import OnchainAgent
        agent = OnchainAgent(date_from=date_from, date_to=date_to)
    else:
        print(f"Agent '{agent_name}' chưa implement. Available: onchain")
        return None

    # Inject raw handler — ghi enriched data vào .jsonl
    agent.raw_handler = lambda name, batch: writer.append_batch(name, batch)

    # Run pipeline
    import json
    result = agent.run()
    print(json.dumps(result.to_dict(), indent=2, default=str))

    # Risk events summary
    if result.risk_scored:
        print(f"\nRisk Events ({len(result.risk_scored)}):")
        for evt in result.risk_scored:
            print(f"  {evt['userid'][:20]:22s} {evt['risk_type']:15s} score={evt['final_score']:.2f}")

    # Finalize Parquet → S3
    s3_result = writer.finalize_agent(agent_name)
    print(f"\nParquet → S3: {json.dumps(s3_result, default=str)}")

    return result


def run_all(date_from, date_to):
    """Chạy tất cả active agents."""
    agents = ["onchain"]  # Thêm pro, buysell, exchange, spot khi implement

    print(f"{'='*60}")
    print(f"OnusReport-AI — Run All Agents")
    print(f"Date: {date_from} -> {date_to}")
    print(f"Agents: {agents}")
    print(f"{'='*60}")

    for name in agents:
        try:
            print(f"\n--- {name} ---")
            run_agent(name, date_from, date_to)
        except Exception as e:
            print(f"Agent {name} FAILED: {e}")

    print(f"\n{'='*60}")
    print("All agents done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OnusReport-AI v5.0 Runner")
    parser.add_argument("--agent", help="Run 1 agent: onchain, pro, buysell, exchange, spot")
    parser.add_argument("--all", action="store_true", help="Run all agents")
    parser.add_argument("--test-fetch", help="Test fetch only (no DB/Parquet)")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    args = parser.parse_args()

    date_from = args.start or args.date
    date_to = args.end or args.date

    print(f"OnusReport-AI v5.0")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.test_fetch:
        test_fetch(args.test_fetch, date_from, date_to)
    elif args.all:
        run_all(date_from, date_to)
    elif args.agent:
        run_agent(args.agent, date_from, date_to)
    else:
        parser.print_help()

    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
