"""
run_agents.py — Entry point chính cho OnusReport-AI.

OpenClaw skill agent_runner sẽ gọi file này.
Hỗ trợ:
  python run_agents.py --all --date 2026-03-18
  python run_agents.py --agent onchain --date 2026-03-18
  python run_agents.py --test-fetch onchain --date 2026-03-18
"""

import sys
import os
import argparse
import json
import logging
from datetime import datetime

# Path setup — dùng chung OnusLibs và OnusReport
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


def test_fetch(agent_name: str, date_from: str, date_to: str):
    """Test fetch data cho 1 agent — không ghi DB, không ghi Parquet."""
    from onuslibs.unified.api import fetch_json
    from onuslibs.config.settings import OnusSettings
    from onuslibs.utils.date_utils import build_date_period
    from config.report_map_ai import REPORT_MAP_AI

    # Map agent → report keys
    AGENT_KEYS = {
        "onchain": ["onchain/vndc_send", "onchain/vndc_receive", "onchain/usdt_send", "onchain/usdt_receive"],
        "pro": ["pro/vndc_send", "pro/vndc_receive", "pro/usdt_send", "pro/usdt_receive"],
        "buysell": ["buysell/buy_system", "buysell/buy_partner", "buysell/sell_system", "buysell/sell_partner"],
        "exchange": ["exchange/vndcacc", "exchange/usdtacc"],
        "spot": ["spot/daily"],
    }

    keys = AGENT_KEYS.get(agent_name)
    if not keys:
        print(f"Unknown agent: {agent_name}")
        print(f"Available: {list(AGENT_KEYS.keys())}")
        return

    st = OnusSettings()
    total = 0
    results = []

    for rk in keys:
        spec = REPORT_MAP_AI.get(rk)
        if not spec:
            print(f"  [SKIP] {rk}: not in report_map_ai")
            continue

        params = {}
        base = spec.get("base_params") or {}
        if isinstance(base, dict):
            params.update(base)
        if spec.get("filter_key") and spec.get("type"):
            params[spec["filter_key"]] = spec["type"]
        extra = spec.get("extra_params")
        if extra and isinstance(extra, dict):
            params.update(extra)
        period = build_date_period(date_from, date_to)
        if period:
            params["datePeriod"] = period

        batch_count = [0]

        def on_batch(batch, _rk=rk):
            batch_count[0] += 1

        try:
            records = fetch_json(
                endpoint=spec["endpoint"],
                params=params,
                fields=spec.get("fields"),
                paginate=True,
                order_by="date asc",
                on_batch=on_batch,
                settings=st,
            )
            count = len(records)
            total += count
            results.append({"report": rk, "records": count, "batches": batch_count[0], "status": "OK"})
            print(f"  {rk}: {count} records ({batch_count[0]} batches)")

            # Show sample
            if records and count > 0:
                r = records[0]
                txn = r.get("transactionNumber", "?")
                date = r.get("date", "?")[:19]
                amount = r.get("amount", "?")
                print(f"    sample: txn={txn} date={date} amount={amount}")

        except Exception as e:
            results.append({"report": rk, "records": 0, "batches": 0, "status": f"ERROR: {e}"})
            print(f"  {rk}: ERROR - {e}")

    # Summary
    print(f"\n{'='*50}")
    print(f"Agent: {agent_name}")
    print(f"Date: {date_from} -> {date_to}")
    print(f"Total: {total} records")
    for r in results:
        icon = "OK" if r["status"] == "OK" else "FAIL"
        print(f"  [{icon}] {r['report']}: {r['records']} records")

    return {"agent": agent_name, "total": total, "details": results}


def run_agent(agent_name: str, date_from: str, date_to: str, no_parquet: bool = False):
    """Chạy 1 agent đầy đủ: fetch → ETL → Parquet → Risk."""
    from datalake.parquet_writer import ParquetWriter

    writer = None if no_parquet else ParquetWriter(date_str=date_from)

    if agent_name == "onchain":
        from agents.onchain_agent import OnchainAgent
        agent = OnchainAgent(parquet_writer=writer)
    else:
        print(f"Agent '{agent_name}' chưa được implement. Available: onchain")
        return None

    print(f"\nRunning {agent_name} agent...")
    result = agent.run(date_from, date_to)
    print(f"\n{result.to_summary()}")

    if result.risk_events:
        print(f"\nRisk Events ({len(result.risk_events)}):")
        for evt in result.risk_events:
            print(f"  - {evt['userid']}: {evt['risk_type']} (score {evt['risk_score']})")

    if result.errors:
        print(f"\nErrors: {result.errors}")

    return result


def run_all(date_from: str, date_to: str, no_parquet: bool = False):
    """Chạy tất cả agents."""
    agents = ["onchain"]  # Sẽ thêm pro, buysell, exchange, spot sau
    results = []

    print(f"{'='*60}")
    print(f"OnusReport-AI — Run All Agents")
    print(f"Date: {date_from} -> {date_to}")
    print(f"Agents: {agents}")
    print(f"{'='*60}")

    for name in agents:
        try:
            result = run_agent(name, date_from, date_to, no_parquet)
            if result:
                results.append(result)
        except Exception as e:
            print(f"Agent {name} failed: {e}")

    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    for r in results:
        print(f"  {r.to_summary()}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OnusReport-AI Runner")
    parser.add_argument("--agent", help="Agent name: onchain, pro, buysell, exchange, spot")
    parser.add_argument("--all", action="store_true", help="Run all agents")
    parser.add_argument("--test-fetch", help="Test fetch only (no DB/Parquet). Agent name.")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"), help="YYYY-MM-DD (single day)")
    parser.add_argument("--start", help="YYYY-MM-DD start date")
    parser.add_argument("--end", help="YYYY-MM-DD end date")
    parser.add_argument("--no-parquet", action="store_true", help="Skip Parquet writing")
    args = parser.parse_args()

    date_from = args.start or args.date
    date_to = args.end or args.date

    print(f"OnusReport-AI v5.0")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.test_fetch:
        test_fetch(args.test_fetch, date_from, date_to)
    elif args.all:
        run_all(date_from, date_to, args.no_parquet)
    elif args.agent:
        run_agent(args.agent, date_from, date_to, args.no_parquet)
    else:
        parser.print_help()

    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
