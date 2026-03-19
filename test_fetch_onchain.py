"""
test_fetch_onchain.py — Test fetch dữ liệu onchain từ Cyclos API.

Chạy: python test_fetch_onchain.py --start 2026-03-18 --end 2026-03-18
Kết quả: in số records, sample data, không ghi DB.
"""

import sys
import os
import argparse
from datetime import datetime

# Thêm path cho OnusLibs và OnusReport
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "OnusLibs"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "OnusReport"))
sys.path.insert(0, os.path.dirname(__file__))

# Load .env
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from onuslibs.unified.api import fetch_json
from onuslibs.config.settings import OnusSettings
from onuslibs.utils.date_utils import build_date_period

from config.report_map_ai import REPORT_MAP_AI


def test_fetch(report_key: str, date_from: str, date_to: str, max_show: int = 3):
    """Fetch 1 report key và in kết quả."""
    spec = REPORT_MAP_AI.get(report_key)
    if not spec:
        print(f"❌ Report key not found: {report_key}")
        return []

    endpoint = spec.get("endpoint", "")
    params = {}

    # Base params
    base = spec.get("base_params") or {}
    if isinstance(base, dict):
        params.update(base)

    # Filter
    filter_key = spec.get("filter_key")
    filter_type = spec.get("type")
    if filter_key and filter_type:
        params[filter_key] = filter_type

    # Extra params
    extra = spec.get("extra_params")
    if extra and isinstance(extra, dict):
        params.update(extra)

    # Date period
    period = build_date_period(date_from, date_to)
    if period:
        params["datePeriod"] = period

    print(f"\n{'='*60}")
    print(f"📡 Fetching: {report_key}")
    print(f"   Endpoint: {endpoint}")
    print(f"   Params: {params}")
    print(f"{'='*60}")

    st = OnusSettings()
    batch_count = [0]
    total_in_batches = [0]

    def on_batch(batch):
        batch_count[0] += 1
        total_in_batches[0] += len(batch)
        print(f"   📦 Batch #{batch_count[0]}: {len(batch)} records (total so far: {total_in_batches[0]})")

    try:
        records = fetch_json(
            endpoint=endpoint,
            params=params,
            fields=spec.get("fields"),
            paginate=True,
            order_by="date asc",
            on_batch=on_batch,
            settings=st,
        )
    except Exception as e:
        print(f"❌ Fetch error: {e}")
        return []

    print(f"\n✅ Total: {len(records)} records in {batch_count[0]} batches")

    # Show sample
    if records:
        print(f"\n📋 Sample (first {min(max_show, len(records))}):")
        for i, r in enumerate(records[:max_show]):
            txn = r.get("transactionNumber", "?")
            date = r.get("date", "?")
            amount = r.get("amount", "?")
            from_id = r.get("from", {}).get("user", {}).get("id", "?")
            to_id = r.get("to", {}).get("user", {}).get("id", "?")
            print(f"   [{i+1}] txn={txn} | date={date} | amount={amount} | from={from_id} → to={to_id}")

    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test fetch onchain data")
    parser.add_argument("--start", default=datetime.now().strftime("%Y-%m-%d"), help="YYYY-MM-DD")
    parser.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"), help="YYYY-MM-DD")
    parser.add_argument("--report", default="all", help="Report key or 'all' for all onchain")
    parser.add_argument("--show", type=int, default=3, help="Max records to show")
    args = parser.parse_args()

    print(f"🚀 OnusReport-AI — Test Fetch Onchain")
    print(f"📅 Date: {args.start} → {args.end}")
    print(f"⏰ Started: {datetime.now().strftime('%H:%M:%S')}")

    if args.report == "all":
        report_keys = [
            "onchain/vndc_send",
            "onchain/vndc_receive",
            "onchain/usdt_send",
            "onchain/usdt_receive",
        ]
    else:
        report_keys = [args.report]

    grand_total = 0
    for rk in report_keys:
        records = test_fetch(rk, args.start, args.end, args.show)
        grand_total += len(records)

    print(f"\n{'='*60}")
    print(f"🏁 Done! Grand total: {grand_total} records across {len(report_keys)} reports")
    print(f"⏰ Finished: {datetime.now().strftime('%H:%M:%S')}")
