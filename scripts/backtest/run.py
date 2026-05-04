"""端到端入口 —— 一行命令拉数据 + 回测 + 出报告。

用法：
    uv run python run.py --inst BTC-USDT-SWAP --days 365
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backtest import run_backtest
from fetch_data import fetch_range, save_parquet, load_parquet
from report import write_report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inst", default="BTC-USDT-SWAP")
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--data", default=str(Path(__file__).parent / "data"))
    ap.add_argument("--report", default=str(Path(__file__).parent / "reports"))
    ap.add_argument("--no-fetch", action="store_true", help="跳过拉数据，使用本地缓存")
    args = ap.parse_args()
    data_dir = Path(args.data)
    rep_dir = Path(args.report)

    if not args.no_fetch:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=args.days)
        for bar in ("15m", "1H"):
            print(f">>> fetch {args.inst} {bar} ({args.days}d)")
            df = fetch_range(args.inst, bar, start, end)
            print(f"    rows = {len(df)}")
            save_parquet(df, args.inst, bar, data_dir)

    df15 = load_parquet(args.inst, "15m", data_dir)
    df1h = load_parquet(args.inst, "1H", data_dir)
    print(f">>> backtest 15m={len(df15)} 1H={len(df1h)}")
    state, eq = run_backtest(args.inst, df15, df1h)
    write_report(state, eq, args.inst, rep_dir)


if __name__ == "__main__":
    main()
