"""批量回测：多币种 / 多时段 / 参数 sweep / 滑点对比 一键全部跑完。

用法：
    uv run python batch.py multi    # 多币种 365 天
    uv run python batch.py bear     # BTC 2022 熊市
    uv run python batch.py sweep    # 参数敏感性
    uv run python batch.py costs    # 滑点+资金费率对比
    uv run python batch.py all      # 全跑
"""
from __future__ import annotations

import argparse
import sys
import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

import strategy_v32  # 用于动态改参数
import trend         # 用于动态改 ATR 阈值
from backtest import run_backtest
from fetch_data import fetch_or_load
from report import calc_metrics


HERE = Path(__file__).parent
DATA = HERE / "data"
REP = HERE / "reports"


def fmt_metrics(m: dict, label: str) -> dict:
    return {
        "case": label,
        "trades": m["trades"],
        "win_rate": f"{m['win_rate']:.1%}",
        "rr": f"1:{m['rr_realized']:.2f}" if m["rr_realized"] != float("inf") else "inf",
        "pf": f"{m['profit_factor']:.2f}" if m["profit_factor"] != float("inf") else "inf",
        "exp_u": f"{m['expectancy_u']:+.3f}",
        "ret": f"{m['return_pct']:+.1%}",
        "mdd": f"{m['max_drawdown_pct']:.1%}",
        "final_u": f"{m['final_balance_u']:.2f}",
    }


def run_one(
    inst: str,
    start: datetime,
    end: datetime,
    tag: str,
    slippage: float = 0.0,
    funding_8h: float = 0.0,
) -> dict:
    df15 = fetch_or_load(inst, "15m", start, end, DATA, tag=tag)
    df1h = fetch_or_load(inst, "1H", start, end, DATA, tag=tag)
    if df15.empty or df1h.empty:
        return {"trades": 0, "win_rate": 0, "rr_realized": 0, "profit_factor": 0,
                "expectancy_u": 0, "return_pct": 0, "max_drawdown_pct": 0,
                "final_balance_u": 31.5, "days": 0}
    state, eq = run_backtest(inst, df15, df1h, slippage=slippage, funding_8h=funding_8h)
    return calc_metrics(state, eq)


def cmd_multi() -> list[dict]:
    """多币种 365 天验证 — 看胜率是否稳定在 40%。"""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=365)
    insts = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP",
             "ADA-USDT-SWAP", "AVAX-USDT-SWAP", "LINK-USDT-SWAP"]
    rows = []
    for inst in insts:
        print(f">>> [multi] {inst} 365d")
        m = run_one(inst, start, end, tag="365d")
        rows.append(fmt_metrics(m, inst))
    return rows


def cmd_bear() -> list[dict]:
    """BTC 在 2022 熊市（5月 LUNA 暴雷 + 11 月 FTX 暴雷）。"""
    rows = []
    cases = [
        ("BTC-USDT-SWAP", datetime(2022, 1, 1, tzinfo=timezone.utc),
         datetime(2022, 12, 31, tzinfo=timezone.utc), "2022"),
        ("ETH-USDT-SWAP", datetime(2022, 1, 1, tzinfo=timezone.utc),
         datetime(2022, 12, 31, tzinfo=timezone.utc), "2022"),
        ("BTC-USDT-SWAP", datetime(2024, 1, 1, tzinfo=timezone.utc),
         datetime(2024, 12, 31, tzinfo=timezone.utc), "2024"),
    ]
    for inst, s, e, tag in cases:
        label = f"{inst} {tag}"
        print(f">>> [bear] {label}")
        m = run_one(inst, s, e, tag=tag)
        rows.append(fmt_metrics(m, label))
    return rows


def cmd_sweep() -> list[dict]:
    """参数敏感性：BTC 365d × ATR 阈值 × 关键位 tolerance。"""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=365)
    inst = "BTC-USDT-SWAP"
    df15 = fetch_or_load(inst, "15m", start, end, DATA, tag="365d")
    df1h = fetch_or_load(inst, "1H", start, end, DATA, tag="365d")
    rows = []
    atr_grid = [0.003, 0.005, 0.008]
    tol_grid = [0.003, 0.005, 0.008]
    # 备份原值
    old_tol = strategy_v32.KEY_LEVEL_TOLERANCE
    for atr_th in atr_grid:
        for tol in tol_grid:
            # 改 trend.classify_trend 的默认阈值通过封装；改 strategy_v32 的 tolerance
            strategy_v32.KEY_LEVEL_TOLERANCE = tol
            # 直接调 run_backtest 但 trend 阈值要在调用时传 → 简化：复制 trend 逻辑，临时改
            from trend import classify_trend
            trend_1h = classify_trend(_with_indicators(df1h.copy()), atr_threshold=atr_th)
            label = f"atr={atr_th} tol={tol}"
            print(f">>> [sweep] {label}")
            m = _run_with_custom_trend(inst, df15, df1h, trend_1h)
            rows.append(fmt_metrics(m, label))
    strategy_v32.KEY_LEVEL_TOLERANCE = old_tol
    return rows


def _with_indicators(df: pd.DataFrame) -> pd.DataFrame:
    from indicators import attach_indicators
    return attach_indicators(df)


def _run_with_custom_trend(inst: str, df15: pd.DataFrame, df1h: pd.DataFrame, trend_1h) -> dict:
    """复制 run_backtest 主循环，但用外部传入的 trend_1h（避开默认阈值）。"""
    from indicators import attach_indicators, attach_patterns
    from key_levels import attach_swings
    from strategy_v32 import assemble_signal
    from backtest import State, can_open, open_trade, close_trade, step_open_position

    df15 = attach_swings(attach_patterns(attach_indicators(df15)), window=5)
    df1h_ind = _with_indicators(df1h)
    trend_1h_ts = df1h_ind["ts"]

    state = State()
    equity_rows = []
    for idx in range(len(df15)):
        row = df15.iloc[idx]
        ts = row["ts"]
        if state.open_trade is not None:
            step_open_position(state, ts, float(row["high"]), float(row["low"]), inst)
        if state.open_trade is None:
            sig = assemble_signal(df15, idx, trend_1h, trend_1h_ts)
            if sig is not None:
                ok, _ = can_open(state, ts, inst, sig.direction)
                if ok:
                    open_trade(state, ts, sig, inst)
        unreal = 0.0
        if state.open_trade is not None:
            t = state.open_trade
            mid = float(row["close"])
            unreal = (mid - t.entry) * t.qty_coin if t.direction == "long" else (t.entry - mid) * t.qty_coin
        equity_rows.append({"ts": ts, "balance": state.balance, "equity": state.balance + unreal})
    if state.open_trade is not None:
        last = df15.iloc[-1]
        close_trade(state, last["ts"], float(last["close"]), "eod", inst)
    eq = pd.DataFrame(equity_rows)
    return calc_metrics(state, eq)


def cmd_costs() -> list[dict]:
    """滑点 + 资金费率对比 — 同一段数据，三档成本。"""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=365)
    inst = "BTC-USDT-SWAP"
    rows = []
    cases = [
        ("无成本", 0.0, 0.0),
        ("中成本（滑 0.02% + 资金 0.005%/8h）", 0.0002, 0.00005),
        ("高成本（滑 0.05% + 资金 0.01%/8h）", 0.0005, 0.0001),
    ]
    for label, slip, fund in cases:
        print(f">>> [costs] {label}")
        m = run_one(inst, start, end, tag="365d", slippage=slip, funding_8h=fund)
        rows.append(fmt_metrics(m, label))
    return rows


def write_table(rows: list[dict], path: Path, title: str) -> None:
    if not rows:
        return
    keys = list(rows[0].keys())
    lines = [f"## {title}", ""]
    lines.append("| " + " | ".join(keys) + " |")
    lines.append("|" + "|".join(["---"] * len(keys)) + "|")
    for r in rows:
        lines.append("| " + " | ".join(str(r.get(k, "")) for k in keys) + " |")
    txt = "\n".join(lines) + "\n"
    print("\n" + txt)
    path.write_text(txt, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["multi", "bear", "sweep", "costs", "all"])
    args = ap.parse_args()
    REP.mkdir(parents=True, exist_ok=True)

    if args.cmd in ("multi", "all"):
        write_table(cmd_multi(), REP / "batch_multi.md", "多币种 365 天")
    if args.cmd in ("bear", "all"):
        write_table(cmd_bear(), REP / "batch_bear.md", "熊市 / 不同年份")
    if args.cmd in ("sweep", "all"):
        write_table(cmd_sweep(), REP / "batch_sweep.md", "参数敏感性（BTC 365d）")
    if args.cmd in ("costs", "all"):
        write_table(cmd_costs(), REP / "batch_costs.md", "滑点 + 资金费率（BTC 365d）")


if __name__ == "__main__":
    main()
