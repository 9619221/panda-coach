"""回测报告输出 —— 关键指标 + 资金曲线 + 逐单明细。"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tabulate import tabulate

from strategy_v32 import INITIAL_BALANCE_U


def calc_metrics(state, equity: pd.DataFrame) -> dict:
    n = len(state.trades)
    days = (equity["ts"].iloc[-1] - equity["ts"].iloc[0]).days or 1 if not equity.empty else 0
    if n == 0:
        return {
            "trades": 0,
            "win_rate": 0.0,
            "avg_win_u": 0.0,
            "avg_loss_u": 0.0,
            "rr_realized": 0.0,
            "profit_factor": 0.0,
            "total_pnl_u": 0.0,
            "return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "expectancy_u": 0.0,
            "final_balance_u": state.balance,
            "days": days,
        }
    wins = [t for t in state.trades if t.pnl_usdt > 0]
    losses = [t for t in state.trades if t.pnl_usdt <= 0]
    gross_win = sum(t.pnl_usdt for t in wins)
    gross_loss = abs(sum(t.pnl_usdt for t in losses))
    avg_win = gross_win / len(wins) if wins else 0
    avg_loss = gross_loss / len(losses) if losses else 0
    rr_realized = avg_win / avg_loss if avg_loss > 0 else float("inf")
    win_rate = len(wins) / n
    total_pnl = state.balance - INITIAL_BALANCE_U
    ret_pct = total_pnl / INITIAL_BALANCE_U

    eq = equity["equity"].values
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak) / peak
    max_dd = float(dd.min()) if len(dd) else 0.0

    # 期望值 = 胜率×平均盈 - 败率×平均亏（以 R 计 = 单笔风险 1.5U）
    expectancy_u = win_rate * avg_win - (1 - win_rate) * avg_loss

    return {
        "trades": n,
        "win_rate": win_rate,
        "avg_win_u": avg_win,
        "avg_loss_u": avg_loss,
        "rr_realized": rr_realized,
        "profit_factor": gross_win / gross_loss if gross_loss > 0 else float("inf"),
        "total_pnl_u": total_pnl,
        "return_pct": ret_pct,
        "max_drawdown_pct": max_dd,
        "expectancy_u": expectancy_u,
        "final_balance_u": state.balance,
        "days": days,
    }


def write_report(state, equity: pd.DataFrame, inst: str, rep_dir: Path) -> None:
    rep_dir.mkdir(parents=True, exist_ok=True)
    metrics = calc_metrics(state, equity)

    # 1. 核心指标表
    rows = [
        ["标的", inst],
        ["回测天数", metrics["days"]],
        ["总单数", metrics["trades"]],
        ["胜率", f"{metrics.get('win_rate', 0):.1%}"],
        ["平均盈 (U)", f"{metrics.get('avg_win_u', 0):.3f}"],
        ["平均亏 (U)", f"{metrics.get('avg_loss_u', 0):.3f}"],
        ["实测 R:R", f"1:{metrics.get('rr_realized', 0):.2f}"],
        ["盈亏因子", f"{metrics.get('profit_factor', 0):.2f}"],
        ["单笔期望值 (U)", f"{metrics.get('expectancy_u', 0):.3f}"],
        ["总盈亏 (U)", f"{metrics.get('total_pnl_u', 0):+.2f}"],
        ["收益率", f"{metrics.get('return_pct', 0):+.1%}"],
        ["最大回撤", f"{metrics.get('max_drawdown_pct', 0):.1%}"],
        ["期末余额 (U)", f"{metrics.get('final_balance_u', 0):.2f}"],
    ]
    summary = tabulate(rows, headers=["指标", "值"], tablefmt="github")
    print(summary)
    (rep_dir / f"{inst}_summary.md").write_text("# 回测报告\n\n" + summary + "\n", encoding="utf-8")

    # 2. 逐单明细
    if state.trades:
        trades_df = pd.DataFrame([
            {
                "open_ts": t.open_ts,
                "close_ts": t.close_ts,
                "dir": t.direction,
                "entry": round(t.entry, 4),
                "stop": round(t.stop, 4),
                "tp": round(t.take_profit, 4),
                "exit": round(t.close_price or 0, 4),
                "reason": t.close_reason,
                "rr_plan": round(t.rr_planned, 2),
                "pnl_u": round(t.pnl_usdt, 3),
                "fees_u": round(t.fees_usdt, 4),
                "why": " + ".join(t.reasons),
            }
            for t in state.trades
        ])
        trades_df.to_csv(rep_dir / f"{inst}_trades.csv", index=False)

    # 3. 资金曲线图
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(equity["ts"], equity["equity"], lw=1, label="Equity")
    ax.axhline(INITIAL_BALANCE_U, color="gray", lw=0.5, ls="--", label="initial")
    ax.set_title(f"Equity Curve — {inst}")
    ax.set_ylabel("USDT")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(rep_dir / f"{inst}_equity.png", dpi=120)
    plt.close(fig)

    print(f"报告写入: {rep_dir}")
