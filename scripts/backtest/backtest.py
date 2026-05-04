"""V3.2 回测引擎 —— 事件驱动循环。

撮合规则（保守贴近实盘）：
- 限价进场用 close 近似（V3.2 §2.5 不挂整数关；本回测忽略此细节）
- 滑点：0
- 手续费：双边 0.05%（taker 0.05% × 2 = 0.1% 来回）
- 一根 K 内若同时穿过止损和止盈，按「先到止损」保守计（high/low 难以确定先后）
- 持仓上限 1（V3.2 §0 ≤3，简化为单仓）
- 方向锁定：当日内不允许反向（V3.2 §禁忌 5 / 09-03 §186-193）

退场红线（V3.2 §0.2）：
- 当日累亏 > 5% → 全停 24h
- 同币止损后 4h 冷静期
- 连续 3 单亏 → 复盘日 24h 不开仓
- 当周累亏 > 10% → 减半仓位 1 周
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

import pandas as pd

from fetch_data import load_parquet
from indicators import attach_indicators, attach_patterns
from key_levels import attach_swings
from strategy_v32 import (
    INITIAL_BALANCE_U,
    RISK_PER_TRADE_U,
    Signal,
    assemble_signal,
)
from trend import classify_trend

FEE_RATE = 0.0005          # taker 0.05%（OKX SWAP 普通账户）
DEFAULT_SLIPPAGE = 0.0     # 滑点（单边比例），实盘 BTC 大约 0.0002~0.0005
DEFAULT_FUNDING_8H = 0.0   # 资金费率（每 8h 比例），实盘 BTC ~0.0001 多头付空头


@dataclass
class Trade:
    open_ts: pd.Timestamp
    direction: str
    entry: float
    stop: float
    take_profit: float
    size_usdt: float          # 名义价值（U）
    qty_coin: float           # 持仓量（币）
    rr_planned: float
    reasons: list[str]
    close_ts: pd.Timestamp | None = None
    close_price: float | None = None
    close_reason: str = ""    # "tp" / "sl" / "eod"
    pnl_usdt: float = 0.0
    fees_usdt: float = 0.0


@dataclass
class State:
    balance: float = INITIAL_BALANCE_U
    open_trade: Trade | None = None
    trades: list[Trade] = field(default_factory=list)
    consec_losses: int = 0
    last_loss_ts_by_inst: dict[str, pd.Timestamp] = field(default_factory=dict)
    daily_pnl: dict[pd.Timestamp, float] = field(default_factory=dict)
    weekly_pnl: dict[pd.Timestamp, float] = field(default_factory=dict)
    halted_until: pd.Timestamp | None = None
    review_day_until: pd.Timestamp | None = None
    weekly_halve_until: pd.Timestamp | None = None
    direction_today: dict[pd.Timestamp, str] = field(default_factory=dict)


def _date_key(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(ts.date())


def _week_key(ts: pd.Timestamp) -> tuple[int, int]:
    iso = ts.isocalendar()
    return (int(iso.year), int(iso.week))


def can_open(state: State, ts: pd.Timestamp, inst: str, direction: str) -> tuple[bool, str]:
    if state.open_trade is not None:
        return False, "已有持仓"
    if state.halted_until is not None and ts < state.halted_until:
        return False, "日 5% 熔断中"
    if state.review_day_until is not None and ts < state.review_day_until:
        return False, "复盘日"
    last_loss = state.last_loss_ts_by_inst.get(inst)
    if last_loss is not None and ts - last_loss < timedelta(hours=4):
        return False, "同币 4h 冷静期"
    dkey = _date_key(ts)
    locked = state.direction_today.get(dkey)
    if locked is not None and locked != direction:
        return False, f"今日已锁{locked}"
    return True, ""


def open_trade(
    state: State,
    ts: pd.Timestamp,
    sig: Signal,
    inst: str,
    slippage: float = DEFAULT_SLIPPAGE,
) -> None:
    """按风险预算定仓位。size_usdt = risk_u / (risk_pct)。
    滑点：进场价对方向不利方向偏移 slippage 比例（多单买更贵 / 空单卖更便宜）。
    """
    risk_u = RISK_PER_TRADE_U
    if state.weekly_halve_until is not None and ts < state.weekly_halve_until:
        risk_u *= 0.5
    fill = sig.entry * (1 + slippage) if sig.direction == "long" else sig.entry * (1 - slippage)
    risk_pct = abs(fill - sig.stop) / fill
    if risk_pct <= 0:
        return
    size_usdt = risk_u / risk_pct
    qty_coin = size_usdt / fill
    fee = size_usdt * FEE_RATE
    state.balance -= fee
    state.open_trade = Trade(
        open_ts=ts,
        direction=sig.direction,
        entry=fill,
        stop=sig.stop,
        take_profit=sig.take_profit,
        size_usdt=size_usdt,
        qty_coin=qty_coin,
        rr_planned=sig.rr,
        reasons=list(sig.reasons),
        fees_usdt=fee,
    )
    state.direction_today[_date_key(ts)] = sig.direction


def close_trade(
    state: State,
    ts: pd.Timestamp,
    price: float,
    reason: str,
    inst: str,
    slippage: float = DEFAULT_SLIPPAGE,
    funding_8h: float = DEFAULT_FUNDING_8H,
) -> None:
    t = state.open_trade
    if t is None:
        return
    fill = price * (1 - slippage) if t.direction == "long" else price * (1 + slippage)
    if t.direction == "long":
        gross = (fill - t.entry) * t.qty_coin
    else:
        gross = (t.entry - fill) * t.qty_coin
    fee = (t.qty_coin * fill) * FEE_RATE
    funding_fee = 0.0
    if funding_8h > 0:
        held_h = (ts - t.open_ts).total_seconds() / 3600.0
        n_periods = held_h / 8.0
        # 简化：双向都付（实际上是单边付，这里保守估计）
        funding_fee = t.size_usdt * funding_8h * n_periods
    pnl = gross - fee - funding_fee
    t.close_ts = ts
    t.close_price = fill
    t.close_reason = reason
    t.pnl_usdt = pnl
    t.fees_usdt += fee + funding_fee
    state.balance += pnl
    state.trades.append(t)
    state.open_trade = None

    # 退场红线状态更新
    is_loss = pnl < 0
    if is_loss:
        state.consec_losses += 1
        state.last_loss_ts_by_inst[inst] = ts
    else:
        state.consec_losses = 0
    if state.consec_losses >= 3:
        state.review_day_until = ts + timedelta(hours=24)

    dkey = _date_key(ts)
    state.daily_pnl[dkey] = state.daily_pnl.get(dkey, 0.0) + pnl
    if state.daily_pnl[dkey] / INITIAL_BALANCE_U <= -0.05:
        state.halted_until = ts + timedelta(hours=24)

    wkey = _week_key(ts)
    state.weekly_pnl[wkey] = state.weekly_pnl.get(wkey, 0.0) + pnl
    if state.weekly_pnl[wkey] / INITIAL_BALANCE_U <= -0.10:
        state.weekly_halve_until = ts + timedelta(days=7)


def step_open_position(
    state: State,
    ts: pd.Timestamp,
    high: float,
    low: float,
    inst: str,
    slippage: float = DEFAULT_SLIPPAGE,
    funding_8h: float = DEFAULT_FUNDING_8H,
) -> None:
    """单根 K 内检查是否触发止损 / 止盈。保守先止损。"""
    t = state.open_trade
    if t is None:
        return
    if t.direction == "long":
        hit_sl = low <= t.stop
        hit_tp = high >= t.take_profit
    else:
        hit_sl = high >= t.stop
        hit_tp = low <= t.take_profit
    if hit_sl and hit_tp:
        close_trade(state, ts, t.stop, "sl", inst, slippage, funding_8h)
    elif hit_sl:
        close_trade(state, ts, t.stop, "sl", inst, slippage, funding_8h)
    elif hit_tp:
        close_trade(state, ts, t.take_profit, "tp", inst, slippage, funding_8h)


def run_backtest(
    inst: str,
    df15: pd.DataFrame,
    df1h: pd.DataFrame,
    slippage: float = DEFAULT_SLIPPAGE,
    funding_8h: float = DEFAULT_FUNDING_8H,
) -> tuple[State, pd.DataFrame]:
    df15 = attach_indicators(df15)
    df15 = attach_patterns(df15)
    df15 = attach_swings(df15, window=5)
    df1h = attach_indicators(df1h)
    trend_1h = classify_trend(df1h)
    trend_1h_ts = df1h["ts"]

    state = State()
    equity_rows: list[dict] = []

    for idx in range(len(df15)):
        row = df15.iloc[idx]
        ts = row["ts"]
        # 1. 先处理已有持仓的出场
        if state.open_trade is not None:
            step_open_position(state, ts, float(row["high"]), float(row["low"]), inst, slippage, funding_8h)
        # 2. 评估新进场
        if state.open_trade is None:
            sig = assemble_signal(df15, idx, trend_1h, trend_1h_ts)
            if sig is not None:
                ok, _ = can_open(state, ts, inst, sig.direction)
                if ok:
                    open_trade(state, ts, sig, inst, slippage)
        # 3. 记录 equity
        unrealized = 0.0
        if state.open_trade is not None:
            t = state.open_trade
            mid = float(row["close"])
            if t.direction == "long":
                unrealized = (mid - t.entry) * t.qty_coin
            else:
                unrealized = (t.entry - mid) * t.qty_coin
        equity_rows.append({
            "ts": ts,
            "balance": state.balance,
            "equity": state.balance + unrealized,
        })

    # 收盘强平任何剩余仓位
    if state.open_trade is not None:
        last = df15.iloc[-1]
        close_trade(state, last["ts"], float(last["close"]), "eod", inst, slippage, funding_8h)

    equity_df = pd.DataFrame(equity_rows)
    return state, equity_df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inst", default="BTC-USDT-SWAP")
    ap.add_argument("--data", default=str(Path(__file__).parent / "data"))
    ap.add_argument("--report", default=str(Path(__file__).parent / "reports"))
    args = ap.parse_args()
    data_dir = Path(args.data)
    rep_dir = Path(args.report)
    rep_dir.mkdir(parents=True, exist_ok=True)

    df15 = load_parquet(args.inst, "15m", data_dir)
    df1h = load_parquet(args.inst, "1H", data_dir)
    print(f"loaded 15m={len(df15)} 1H={len(df1h)}")

    state, eq = run_backtest(args.inst, df15, df1h)

    # 直接调 report 模块输出
    from report import write_report
    write_report(state, eq, args.inst, rep_dir)


if __name__ == "__main__":
    main()
