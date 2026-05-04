"""诊断信号过滤器衰减 —— 看每一层卡掉多少 K。"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from fetch_data import load_parquet
from indicators import attach_indicators, attach_patterns
from key_levels import attach_swings, nearest_resistance, nearest_support, near_key_level
from strategy_v32 import KEY_LEVEL_TOLERANCE, MIN_RR, STOP_BUFFER, TP_TARGET_RR
from trend import BULL, BEAR, RANGE, classify_trend, trend_at


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inst", default="BTC-USDT-SWAP")
    ap.add_argument("--data", default=str(Path(__file__).parent / "data"))
    args = ap.parse_args()
    data_dir = Path(args.data)

    df15 = load_parquet(args.inst, "15m", data_dir)
    df1h = load_parquet(args.inst, "1H", data_dir)
    df15 = attach_swings(attach_patterns(attach_indicators(df15)), window=5)
    df1h = attach_indicators(df1h)
    trend_1h = classify_trend(df1h)
    trend_1h_ts = df1h["ts"]

    n_total = len(df15)
    n_after_warmup = 0
    n_bull_bg = 0
    n_bear_bg = 0
    n_range_bg = 0
    n_atr_pass = 0
    n_at_support = 0
    n_at_resistance = 0
    n_pat_at_support = 0
    n_pat_at_resistance = 0
    n_rr_ok_long = 0
    n_rr_ok_short = 0

    for idx in range(len(df15)):
        if idx < 200:
            continue
        n_after_warmup += 1
        row = df15.iloc[idx]
        ts = row["ts"]
        big = trend_at(ts, trend_1h, trend_1h_ts)
        if big == BULL:
            n_bull_bg += 1
        elif big == BEAR:
            n_bear_bg += 1
        else:
            n_range_bg += 1
            continue
        if pd.isna(row["atr_pct"]) or row["atr_pct"] < 0.003:
            continue
        n_atr_pass += 1
        close = float(row["close"])
        if big == BULL:
            sup = nearest_support(df15, idx, 300)
            if near_key_level(close, sup, KEY_LEVEL_TOLERANCE):
                n_at_support += 1
                if bool(row["pat_bull_any"]):
                    n_pat_at_support += 1
                    res = nearest_resistance(df15, idx, 300)
                    if res and sup:
                        stop = sup * (1 - STOP_BUFFER)
                        risk = close - stop
                        if risk > 0:
                            rr_kl = (res - close) / risk
                            rr = max(rr_kl, TP_TARGET_RR) if rr_kl >= MIN_RR else TP_TARGET_RR
                            if rr >= MIN_RR:
                                n_rr_ok_long += 1
        else:
            res = nearest_resistance(df15, idx, 300)
            if near_key_level(close, res, KEY_LEVEL_TOLERANCE):
                n_at_resistance += 1
                if bool(row["pat_bear_any"]):
                    n_pat_at_resistance += 1
                    sup = nearest_support(df15, idx, 300)
                    if sup and res:
                        stop = res * (1 + STOP_BUFFER)
                        risk = stop - close
                        if risk > 0:
                            rr_kl = (close - sup) / risk
                            rr = max(rr_kl, TP_TARGET_RR) if rr_kl >= MIN_RR else TP_TARGET_RR
                            if rr >= MIN_RR:
                                n_rr_ok_short += 1

    print(f"K 线总数 = {n_total}")
    print(f"warmup 后 = {n_after_warmup}")
    print(f"  1H 多头背景 = {n_bull_bg} ({n_bull_bg / n_after_warmup:.1%})")
    print(f"  1H 空头背景 = {n_bear_bg} ({n_bear_bg / n_after_warmup:.1%})")
    print(f"  1H 震荡   = {n_range_bg} ({n_range_bg / n_after_warmup:.1%})")
    print(f"ATR 过滤通过 = {n_atr_pass}")
    print(f"接近支撑（多）= {n_at_support}")
    print(f"  + 看涨金K = {n_pat_at_support}")
    print(f"    + R:R≥{MIN_RR} = {n_rr_ok_long}")
    print(f"接近压力（空）= {n_at_resistance}")
    print(f"  + 看跌金K = {n_pat_at_resistance}")
    print(f"    + R:R≥{MIN_RR} = {n_rr_ok_short}")
    print(f"总潜在信号 = {n_rr_ok_long + n_rr_ok_short}")

    # 12 金K 出现频率
    print("\n12 金K 频率：")
    for col in ["pat_bull_engulf", "pat_bear_engulf", "pat_hammer", "pat_shoot_star",
                "pat_morning", "pat_evening", "pat_piercing", "pat_dark_cloud"]:
        print(f"  {col:24s} = {df15[col].sum()}")

    print(f"\nswing_high = {df15['swing_high'].sum()}, swing_low = {df15['swing_low'].sum()}")


if __name__ == "__main__":
    main()
