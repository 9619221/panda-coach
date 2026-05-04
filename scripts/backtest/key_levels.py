"""15m 关键位识别（简化版）。

V3.2 §2.1 三类共识点：
- 水平：前高 / 前低 / 极值点 / 反复横盘区
- 均线：EMA20 / EMA144-169 双隧道（已在 indicators 里算好）
- 通道线：趋势线 / 楔形 / FVG —— **跳过**（人眼判断为主，工程量太大）

本模块只做「水平关键位」—— swing high / swing low（fractal）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def find_swings(df: pd.DataFrame, window: int = 5) -> tuple[pd.Series, pd.Series]:
    """fractal 法找 swing high / low。
    swing high: 该 K 的 high 严格大于前后 window 根 K 的 high。
    返回两列布尔序列（True 表示该位置是 swing 极值）。
    """
    h = df["high"].values
    l = df["low"].values
    n = len(df)
    sh = np.zeros(n, dtype=bool)
    sl = np.zeros(n, dtype=bool)
    for i in range(window, n - window):
        if h[i] == max(h[i - window : i + window + 1]) and h[i] > h[i - 1]:
            sh[i] = True
        if l[i] == min(l[i - window : i + window + 1]) and l[i] < l[i - 1]:
            sl[i] = True
    return pd.Series(sh, index=df.index), pd.Series(sl, index=df.index)


def attach_swings(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    out = df.copy()
    sh, sl = find_swings(out, window=window)
    out["swing_high"] = sh
    out["swing_low"] = sl
    return out


def nearest_resistance(df: pd.DataFrame, idx: int, lookback: int = 200) -> float | None:
    """从 idx 往前 lookback 根，找到最近一个 swing_high 价位（高于当前 close 的最低 swing high）。"""
    start = max(0, idx - lookback)
    seg = df.iloc[start:idx]
    cur_close = df["close"].iloc[idx]
    highs = seg.loc[seg["swing_high"], "high"]
    above = highs[highs > cur_close]
    if above.empty:
        return None
    return float(above.min())


def nearest_support(df: pd.DataFrame, idx: int, lookback: int = 200) -> float | None:
    start = max(0, idx - lookback)
    seg = df.iloc[start:idx]
    cur_close = df["close"].iloc[idx]
    lows = seg.loc[seg["swing_low"], "low"]
    below = lows[lows < cur_close]
    if below.empty:
        return None
    return float(below.max())


def near_key_level(price: float, level: float | None, tolerance: float = 0.005) -> bool:
    """price 是否在 level 的 ±tolerance（默认 0.5%）范围内。"""
    if level is None:
        return False
    return abs(price - level) / level <= tolerance
