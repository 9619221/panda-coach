"""技术指标 + 12 金K 形态识别。

V3.2 对齐：
- EMA20 / EMA60 / EMA144 / EMA169（趋势 + Vegas 隧道）
- ATR(14) → 震荡过滤（ATR/价 < 2% 标记震荡）
- 12 金K 中可机械化的 6 个：
  看涨：启明星(MorningStar) / 看涨吞没(BullishEngulfing) / 锤头(Hammer) / 刺透(Piercing)
  看跌：黄昏星(EveningStar) / 看跌吞没(BearishEngulfing) / 流星(ShootingStar) / 乌云盖顶(DarkCloud)
- 跳过：白三兵 / 三只乌鸦 / 上吊 / 倒锤（要么主观要么和锤头/流星重合）
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# ---------- 基础指标 ----------

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    down = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = up / down.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def attach_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """给 K 线 DataFrame 附加常用指标列。原地不动，返回新 df。"""
    out = df.copy()
    out["ema20"] = ema(out["close"], 20)
    out["ema60"] = ema(out["close"], 60)
    out["ema144"] = ema(out["close"], 144)
    out["ema169"] = ema(out["close"], 169)
    out["atr14"] = atr(out, 14)
    out["atr_pct"] = out["atr14"] / out["close"]  # ATR/价
    out["rsi14"] = rsi(out["close"], 14)
    out["body"] = (out["close"] - out["open"]).abs()
    out["range"] = out["high"] - out["low"]
    out["upper_wick"] = out["high"] - out[["open", "close"]].max(axis=1)
    out["lower_wick"] = out[["open", "close"]].min(axis=1) - out["low"]
    out["is_bull"] = out["close"] > out["open"]
    out["is_bear"] = out["close"] < out["open"]
    return out


# ---------- 12 金K 形态识别（输出布尔列） ----------

def detect_bullish_engulfing(df: pd.DataFrame) -> pd.Series:
    """看涨吞没：前一根阴 K，当前阳 K 实体完全吞没前一根实体。"""
    prev_open = df["open"].shift(1)
    prev_close = df["close"].shift(1)
    prev_bear = prev_close < prev_open
    cur_bull = df["close"] > df["open"]
    engulf = (df["open"] <= prev_close) & (df["close"] >= prev_open)
    return prev_bear & cur_bull & engulf


def detect_bearish_engulfing(df: pd.DataFrame) -> pd.Series:
    prev_open = df["open"].shift(1)
    prev_close = df["close"].shift(1)
    prev_bull = prev_close > prev_open
    cur_bear = df["close"] < df["open"]
    engulf = (df["open"] >= prev_close) & (df["close"] <= prev_open)
    return prev_bull & cur_bear & engulf


def detect_hammer(df: pd.DataFrame) -> pd.Series:
    """锤头：实体小、下影长（≥2 倍实体），上影短（≤实体）。位置过滤交给上层。"""
    body = df["body"]
    rng = df["range"].replace(0, np.nan)
    return (
        (df["lower_wick"] >= 2 * body)
        & (df["upper_wick"] <= body)
        & (body / rng <= 0.35)
        & (body > 0)
    )


def detect_shooting_star(df: pd.DataFrame) -> pd.Series:
    body = df["body"]
    rng = df["range"].replace(0, np.nan)
    return (
        (df["upper_wick"] >= 2 * body)
        & (df["lower_wick"] <= body)
        & (body / rng <= 0.35)
        & (body > 0)
    )


def detect_morning_star(df: pd.DataFrame) -> pd.Series:
    """启明星：3 根 K — 长阴、十字/小实体、长阳收复 ≥ 第一根 50%。"""
    o0, c0 = df["open"].shift(2), df["close"].shift(2)
    o1, c1 = df["open"].shift(1), df["close"].shift(1)
    o2, c2 = df["open"], df["close"]
    body0 = (o0 - c0).abs()
    body1 = (o1 - c1).abs()
    body2 = (c2 - o2).abs()

    bar0_long_bear = (c0 < o0) & (body0 > body0.rolling(20).mean())
    bar1_small = body1 < 0.4 * body0
    bar2_long_bull = (c2 > o2) & (body2 > 0.5 * body0)
    recover = c2 > (o0 + c0) / 2
    return bar0_long_bear & bar1_small & bar2_long_bull & recover


def detect_evening_star(df: pd.DataFrame) -> pd.Series:
    o0, c0 = df["open"].shift(2), df["close"].shift(2)
    o1, c1 = df["open"].shift(1), df["close"].shift(1)
    o2, c2 = df["open"], df["close"]
    body0 = (c0 - o0).abs()
    body1 = (o1 - c1).abs()
    body2 = (o2 - c2).abs()

    bar0_long_bull = (c0 > o0) & (body0 > body0.rolling(20).mean())
    bar1_small = body1 < 0.4 * body0
    bar2_long_bear = (c2 < o2) & (body2 > 0.5 * body0)
    drop = c2 < (o0 + c0) / 2
    return bar0_long_bull & bar1_small & bar2_long_bear & drop


def detect_piercing(df: pd.DataFrame) -> pd.Series:
    """刺透：前阴 K，当前阳 K 开盘低于前低、收盘高于前 K 中点但低于前 K 开盘。"""
    o0, c0 = df["open"].shift(1), df["close"].shift(1)
    o1, c1 = df["open"], df["close"]
    prev_bear = c0 < o0
    cur_bull = c1 > o1
    open_below = o1 < c0
    close_above_mid = c1 > (o0 + c0) / 2
    close_below_open = c1 < o0
    return prev_bear & cur_bull & open_below & close_above_mid & close_below_open


def detect_dark_cloud(df: pd.DataFrame) -> pd.Series:
    o0, c0 = df["open"].shift(1), df["close"].shift(1)
    o1, c1 = df["open"], df["close"]
    prev_bull = c0 > o0
    cur_bear = c1 < o1
    open_above = o1 > c0
    close_below_mid = c1 < (o0 + c0) / 2
    close_above_open = c1 > o0
    return prev_bull & cur_bear & open_above & close_below_mid & close_above_open


def attach_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """给 df 附加 8 个金K 布尔列。"""
    out = df.copy()
    out["pat_bull_engulf"] = detect_bullish_engulfing(out)
    out["pat_bear_engulf"] = detect_bearish_engulfing(out)
    out["pat_hammer"] = detect_hammer(out)
    out["pat_shoot_star"] = detect_shooting_star(out)
    out["pat_morning"] = detect_morning_star(out)
    out["pat_evening"] = detect_evening_star(out)
    out["pat_piercing"] = detect_piercing(out)
    out["pat_dark_cloud"] = detect_dark_cloud(out)
    out["pat_bull_any"] = (
        out["pat_bull_engulf"]
        | out["pat_hammer"]
        | out["pat_morning"]
        | out["pat_piercing"]
    )
    out["pat_bear_any"] = (
        out["pat_bear_engulf"]
        | out["pat_shoot_star"]
        | out["pat_evening"]
        | out["pat_dark_cloud"]
    )
    return out
