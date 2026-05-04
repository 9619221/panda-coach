"""1H 参考周期趋势判断 + ATR 震荡过滤。

V3.2 §1：四大判断方法选一研究透。本回测用「均线」法（裸K + 形态法不易程序化）。
- 多头：close > EMA20 > EMA60，且 EMA20 上行
- 空头：close < EMA20 < EMA60，且 EMA20 下行
- 震荡：ATR/价 < 2%（V3.2 §1.5 原文）或 EMA 缠绕
"""
from __future__ import annotations

import pandas as pd

# 趋势状态常量
BULL = "bull"
BEAR = "bear"
RANGE = "range"


def classify_trend(df_1h: pd.DataFrame, atr_threshold: float = 0.005) -> pd.Series:
    """对 1H K 线给出每根 K 当时的趋势标签 (bull/bear/range)。

    V3.2 §1.5 原文「ATR/价 < 2% → 震荡」是日线口径；1H 周期 ATR 量级
    小约 4-5 倍，对应阈值降到 0.5% 比较贴 BTC/ETH 主流币的实际波动。
    df_1h 需已经 attach_indicators（含 ema20 / ema60 / atr_pct）。
    """
    close = df_1h["close"]
    e20 = df_1h["ema20"]
    e60 = df_1h["ema60"]
    e20_up = e20.diff() > 0
    e20_dn = e20.diff() < 0
    atr_pct = df_1h["atr_pct"]

    bull = (close > e20) & (e20 > e60) & e20_up
    bear = (close < e20) & (e20 < e60) & e20_dn
    is_range_atr = atr_pct < atr_threshold
    is_range_ema = (e20 - e60).abs() / close < 0.001  # EMA 缠绕

    out = pd.Series(RANGE, index=df_1h.index, dtype="object")
    out[bull] = BULL
    out[bear] = BEAR
    # 震荡硬闸只在「ATR 小 + EMA 缠绕」双条件下覆盖（避免误杀强趋势）
    out[is_range_atr & is_range_ema] = RANGE
    return out


def trend_at(ts: pd.Timestamp, trend_series: pd.Series, ts_series: pd.Series) -> str:
    """给定 15m 时间戳，返回该时刻最近一根已收盘 1H K 的趋势。

    用 searchsorted 二分查找，避免对齐错位。返回 RANGE 当兜底。
    """
    if trend_series.empty:
        return RANGE
    pos = ts_series.searchsorted(ts, side="right") - 1
    if pos < 0:
        return RANGE
    return str(trend_series.iloc[pos])
