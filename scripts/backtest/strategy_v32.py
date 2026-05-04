"""V3.2 五步骨架的程序化实现 —— 入场信号 + 出场计划。

诚实声明（README 同步）：
- Step 0 道：只用「数学正期望」、「不预测」原则 → 通过硬规体现
- Step 1 趋势：1H 均线法（已机械化）；初/中/末期 → 跳过
- Step 2 关键位：水平 swing + EMA 双隧道；颈线 / FVG / 通道 → 跳过
- Step 3 信号：4 信号族中实现 2 族 —— 12 金K + 均线（金叉死叉）
  + 必2加2：必2 = 关键位 + swing 极值；加2 = 多族共振 + 顺大趋势
  + 假突破过滤：3K 不反 + 量能放大
  + 妃子规则（缠论/谐波/SMC/波浪） → 全部跳过
- Step 4 计划：硬规 R:R ≥ 1:1.5、止损 = 关键位外 0.3%、止盈 = 反向阻/支
- Step 5 执行：保二争五 → 三档止盈（T1 平 75% 移止损到开仓价 / T2 全平）
- 退场红线：日 5% / 周 10% / 连 3 → backtest.py 状态机
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from key_levels import nearest_resistance, nearest_support, near_key_level
from trend import BULL, BEAR, RANGE, trend_at


@dataclass
class Signal:
    direction: str          # "long" / "short"
    entry: float            # 入场价（用 close 近似限价单成交）
    stop: float             # 止损价
    take_profit: float      # T1 止盈价
    rr: float               # R:R
    reasons: list[str]      # 进场理由（≥2 条 V3.2 §4 要求）
    score: int              # 加分项数量（仅供分析）


# 单笔风险（U）— V3.2 §0：本金 31.5 U, 单笔 ≤ 1.5 U
RISK_PER_TRADE_U = 1.5
INITIAL_BALANCE_U = 31.5

# 硬规
MIN_RR = 1.5                  # V3.2 §2.4
STOP_BUFFER = 0.003           # 止损距关键位 0.3%（V3.2 §2.4 「外侧 0.2%」）
TP_TARGET_RR = 2.0            # 标准目标 1:2（V3.2 §2.4）
KEY_LEVEL_TOLERANCE = 0.005   # 价格在关键位 ±0.5% 视为「附近」


def assemble_signal(
    df15: pd.DataFrame,
    idx: int,
    trend_1h: pd.Series,
    trend_1h_ts: pd.Series,
) -> Signal | None:
    """对 15m 第 idx 根 K 评估是否进场。返回 Signal 或 None。

    df15 必须已经 attach_indicators + attach_patterns + attach_swings。
    trend_1h / trend_1h_ts 由 1H 数据生成，用于反查参考周期趋势。
    """
    if idx < 200:                 # 至少 200 根做历史
        return None
    row = df15.iloc[idx]
    ts = row["ts"]
    close = float(row["close"])

    # ------ Step 1：参考周期趋势硬闸 ------
    big = trend_at(ts, trend_1h, trend_1h_ts)
    if big == RANGE:              # V3.2 §1.1 震荡直接 STOP
        return None

    # ------ Step 1.5：交易周期 ATR 也不能太死 ------
    if pd.isna(row.get("atr_pct")) or row["atr_pct"] < 0.003:
        return None

    # ------ Step 2：找关键位（必2 之一）------
    direction = "long" if big == BULL else "short"
    if direction == "long":
        support = nearest_support(df15, idx, lookback=300)
        if not near_key_level(close, support, KEY_LEVEL_TOLERANCE):
            return None
        # 找上方阻力 = T1
        resistance = nearest_resistance(df15, idx, lookback=300)
        if resistance is None:
            return None
        # 关键位
        key_level = support
        # ------ Step 2.4：止损 / 止盈 ------
        stop = key_level * (1 - STOP_BUFFER)
        risk = close - stop
        if risk <= 0:
            return None
        take_profit_kl = resistance
        rr_kl = (take_profit_kl - close) / risk
        # 用关键位 vs 标准 1:2，取大者作为目标，但至少需 ≥ 1.5
        rr_std = TP_TARGET_RR
        rr = max(rr_kl, rr_std) if rr_kl >= MIN_RR else rr_std
        take_profit = close + risk * rr
        if rr < MIN_RR:
            return None
    else:  # short
        resistance = nearest_resistance(df15, idx, lookback=300)
        if not near_key_level(close, resistance, KEY_LEVEL_TOLERANCE):
            return None
        support = nearest_support(df15, idx, lookback=300)
        if support is None:
            return None
        key_level = resistance
        stop = key_level * (1 + STOP_BUFFER)
        risk = stop - close
        if risk <= 0:
            return None
        take_profit_kl = support
        rr_kl = (close - take_profit_kl) / risk
        rr_std = TP_TARGET_RR
        rr = max(rr_kl, rr_std) if rr_kl >= MIN_RR else rr_std
        take_profit = close - risk * rr
        if rr < MIN_RR:
            return None

    # ------ Step 3：入场信号（必2 之二 = 在关键位附近 + 12 金K） ------
    reasons: list[str] = []
    score = 0

    if direction == "long":
        if not bool(row["pat_bull_any"]):
            return None
        # 哪根金K
        for col, name in (
            ("pat_bull_engulf", "看涨吞没"),
            ("pat_hammer", "锤头"),
            ("pat_morning", "启明星"),
            ("pat_piercing", "刺透"),
        ):
            if bool(row[col]):
                reasons.append(name)
                break
        reasons.append("接近支撑位")
        # 加分项
        if bool(row.get("swing_low", False)) or bool(df15["swing_low"].iloc[max(0, idx - 3) : idx + 1].any()):
            score += 1
            reasons.append("近期 swing 低点")
        # 顺大趋势（已校验 BULL）
        score += 1
        reasons.append("顺 1H 多头趋势")
        # 假突破过滤：前 3 根至少有 1 根反弹（不一直破位）
        if idx >= 3:
            prev3_low = df15["low"].iloc[idx - 3 : idx].min()
            if close > prev3_low * 1.001:
                score += 1
                reasons.append("非破位延续")
        # 量能放大
        if idx >= 20:
            vol_ma = df15["vol"].iloc[idx - 20 : idx].mean()
            if vol_ma > 0 and row["vol"] > 1.3 * vol_ma:
                score += 1
                reasons.append("放量")
    else:  # short
        if not bool(row["pat_bear_any"]):
            return None
        for col, name in (
            ("pat_bear_engulf", "看跌吞没"),
            ("pat_shoot_star", "流星"),
            ("pat_evening", "黄昏星"),
            ("pat_dark_cloud", "乌云盖顶"),
        ):
            if bool(row[col]):
                reasons.append(name)
                break
        reasons.append("接近压力位")
        if bool(row.get("swing_high", False)) or bool(df15["swing_high"].iloc[max(0, idx - 3) : idx + 1].any()):
            score += 1
            reasons.append("近期 swing 高点")
        score += 1
        reasons.append("顺 1H 空头趋势")
        if idx >= 3:
            prev3_high = df15["high"].iloc[idx - 3 : idx].max()
            if close < prev3_high * 0.999:
                score += 1
                reasons.append("非破位延续")
        if idx >= 20:
            vol_ma = df15["vol"].iloc[idx - 20 : idx].mean()
            if vol_ma > 0 and row["vol"] > 1.3 * vol_ma:
                score += 1
                reasons.append("放量")

    # 必2 加 2：必 2（关键位 + 金K）已满足；加 2 中至少 1 项
    if score < 1:
        return None
    if len(reasons) < 2:
        return None

    return Signal(
        direction=direction,
        entry=close,
        stop=stop,
        take_profit=take_profit,
        rr=rr,
        reasons=reasons,
        score=score,
    )
