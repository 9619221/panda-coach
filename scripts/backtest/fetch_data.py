"""从 OKX 公开 REST API 拉历史 K 线，落地到 parquet。

OKX endpoint: GET https://www.okx.com/api/v5/market/history-candles
- 单次返回 ≤ 100 根，from/to 用毫秒时间戳，limit 上限 100
- 公开端点无需 API key
- 文档：https://www.okx.com/docs-v5/en/#public-data-rest-api-get-history-candlesticks
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pandas as pd

OKX_BASE = "https://www.okx.com"
PATH_HISTORY = "/api/v5/market/history-candles"

# bar 周期 -> 毫秒
BAR_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1H": 3_600_000,
    "4H": 14_400_000,
    "1D": 86_400_000,
}


def fetch_chunk(
    client: httpx.Client, inst_id: str, bar: str, after_ms: int | None = None
) -> list[list[str]]:
    """拉一页（≤100 根，按时间倒序，最新在前）。after_ms 表示此时间戳之前的 K。"""
    params = {"instId": inst_id, "bar": bar, "limit": "100"}
    if after_ms is not None:
        params["after"] = str(after_ms)
    r = client.get(OKX_BASE + PATH_HISTORY, params=params, timeout=30)
    r.raise_for_status()
    body = r.json()
    if body.get("code") != "0":
        raise RuntimeError(f"OKX error: {body}")
    return body.get("data", [])


def fetch_range(inst_id: str, bar: str, start: datetime, end: datetime) -> pd.DataFrame:
    """从 start 到 end 全量拉，自动分页。返回按时间正序的 DataFrame。"""
    if bar not in BAR_MS:
        raise ValueError(f"unsupported bar {bar}")
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    rows: list[list[str]] = []
    cursor = end_ms
    with httpx.Client(headers={"User-Agent": "panda-backtest/0.1"}) as client:
        while True:
            page = fetch_chunk(client, inst_id, bar, after_ms=cursor)
            if not page:
                break
            rows.extend(page)
            oldest_ms = int(page[-1][0])
            cursor = oldest_ms
            if oldest_ms <= start_ms:
                break
            time.sleep(0.12)
    if not rows:
        return pd.DataFrame()

    cols = ["ts", "open", "high", "low", "close", "vol", "vol_ccy", "vol_ccy_quote", "confirm"]
    df = pd.DataFrame(rows, columns=cols)
    df["ts"] = pd.to_datetime(df["ts"].astype("int64"), unit="ms", utc=True)
    for c in ("open", "high", "low", "close", "vol", "vol_ccy", "vol_ccy_quote"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values("ts").drop_duplicates("ts").reset_index(drop=True)
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    df = df[(df["ts"] >= start_ts) & (df["ts"] <= end_ts)]
    return df.reset_index(drop=True)


def save_parquet(df: pd.DataFrame, inst_id: str, bar: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{inst_id.replace('-', '_')}_{bar}.parquet"
    path = out_dir / fname
    df.to_parquet(path, index=False)
    return path


def load_parquet(inst_id: str, bar: str, out_dir: Path) -> pd.DataFrame:
    fname = f"{inst_id.replace('-', '_')}_{bar}.parquet"
    return pd.read_parquet(out_dir / fname)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inst", default="BTC-USDT-SWAP")
    ap.add_argument("--bar", default="15m", choices=list(BAR_MS.keys()))
    ap.add_argument("--days", type=int, default=365, help="向回拉多少天")
    ap.add_argument("--out", default=str(Path(__file__).parent / "data"))
    args = ap.parse_args()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)
    print(f"fetch {args.inst} {args.bar} from {start.date()} to {end.date()}")
    df = fetch_range(args.inst, args.bar, start, end)
    print(f"got {len(df)} rows, range = {df['ts'].min()} ~ {df['ts'].max()}")
    p = save_parquet(df, args.inst, args.bar, Path(args.out))
    print(f"saved: {p}")


if __name__ == "__main__":
    main()
