from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


DB_PATH = Path("data/polymarket.db")
BACKTEST_HISTORY_PATH = Path("data/backtest_history.db")
TRACKED_CONFIGS_PATH = Path("tracked_configs.json")

LIVE_TO_BACKTEST_STRATEGY = {
    "Kinetic Sniper": "kinetic_sniper",
    "1-Min Mom": "momentum",
    "Mid-Game Arb": "mid_arb",
    "OTM Bargain": "otm",
    "L2 Spread Scalper": "l2_spread_scalper",
    "Volatility Mean Reversion": "vol_mean_reversion",
    "Mean Reversion OBI": "mean_reversion_obi",
    "Spread Compression": "spread_compression",
    "Divergence Imbalance": "divergence_imbalance",
    "OBI Acceleration": "obi_acceleration",
    "Volatility Compression Breakout": "volatility_compression_breakout",
    "Absorption Pattern": "absorption_pattern",
    "Cross Market Spillover": "cross_market_spillover",
    "Session Based Edge": "session_based_edge",
    "Settlement Convergence": "settlement_convergence",
    "Liquidity Vacuum": "liquidity_vacuum",
    "Micro Pullback Continuation": "micro_pullback_continuation",
    "Synthetic Arbitrage": "synthetic_arbitrage",
}

BACKTEST_TO_LIVE_STRATEGY = {value: key for key, value in LIVE_TO_BACKTEST_STRATEGY.items()}


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def normalize_strategy_key(value: str | None) -> str:
    if not value:
        return ""
    stripped = value.strip()
    if stripped in LIVE_TO_BACKTEST_STRATEGY:
        return LIVE_TO_BACKTEST_STRATEGY[stripped]
    return stripped.lower().replace("-", " ").replace("/", " ").replace(" ", "_")


def get_latest_session_id(db_path: Path | str = DB_PATH) -> str | None:
    db_path = Path(db_path)
    if not db_path.exists():
        return None

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT session_id FROM trade_logs_v10 "
            "WHERE session_id IS NOT NULL "
            "GROUP BY session_id ORDER BY MAX(COALESCE(exit_time, entry_time)) DESC LIMIT 1"
        ).fetchone()
        if row and row[0]:
            return row[0]
        row = conn.execute(
            "SELECT session_id FROM market_logs_v11 "
            "WHERE session_id IS NOT NULL ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def load_tracked_configs(path: Path | str = TRACKED_CONFIGS_PATH) -> dict[str, dict]:
    path = Path(path)
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {market_key(item): item for item in payload}


def load_best_backtest_configs(db_path: Path | str = BACKTEST_HISTORY_PATH) -> dict[str, dict[str, dict]]:
    db_path = Path(db_path)
    if not db_path.exists():
        return {}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT symbol, timeframe, strategy, parameters, pnl_percent, win_rate, trades_count "
            "FROM optimization_logs "
            "ORDER BY symbol, timeframe, strategy, pnl_percent DESC, win_rate DESC, trades_count DESC"
        ).fetchall()
    finally:
        conn.close()

    best: dict[str, dict[str, dict]] = {}
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        key = (row["symbol"], row["timeframe"], row["strategy"])
        if key in seen:
            continue
        seen.add(key)
        market = f"{row['symbol']}_{row['timeframe']}"
        params = json.loads(row["parameters"])
        params["wr"] = round(float(row["win_rate"] or 0.0), 1)
        params["_pnl_percent"] = float(row["pnl_percent"] or 0.0)
        params["_trades_count"] = int(row["trades_count"] or 0)
        best.setdefault(market, {})[row["strategy"]] = params
    return best


def market_key(cfg: dict) -> str:
    return f"{cfg['symbol']}_{cfg['timeframe']}"


def load_trades(
    db_path: Path | str = DB_PATH,
    session_id: str | None = None,
) -> pd.DataFrame:
    db_path = Path(db_path)
    if not db_path.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(db_path)
    try:
        if session_id:
            df = pd.read_sql_query(
                "SELECT * FROM trade_logs_v10 WHERE session_id = ? ORDER BY entry_time ASC",
                conn,
                params=(session_id,),
            )
        else:
            df = pd.read_sql_query("SELECT * FROM trade_logs_v10 ORDER BY entry_time ASC", conn)
    finally:
        conn.close()

    if df.empty:
        return df

    df["strategy_key"] = df["strategy"].apply(normalize_strategy_key)
    df["entry_dt"] = pd.to_datetime(df["entry_time"], errors="coerce")
    df["exit_dt"] = pd.to_datetime(df["exit_time"], errors="coerce")
    df["entry_return_pct"] = np.where(
        df["entry_price"].fillna(0.0) > 0,
        ((df["exit_price"].fillna(0.0) - df["entry_price"].fillna(0.0)) / df["entry_price"].fillna(1.0)) * 100.0,
        0.0,
    )
    return df


def load_market_data(
    db_path: Path | str = DB_PATH,
    session_id: str | None = None,
    all_history: bool = False,
) -> pd.DataFrame:
    db_path = Path(db_path)
    if not db_path.exists():
        return pd.DataFrame()

    effective_session = None if all_history else (session_id or get_latest_session_id(db_path))
    conn = sqlite3.connect(db_path)
    try:
        if effective_session:
            df = pd.read_sql_query(
                "SELECT * FROM market_logs_v11 "
                "WHERE session_id = ? AND (buy_up > 0 OR buy_down > 0) "
                "ORDER BY fetched_at ASC",
                conn,
                params=(effective_session,),
            )
        else:
            df = pd.read_sql_query(
                "SELECT * FROM market_logs_v11 "
                "WHERE (buy_up > 0 OR buy_down > 0) "
                "ORDER BY fetched_at ASC",
                conn,
            )
    finally:
        conn.close()

    if df.empty:
        return df

    df["fetched_at"] = pd.to_datetime(df["fetched_at"], format="ISO8601", errors="coerce")
    timeframe_parts = df["timeframe"].str.split("_", n=1, expand=True)
    df["symbol"] = timeframe_parts[0]
    df["interval_label"] = timeframe_parts[1]
    df["session_hour"] = df["fetched_at"].dt.hour
    df["session_minute"] = df["fetched_at"].dt.minute
    df["up_spread"] = (df["sell_up"] - df["buy_up"]).clip(lower=0.0)
    df["dn_spread"] = (df["sell_down"] - df["buy_down"]).clip(lower=0.0)
    df["up_total_vol"] = df["buy_up_vol"] + df["sell_up_vol"]
    df["dn_total_vol"] = df["buy_down_vol"] + df["sell_down_vol"]
    df["up_vol_imbalance"] = (df["buy_up_vol"] - df["sell_up_vol"]) / (df["up_total_vol"].replace(0, np.nan))
    df["dn_vol_imbalance"] = (df["buy_down_vol"] - df["sell_down_vol"]) / (df["dn_total_vol"].replace(0, np.nan))
    df["up_vol_imbalance"] = df["up_vol_imbalance"].fillna(0.0)
    df["dn_vol_imbalance"] = df["dn_vol_imbalance"].fillna(0.0)
    max_times = df.groupby("market_id")["fetched_at"].transform("max")
    min_times = df.groupby("market_id")["fetched_at"].transform("min")
    df["sec_left"] = (max_times - df["fetched_at"]).dt.total_seconds()
    df["sec_since_start"] = (df["fetched_at"] - min_times).dt.total_seconds()
    df["asset_jump"] = df.groupby("market_id")["live_price"].diff()
    df["up_change"] = df.groupby("market_id")["buy_up"].diff().abs().fillna(0.0)
    df["dn_change"] = df.groupby("market_id")["buy_down"].diff().abs().fillna(0.0)
    df["live_pct_change"] = df.groupby("market_id")["live_price"].pct_change().fillna(0.0)
    last_ticks = df.groupby("market_id").last()
    winning_up_markets = last_ticks[last_ticks["live_price"] >= last_ticks["target_price"]].index.tolist()
    df["won_up"] = df["market_id"].isin(winning_up_markets)
    return df


def prepare_fast_markets(df_markets: pd.DataFrame) -> list[dict]:
    markets: list[dict] = []
    ordered = df_markets.sort_values(["fetched_at", "market_id"])
    for market_id, group in ordered.groupby("market_id", sort=False):
        timestamps = group["fetched_at"].astype("int64") / 1_000_000_000
        markets.append(
            {
                "m_id": market_id,
                "symbol": group["symbol"].iloc[0],
                "interval": group["interval_label"].iloc[0],
                "session_id": group["session_id"].iloc[0] if "session_id" in group.columns else "",
                "target": group["target_price"].iloc[0],
                "won_up": bool(group["won_up"].iloc[0]),
                "fetched_at": group["fetched_at"].tolist(),
                "fetched_ts": timestamps.to_numpy(),
                "sec_left": group["sec_left"].to_numpy(),
                "sec_since_start": group["sec_since_start"].to_numpy(),
                "live": group["live_price"].to_numpy(),
                "live_pct_change": group["live_pct_change"].to_numpy(),
                "up_change": group["up_change"].to_numpy(),
                "dn_change": group["dn_change"].to_numpy(),
                "b_up": group["buy_up"].to_numpy(),
                "b_dn": group["buy_down"].to_numpy(),
                "s_up": group["sell_up"].to_numpy(),
                "s_dn": group["sell_down"].to_numpy(),
                "buy_up_vol": group["buy_up_vol"].to_numpy(),
                "sell_up_vol": group["sell_up_vol"].to_numpy(),
                "buy_down_vol": group["buy_down_vol"].to_numpy(),
                "sell_down_vol": group["sell_down_vol"].to_numpy(),
                "up_obi": group["up_obi"].to_numpy(),
                "dn_obi": group["dn_obi"].to_numpy(),
                "up_spread": group["up_spread"].to_numpy(),
                "dn_spread": group["dn_spread"].to_numpy(),
                "up_total_vol": group["up_total_vol"].to_numpy(),
                "dn_total_vol": group["dn_total_vol"].to_numpy(),
                "up_vol_imbalance": group["up_vol_imbalance"].to_numpy(),
                "dn_vol_imbalance": group["dn_vol_imbalance"].to_numpy(),
            }
        )
    return markets
