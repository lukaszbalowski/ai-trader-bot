from __future__ import annotations

import argparse
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime

import pandas as pd

from analytics_db import (
    BACKTEST_HISTORY_PATH,
    DB_PATH,
    TRACKED_CONFIGS_PATH,
    get_latest_session_id,
    load_best_backtest_configs,
    load_market_data,
    load_tracked_configs,
    load_trades,
    prepare_fast_markets,
)
from trade_decision_review import (
    build_scenarios,
    format_dt,
    load_market_ticks,
    select_best_simple_rule,
)


SUPPORTED_REPLAY_STRATEGIES = ("kinetic_sniper", "momentum", "mid_arb", "otm")
EXECUTION_REPORTS_DIR = Path("data/execution_analysis/reports")


@dataclass
class ReplayTrade:
    market_id: str
    timeframe: str
    symbol: str
    strategy_key: str
    direction: str
    entry_idx: int
    entry_time: object
    entry_price: float
    exit_idx: int
    exit_time: object
    exit_price: float
    return_pct: float
    pnl_usd: float
    exit_reason: str
    session_id: str
    config_source: str


def compute_return_pct(entry_price: float, exit_price: float) -> float:
    if entry_price <= 0:
        return 0.0
    return ((exit_price - entry_price) / entry_price) * 100.0


def resolve_settlement_price(mkt: dict, direction: str) -> float:
    if direction == "UP":
        return 1.0 if mkt["won_up"] else 0.0
    return 0.0 if mkt["won_up"] else 1.0


def make_replay_trade(
    mkt: dict,
    strategy_key: str,
    direction: str,
    entry_idx: int,
    entry_price: float,
    exit_idx: int,
    exit_price: float,
    stake: float,
    exit_reason: str,
    config_source: str,
) -> ReplayTrade:
    shares = stake / entry_price if entry_price > 0 else 0.0
    pnl_usd = (exit_price * shares) - stake
    return ReplayTrade(
        market_id=mkt["m_id"],
        timeframe=f"{mkt['symbol']}_{mkt['interval']}",
        symbol=mkt["symbol"],
        strategy_key=strategy_key,
        direction=direction,
        entry_idx=entry_idx,
        entry_time=mkt["fetched_at"][entry_idx],
        entry_price=float(entry_price),
        exit_idx=exit_idx,
        exit_time=mkt["fetched_at"][exit_idx],
        exit_price=float(exit_price),
        return_pct=compute_return_pct(float(entry_price), float(exit_price)),
        pnl_usd=float(pnl_usd),
        exit_reason=exit_reason,
        session_id=mkt.get("session_id", ""),
        config_source=config_source,
    )


def simulate_kinetic_trade(
    mkt: dict,
    entry_idx: int,
    direction: str,
    entry_price: float,
    stake: float,
    global_sec_rule: float,
    config_source: str,
) -> ReplayTrade:
    ticks_down = 0
    last_bid = entry_price
    for tick in range(entry_idx + 1, len(mkt["sec_left"])):
        sec_left = mkt["sec_left"][tick]
        current_bid = mkt["b_up"][tick] if direction == "UP" else mkt["b_dn"][tick]
        if current_bid <= 0:
            continue
        if sec_left <= global_sec_rule and current_bid > entry_price:
            return make_replay_trade(
                mkt,
                "kinetic_sniper",
                direction,
                entry_idx,
                entry_price,
                tick,
                current_bid,
                stake,
                "replay_secure_before_expiry",
                config_source,
            )
        if current_bid >= entry_price * 1.50:
            return make_replay_trade(
                mkt,
                "kinetic_sniper",
                direction,
                entry_idx,
                entry_price,
                tick,
                current_bid,
                stake,
                "replay_kinetic_tp_50",
                config_source,
            )
        if tick - entry_idx >= 10 and current_bid > entry_price:
            return make_replay_trade(
                mkt,
                "kinetic_sniper",
                direction,
                entry_idx,
                entry_price,
                tick,
                current_bid,
                stake,
                "replay_kinetic_sweep_10_ticks",
                config_source,
            )
        if current_bid < last_bid and current_bid > entry_price:
            ticks_down += 1
        elif current_bid > last_bid:
            ticks_down = 0
        if ticks_down >= 2 and current_bid > entry_price:
            return make_replay_trade(
                mkt,
                "kinetic_sniper",
                direction,
                entry_idx,
                entry_price,
                tick,
                current_bid,
                stake,
                "replay_kinetic_reversal_2_down",
                config_source,
            )
        if current_bid <= entry_price * 0.90:
            exit_tick = min(tick + 10, len(mkt["sec_left"]) - 1)
            exit_bid = mkt["b_up"][exit_tick] if direction == "UP" else mkt["b_dn"][exit_tick]
            if exit_bid <= 0:
                exit_bid = resolve_settlement_price(mkt, direction)
            return make_replay_trade(
                mkt,
                "kinetic_sniper",
                direction,
                entry_idx,
                entry_price,
                exit_tick,
                exit_bid,
                stake,
                "replay_kinetic_timeout_sl",
                config_source,
            )
        last_bid = current_bid

    settlement_price = resolve_settlement_price(mkt, direction)
    return make_replay_trade(
        mkt,
        "kinetic_sniper",
        direction,
        entry_idx,
        entry_price,
        len(mkt["sec_left"]) - 1,
        settlement_price,
        stake,
        "replay_resolution_fallback",
        config_source,
    )


def simulate_simple_trade(
    mkt: dict,
    strategy_key: str,
    entry_idx: int,
    direction: str,
    entry_price: float,
    stake: float,
    global_sec_rule: float,
    config_source: str,
) -> ReplayTrade:
    for tick in range(entry_idx + 1, len(mkt["sec_left"])):
        sec_left = mkt["sec_left"][tick]
        current_bid = mkt["b_up"][tick] if direction == "UP" else mkt["b_dn"][tick]
        if current_bid <= 0:
            continue
        if sec_left <= global_sec_rule and current_bid > entry_price:
            return make_replay_trade(
                mkt,
                strategy_key,
                direction,
                entry_idx,
                entry_price,
                tick,
                current_bid,
                stake,
                "replay_secure_before_expiry",
                config_source,
            )
        if current_bid >= entry_price * 3.0:
            return make_replay_trade(
                mkt,
                strategy_key,
                direction,
                entry_idx,
                entry_price,
                tick,
                current_bid,
                stake,
                "replay_global_tp_200",
                config_source,
            )

    settlement_price = resolve_settlement_price(mkt, direction)
    return make_replay_trade(
        mkt,
        strategy_key,
        direction,
        entry_idx,
        entry_price,
        len(mkt["sec_left"]) - 1,
        settlement_price,
        stake,
        "replay_resolution_fallback",
        config_source,
    )


def replay_market_strategy(mkt: dict, strategy_key: str, params: dict, config_source: str) -> ReplayTrade | None:
    stake = 1.0
    if strategy_key == "kinetic_sniper":
        trigger_pct = float(params.get("trigger_pct", 0.0))
        max_price = float(params.get("max_price", 0.85))
        max_slippage = float(params.get("max_slippage", 0.025))
        max_target_dist = float(params.get("max_target_dist", float("inf")))
        global_sec_rule = float(params.get("g_sec", 3.0))
        window_sec = max(float(params.get("window_ms", 1000)) / 1000.0, 0.001)
        start_idx = 0
        for tick in range(len(mkt["sec_left"])):
            if mkt["sec_left"][tick] <= 10:
                break
            while start_idx < tick and (mkt["fetched_ts"][tick] - mkt["fetched_ts"][start_idx]) > window_sec:
                start_idx += 1
            oldest_price = mkt["live"][start_idx]
            if oldest_price <= 0:
                continue
            if abs(mkt["live"][tick] - mkt["target"]) > max_target_dist:
                continue
            price_delta_pct = (mkt["live"][tick] - oldest_price) / oldest_price
            if (
                price_delta_pct >= trigger_pct
                and abs(mkt["up_change"][tick]) <= max_slippage
                and 0 < mkt["b_up"][tick] <= max_price
            ):
                return simulate_kinetic_trade(
                    mkt,
                    tick,
                    "UP",
                    float(mkt["b_up"][tick]),
                    stake,
                    global_sec_rule,
                    config_source,
                )
            if (
                price_delta_pct <= -trigger_pct
                and abs(mkt["dn_change"][tick]) <= max_slippage
                and 0 < mkt["b_dn"][tick] <= max_price
            ):
                return simulate_kinetic_trade(
                    mkt,
                    tick,
                    "DOWN",
                    float(mkt["b_dn"][tick]),
                    stake,
                    global_sec_rule,
                    config_source,
                )
        return None

    if strategy_key == "momentum":
        delta_limit = float(params.get("delta", 0.0))
        max_price = float(params.get("max_p", 0.999))
        win_start = float(params.get("win_start", 0))
        win_end = float(params.get("win_end", 0))
        global_sec_rule = float(params.get("g_sec", 2.0))
        for tick in range(len(mkt["sec_left"])):
            sec_left = mkt["sec_left"][tick]
            if sec_left > win_start:
                continue
            if sec_left < win_end:
                break
            delta = mkt["live"][tick] - mkt["target"]
            if delta >= delta_limit and 0 < mkt["b_up"][tick] <= max_price:
                return simulate_simple_trade(
                    mkt,
                    strategy_key,
                    tick,
                    "UP",
                    float(mkt["b_up"][tick]),
                    stake,
                    global_sec_rule,
                    config_source,
                )
            if delta <= -delta_limit and 0 < mkt["b_dn"][tick] <= max_price:
                return simulate_simple_trade(
                    mkt,
                    strategy_key,
                    tick,
                    "DOWN",
                    float(mkt["b_dn"][tick]),
                    stake,
                    global_sec_rule,
                    config_source,
                )
        return None

    if strategy_key == "mid_arb":
        delta_limit = float(params.get("delta", 0.0))
        max_price = float(params.get("max_p", 0.999))
        win_start = float(params.get("win_start", 0))
        win_end = float(params.get("win_end", 0))
        global_sec_rule = float(params.get("g_sec", 2.0))
        for tick in range(len(mkt["sec_left"])):
            sec_left = mkt["sec_left"][tick]
            if not (win_start > sec_left > win_end):
                continue
            delta = mkt["live"][tick] - mkt["target"]
            if delta > delta_limit and 0 < mkt["b_up"][tick] <= max_price:
                return simulate_simple_trade(
                    mkt,
                    strategy_key,
                    tick,
                    "UP",
                    float(mkt["b_up"][tick]),
                    stake,
                    global_sec_rule,
                    config_source,
                )
            if delta < -delta_limit and 0 < mkt["b_dn"][tick] <= max_price:
                return simulate_simple_trade(
                    mkt,
                    strategy_key,
                    tick,
                    "DOWN",
                    float(mkt["b_dn"][tick]),
                    stake,
                    global_sec_rule,
                    config_source,
                )
        return None

    if strategy_key == "otm":
        max_price = float(params.get("max_p", 0.999))
        win_start = float(params.get("win_start", 0))
        win_end = float(params.get("win_end", 0))
        global_sec_rule = float(params.get("g_sec", 2.0))
        max_delta_abs = float(params.get("max_delta_abs", 40.0))
        for tick in range(len(mkt["sec_left"])):
            sec_left = mkt["sec_left"][tick]
            if not (win_start >= sec_left >= win_end):
                continue
            if abs(mkt["live"][tick] - mkt["target"]) >= max_delta_abs:
                continue
            if 0 < mkt["b_up"][tick] <= max_price:
                return simulate_simple_trade(
                    mkt,
                    strategy_key,
                    tick,
                    "UP",
                    float(mkt["b_up"][tick]),
                    stake,
                    global_sec_rule,
                    config_source,
                )
            if 0 < mkt["b_dn"][tick] <= max_price:
                return simulate_simple_trade(
                    mkt,
                    strategy_key,
                    tick,
                    "DOWN",
                    float(mkt["b_dn"][tick]),
                    stake,
                    global_sec_rule,
                    config_source,
                )
        return None

    return None


def replay_session(
    markets: list[dict],
    configs_by_market: dict[str, dict],
    config_source: str,
) -> list[ReplayTrade]:
    replays: list[ReplayTrade] = []
    for mkt in markets:
        market_key = f"{mkt['symbol']}_{mkt['interval']}"
        market_cfg = configs_by_market.get(market_key, {})
        for strategy_key in SUPPORTED_REPLAY_STRATEGIES:
            params = market_cfg.get(strategy_key)
            if not params:
                continue
            replay_trade = replay_market_strategy(mkt, strategy_key, params, config_source)
            if replay_trade:
                replays.append(replay_trade)
    return replays


def pair_replays_to_live(replays: list[ReplayTrade], live_trades: pd.DataFrame):
    replay_by_key: dict[tuple[str, str], list[ReplayTrade]] = defaultdict(list)
    live_by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)

    for replay in replays:
        replay_by_key[(replay.market_id, replay.strategy_key)].append(replay)
    for live_trade in live_trades.to_dict("records"):
        key = (str(live_trade["market_id"]), str(live_trade["strategy_key"]))
        live_by_key[key].append(live_trade)

    matched = []
    unmatched_replays: list[ReplayTrade] = []
    unmatched_live: list[dict] = []

    all_keys = set(replay_by_key) | set(live_by_key)
    for key in all_keys:
        replay_list = sorted(replay_by_key.get(key, []), key=lambda item: item.entry_time)
        live_list = sorted(live_by_key.get(key, []), key=lambda item: item.get("entry_dt"))
        pair_count = min(len(replay_list), len(live_list))
        for idx in range(pair_count):
            matched.append((replay_list[idx], live_list[idx]))
        unmatched_replays.extend(replay_list[pair_count:])
        unmatched_live.extend(live_list[pair_count:])
    return matched, unmatched_replays, unmatched_live


def build_alternative_exit_summary(conn: sqlite3.Connection, live_trades: pd.DataFrame):
    results = []
    strategy_counter: dict[str, Counter] = defaultdict(Counter)
    strategy_improvement: dict[str, list[float]] = defaultdict(list)

    for trade in live_trades.to_dict("records"):
        entry_time = trade.get("entry_dt")
        if pd.isna(entry_time):
            continue
        ticks = load_market_ticks(conn, trade["market_id"], entry_time.to_pydatetime(), trade.get("exit_dt").to_pydatetime() if pd.notna(trade.get("exit_dt")) else None)
        scenarios, error = build_scenarios(trade, ticks)
        if error:
            results.append({"trade_id": trade["trade_id"], "strategy_key": trade["strategy_key"], "error": error})
            continue

        scenario_map = {scenario.label: scenario for scenario in scenarios}
        actual = scenario_map["actual"]
        best_simple = select_best_simple_rule(scenarios)
        hold = scenario_map.get("hold_to_resolution")
        if best_simple:
            improvement = best_simple.return_pct - actual.return_pct
            strategy_counter[trade["strategy_key"]][best_simple.label] += 1
            strategy_improvement[trade["strategy_key"]].append(improvement)
        else:
            improvement = 0.0

        results.append(
            {
                "trade_id": trade["trade_id"],
                "strategy_key": trade["strategy_key"],
                "actual_return_pct": actual.return_pct,
                "best_simple_rule": best_simple.label if best_simple else None,
                "best_simple_return_pct": best_simple.return_pct if best_simple else None,
                "best_simple_improvement_pp": improvement,
                "hold_to_resolution_return_pct": hold.return_pct if hold else None,
                "hold_to_resolution_detail": hold.detail if hold else None,
            }
        )

    rule_summary = {}
    for strategy_key, counter in strategy_counter.items():
        label, wins = counter.most_common(1)[0]
        avg_improvement = sum(strategy_improvement[strategy_key]) / len(strategy_improvement[strategy_key])
        rule_summary[strategy_key] = {
            "best_rule": label,
            "wins": wins,
            "avg_improvement_pp": avg_improvement,
        }

    return pd.DataFrame(results), rule_summary


def build_hold_to_resolution_summary(live_trades: pd.DataFrame, market_df: pd.DataFrame) -> pd.DataFrame:
    if live_trades.empty or market_df.empty:
        return pd.DataFrame()

    market_results = (
        market_df.sort_values("fetched_at")
        .groupby("market_id")
        .agg(
            won_up=("won_up", "last"),
            target_price=("target_price", "last"),
            live_price=("live_price", "last"),
        )
        .reset_index()
    )
    merged = live_trades.merge(market_results, on="market_id", how="left")
    if merged.empty:
        return merged

    merged["resolution_exit_price"] = merged.apply(
        lambda row: (
            row["settlement_value"]
            if pd.notna(row.get("settlement_value")) and float(row.get("settlement_value") or 0.0) > 0
            else (1.0 if ((row["direction"] == "UP" and bool(row.get("won_up"))) or (row["direction"] == "DOWN" and not bool(row.get("won_up")))) else 0.0)
        ),
        axis=1,
    )
    merged["actual_return_pct"] = merged.apply(
        lambda row: compute_return_pct(float(row.get("entry_price") or 0.0), float(row.get("exit_price") or 0.0)),
        axis=1,
    )
    merged["resolution_return_pct"] = merged.apply(
        lambda row: compute_return_pct(float(row.get("entry_price") or 0.0), float(row.get("resolution_exit_price") or 0.0)),
        axis=1,
    )
    merged["resolution_improvement_pp"] = merged["resolution_return_pct"] - merged["actual_return_pct"]
    return merged[
        [
            "trade_id",
            "market_id",
            "timeframe",
            "strategy",
            "strategy_key",
            "direction",
            "entry_price",
            "exit_price",
            "resolution_exit_price",
            "actual_return_pct",
            "resolution_return_pct",
            "resolution_improvement_pp",
        ]
    ]


def summarize_replay(replays: list[ReplayTrade]) -> pd.DataFrame:
    if not replays:
        return pd.DataFrame()
    replay_df = pd.DataFrame([trade.__dict__ for trade in replays])
    return (
        replay_df.groupby(["timeframe", "strategy_key"])
        .agg(
            replay_trades=("market_id", "count"),
            replay_total_pnl=("pnl_usd", "sum"),
            replay_avg_return_pct=("return_pct", "mean"),
            replay_win_rate=("return_pct", lambda values: (values.gt(0).mean() * 100.0) if len(values) else 0.0),
        )
        .reset_index()
    )


def summarize_live(live_trades: pd.DataFrame) -> pd.DataFrame:
    if live_trades.empty:
        return pd.DataFrame()
    return (
        live_trades.groupby(["timeframe", "strategy_key"])
        .agg(
            live_trades=("trade_id", "count"),
            live_total_pnl=("pnl", "sum"),
            live_avg_return_pct=("entry_return_pct", "mean"),
            live_win_rate=("entry_return_pct", lambda values: (values.gt(0).mean() * 100.0) if len(values) else 0.0),
        )
        .reset_index()
    )


def build_recommendations(
    replay_summary: pd.DataFrame,
    live_summary: pd.DataFrame,
    missed_signals: list[ReplayTrade],
    alt_exit_df: pd.DataFrame,
    hold_df: pd.DataFrame,
    vault_configs: dict[str, dict[str, dict]],
) -> pd.DataFrame:
    frames = []
    if not replay_summary.empty:
        frames.append(replay_summary)
    if not live_summary.empty:
        frames.append(live_summary)

    if not frames:
        return pd.DataFrame()

    summary = frames[0]
    for frame in frames[1:]:
        summary = summary.merge(frame, on=["timeframe", "strategy_key"], how="outer")

    missed_df = pd.DataFrame(
        [
            {"timeframe": replay.timeframe, "strategy_key": replay.strategy_key, "missed_signals": 1}
            for replay in missed_signals
        ]
    )
    if not missed_df.empty:
        missed_df = missed_df.groupby(["timeframe", "strategy_key"]).agg(missed_signals=("missed_signals", "sum")).reset_index()
        summary = summary.merge(missed_df, on=["timeframe", "strategy_key"], how="left")

    if not alt_exit_df.empty:
        alt_summary = (
            alt_exit_df.dropna(subset=["best_simple_improvement_pp"])
            .groupby("strategy_key")
            .agg(
                avg_alt_exit_improvement_pp=("best_simple_improvement_pp", "mean"),
                top_exit_rule=("best_simple_rule", lambda values: values.mode().iloc[0] if not values.mode().empty else None),
            )
            .reset_index()
        )
        summary = summary.merge(alt_summary, on="strategy_key", how="left")

    if not hold_df.empty:
        hold_summary = (
            hold_df.groupby("strategy_key")
            .agg(avg_resolution_improvement_pp=("resolution_improvement_pp", "mean"))
            .reset_index()
        )
        summary = summary.merge(hold_summary, on="strategy_key", how="left")

    summary["missed_signals"] = summary["missed_signals"].fillna(0).astype(int)
    summary["replay_trades"] = summary["replay_trades"].fillna(0).astype(int)
    summary["live_trades"] = summary["live_trades"].fillna(0).astype(int)
    summary["live_total_pnl"] = summary["live_total_pnl"].fillna(0.0)
    summary["replay_total_pnl"] = summary["replay_total_pnl"].fillna(0.0)
    summary["avg_alt_exit_improvement_pp"] = summary["avg_alt_exit_improvement_pp"].fillna(0.0)
    summary["avg_resolution_improvement_pp"] = summary["avg_resolution_improvement_pp"].fillna(0.0)
    summary["entry_capture_ratio"] = summary.apply(
        lambda row: (row["live_trades"] / row["replay_trades"]) if row["replay_trades"] else 0.0,
        axis=1,
    )
    summary["recommendation"] = summary.apply(classify_recommendation, axis=1)
    summary["vault_best_pnl_percent"] = summary.apply(
        lambda row: (
            vault_configs.get(row["timeframe"], {})
            .get(row["strategy_key"], {})
            .get("_pnl_percent")
        ),
        axis=1,
    )
    summary = summary.sort_values(
        ["recommendation", "live_total_pnl", "replay_total_pnl", "avg_alt_exit_improvement_pp"],
        ascending=[True, False, False, False],
    )
    return summary


def classify_recommendation(row: pd.Series) -> str:
    if row["replay_trades"] > 0 and row["live_trades"] == 0:
        return "review_entry_filters"
    if row["live_total_pnl"] < 0 and row["replay_total_pnl"] > 0:
        return "execution_drift"
    if row["avg_alt_exit_improvement_pp"] >= 10.0:
        return "improve_exit_rules"
    if row["avg_resolution_improvement_pp"] >= 10.0:
        return "consider_hold_longer"
    if row["live_total_pnl"] > 0 or row["replay_total_pnl"] > 0:
        return "keep_or_expand"
    return "low_edge"


def format_replay_section(replays: list[ReplayTrade]) -> list[str]:
    lines = ["## 1. Replay Session", ""]
    if not replays:
        lines.append("No replayable strategies/configs were found for the selected session.")
        lines.append("")
        return lines

    lines.append("| Market | Strategy | Direction | Entry | Exit | Return % | Exit rule |")
    lines.append("| --- | --- | --- | --- | --- | ---: | --- |")
    for replay in sorted(replays, key=lambda item: (item.timeframe, item.strategy_key, item.entry_time)):
        lines.append(
            f"| {replay.timeframe}/{replay.market_id} | {replay.strategy_key} | {replay.direction} | "
            f"{format_dt(replay.entry_time.to_pydatetime() if hasattr(replay.entry_time, 'to_pydatetime') else replay.entry_time)} @ {replay.entry_price:.4f} | "
            f"{format_dt(replay.exit_time.to_pydatetime() if hasattr(replay.exit_time, 'to_pydatetime') else replay.exit_time)} @ {replay.exit_price:.4f} | "
            f"{replay.return_pct:+.2f}% | {replay.exit_reason} |"
        )
    lines.append("")
    return lines


def format_live_vs_replay_section(matched_pairs, unmatched_replays, unmatched_live) -> list[str]:
    lines = ["## 2. Backtest-Trade vs Live-Trade Delta", ""]
    if not matched_pairs and not unmatched_replays and not unmatched_live:
        lines.append("No live trades or replay trades were available for matching.")
        lines.append("")
        return lines

    if matched_pairs:
        lines.append("| Market | Strategy | Replay dir | Live dir | Replay ret % | Live ret % | Delta pp | Entry diff |")
        lines.append("| --- | --- | --- | --- | ---: | ---: | ---: | ---: |")
        for replay, live_trade in matched_pairs:
            lines.append(
                f"| {replay.timeframe}/{replay.market_id} | {replay.strategy_key} | {replay.direction} | {live_trade['direction']} | "
                f"{replay.return_pct:+.2f}% | {float(live_trade['entry_return_pct']):+.2f}% | "
                f"{float(live_trade['entry_return_pct']) - replay.return_pct:+.2f} | "
                f"{float(live_trade['entry_price']) - replay.entry_price:+.4f} |"
            )
        lines.append("")
    else:
        lines.append("No one-to-one market/strategy matches were found between replay trades and recorded live trades.")
        lines.append("")

    lines.append(f"- No-trade despite replay signal: `{len(unmatched_replays)}`")
    lines.append(f"- Live trade without replay signal: `{len(unmatched_live)}`")
    lines.append("")
    return lines


def format_alternative_exit_section(alt_exit_df: pd.DataFrame) -> list[str]:
    lines = ["## 3. Actual vs Alternative Exits", ""]
    if alt_exit_df.empty:
        lines.append("No alternative exit analysis could be computed.")
        lines.append("")
        return lines

    visible = alt_exit_df.dropna(subset=["best_simple_rule"]).sort_values("best_simple_improvement_pp", ascending=False)
    if visible.empty:
        lines.append("Trades were found, but none produced an alternative exit rule better than the recorded exit.")
        lines.append("")
        return lines

    lines.append("| Trade | Strategy | Actual ret % | Best rule | Best ret % | Improvement pp |")
    lines.append("| --- | --- | ---: | --- | ---: | ---: |")
    for row in visible.head(25).itertuples(index=False):
        lines.append(
            f"| {row.trade_id} | {row.strategy_key} | {row.actual_return_pct:+.2f}% | "
            f"{row.best_simple_rule} | {float(row.best_simple_return_pct):+.2f}% | {float(row.best_simple_improvement_pp):+.2f} |"
        )
    lines.append("")
    return lines


def format_hold_section(hold_df: pd.DataFrame) -> list[str]:
    lines = ["## 4. Hold-to-Resolution vs Actual Exit", ""]
    if hold_df.empty:
        lines.append("No resolution comparison could be built.")
        lines.append("")
        return lines

    visible = hold_df.sort_values("resolution_improvement_pp", ascending=False)
    lines.append("| Trade | Strategy | Actual ret % | Resolution ret % | Improvement pp |")
    lines.append("| --- | --- | ---: | ---: | ---: |")
    for row in visible.head(25).itertuples(index=False):
        lines.append(
            f"| {row.trade_id} | {row.strategy_key} | {float(row.actual_return_pct):+.2f}% | "
            f"{float(row.resolution_return_pct):+.2f}% | {float(row.resolution_improvement_pp):+.2f} |"
        )
    lines.append("")
    return lines


def format_recommendations_section(recommendations: pd.DataFrame) -> list[str]:
    lines = ["## 5. Strategy Recommendations", ""]
    if recommendations.empty:
        lines.append("No recommendation set could be produced.")
        lines.append("")
        return lines

    lines.append("| Market | Strategy | Replay trades | Live trades | Missed | Replay pnl | Live pnl | Alt exit pp | Resolution pp | Status | Top exit rule |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |")
    for row in recommendations.itertuples(index=False):
        top_exit_rule = getattr(row, "top_exit_rule", None)
        if pd.isna(top_exit_rule):
            top_exit_rule = None
        lines.append(
            f"| {row.timeframe} | {row.strategy_key} | {int(row.replay_trades)} | {int(row.live_trades)} | "
            f"{int(row.missed_signals)} | {float(row.replay_total_pnl):+.2f} | {float(row.live_total_pnl):+.2f} | "
            f"{float(row.avg_alt_exit_improvement_pp):+.2f} | {float(row.avg_resolution_improvement_pp):+.2f} | "
            f"{row.recommendation} | {top_exit_rule or 'n/a'} |"
        )
    lines.append("")
    return lines


def generate_report(
    db_path: Path,
    history_db_path: Path,
    tracked_configs_path: Path,
    session_id: str | None,
    all_history: bool,
    config_source: str,
) -> str:
    effective_session = None if all_history else (session_id or get_latest_session_id(db_path))
    market_df = load_market_data(db_path=db_path, session_id=effective_session, all_history=all_history)
    live_trades = load_trades(db_path=db_path, session_id=effective_session)
    tracked_configs = load_tracked_configs(tracked_configs_path)
    vault_configs = load_best_backtest_configs(history_db_path)
    replay_configs = tracked_configs if config_source == "tracked" else vault_configs
    replays = replay_session(prepare_fast_markets(market_df), replay_configs, config_source)
    matched_pairs, unmatched_replays, unmatched_live = pair_replays_to_live(replays, live_trades)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        alt_exit_df, _ = build_alternative_exit_summary(conn, live_trades)
    finally:
        conn.close()

    hold_df = build_hold_to_resolution_summary(live_trades, market_df)
    replay_summary = summarize_replay(replays)
    live_summary = summarize_live(live_trades)
    recommendations = build_recommendations(
        replay_summary,
        live_summary,
        unmatched_replays,
        alt_exit_df,
        hold_df,
        vault_configs,
    )

    lines = [
        "# Execution Analysis",
        "",
        f"- Database: `{db_path}`",
        f"- Backtest history: `{history_db_path}`",
        f"- Tracked configs: `{tracked_configs_path}`",
        f"- Session: `{effective_session or 'ALL'}`",
        f"- Config source for replay: `{config_source}`",
        f"- Live trades reviewed: `{len(live_trades)}`",
        f"- Replay trades generated: `{len(replays)}`",
        "",
    ]
    lines.extend(format_replay_section(replays))
    lines.extend(format_live_vs_replay_section(matched_pairs, unmatched_replays, unmatched_live))
    lines.extend(format_alternative_exit_section(alt_exit_df))
    lines.extend(format_hold_section(hold_df))
    lines.extend(format_recommendations_section(recommendations))
    lines.append("## Notes")
    lines.append("")
    lines.append("- Replay currently covers the live strategies that are actually traded in `main.py`: `kinetic_sniper`, `momentum`, `mid_arb`, `otm`.")
    lines.append("- Replay uses current tracked configs by default, so `no-trade despite signal` highlights execution/filter drift rather than vault-only theoretical ideas.")
    lines.append("- `hold_to_resolution` falls back to binary market settlement (1.0/0.0) when explicit `settlement_value` is absent.")
    lines.append("- Recommendation statuses are heuristic and intended to narrow review scope, not auto-disable strategies.")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Session replay and execution drift analysis for live vs backtest trades.")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to polymarket SQLite database.")
    parser.add_argument("--history-db", default=str(BACKTEST_HISTORY_PATH), help="Path to backtest history SQLite database.")
    parser.add_argument("--tracked-configs", default=str(TRACKED_CONFIGS_PATH), help="Path to tracked configs JSON.")
    parser.add_argument("--session", default=None, help="Optional session_id to isolate analysis.")
    parser.add_argument("--all-history", action="store_true", help="Analyze the full DB instead of a single session.")
    parser.add_argument(
        "--config-source",
        choices=("tracked", "vault"),
        default="tracked",
        help="Use deployed tracked configs or best vault configs when replaying session signals.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output path for the markdown report. If omitted, a timestamped file is created in data/execution_analysis/reports/.",
    )
    return parser.parse_args()


def resolve_output_path(requested_path: str | None, session_id: str | None, all_history: bool) -> Path:
    if requested_path:
        return Path(requested_path)

    EXECUTION_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_label = "all_history" if all_history else (session_id or "latest_session")
    safe_session_label = session_label.replace("/", "_").replace(" ", "_")
    return EXECUTION_REPORTS_DIR / f"execution_analysis_{safe_session_label}_{timestamp}.md"


def main() -> None:
    args = parse_args()
    report = generate_report(
        db_path=Path(args.db),
        history_db_path=Path(args.history_db),
        tracked_configs_path=Path(args.tracked_configs),
        session_id=args.session,
        all_history=args.all_history,
        config_source=args.config_source,
    )
    output_path = resolve_output_path(args.output, args.session, args.all_history)
    output_path.write_text(report, encoding="utf-8")
    print(f"Report written to {output_path}")
    print("")
    print(report)


if __name__ == "__main__":
    main()
