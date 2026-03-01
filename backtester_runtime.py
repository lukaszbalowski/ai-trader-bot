from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import random
import sqlite3
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

from backtester import (
    MONTE_CARLO_LIMIT,
    QUICK_MONTE_CARLO_LIMIT,
    TRACKED_CONFIGS_FILE,
    TRACKED_CONFIG_MARKETS,
    TRACKED_CONFIG_STRATEGIES,
    calculate_composite_score,
    create_crash_dump,
    format_sampling_reason,
    get_latest_session_id,
    init_history_db,
    jitter_values,
    load_and_prepare_data,
    prepare_fast_markets,
    refresh_tracked_configs_from_history,
    save_optimization_result,
    select_optimal_result,
    validate_market_rules,
    worker_1min_momentum,
    worker_absorption_pattern,
    worker_cross_market_spillover,
    worker_divergence_imbalance,
    worker_kinetic_sniper,
    worker_l2_spread_scalper,
    worker_liquidity_vacuum,
    worker_mean_reversion_obi,
    worker_micro_pullback_continuation,
    worker_mid_arb,
    worker_obi_acceleration,
    worker_otm,
    worker_session_based_edge,
    worker_settlement_convergence,
    worker_spread_compression,
    worker_synthetic_arbitrage,
    worker_vol_mean_reversion,
    worker_volatility_compression_breakout,
)


DB_PATH = Path("data/polymarket.db")
BACKTEST_HISTORY_PATH = Path("data/backtest_history.db")
RUNTIME_DIR = Path("data/backtester/runtime")
LATEST_STATE_PATH = RUNTIME_DIR / "latest_state.json"
WORKSPACE_PATH = Path(__file__).resolve().parent
TRACKED_CONFIGS_PATH = (WORKSPACE_PATH / TRACKED_CONFIGS_FILE).resolve()
PENDING_TRACKED_UPDATES_PATH = RUNTIME_DIR / "pending_tracked_updates.json"
SYMBOL_ORDER = {"BTC": 0, "ETH": 1, "SOL": 2, "XRP": 3}
TIMEFRAME_ORDER = {"5m": 0, "15m": 1, "1h": 2, "4h": 3}
FULL_ENUMERATION_LIMIT = 250_000


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def format_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "--"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def strategy_label(strategy_key: str) -> str:
    return STRATEGY_SPECS[strategy_key]["label"]


def normalize_scope(value: str | None) -> str:
    if value in {"all", "all_history"}:
        return "all"
    return "latest_session"


def normalize_mode(value: str | None) -> str:
    if value == "quick":
        return "quick"
    if value == "fast-track":
        return "fast-track"
    return "full"


def normalize_adaptive(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def format_mode_label(mode: str, adaptive: bool = False) -> str:
    return f"{mode} + adaptive" if adaptive else mode


def market_sort_key(market_key: str) -> tuple[int, int, str]:
    symbol, timeframe = market_key.split("_", 1)
    return (TIMEFRAME_ORDER.get(timeframe, 99), SYMBOL_ORDER.get(symbol, 99), market_key)


def parse_market_key(market_key: str) -> tuple[str, str]:
    if "_" not in market_key:
        raise ValueError(f"Niepoprawny market_key: {market_key}")
    return market_key.split("_", 1)


def timeframe_to_interval_seconds(timeframe: str) -> int:
    if timeframe.endswith("m"):
        return int(timeframe[:-1]) * 60
    if timeframe.endswith("h"):
        return int(timeframe[:-1]) * 3600
    raise ValueError(f"Nieobsługiwany timeframe: {timeframe}")


def atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=4), encoding="utf-8")
    tmp_path.replace(path)


def history_rank_key(item: dict | sqlite3.Row) -> tuple[float, float, float, int]:
    return (
        float(item["pnl_usd"] or 0.0),
        float(item["pnl_percent"] or 0.0),
        float(item["win_rate"] or 0.0),
        int(item["trades_count"] or 0),
    )


def load_history_variants(
    symbol: str,
    timeframe: str,
    strategy_key: str,
    *,
    order_by: str,
    limit: int,
) -> list[dict]:
    if not BACKTEST_HISTORY_PATH.exists():
        return []

    conn = sqlite3.connect(BACKTEST_HISTORY_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT
                rr.created_at AS timestamp,
                rr.symbol,
                rr.timeframe,
                rr.strategy,
                rr.parameters,
                rr.pnl_usd,
                rr.pnl_percent,
                rr.win_rate,
                rr.trades_count,
                CASE
                    WHEN LOWER(COALESCE(r.mode, '')) LIKE '%adaptive%' THEN 1
                    ELSE 0
                END AS adaptive
            FROM backtest_run_results rr
            LEFT JOIN backtest_runs r ON r.run_id = rr.run_id
            WHERE rr.symbol = ? AND rr.timeframe = ? AND rr.strategy = ?
            ORDER BY {order_by.replace("timestamp", "rr.created_at")}
            LIMIT ?
            """,
            (symbol, timeframe, strategy_key, limit),
        ).fetchall()
        if not rows:
            rows = conn.execute(
                f"""
                SELECT
                    timestamp,
                    symbol,
                    timeframe,
                    strategy,
                    parameters,
                    pnl_usd,
                    pnl_percent,
                    win_rate,
                    trades_count,
                    0 AS adaptive
                FROM optimization_logs
                WHERE symbol = ? AND timeframe = ? AND strategy = ?
                ORDER BY {order_by}
                LIMIT ?
                """,
                (symbol, timeframe, strategy_key, limit),
            ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    finally:
        conn.close()

    variants = []
    for row in rows:
        try:
            parameters = json.loads(row["parameters"]) if row["parameters"] else {}
        except json.JSONDecodeError:
            parameters = {}
        variants.append(
            {
                "market_key": f"{row['symbol']}_{row['timeframe']}",
                "market_label": f"{row['symbol']} {row['timeframe']}",
                "strategy_key": row["strategy"],
                "strategy_label": strategy_label(row["strategy"]) if row["strategy"] in STRATEGY_SPECS else row["strategy"],
                "timestamp": row["timestamp"],
                "pnl_usd": float(row["pnl_usd"] or 0.0),
                "pnl_percent": float(row["pnl_percent"] or 0.0),
                "win_rate": float(row["win_rate"] or 0.0),
                "trades_count": int(row["trades_count"] or 0),
                "parameters": parameters,
                "adaptive": bool(row["adaptive"]),
            }
        )
    return variants


def get_recent_history_variants(symbol: str, timeframe: str, strategy_key: str, limit: int = 10) -> list[dict]:
    variants = load_history_variants(
        symbol,
        timeframe,
        strategy_key,
        order_by="timestamp DESC, pnl_usd DESC, pnl_percent DESC, win_rate DESC",
        limit=limit,
    )
    if not variants:
        return variants
    best = max(variants, key=history_rank_key)
    for variant in variants:
        variant["is_best"] = variant == best
    return variants


def get_top_history_variants(symbol: str, timeframe: str, strategy_key: str, limit: int = 5) -> list[dict]:
    variants = load_history_variants(
        symbol,
        timeframe,
        strategy_key,
        order_by="pnl_usd DESC, pnl_percent DESC, win_rate DESC, trades_count DESC, timestamp DESC",
        limit=limit,
    )
    for index, variant in enumerate(variants):
        variant["rank"] = index + 1
        variant["is_best"] = index == 0
    return variants


def ensure_strategy_id(symbol: str, timeframe: str, strategy_key: str, parameters: dict) -> dict:
    payload = dict(parameters or {})
    if payload.get("id"):
        return payload
    id_prefix = STRATEGY_SPECS.get(strategy_key, {}).get("id_prefix", strategy_key)
    payload["id"] = f"{symbol.lower()}_{timeframe}_{id_prefix}_{uuid.uuid4().hex[:8]}"
    return payload


def build_tracked_strategy_payload(
    symbol: str,
    timeframe: str,
    strategy_key: str,
    parameters: dict,
    win_rate: float,
) -> dict:
    if strategy_key not in TRACKED_CONFIG_STRATEGIES:
        raise ValueError(f"Strategia {strategy_key} nie jest obsługiwana przez tracked configs.")

    prepared = ensure_strategy_id(symbol, timeframe, strategy_key, parameters)
    prepared["wr"] = round(float(win_rate or 0.0), 1)

    if strategy_key == "kinetic_sniper":
        return {
            "window_ms": prepared.get("window_ms", 1000),
            "trigger_pct": prepared.get("trigger_pct", 0.0),
            "max_price": prepared.get("max_price", 0.85),
            "max_slippage": prepared.get("max_slippage", 0.025),
            "id": prepared.get("id", ""),
            "wr": prepared.get("wr", 0.0),
        }

    return {
        key: value
        for key, value in prepared.items()
        if key not in {"g_sec", "max_delta_abs"}
    }


def load_tracked_configs_payload() -> list[dict]:
    if not TRACKED_CONFIGS_PATH.exists():
        return []
    return json.loads(TRACKED_CONFIGS_PATH.read_text(encoding="utf-8"))


def build_market_defaults(symbol: str, timeframe: str, existing_configs: list[dict]) -> dict:
    market_key = f"{symbol}_{timeframe}"
    for config in existing_configs:
        if config.get("symbol") == symbol and config.get("timeframe") == timeframe:
            return {
                "symbol": config.get("symbol", symbol),
                "pair": config.get("pair", f"{symbol}USDT"),
                "timeframe": config.get("timeframe", timeframe),
                "interval": int(config.get("interval", timeframe_to_interval_seconds(timeframe))),
                "decimals": int(config.get("decimals", 2)),
                "offset": float(config.get("offset", 0.0)),
            }

    for known_symbol, known_timeframe, decimals in TRACKED_CONFIG_MARKETS:
        if f"{known_symbol}_{known_timeframe}" == market_key:
            return {
                "symbol": symbol,
                "pair": f"{symbol}USDT",
                "timeframe": timeframe,
                "interval": timeframe_to_interval_seconds(timeframe),
                "decimals": decimals,
                "offset": 0.0,
            }

    same_symbol = next((config for config in existing_configs if config.get("symbol") == symbol), None)
    return {
        "symbol": symbol,
        "pair": f"{symbol}USDT",
        "timeframe": timeframe,
        "interval": timeframe_to_interval_seconds(timeframe),
        "decimals": int((same_symbol or {}).get("decimals", 2)),
        "offset": float((same_symbol or {}).get("offset", 0.0)),
    }


def read_pending_tracked_updates() -> dict:
    if not PENDING_TRACKED_UPDATES_PATH.exists():
        return {"updated_at": None, "items": {}}
    payload = json.loads(PENDING_TRACKED_UPDATES_PATH.read_text(encoding="utf-8"))
    payload.setdefault("updated_at", None)
    payload.setdefault("items", {})
    return payload


def write_pending_tracked_updates(payload: dict) -> None:
    atomic_write_json(PENDING_TRACKED_UPDATES_PATH, payload)


def get_pending_tracked_updates() -> dict:
    payload = read_pending_tracked_updates()
    items = payload.get("items", {})
    return {
        "updated_at": payload.get("updated_at"),
        "count": len(items),
        "combo_keys": sorted(items.keys()),
        "items": items,
        "tracked_configs_path": str(TRACKED_CONFIGS_PATH),
    }


def get_strategy_history_details(market_key: str, strategy_key: str) -> dict:
    symbol, timeframe = parse_market_key(market_key)
    top_history = get_top_history_variants(symbol, timeframe, strategy_key, limit=5)
    recent_variants = get_recent_history_variants(symbol, timeframe, strategy_key, limit=10)
    pending = get_pending_tracked_updates().get("items", {}).get(f"{market_key}|{strategy_key}")
    best_variant = top_history[0] if top_history else next((item for item in recent_variants if item.get("is_best")), None)
    return {
        "combo_key": f"{market_key}|{strategy_key}",
        "market_key": market_key,
        "market_label": f"{symbol} {timeframe}",
        "strategy_key": strategy_key,
        "strategy_label": strategy_label(strategy_key) if strategy_key in STRATEGY_SPECS else strategy_key,
        "top_history": top_history,
        "recent_variants": recent_variants,
        "best_variant": best_variant,
        "queued_update": pending,
        "tracked_configs_path": str(TRACKED_CONFIGS_PATH),
        "supports_tracked_config": strategy_key in TRACKED_CONFIG_STRATEGIES,
    }


def queue_tracked_strategy_update(market_key: str, strategy_key: str) -> dict:
    details = get_strategy_history_details(market_key, strategy_key)
    selected_variant = details.get("best_variant")
    return queue_tracked_strategy_update_variant(market_key, strategy_key, selected_variant)


def queue_tracked_strategy_update_variant(
    market_key: str,
    strategy_key: str,
    selected_variant: dict | None,
) -> dict:
    details = get_strategy_history_details(market_key, strategy_key)
    if not selected_variant:
        raise ValueError("Brak historycznego wariantu do dodania do tracked configs.")
    if not isinstance(selected_variant, dict):
        raise ValueError("Niepoprawny wariant historyczny.")
    if not selected_variant.get("parameters"):
        raise ValueError("Wybrany wariant nie ma parametrów.")

    symbol, timeframe = parse_market_key(market_key)
    config_payload = build_tracked_strategy_payload(
        symbol,
        timeframe,
        strategy_key,
        selected_variant.get("parameters", {}),
        selected_variant.get("win_rate", 0.0),
    )

    store = read_pending_tracked_updates()
    combo_key = f"{market_key}|{strategy_key}"
    store["items"][combo_key] = {
        "combo_key": combo_key,
        "market_key": market_key,
        "market_label": f"{symbol} {timeframe}",
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_key": strategy_key,
        "strategy_label": details["strategy_label"],
        "selected_at": utc_now_iso(),
        "source": "historical_best",
        "selected_variant": selected_variant,
        "config_payload": config_payload,
    }
    store["updated_at"] = utc_now_iso()
    write_pending_tracked_updates(store)
    return {
        "queued": store["items"][combo_key],
        "tracked_updates": get_pending_tracked_updates(),
    }


def apply_pending_tracked_updates() -> dict:
    store = read_pending_tracked_updates()
    pending_items = list(store.get("items", {}).values())
    if not pending_items:
        return {
            "applied_count": 0,
            "tracked_updates": get_pending_tracked_updates(),
            "tracked_configs_path": str(TRACKED_CONFIGS_PATH),
        }

    tracked_configs = load_tracked_configs_payload()
    market_index = {
        f"{item.get('symbol')}_{item.get('timeframe')}": index
        for index, item in enumerate(tracked_configs)
    }

    for pending in pending_items:
        market_key = pending["market_key"]
        if market_key not in market_index:
            tracked_configs.append(
                build_market_defaults(pending["symbol"], pending["timeframe"], tracked_configs)
            )
            market_index[market_key] = len(tracked_configs) - 1
        config = dict(tracked_configs[market_index[market_key]])
        config[pending["strategy_key"]] = pending["config_payload"]
        tracked_configs[market_index[market_key]] = config

    tracked_configs.sort(key=lambda item: market_sort_key(f"{item['symbol']}_{item['timeframe']}"))
    atomic_write_json(TRACKED_CONFIGS_PATH, tracked_configs)
    write_pending_tracked_updates({"updated_at": utc_now_iso(), "items": {}})
    return {
        "applied_count": len(pending_items),
        "tracked_updates": get_pending_tracked_updates(),
        "tracked_configs_path": str(TRACKED_CONFIGS_PATH),
    }


def build_parameter_rows(param_axes: dict[str, list]) -> list[dict]:
    rows: list[dict] = []
    for name, axis in param_axes.items():
        values = list(dict.fromkeys(axis))
        sorted_values = sorted(values)
        step = None
        if len(sorted_values) >= 2:
            delta = sorted_values[1] - sorted_values[0]
            if isinstance(delta, float):
                step = round(delta, 6)
            else:
                step = delta
        rows.append(
            {
                "name": name,
                "min": sorted_values[0],
                "max": sorted_values[-1],
                "step": step,
                "variants": len(sorted_values),
            }
        )
    return rows


def product_size(param_axes: dict[str, list]) -> int:
    total = 1
    for axis in param_axes.values():
        total *= max(1, len(list(dict.fromkeys(axis))))
    return total


def compute_auto_sample_target(total_combinations: int, mode: str) -> tuple[int, str | None]:
    if total_combinations <= 0:
        return 0, None
    if mode == "quick":
        if 100_001 <= total_combinations <= 1_000_000:
            return QUICK_MONTE_CARLO_LIMIT, "quick_100k"
        if total_combinations > 1_000_000:
            return min(MONTE_CARLO_LIMIT, max(1, int(total_combinations * 0.10))), "quick_10pct_capped"
        return total_combinations, None
    if total_combinations > MONTE_CARLO_LIMIT:
        return MONTE_CARLO_LIMIT, "default_limit"
    return total_combinations, None


def build_seed(*parts: object) -> str:
    return "::".join(str(part) for part in parts)


def infer_axis_metadata(param_axes: dict[str, list]) -> dict[str, dict]:
    metadata: dict[str, dict] = {}
    for name, axis in param_axes.items():
        values = sorted(dict.fromkeys(axis))
        if not values:
            continue
        center_index = len(values) // 2
        precision = 0
        for value in values:
            if isinstance(value, float):
                rendered = f"{value:.12f}".rstrip("0").rstrip(".")
                if "." in rendered:
                    precision = max(precision, len(rendered.split(".", 1)[1]))
        metadata[name] = {
            "name": name,
            "base_value": values[center_index],
            "min_value": values[0],
            "max_value": values[-1],
            "integer": all(isinstance(value, int) and not isinstance(value, bool) for value in values),
            "precision": precision,
        }
    return metadata


def quantize_adaptive_value(meta: dict, pct_offset: float) -> int | float:
    base_value = meta["base_value"]
    raw_value = base_value * (1.0 + (pct_offset / 100.0))
    clipped = min(meta["max_value"], max(meta["min_value"], raw_value))
    if meta["integer"]:
        return int(round(clipped))
    return round(float(clipped), meta["precision"])


def estimate_adaptive_sample_count(param_axes: dict[str, list]) -> int:
    if not param_axes:
        return 0
    return 1 + (len(param_axes) * 19)


def adaptive_sampling_note(param_axes: dict[str, list], mode: str) -> str:
    upper_bound = estimate_adaptive_sample_count(param_axes)
    mode_note = "Quick zachowuje się tu tak samo jak Full, bo siatka adaptacyjna jest mała i deterministyczna."
    if mode == "fast-track":
        mode_note = "Tryb adaptive nie jest używany w fast-track."
    return (
        "Adaptive grid: start od punktu bazowego, potem dla każdego parametru etap 1 (-50%..+50% co 10%), "
        "etap 2 (+/-5% wokół najlepszego kandydata) i etap 3 (+/-2% co 1%) tylko po poprawie wyniku. "
        f"Górny limit próbek dla tego testu: {upper_bound}. {mode_note}"
    )


def build_combinations(
    param_axes: dict[str, list],
    sample_target: int,
    post_filter=None,
    seed: str = "catalog",
) -> tuple[list[dict], int | None]:
    keys = list(param_axes.keys())
    axes = [list(dict.fromkeys(param_axes[key])) for key in keys]
    raw_total = product_size(param_axes)
    if raw_total <= 0 or sample_target <= 0:
        return [], 0

    rng = random.Random(seed)
    if raw_total <= FULL_ENUMERATION_LIMIT or sample_target >= raw_total:
        combinations = []
        for values in itertools.product(*axes):
            combo = dict(zip(keys, values))
            if post_filter and not post_filter(combo):
                continue
            combinations.append(combo)
        filtered_total = len(combinations)
        if filtered_total <= sample_target:
            return combinations, filtered_total
        rng.shuffle(combinations)
        return combinations[:sample_target], filtered_total

    sampled: dict[tuple, dict] = {}
    attempts = 0
    max_attempts = max(sample_target * 50, 20_000)
    while len(sampled) < sample_target and attempts < max_attempts:
        values = tuple(rng.choice(axis) for axis in axes)
        combo = dict(zip(keys, values))
        attempts += 1
        if post_filter and not post_filter(combo):
            continue
        sampled[values] = combo
    return list(sampled.values()), None


def build_sampling_plan(
    strategy_key: str,
    symbol: str,
    timeframe: str,
    mode: str,
    adaptive: bool = False,
    sample_override: int | None = None,
    seed: str = "catalog",
    include_combinations: bool = False,
) -> dict:
    spec = STRATEGY_SPECS[strategy_key]
    if strategy_key == "otm" and not validate_market_rules("otm", symbol, timeframe):
        return {
            "strategy_key": strategy_key,
            "enabled": False,
            "reason": "Strategia zablokowana regułami bezpieczeństwa dla tego rynku.",
        }

    param_axes = spec["param_builder"](symbol, timeframe)
    raw_total = product_size(param_axes)
    adaptive_meta = infer_axis_metadata(param_axes)
    if adaptive:
        adaptive_count = estimate_adaptive_sample_count(param_axes)
        return {
            "enabled": True,
            "strategy_key": strategy_key,
            "strategy_label": spec["label"],
            "summary": spec["summary"],
            "parameters": build_parameter_rows(param_axes),
            "raw_total_combinations": raw_total,
            "filtered_total_combinations": None,
            "sample_target": adaptive_count,
            "effective_sample_count": adaptive_count,
            "sampling_reason": "adaptive_grid",
            "sampling_note": adaptive_sampling_note(param_axes, mode),
            "param_axes": param_axes if include_combinations else None,
            "adaptive_meta": adaptive_meta if include_combinations else None,
            "combinations": None,
        }
    if sample_override is not None:
        sample_target = max(1, min(raw_total, int(sample_override)))
        sampling_reason = "manual_override" if sample_target < raw_total else None
        sampling_note = (
            f"Ręcznie ustawiono próbkowanie Monte Carlo na {sample_target} kombinacji."
            if sampling_reason
            else "Pełna siatka parametrów mieści się w ręcznie ustawionym limicie."
        )
    else:
        sample_target, auto_reason = compute_auto_sample_target(raw_total, mode)
        sampling_reason = auto_reason
        sampling_note = format_sampling_reason(auto_reason, raw_total, sample_target) if auto_reason else "Pełna siatka parametrów zostanie przetestowana."

    filtered_total = None
    combinations: list[dict] = []
    if include_combinations or raw_total <= FULL_ENUMERATION_LIMIT:
        combinations, filtered_total = build_combinations(
            param_axes,
            sample_target,
            post_filter=spec.get("post_filter"),
            seed=seed,
        )
    effective_sample_count = len(combinations) if combinations else sample_target
    if filtered_total is not None and filtered_total < effective_sample_count:
        effective_sample_count = filtered_total

    return {
        "enabled": True,
        "strategy_key": strategy_key,
        "strategy_label": spec["label"],
        "summary": spec["summary"],
        "parameters": build_parameter_rows(param_axes),
        "raw_total_combinations": raw_total,
        "filtered_total_combinations": filtered_total,
        "sample_target": sample_target,
        "effective_sample_count": effective_sample_count,
        "sampling_reason": sampling_reason,
        "sampling_note": sampling_note,
        "param_axes": param_axes if include_combinations else None,
        "adaptive_meta": adaptive_meta if include_combinations else None,
        "combinations": combinations if include_combinations else None,
    }


def load_available_markets(scope: str) -> dict:
    scope = normalize_scope(scope)
    if not DB_PATH.exists():
        return {
            "scope": scope,
            "session_id": None,
            "markets": [],
            "market_map": {},
        }

    conn = sqlite3.connect(DB_PATH)
    try:
        latest_session_id = get_latest_session_id(DB_PATH) if scope == "latest_session" else None
        if latest_session_id:
            rows = conn.execute(
                """
                SELECT timeframe, COUNT(*) AS ticks_count, COUNT(DISTINCT market_id) AS market_instances
                FROM market_logs_v11
                WHERE session_id = ? AND (buy_up > 0 OR buy_down > 0)
                GROUP BY timeframe
                """,
                (latest_session_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT timeframe, COUNT(*) AS ticks_count, COUNT(DISTINCT market_id) AS market_instances
                FROM market_logs_v11
                WHERE buy_up > 0 OR buy_down > 0
                GROUP BY timeframe
                """
            ).fetchall()
    finally:
        conn.close()

    markets = []
    market_map = {}
    for market_key, ticks_count, market_instances in rows:
        if "_" not in market_key:
            continue
        symbol, timeframe = market_key.split("_", 1)
        item = {
            "market_key": market_key,
            "symbol": symbol,
            "timeframe": timeframe,
            "label": f"{symbol} {timeframe}",
            "ticks_count": int(ticks_count or 0),
            "market_instances": int(market_instances or 0),
        }
        markets.append(item)
        market_map[market_key] = item

    markets.sort(key=lambda item: market_sort_key(item["market_key"]))
    return {
        "scope": scope,
        "session_id": latest_session_id if scope == "latest_session" else None,
        "markets": markets,
        "market_map": market_map,
    }


def build_catalog(
    scope: str = "latest_session",
    mode: str = "full",
    adaptive: bool = False,
    selected_markets: list[str] | None = None,
    selected_strategies: list[str] | None = None,
    manual_samples: dict[str, int] | None = None,
) -> dict:
    scope = normalize_scope(scope)
    mode = normalize_mode(mode)
    adaptive = normalize_adaptive(adaptive) and mode != "fast-track"
    availability = load_available_markets(scope)
    market_map = availability["market_map"]
    markets = availability["markets"]

    selected_market_keys = selected_markets or [item["market_key"] for item in markets]
    selected_market_keys = [key for key in selected_market_keys if key in market_map]
    selected_market_keys.sort(key=market_sort_key)

    strategies = selected_strategies or list(STRATEGY_SPECS.keys())
    strategies = [key for key in strategies if key in STRATEGY_SPECS]

    combination_details = []
    total_grid_points = 0
    total_operations = 0
    for market_key in selected_market_keys:
        market_info = market_map[market_key]
        symbol = market_info["symbol"]
        timeframe = market_info["timeframe"]
        ticks_count = market_info["ticks_count"]
        for strategy_key in strategies:
            combo_key = f"{market_key}|{strategy_key}"
            sample_override = None
            if manual_samples and combo_key in manual_samples:
                sample_override = int(manual_samples[combo_key])
            detail = build_sampling_plan(
                strategy_key,
                symbol,
                timeframe,
                mode,
                adaptive=adaptive,
                sample_override=sample_override,
                seed=build_seed("catalog", market_key, strategy_key, sample_override or "auto", mode, "adaptive" if adaptive else "classic"),
                include_combinations=False,
            )
            if not detail["enabled"]:
                continue
            estimated_operations = detail["effective_sample_count"] * ticks_count
            total_grid_points += detail["effective_sample_count"]
            total_operations += estimated_operations
            combination_details.append(
                {
                    "combo_key": combo_key,
                    "market_key": market_key,
                    "market_label": market_info["label"],
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "strategy_key": strategy_key,
                    "strategy_label": detail["strategy_label"],
                    "adaptive": adaptive,
                    "summary": detail["summary"],
                    "ticks_count": ticks_count,
                    "market_instances": market_info["market_instances"],
                    "raw_total_combinations": detail["raw_total_combinations"],
                    "filtered_total_combinations": detail["filtered_total_combinations"],
                    "effective_sample_count": detail["effective_sample_count"],
                    "estimated_operations": estimated_operations,
                    "sampling_reason": detail["sampling_reason"],
                    "sampling_note": detail["sampling_note"],
                    "manual_sample_override": sample_override,
                    "parameters": detail["parameters"],
                }
            )

    return {
        "scope": scope,
        "mode": mode,
        "adaptive": adaptive,
        "mode_label": format_mode_label(mode, adaptive),
        "session_id": availability["session_id"],
        "available_markets": markets,
        "available_strategies": [
            {
                "strategy_key": key,
                "strategy_label": value["label"],
                "summary": value["summary"],
            }
            for key, value in STRATEGY_SPECS.items()
        ],
        "selected_markets": selected_market_keys,
        "selected_strategies": strategies,
        "summary": {
            "total_tests": len(combination_details),
            "total_grid_points": total_grid_points,
            "total_estimated_operations": total_operations,
        },
        "combination_details": combination_details,
    }


def ensure_run_tables(db_path: Path = BACKTEST_HISTORY_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id TEXT PRIMARY KEY,
                created_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                scope TEXT,
                mode TEXT,
                session_id TEXT,
                selected_markets TEXT,
                selected_strategies TEXT,
                total_tests INTEGER,
                total_grid_points INTEGER,
                total_estimated_operations INTEGER,
                elapsed_sec REAL,
                best_strategy TEXT,
                best_market TEXT,
                best_pnl_usd REAL,
                best_pnl_percent REAL,
                summary_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_run_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                created_at TEXT,
                market_key TEXT,
                symbol TEXT,
                timeframe TEXT,
                strategy TEXT,
                parameters TEXT,
                pnl_usd REAL,
                pnl_percent REAL,
                win_rate REAL,
                trades_count INTEGER,
                sampled_count INTEGER,
                total_param_combinations INTEGER,
                ticks_count INTEGER,
                estimated_operations INTEGER
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_run_record(run_id: str, plan: dict) -> None:
    ensure_run_tables()
    conn = sqlite3.connect(BACKTEST_HISTORY_PATH)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO backtest_runs (
                run_id, created_at, started_at, finished_at, status, scope, mode, session_id,
                selected_markets, selected_strategies, total_tests, total_grid_points,
                total_estimated_operations, elapsed_sec, summary_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                utc_now_iso(),
                utc_now_iso(),
                None,
                "running",
                plan["scope"],
                format_mode_label(plan["mode"], plan.get("adaptive", False)),
                plan.get("session_id"),
                json.dumps(plan["selected_markets"]),
                json.dumps(plan["selected_strategies"]),
                plan["summary"]["total_tests"],
                plan["summary"]["total_grid_points"],
                plan["summary"]["total_estimated_operations"],
                0.0,
                json.dumps(plan["summary"]),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def finish_run_record(run_id: str, status: str, elapsed_sec: float, summary: dict, best_result: dict | None) -> None:
    conn = sqlite3.connect(BACKTEST_HISTORY_PATH)
    try:
        conn.execute(
            """
            UPDATE backtest_runs
            SET finished_at = ?, status = ?, elapsed_sec = ?, best_strategy = ?, best_market = ?,
                best_pnl_usd = ?, best_pnl_percent = ?, summary_json = ?
            WHERE run_id = ?
            """,
            (
                utc_now_iso(),
                status,
                elapsed_sec,
                best_result.get("strategy_key") if best_result else None,
                best_result.get("market_key") if best_result else None,
                best_result.get("pnl_usd") if best_result else None,
                best_result.get("pnl_percent") if best_result else None,
                json.dumps(summary),
                run_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def save_run_result(run_id: str, entry: dict, result: dict, meta: dict) -> None:
    conn = sqlite3.connect(BACKTEST_HISTORY_PATH)
    try:
        conn.execute(
            """
            INSERT INTO backtest_run_results (
                run_id, created_at, market_key, symbol, timeframe, strategy, parameters,
                pnl_usd, pnl_percent, win_rate, trades_count, sampled_count,
                total_param_combinations, ticks_count, estimated_operations
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                utc_now_iso(),
                entry["market_key"],
                entry["symbol"],
                entry["timeframe"],
                entry["strategy_key"],
                json.dumps(result["p"]),
                float(result["pnl"]),
                float(result["pnl_proc"]),
                float(result["wr"]),
                int(result["t"]),
                int(meta["sampled_count"]),
                int(meta["total_param_combinations"]),
                int(meta["ticks_count"]),
                int(meta["estimated_operations"]),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_historical_top3() -> list[dict]:
    if not BACKTEST_HISTORY_PATH.exists():
        return []
    conn = sqlite3.connect(BACKTEST_HISTORY_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT symbol, timeframe, strategy, pnl_usd, pnl_percent, win_rate, trades_count, parameters, timestamp
            FROM optimization_logs
            ORDER BY pnl_usd DESC, pnl_percent DESC, win_rate DESC
            LIMIT 3
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    finally:
        conn.close()

    results = []
    for row in rows:
        results.append(
            {
                "market_key": f"{row['symbol']}_{row['timeframe']}",
                "market_label": f"{row['symbol']} {row['timeframe']}",
                "strategy_key": row["strategy"],
                "strategy_label": strategy_label(row["strategy"]) if row["strategy"] in STRATEGY_SPECS else row["strategy"],
                "pnl_usd": float(row["pnl_usd"] or 0.0),
                "pnl_percent": float(row["pnl_percent"] or 0.0),
                "win_rate": float(row["win_rate"] or 0.0),
                "trades_count": int(row["trades_count"] or 0),
                "timestamp": row["timestamp"],
                "parameters": json.loads(row["parameters"]) if row["parameters"] else {},
            }
        )
    return results


def serialize_run_result_row(row: sqlite3.Row) -> dict:
    return {
        "combo_key": f"{row['market_key']}|{row['strategy']}",
        "market_key": row["market_key"],
        "market_label": f"{row['symbol']} {row['timeframe']}",
        "strategy_key": row["strategy"],
        "strategy_label": strategy_label(row["strategy"]) if row["strategy"] in STRATEGY_SPECS else row["strategy"],
        "pnl_usd": float(row["pnl_usd"] or 0.0),
        "pnl_percent": float(row["pnl_percent"] or 0.0),
        "win_rate": float(row["win_rate"] or 0.0),
        "trades_count": int(row["trades_count"] or 0),
        "parameters": json.loads(row["parameters"]) if row["parameters"] else {},
        "sampled_count": int(row["sampled_count"] or 0),
        "total_param_combinations": int(row["total_param_combinations"] or 0),
        "ticks_count": int(row["ticks_count"] or 0),
        "estimated_operations": int(row["estimated_operations"] or 0),
        "created_at": row["created_at"],
        "adaptive": False,
    }


def get_last_run_summary() -> dict:
    if not BACKTEST_HISTORY_PATH.exists():
        return {"last_run": None, "historical_top_3": [], "completed_results": [], "best_of_run": None}

    ensure_run_tables()
    conn = sqlite3.connect(BACKTEST_HISTORY_PATH)
    conn.row_factory = sqlite3.Row
    try:
        run_row = conn.execute(
            """
            SELECT *
            FROM backtest_runs
            ORDER BY COALESCE(finished_at, started_at, created_at) DESC
            LIMIT 1
            """
        ).fetchone()
        result_row = None
        result_rows = []
        if run_row:
            result_row = conn.execute(
                """
                SELECT *
                FROM backtest_run_results
                WHERE run_id = ?
                ORDER BY pnl_usd DESC, pnl_percent DESC, win_rate DESC
                LIMIT 1
                """,
                (run_row["run_id"],),
            ).fetchone()
            result_rows = conn.execute(
                """
                SELECT *
                FROM backtest_run_results
                WHERE run_id = ?
                ORDER BY pnl_usd DESC, pnl_percent DESC, win_rate DESC
                LIMIT 8
                """,
                (run_row["run_id"],),
            ).fetchall()
    finally:
        conn.close()

    last_run = None
    completed_results = [serialize_run_result_row(row) for row in result_rows]
    if run_row:
        run_adaptive = "adaptive" in str(run_row["mode"] or "").lower()
        completed_results = [{**item, "adaptive": run_adaptive} for item in completed_results]
        last_run = {
            "run_id": run_row["run_id"],
            "status": run_row["status"],
            "scope": run_row["scope"],
            "mode": run_row["mode"],
            "started_at": run_row["started_at"],
            "finished_at": run_row["finished_at"],
            "elapsed_sec": float(run_row["elapsed_sec"] or 0.0),
            "elapsed_label": format_duration(float(run_row["elapsed_sec"] or 0.0)),
            "total_tests": int(run_row["total_tests"] or 0),
            "total_grid_points": int(run_row["total_grid_points"] or 0),
            "total_estimated_operations": int(run_row["total_estimated_operations"] or 0),
            "best_result": None,
            "results": completed_results,
            "adaptive": run_adaptive,
        }
        if result_row:
            last_run["best_result"] = {
                "market_key": result_row["market_key"],
                "market_label": f"{result_row['symbol']} {result_row['timeframe']}",
                "strategy_key": result_row["strategy"],
                "strategy_label": strategy_label(result_row["strategy"]) if result_row["strategy"] in STRATEGY_SPECS else result_row["strategy"],
                "pnl_usd": float(result_row["pnl_usd"] or 0.0),
                "pnl_percent": float(result_row["pnl_percent"] or 0.0),
                "win_rate": float(result_row["win_rate"] or 0.0),
                "trades_count": int(result_row["trades_count"] or 0),
                "parameters": json.loads(result_row["parameters"]) if result_row["parameters"] else {},
                "adaptive": run_adaptive,
            }
    return {
        "last_run": last_run,
        "historical_top_3": get_historical_top3(),
        "completed_results": completed_results,
        "best_of_run": completed_results[0] if completed_results else (last_run["best_result"] if last_run else None),
    }


def write_state(payload: dict) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = LATEST_STATE_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp_path.replace(LATEST_STATE_PATH)


def build_idle_state() -> dict:
    summary = get_last_run_summary()
    return {
        "status": "idle",
        "generated_at": utc_now_iso(),
        "adaptive": False,
        "progress": {
            "current_test_index": 0,
            "total_tests": 0,
            "current_test_progress_pct": 0.0,
            "total_progress_pct": 0.0,
            "elapsed_sec": 0.0,
            "elapsed_label": "00:00",
            "eta_sec": None,
            "eta_label": "--",
            "processed_operations": 0,
            "total_estimated_operations": 0,
            "operations_per_sec": 0.0,
            "current_test_points_done": 0,
            "current_test_points_total": 0,
        },
        "results": summary,
        "log_tail": [],
    }


def read_state() -> dict:
    if not LATEST_STATE_PATH.exists():
        return build_idle_state()
    payload = json.loads(LATEST_STATE_PATH.read_text(encoding="utf-8"))
    payload.setdefault("adaptive", False)
    payload.setdefault("results", get_last_run_summary())
    return payload


class RuntimeState:
    def __init__(self, run_id: str, plan: dict) -> None:
        self.run_id = run_id
        self.plan = plan
        self.started_at_monotonic = time.time()
        self.state = {
            "run_id": run_id,
            "pid": os.getpid(),
            "status": "running",
            "generated_at": utc_now_iso(),
            "started_at": utc_now_iso(),
            "finished_at": None,
            "scope": plan["scope"],
            "mode": plan["mode"],
            "adaptive": plan.get("adaptive", False),
            "session_id": plan.get("session_id"),
            "selected_markets": plan["selected_markets"],
            "selected_strategies": plan["selected_strategies"],
            "summary": plan["summary"],
            "plan": {
                "combination_details": plan["combination_details"],
            },
            "progress": {
                "current_test_index": 0,
                "total_tests": plan["summary"]["total_tests"],
                "current_test_progress_pct": 0.0,
                "total_progress_pct": 0.0,
                "elapsed_sec": 0.0,
                "elapsed_label": "00:00",
                "eta_sec": None,
                "eta_label": "--",
                "processed_operations": 0,
                "total_estimated_operations": plan["summary"]["total_estimated_operations"],
                "operations_per_sec": 0.0,
                "current_test_points_done": 0,
                "current_test_points_total": 0,
                "current_test_label": "",
                "current_test_market": "",
                "current_test_strategy": "",
            },
            "results": {
                **get_last_run_summary(),
                "completed_results": [],
                "best_of_run": None,
            },
            "log_tail": [],
        }
        write_state(self.state)

    def log(self, message: str) -> None:
        line = f"{datetime.now().strftime('%H:%M:%S')}  {message}"
        self.state["log_tail"] = (self.state.get("log_tail", []) + [line])[-24:]
        self.flush()

    def update_progress(
        self,
        current_test_index: int,
        total_tests: int,
        current_test_label: str,
        current_test_market: str,
        current_test_strategy: str,
        current_test_points_done: int,
        current_test_points_total: int,
        processed_operations: int,
    ) -> None:
        elapsed_sec = max(0.0, time.time() - self.started_at_monotonic)
        total_operations = self.state["progress"]["total_estimated_operations"]
        operations_per_sec = processed_operations / elapsed_sec if elapsed_sec > 0 else 0.0
        remaining_operations = max(0, total_operations - processed_operations)
        eta_sec = (remaining_operations / operations_per_sec) if operations_per_sec > 0 else None
        current_test_progress = (
            (current_test_points_done / current_test_points_total) * 100
            if current_test_points_total > 0
            else 0.0
        )
        total_progress = (
            (processed_operations / total_operations) * 100
            if total_operations > 0
            else 100.0
        )
        self.state["generated_at"] = utc_now_iso()
        self.state["progress"] = {
            "current_test_index": current_test_index,
            "total_tests": total_tests,
            "current_test_progress_pct": round(current_test_progress, 2),
            "total_progress_pct": round(total_progress, 2),
            "elapsed_sec": round(elapsed_sec, 2),
            "elapsed_label": format_duration(elapsed_sec),
            "eta_sec": round(eta_sec, 2) if eta_sec is not None else None,
            "eta_label": format_duration(eta_sec),
            "processed_operations": int(processed_operations),
            "total_estimated_operations": int(total_operations),
            "operations_per_sec": round(operations_per_sec, 2),
            "current_test_points_done": int(current_test_points_done),
            "current_test_points_total": int(current_test_points_total),
            "current_test_label": current_test_label,
            "current_test_market": current_test_market,
            "current_test_strategy": current_test_strategy,
        }
        self.flush()

    def complete(self, status: str, best_of_run: dict | None, completed_results: list[dict]) -> None:
        elapsed_sec = max(0.0, time.time() - self.started_at_monotonic)
        self.state["status"] = status
        self.state["finished_at"] = utc_now_iso()
        self.state["results"]["completed_results"] = completed_results
        self.state["results"]["best_of_run"] = best_of_run
        self.state["results"].update(get_last_run_summary())
        self.state["progress"]["elapsed_sec"] = round(elapsed_sec, 2)
        self.state["progress"]["elapsed_label"] = format_duration(elapsed_sec)
        self.state["progress"]["eta_sec"] = 0.0
        self.state["progress"]["eta_label"] = "00:00"
        if status == "completed":
            self.state["progress"]["current_test_progress_pct"] = 100.0
            self.state["progress"]["total_progress_pct"] = 100.0
            self.state["progress"]["processed_operations"] = self.state["progress"]["total_estimated_operations"]
        self.flush()

    def flush(self) -> None:
        write_state(self.state)


def build_runtime_plan(job: dict) -> tuple[dict, dict]:
    scope = normalize_scope(job.get("scope"))
    mode = normalize_mode(job.get("mode"))
    adaptive = normalize_adaptive(job.get("adaptive")) and mode != "fast-track"
    selected_markets = job.get("selected_markets") or []
    selected_strategies = job.get("selected_strategies") or list(STRATEGY_SPECS.keys())
    manual_samples = job.get("manual_samples") or {}

    catalog = build_catalog(
        scope=scope,
        mode=mode,
        adaptive=adaptive,
        selected_markets=selected_markets,
        selected_strategies=selected_strategies,
        manual_samples=manual_samples,
    )

    if mode == "fast-track":
        plan = {
            "run_id": job["run_id"],
            "scope": scope,
            "mode": mode,
            "adaptive": False,
            "session_id": catalog["session_id"],
            "selected_markets": catalog["selected_markets"],
            "selected_strategies": catalog["selected_strategies"],
            "summary": {
                "total_tests": 1,
                "total_grid_points": 1,
                "total_estimated_operations": 1,
            },
            "combination_details": catalog["combination_details"],
        }
        return plan, {"entries": []}

    historical_data = load_and_prepare_data(all_history=(scope == "all"))
    if historical_data.empty:
        raise RuntimeError("Nie znaleziono danych historycznych do backtestu.")

    selected_market_set = set(catalog["selected_markets"])
    historical_data = historical_data[historical_data["timeframe"].isin(selected_market_set)].copy()
    if historical_data.empty:
        raise RuntimeError("Brak danych dla wybranych rynków.")

    market_frames = {
        market_key: historical_data[historical_data["timeframe"] == market_key].copy()
        for market_key in catalog["selected_markets"]
    }
    plan_entries = []
    combination_details = []
    total_grid_points = 0
    total_operations = 0

    for detail in catalog["combination_details"]:
        market_key = detail["market_key"]
        df_interval = market_frames.get(market_key)
        if df_interval is None or df_interval.empty:
            continue
        sample_override = manual_samples.get(detail["combo_key"])
        sampling = build_sampling_plan(
            detail["strategy_key"],
            detail["symbol"],
            detail["timeframe"],
            mode,
            adaptive=adaptive,
            sample_override=sample_override,
            seed=build_seed(job["run_id"], detail["combo_key"], sample_override or "auto", mode, "adaptive" if adaptive else "classic"),
            include_combinations=True,
        )
        ticks_count = len(df_interval)
        sampled_count = sampling["effective_sample_count"]
        combinations = sampling.get("combinations") or []
        if not adaptive and sampled_count <= 0:
            continue
        estimated_operations = sampled_count * ticks_count
        total_grid_points += sampled_count
        total_operations += estimated_operations

        runtime_detail = {
            **detail,
            "adaptive": adaptive,
            "effective_sample_count": sampled_count,
            "raw_total_combinations": sampling["raw_total_combinations"],
            "filtered_total_combinations": sampling["filtered_total_combinations"],
            "sampling_reason": sampling["sampling_reason"],
            "sampling_note": sampling["sampling_note"],
            "estimated_operations": estimated_operations,
        }
        combination_details.append(runtime_detail)
        plan_entries.append(
            {
                "combo_key": detail["combo_key"],
                "market_key": market_key,
                "market_label": detail["market_label"],
                "symbol": detail["symbol"],
                "timeframe": detail["timeframe"],
                "strategy_key": detail["strategy_key"],
                "strategy_label": detail["strategy_label"],
                "adaptive": adaptive,
                "df_markets": df_interval,
                "combinations": combinations,
                "param_axes": sampling.get("param_axes"),
                "adaptive_meta": sampling.get("adaptive_meta"),
                "sampled_count": sampled_count,
                "total_param_combinations": sampling["raw_total_combinations"],
                "ticks_count": ticks_count,
                "estimated_operations": estimated_operations,
                "worker": STRATEGY_SPECS[detail["strategy_key"]]["worker"],
                "id_prefix": STRATEGY_SPECS[detail["strategy_key"]]["id_prefix"],
            }
        )

    plan = {
        "run_id": job["run_id"],
        "scope": scope,
        "mode": mode,
        "adaptive": adaptive,
        "session_id": catalog["session_id"],
        "selected_markets": catalog["selected_markets"],
        "selected_strategies": catalog["selected_strategies"],
        "summary": {
            "total_tests": len(plan_entries),
            "total_grid_points": total_grid_points,
            "total_estimated_operations": total_operations,
        },
        "combination_details": combination_details,
    }
    return plan, {"entries": plan_entries}


def execute_worker_with_progress(worker_func, combinations: list[dict], fast_markets: list[dict], progress_callback=None) -> list[dict]:
    if not combinations:
        return []
    num_cores = os.cpu_count() or 4
    chunk_size = max(1, len(combinations) // (num_cores * 4))
    chunks = [combinations[i:i + chunk_size] for i in range(0, len(combinations), chunk_size)]
    results = []
    completed_points = 0
    total_points = len(combinations)
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        future_sizes = {
            executor.submit(worker_func, chunk, fast_markets): len(chunk)
            for chunk in chunks
        }
        for future in as_completed(future_sizes):
            results.extend(future.result())
            completed_points += future_sizes[future]
            if progress_callback:
                progress_callback(completed_points, total_points)
    return results


def serialize_params(params: dict) -> str:
    return json.dumps(params, sort_keys=True, separators=(",", ":"))


def placeholder_result(params: dict) -> dict:
    return {
        "p": dict(params),
        "pnl": 0.0,
        "pnl_proc": 0.0,
        "wr": 0.0,
        "t": 0,
        "score": 0.0,
    }


def choose_best_result(candidates: list[dict]) -> dict | None:
    if not candidates:
        return None
    selected = select_optimal_result([dict(item) for item in candidates])
    if selected is None:
        return None
    for candidate in candidates:
        if serialize_params(candidate["p"]) == serialize_params(selected["p"]):
            return candidate
    return selected


def build_adaptive_candidates(current_params: dict, meta: dict, pct_offsets: list[int]) -> list[tuple[dict, int]]:
    candidates: list[tuple[dict, int]] = []
    seen: set[str] = set()
    for pct_offset in pct_offsets:
        value = quantize_adaptive_value(meta, pct_offset)
        params = dict(current_params)
        params[meta["name"]] = value
        key = serialize_params(params)
        if key in seen:
            continue
        seen.add(key)
        candidates.append((params, pct_offset))
    return candidates


def evaluate_candidate_batch(
    worker_func,
    fast_markets: list[dict],
    candidates: list[tuple[dict, int]],
    progress_callback,
) -> tuple[list[dict], dict[str, int]]:
    combinations = [params for params, _ in candidates]
    raw_results = execute_worker_with_progress(
        worker_func,
        combinations,
        fast_markets,
        progress_callback=progress_callback,
    )
    results_by_key = {serialize_params(result["p"]): result for result in raw_results}
    evaluated: list[dict] = []
    pct_by_key: dict[str, int] = {}
    for params, pct_offset in candidates:
        key = serialize_params(params)
        pct_by_key[key] = pct_offset
        evaluated.append(results_by_key.get(key, placeholder_result(params)))
    return evaluated, pct_by_key


def run_adaptive_strategy(
    entry: dict,
    fast_markets: list[dict],
    update_progress,
    log_progress,
) -> tuple[dict | None, int]:
    adaptive_meta = entry.get("adaptive_meta") or {}
    if not adaptive_meta:
        return None, 0

    ordered_meta = [adaptive_meta[name] for name in entry["param_axes"].keys() if name in adaptive_meta]
    current_params = {meta["name"]: meta["base_value"] for meta in ordered_meta}
    current_best = placeholder_result(current_params)
    points_done = 0

    def run_stage(candidates: list[tuple[dict, int]], stage_label: str) -> tuple[list[dict], dict[str, int]]:
        nonlocal points_done
        if not candidates:
            return [], {}

        def on_stage_progress(done_points: int, _total_points: int) -> None:
            update_progress(points_done + done_points, stage_label)

        evaluated, pct_by_key = evaluate_candidate_batch(
            entry["worker"],
            fast_markets,
            candidates,
            on_stage_progress,
        )
        points_done += len(candidates)
        update_progress(points_done, stage_label)
        return evaluated, pct_by_key

    base_results, _ = run_stage([(current_params, 0)], "Adaptive baseline")
    if base_results:
        current_best = base_results[0]

    for meta in ordered_meta:
        param_name = meta["name"]
        stage1_results, stage1_pct_map = run_stage(
            build_adaptive_candidates(current_best["p"], meta, list(range(-50, 51, 10))),
            f"Adaptive stage 1 · {param_name}",
        )
        stage1_best = choose_best_result([current_best, *stage1_results])
        if stage1_best is None or serialize_params(stage1_best["p"]) == serialize_params(current_best["p"]):
            continue

        best_stage1_pct = stage1_pct_map.get(serialize_params(stage1_best["p"]), 0)
        log_progress(f"Adaptive: {entry['market_label']} / {entry['strategy_label']} | {param_name} etap 1 -> {best_stage1_pct:+d}%")
        current_best = stage1_best

        stage2_results, stage2_pct_map = run_stage(
            build_adaptive_candidates(current_best["p"], meta, [best_stage1_pct - 5, best_stage1_pct, best_stage1_pct + 5]),
            f"Adaptive stage 2 · {param_name}",
        )
        stage2_best = choose_best_result([current_best, *stage2_results])
        if stage2_best is None or serialize_params(stage2_best["p"]) == serialize_params(current_best["p"]):
            continue

        best_stage2_pct = stage2_pct_map.get(serialize_params(stage2_best["p"]), best_stage1_pct)
        log_progress(f"Adaptive: {entry['market_label']} / {entry['strategy_label']} | {param_name} etap 2 -> {best_stage2_pct:+d}%")
        current_best = stage2_best

        stage3_results, stage3_pct_map = run_stage(
            build_adaptive_candidates(
                current_best["p"],
                meta,
                [best_stage2_pct - 2, best_stage2_pct - 1, best_stage2_pct, best_stage2_pct + 1, best_stage2_pct + 2],
            ),
            f"Adaptive stage 3 · {param_name}",
        )
        stage3_best = choose_best_result([current_best, *stage3_results])
        if stage3_best is not None and serialize_params(stage3_best["p"]) != serialize_params(current_best["p"]):
            best_stage3_pct = stage3_pct_map.get(serialize_params(stage3_best["p"]), best_stage2_pct)
            log_progress(f"Adaptive: {entry['market_label']} / {entry['strategy_label']} | {param_name} etap 3 -> {best_stage3_pct:+d}%")
            current_best = stage3_best

    selected = current_best if current_best.get("t", 0) > 0 else None
    return selected, points_done


def run_single_strategy(entry: dict, progress_state: RuntimeState, current_test_index: int, total_tests: int, completed_operations_before: int) -> tuple[dict | None, int]:
    fast_markets = prepare_fast_markets(entry["df_markets"])
    current_processed = completed_operations_before

    def on_progress(done_points: int, total_points: int) -> None:
        nonlocal current_processed
        current_processed = completed_operations_before + (done_points * entry["ticks_count"])
        progress_state.update_progress(
            current_test_index=current_test_index,
            total_tests=total_tests,
            current_test_label=f"Test {current_test_index} z {total_tests}",
            current_test_market=entry["market_label"],
            current_test_strategy=entry["strategy_label"],
            current_test_points_done=done_points,
            current_test_points_total=total_points,
            processed_operations=current_processed,
        )

    def update_adaptive_progress(done_points: int, stage_label: str) -> None:
        nonlocal current_processed
        current_processed = completed_operations_before + (done_points * entry["ticks_count"])
        progress_state.update_progress(
            current_test_index=current_test_index,
            total_tests=total_tests,
            current_test_label=stage_label,
            current_test_market=entry["market_label"],
            current_test_strategy=entry["strategy_label"],
            current_test_points_done=done_points,
            current_test_points_total=entry["sampled_count"],
            processed_operations=current_processed,
        )

    progress_state.log(
        f"Start {entry['market_label']} / {entry['strategy_label']} | "
        f"{'adaptive' if entry.get('adaptive') else 'próbek'}: {entry['sampled_count']}"
    )
    progress_state.update_progress(
        current_test_index=current_test_index,
        total_tests=total_tests,
        current_test_label=f"Test {current_test_index} z {total_tests}",
        current_test_market=entry["market_label"],
        current_test_strategy=entry["strategy_label"],
        current_test_points_done=0,
        current_test_points_total=entry["sampled_count"],
        processed_operations=completed_operations_before,
    )

    try:
        if entry.get("adaptive"):
            best_result, sampled_points = run_adaptive_strategy(
                entry,
                fast_markets,
                update_adaptive_progress,
                progress_state.log,
            )
            entry["sampled_count"] = sampled_points
            entry["estimated_operations"] = sampled_points * entry["ticks_count"]
        else:
            best_results = execute_worker_with_progress(
                entry["worker"],
                entry["combinations"],
                fast_markets,
                progress_callback=on_progress,
            )
            best_result = select_optimal_result(best_results)
    except Exception as exc:
        create_crash_dump(entry["strategy_key"], entry["symbol"], entry["timeframe"], exc)
        raise

    if best_result:
        best_result["p"]["id"] = f"{entry['symbol'].lower()}_{entry['timeframe']}_{entry['id_prefix']}_{uuid.uuid4().hex[:8]}"
    progress_state.update_progress(
        current_test_index=current_test_index,
        total_tests=total_tests,
        current_test_label=f"Test {current_test_index} z {total_tests}",
        current_test_market=entry["market_label"],
        current_test_strategy=entry["strategy_label"],
        current_test_points_done=entry["sampled_count"],
        current_test_points_total=entry["sampled_count"],
        processed_operations=completed_operations_before + entry["estimated_operations"],
    )
    return best_result, entry["estimated_operations"]


def result_payload(entry: dict, best_result: dict) -> dict:
    return {
        "combo_key": entry["combo_key"],
        "market_key": entry["market_key"],
        "market_label": entry["market_label"],
        "strategy_key": entry["strategy_key"],
        "strategy_label": entry["strategy_label"],
        "pnl_usd": float(best_result["pnl"]),
        "pnl_percent": float(best_result["pnl_proc"]),
        "win_rate": float(best_result["wr"]),
        "trades_count": int(best_result["t"]),
        "parameters": best_result["p"],
        "sampled_count": entry["sampled_count"],
        "total_param_combinations": entry["total_param_combinations"],
        "ticks_count": entry["ticks_count"],
        "estimated_operations": entry["estimated_operations"],
        "adaptive": bool(entry.get("adaptive")),
    }


def run_fast_track_job(plan: dict, progress_state: RuntimeState) -> tuple[list[dict], dict | None]:
    progress_state.log("Tryb fast-track: brak pełnego backtestu, pobieram najlepsze historyczne konfiguracje.")
    refresh_tracked_configs_from_history(db_path=str(BACKTEST_HISTORY_PATH), config_file=str(TRACKED_CONFIGS_PATH))
    progress_state.log(f"tracked_configs.json zaktualizowany automatycznie: {TRACKED_CONFIGS_PATH}")
    historical_top = get_historical_top3()
    selected = [
        item
        for item in historical_top
        if item["market_key"] in plan["selected_markets"] and item["strategy_key"] in plan["selected_strategies"]
    ]
    completed_results = []
    for item in selected[:3]:
        completed_results.append(
            {
                "combo_key": f"{item['market_key']}|{item['strategy_key']}",
                "market_key": item["market_key"],
                "market_label": item["market_label"],
                "strategy_key": item["strategy_key"],
                "strategy_label": item["strategy_label"],
                "pnl_usd": item["pnl_usd"],
                "pnl_percent": item["pnl_percent"],
                "win_rate": item["win_rate"],
                "trades_count": item["trades_count"],
                "parameters": item["parameters"],
                "sampled_count": 0,
                "total_param_combinations": 0,
                "ticks_count": 0,
                "estimated_operations": 0,
                "adaptive": False,
            }
        )
    best_of_run = completed_results[0] if completed_results else None
    progress_state.update_progress(
        current_test_index=1 if completed_results else 0,
        total_tests=max(1, len(completed_results)),
        current_test_label="Fast-track",
        current_test_market=best_of_run["market_label"] if best_of_run else "",
        current_test_strategy=best_of_run["strategy_label"] if best_of_run else "",
        current_test_points_done=1 if completed_results else 0,
        current_test_points_total=1 if completed_results else 0,
        processed_operations=plan["summary"]["total_estimated_operations"],
    )
    return completed_results, best_of_run


def run_job(job: dict) -> None:
    init_history_db()
    ensure_run_tables()

    plan, runtime_context = build_runtime_plan(job)
    create_run_record(job["run_id"], plan)
    progress_state = RuntimeState(job["run_id"], plan)
    progress_state.log(
        f"Przygotowano {plan['summary']['total_tests']} testów | punkty siatki: {plan['summary']['total_grid_points']} | koszt: {plan['summary']['total_estimated_operations']}"
    )

    completed_results: list[dict] = []
    best_of_run: dict | None = None
    completed_operations = 0
    start_ts = time.time()

    if plan["mode"] == "fast-track":
        completed_results, best_of_run = run_fast_track_job(plan, progress_state)
        finish_run_record(job["run_id"], "completed", time.time() - start_ts, plan["summary"], best_of_run)
        progress_state.complete("completed", best_of_run, completed_results)
        return

    total_tests = len(runtime_context["entries"])
    for index, entry in enumerate(runtime_context["entries"], start=1):
        best_result, finished_operations = run_single_strategy(
            entry,
            progress_state,
            current_test_index=index,
            total_tests=total_tests,
            completed_operations_before=completed_operations,
        )
        completed_operations += finished_operations
        if not best_result:
            progress_state.log(f"Brak wyniku dla {entry['market_label']} / {entry['strategy_label']}.")
            continue
        save_optimization_result(entry["symbol"], entry["timeframe"], entry["strategy_key"], best_result)
        save_run_result(
            job["run_id"],
            entry,
            best_result,
            {
                "sampled_count": entry["sampled_count"],
                "total_param_combinations": entry["total_param_combinations"],
                "ticks_count": entry["ticks_count"],
                "estimated_operations": entry["estimated_operations"],
            },
        )
        payload = result_payload(entry, best_result)
        completed_results.append(payload)
        progress_state.state["results"]["completed_results"] = sorted(
            completed_results,
            key=lambda item: (item["pnl_usd"], item["pnl_percent"], item["win_rate"]),
            reverse=True,
        )[:8]
        best_of_run = progress_state.state["results"]["completed_results"][0]
        progress_state.state["results"]["best_of_run"] = best_of_run
        progress_state.flush()
        progress_state.log(
            f"Zakończono {entry['market_label']} / {entry['strategy_label']} | PnL {payload['pnl_usd']:+.2f}$ | WR {payload['win_rate']:.1f}%"
        )

    elapsed_sec = time.time() - start_ts
    refresh_tracked_configs_from_history(db_path=str(BACKTEST_HISTORY_PATH), config_file=str(TRACKED_CONFIGS_PATH))
    progress_state.log(f"tracked_configs.json zaktualizowany automatycznie po backteście: {TRACKED_CONFIGS_PATH}")
    finish_run_record(job["run_id"], "completed", elapsed_sec, plan["summary"], best_of_run)
    progress_state.complete("completed", best_of_run, progress_state.state["results"]["completed_results"])


def build_job_payload(payload: dict | None = None) -> dict:
    payload = payload or {}
    return {
        "run_id": payload.get("run_id") or uuid.uuid4().hex,
        "scope": normalize_scope(payload.get("scope")),
        "mode": normalize_mode(payload.get("mode")),
        "adaptive": normalize_adaptive(payload.get("adaptive")),
        "selected_markets": payload.get("selected_markets") or [],
        "selected_strategies": payload.get("selected_strategies") or list(STRATEGY_SPECS.keys()),
        "manual_samples": payload.get("manual_samples") or {},
    }


def _kinetic_axes(symbol: str, timeframe: str) -> dict[str, list]:
    trigger_base_map = {"BTC": 0.00075, "ETH": 0.00105, "SOL": 0.0022, "XRP": 0.0027}
    return {
        "trigger_pct": jitter_values(trigger_base_map.get(symbol, 0.0010), precision=6, min_value=0.0),
        "max_slippage": jitter_values(0.035, precision=4, min_value=0.0001),
        "max_price": jitter_values(0.85, precision=3, min_value=0.05, max_value=0.999),
        "g_sec": jitter_values(3.0, precision=3, min_value=0.1),
        "window_ms": jitter_values(1000, integer=True, min_value=1),
    }


def _momentum_axes(symbol: str, timeframe: str) -> dict[str, list]:
    delta_base_map = {"BTC": 7.5, "ETH": 0.5, "SOL": 0.05, "XRP": 0.0005}
    return {
        "delta": jitter_values(delta_base_map.get(symbol, 1.0), precision=6, min_value=0.0),
        "max_p": jitter_values(0.725, precision=3, min_value=0.05, max_value=0.999),
        "win_start": jitter_values(150, integer=True, min_value=1),
        "win_end": jitter_values(28, integer=True, min_value=0),
        "g_sec": jitter_values(2.0, precision=3, min_value=0.1),
    }


def _mid_arb_axes(symbol: str, timeframe: str) -> dict[str, list]:
    delta_base_map = {"BTC": 5.0, "ETH": 0.3, "SOL": 0.02, "XRP": 0.0002}
    return {
        "delta": jitter_values(delta_base_map.get(symbol, 1.0), precision=6, min_value=0.0),
        "max_p": jitter_values(0.625, precision=3, min_value=0.05, max_value=0.999),
        "win_start": jitter_values(1200, integer=True, min_value=1),
        "win_end": jitter_values(180, integer=True, min_value=0),
        "g_sec": jitter_values(2.0, precision=3, min_value=0.1),
    }


def _otm_axes(symbol: str, timeframe: str) -> dict[str, list]:
    if symbol == "BTC":
        max_delta = 120.0
    elif symbol == "ETH":
        max_delta = 6.0
    elif symbol == "SOL":
        max_delta = 0.3
    elif symbol == "XRP":
        max_delta = 0.003
    else:
        max_delta = 10.0
    return {
        "win_start": jitter_values(90, integer=True, min_value=1),
        "win_end": jitter_values(35, integer=True, min_value=0),
        "max_p": jitter_values(0.135, precision=3, min_value=0.001, max_value=0.999),
        "g_sec": jitter_values(2.0, precision=3, min_value=0.1),
        "max_delta_abs": jitter_values(max_delta, precision=6, min_value=0.0),
    }


def _l2_scalper_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "min_spread_threshold": jitter_values(0.055, precision=4, min_value=0.0001),
        "skew_allowance": jitter_values(0.0625, precision=4, min_value=0.0001),
        "max_hold_sec": jitter_values(12.5, precision=3, min_value=0.1),
    }


def _vol_rev_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "binance_vol_multiplier": jitter_values(3.0, precision=3, min_value=0.01),
        "clob_panic_drop": jitter_values(0.048, precision=4, min_value=0.0001),
        "reversion_tp_multiplier": jitter_values(1.275, precision=4, min_value=1.001),
    }


def _mean_reversion_obi_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "obi_extreme": jitter_values([0.90], precision=3, min_value=0.55),
        "neutral_band": jitter_values([0.20], precision=3, min_value=0.05),
        "max_price": jitter_values([0.45], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([18], integer=True, min_value=4),
        "tp_mult": jitter_values([1.12], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.88], precision=3, min_value=0.50),
    }


def _spread_compression_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "spread_percentile": jitter_values([90], integer=True, min_value=70, max_value=100),
        "vol_ratio": jitter_values([1.80], precision=3, min_value=1.05),
        "median_revert_mult": jitter_values([1.10], precision=3, min_value=0.5),
        "max_price": jitter_values([0.75], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([16], integer=True, min_value=4),
        "tp_mult": jitter_values([1.10], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.90], precision=3, min_value=0.50),
    }


def _divergence_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "obi_threshold": jitter_values([0.65], precision=3, min_value=0.15),
        "divergence_gap": jitter_values([0.50], precision=3, min_value=0.10),
        "max_price": jitter_values([0.70], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([14], integer=True, min_value=4),
        "tp_mult": jitter_values([1.12], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.88], precision=3, min_value=0.50),
    }


def _obi_acceleration_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "delta_obi": jitter_values([0.18], precision=4, min_value=0.02),
        "vol_accel": jitter_values([1.25], precision=3, min_value=1.01),
        "max_price": jitter_values([0.82], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([15], integer=True, min_value=4),
        "tp_mult": jitter_values([1.15], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.90], precision=3, min_value=0.50),
    }


def _vcb_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "lookback_ticks": jitter_values([10], integer=True, min_value=4),
        "compression_pct": jitter_values([0.0008], precision=6, min_value=0.00005),
        "vol_build_mult": jitter_values([1.30], precision=3, min_value=1.01),
        "max_price": jitter_values([0.85], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([18], integer=True, min_value=4),
        "tp_mult": jitter_values([1.16], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.90], precision=3, min_value=0.50),
    }


def _absorption_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "lookback_ticks": jitter_values([8], integer=True, min_value=3),
        "price_stall_pct": jitter_values([0.0006], precision=6, min_value=0.00005),
        "absorption_ratio": jitter_values([2.0], precision=3, min_value=1.05),
        "max_price": jitter_values([0.75], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([16], integer=True, min_value=4),
        "tp_mult": jitter_values([1.12], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.90], precision=3, min_value=0.50),
    }


def _spillover_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "peer_signal": jitter_values([0.30], precision=3, min_value=0.05),
        "lag_tolerance": jitter_values([0.10], precision=3, min_value=0.01),
        "memory_markets": jitter_values([3], integer=True, min_value=1),
        "max_price": jitter_values([0.80], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([14], integer=True, min_value=4),
        "tp_mult": jitter_values([1.10], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.90], precision=3, min_value=0.50),
    }


def _session_edge_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "hour_start": jitter_values([13], integer=True, min_value=0, max_value=23),
        "hour_end": jitter_values([20], integer=True, min_value=0, max_value=23),
        "liquidity_cutoff": jitter_values([500.0], precision=3, min_value=10.0),
        "obi_extreme": jitter_values([0.70], precision=3, min_value=0.10),
        "delta_obi": jitter_values([0.14], precision=3, min_value=0.02),
        "max_price": jitter_values([0.80], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([14], integer=True, min_value=4),
        "tp_mult": jitter_values([1.10], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.90], precision=3, min_value=0.50),
    }


def _settlement_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "enter_sec_left": jitter_values([45], integer=True, min_value=2),
        "prob_threshold": jitter_values([0.85], precision=3, min_value=0.50),
        "obi_confirm": jitter_values([0.25], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([12], integer=True, min_value=2),
        "tp_mult": jitter_values([1.08], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.93], precision=3, min_value=0.50),
    }


def _vacuum_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "min_counter_vol": jitter_values([40.0], precision=3, min_value=1.0),
        "dominant_vol": jitter_values([180.0], precision=3, min_value=5.0),
        "max_price": jitter_values([0.82], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([12], integer=True, min_value=3),
        "tp_mult": jitter_values([1.10], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.92], precision=3, min_value=0.50),
    }


def _pullback_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "lookback_ticks": jitter_values([6], integer=True, min_value=2),
        "impulse_ticks": jitter_values([0.06], precision=4, min_value=0.005),
        "pullback_ticks": jitter_values([0.02], precision=4, min_value=0.002),
        "obi_floor": jitter_values([0.15], precision=3, min_value=0.02),
        "max_price": jitter_values([0.82], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([12], integer=True, min_value=3),
        "tp_mult": jitter_values([1.10], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.92], precision=3, min_value=0.50),
    }


def _synthetic_axes(symbol: str, timeframe: str) -> dict[str, list]:
    return {
        "arb_gap": jitter_values([0.03], precision=4, min_value=0.002),
        "revert_band": jitter_values([0.01], precision=4, min_value=0.001),
        "max_price": jitter_values([0.85], precision=3, min_value=0.05),
        "max_hold_ticks": jitter_values([10], integer=True, min_value=2),
        "tp_mult": jitter_values([1.08], precision=3, min_value=1.01),
        "sl_mult": jitter_values([0.94], precision=3, min_value=0.50),
    }


STRATEGY_SPECS = {
    "kinetic_sniper": {
        "label": "Kinetic Sniper",
        "summary": "Skanuje gwałtowny impuls ceny i łapie krótki ruch po wybiciu.",
        "worker": worker_kinetic_sniper,
        "param_builder": _kinetic_axes,
        "id_prefix": "kin",
    },
    "momentum": {
        "label": "1-Min Momentum",
        "summary": "Wejście w oknie końcówki rynku przy odchyleniu ceny od strike.",
        "worker": worker_1min_momentum,
        "param_builder": _momentum_axes,
        "id_prefix": "mom",
        "post_filter": lambda combo: combo["win_start"] > combo["win_end"] + 10,
    },
    "mid_arb": {
        "label": "Mid-Game Arb",
        "summary": "Szuka arbitrażowego odchylenia w środkowej fazie życia rynku.",
        "worker": worker_mid_arb,
        "param_builder": _mid_arb_axes,
        "id_prefix": "arb",
        "post_filter": lambda combo: combo["win_start"] > combo["win_end"] + 60,
    },
    "otm": {
        "label": "OTM Bargain",
        "summary": "Testuje tanie wejścia blisko strike w końcówce sesji.",
        "worker": worker_otm,
        "param_builder": _otm_axes,
        "id_prefix": "otm",
        "post_filter": lambda combo: combo["win_start"] > combo["win_end"],
    },
    "l2_spread_scalper": {
        "label": "L2 Spread Scalper",
        "summary": "Poluje na kompresję spreadu przy neutralnym skew orderbooka.",
        "worker": worker_l2_spread_scalper,
        "param_builder": _l2_scalper_axes,
        "id_prefix": "l2scalp",
    },
    "vol_mean_reversion": {
        "label": "Vol Mean Reversion",
        "summary": "Łapie odwrócenie po panicznym zrzucie i skoku wolumenu.",
        "worker": worker_vol_mean_reversion,
        "param_builder": _vol_rev_axes,
        "id_prefix": "volrev",
    },
    "mean_reversion_obi": {
        "label": "Mean Reversion OBI",
        "summary": "Kontruje skrajny imbalance i zamyka po powrocie do neutralności.",
        "worker": worker_mean_reversion_obi,
        "param_builder": _mean_reversion_obi_axes,
        "id_prefix": "obi_mr",
    },
    "spread_compression": {
        "label": "Spread Compression",
        "summary": "Wchodzi przy ekstremalnym spreadzie i wychodzi na powrocie do mediany.",
        "worker": worker_spread_compression,
        "param_builder": _spread_compression_axes,
        "id_prefix": "spread_cmp",
    },
    "divergence_imbalance": {
        "label": "Divergence Imbalance",
        "summary": "Szuka rozjazdu imbalance między stroną UP i DOWN.",
        "worker": worker_divergence_imbalance,
        "param_builder": _divergence_axes,
        "id_prefix": "div_obi",
    },
    "obi_acceleration": {
        "label": "OBI Acceleration",
        "summary": "Reaguje na nagłe przyspieszenie imbalance i wolumenu.",
        "worker": worker_obi_acceleration,
        "param_builder": _obi_acceleration_axes,
        "id_prefix": "obi_acc",
    },
    "volatility_compression_breakout": {
        "label": "Volatility Compression Breakout",
        "summary": "Testuje breakout po kompresji zakresu i odbudowie wolumenu.",
        "worker": worker_volatility_compression_breakout,
        "param_builder": _vcb_axes,
        "id_prefix": "vcb",
    },
    "absorption_pattern": {
        "label": "Absorption Pattern",
        "summary": "Szuka absorpcji po zatrzymaniu ruchu ceny i przewadze jednej strony booka.",
        "worker": worker_absorption_pattern,
        "param_builder": _absorption_axes,
        "id_prefix": "absorb",
    },
    "cross_market_spillover": {
        "label": "Cross-Market Spillover",
        "summary": "Wykorzystuje sentyment z poprzednich rynków tego samego interwału.",
        "worker": worker_cross_market_spillover,
        "param_builder": _spillover_axes,
        "id_prefix": "spill",
    },
    "session_based_edge": {
        "label": "Session Based Edge",
        "summary": "Zmienna logika zależna od godziny i płynności sesji.",
        "worker": worker_session_based_edge,
        "param_builder": _session_edge_axes,
        "id_prefix": "session",
        "post_filter": lambda combo: combo["hour_start"] <= combo["hour_end"],
    },
    "settlement_convergence": {
        "label": "Settlement Convergence",
        "summary": "Wchodzi w końcówce, gdy prawdopodobieństwo i OBI potwierdzają kierunek.",
        "worker": worker_settlement_convergence,
        "param_builder": _settlement_axes,
        "id_prefix": "settle",
    },
    "liquidity_vacuum": {
        "label": "Liquidity Vacuum",
        "summary": "Poluje na próżnię płynności po stronie przeciwnej.",
        "worker": worker_liquidity_vacuum,
        "param_builder": _vacuum_axes,
        "id_prefix": "vacuum",
    },
    "micro_pullback_continuation": {
        "label": "Micro Pullback Continuation",
        "summary": "Kupuje kontynuację po krótkim cofnięciu impulsu.",
        "worker": worker_micro_pullback_continuation,
        "param_builder": _pullback_axes,
        "id_prefix": "pullback",
    },
    "synthetic_arbitrage": {
        "label": "Synthetic Arbitrage",
        "summary": "Szuka błędnej sumy cen UP+DOWN i wyjścia po rewersie do parytetu.",
        "worker": worker_synthetic_arbitrage,
        "param_builder": _synthetic_axes,
        "id_prefix": "synarb",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtester dashboard runtime")
    parser.add_argument("--job-file", required=True, help="Path to dashboard job JSON file")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    job_file = Path(args.job_file)
    job = json.loads(job_file.read_text(encoding="utf-8"))
    try:
        run_job(job)
    except KeyboardInterrupt:
        elapsed = 0.0
        if LATEST_STATE_PATH.exists():
            state = read_state()
            state["status"] = "stopped"
            state["finished_at"] = utc_now_iso()
            state["results"] = get_last_run_summary()
            write_state(state)
        finish_run_record(job["run_id"], "stopped", elapsed, {"total_tests": 0}, None)
        raise
    except Exception as exc:
        state = read_state()
        state["status"] = "failed"
        state["finished_at"] = utc_now_iso()
        state["error"] = str(exc)
        state["log_tail"] = (state.get("log_tail", []) + [f"{datetime.now().strftime('%H:%M:%S')}  ERROR {exc}"])[-24:]
        state["results"] = get_last_run_summary()
        write_state(state)
        finish_run_record(job["run_id"], "failed", 0.0, {"total_tests": 0}, None)
        raise


if __name__ == "__main__":
    main()
