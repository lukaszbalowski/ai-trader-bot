import sqlite3
import pandas as pd
import numpy as np
import random
import time
import itertools
import os
import json
import argparse
import uuid
import math
import traceback
import gc
import builtins
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

PARAM_JITTER_MIN_PCT = -0.85
PARAM_JITTER_MAX_PCT = 0.85
PARAM_JITTER_STEP_PCT = 0.05
MONTE_CARLO_LIMIT = 1_000_000
QUICK_MONTE_CARLO_LIMIT = 100_000
TERMINAL_LOG_BUFFER = []
QUICK_MODE = False


def print(*args, **kwargs):
    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "\n")
    TERMINAL_LOG_BUFFER.append(sep.join(str(arg) for arg in args) + end)
    builtins.print(*args, **kwargs)


def save_terminal_summary_txt():
    os.makedirs("data/backtest_reports", exist_ok=True)
    report_file = f"data/backtest_reports/backtest_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write("".join(TERMINAL_LOG_BUFFER))
    print(f"📝 Zapisano pełny log testu do {report_file}.")
    return report_file

# ==========================================
# 0A. ALPHA VAULT (OPTIMIZATION HISTORY)
# ==========================================
def init_history_db(db_path="data/backtest_history.db"):
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE IF NOT EXISTS optimization_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        symbol TEXT,
        timeframe TEXT,
        strategy TEXT,
        parameters TEXT,
        pnl_usd REAL,
        pnl_percent REAL,
        win_rate REAL,
        trades_count INTEGER
    )''')
    conn.commit()
    conn.close()

def save_optimization_result(symbol, timeframe, strategy, result_obj, db_path="data/backtest_history.db"):
    conn = sqlite3.connect(db_path)
    conn.execute('''INSERT INTO optimization_logs 
        (timestamp, symbol, timeframe, strategy, parameters, pnl_usd, pnl_percent, win_rate, trades_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
        datetime.now().isoformat(),
        symbol,
        timeframe,
        strategy,
        json.dumps(result_obj['p']),  
        result_obj['pnl'],
        result_obj['pnl_proc'],
        result_obj['wr'],
        result_obj['t']
    ))
    conn.commit()
    conn.close()

def get_top_3_historical(symbol, timeframe, strategy, db_path="data/backtest_history.db"):
    if not os.path.exists(db_path):
        return []
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute('''SELECT timestamp, win_rate, pnl_percent, parameters, trades_count 
                       FROM optimization_logs 
                       WHERE symbol=? AND timeframe=? AND strategy=? 
                       ORDER BY pnl_percent DESC, win_rate DESC LIMIT 3''', (symbol, timeframe, strategy))
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception:
        return []

# ==========================================
# 0B. CORE EVALUATION, RISK & CRASH DUMPS
# ==========================================
def calculate_composite_score(pnl: float, trades_count: int) -> float:
    if trades_count <= 0 or pnl <= 0:
        return 0.0
    return pnl * math.log1p(trades_count)

def validate_market_rules(strategy: str, symbol: str, timeframe: str) -> bool:
    if strategy == "otm":
        if timeframe == "1h": 
            return False
        if timeframe == "15m" and symbol in ["ETH", "SOL", "XRP"]: 
            return False
    return True

def create_crash_dump(strategy_name, symbol, interval, exception):
    """Zrzuca stan pamięci do pliku tekstowego w przypadku błędu OOM lub innych awarii."""
    os.makedirs("data/crashes", exist_ok=True)
    dump_file = f"data/crashes/crash_dump_{symbol}_{interval}_{strategy_name}_{int(time.time())}.log"
    try:
        with open(dump_file, "w", encoding="utf-8") as f:
            f.write(f"=== WATCHER HFT CRASH DUMP ===\n")
            f.write(f"Data: {datetime.now().isoformat()}\n")
            f.write(f"Rynek: {symbol}_{interval}\n")
            f.write(f"Strategia: {strategy_name}\n")
            f.write(f"Błąd główny: {str(exception)}\n")
            f.write(f"\n--- TRACEBACK ---\n")
            traceback.print_exc(file=f)
        print(f"\n   ❌ KRYTYCZNY BŁĄD PROCESU (Prawdopodobnie brak RAM). Test przerwany!")
        print(f"   💾 Zrzut logów zapisano do pliku: {dump_file}")
    except Exception as log_err:
        print(f"\n   ❌ BŁĄD KRYTYCZNY: Nie udało się nawet zapisać zrzutu pamięci ({log_err})")

# ==========================================
# 0C. FAST-TRACK COMPILER & POST MORTEM
# ==========================================
def compile_best_from_vault(db_path="data/backtest_history.db"):
    print("\n" + "=" * 80)
    print(" 🚀 FAST-TRACK COMPILER: Budowanie bazy najlepszych parametrów z Alpha Vault")
    print("=" * 80)
    if not os.path.exists(db_path):
        print("Brak pliku Alpha Vault (backtest_history.db). Przerywam kompilację.")
        return {}
    optimizations = {}
    markets = [
        ('BTC', '5m', 2), ('ETH', '5m', 2), ('SOL', '5m', 3), ('XRP', '5m', 4),
        ('BTC', '15m', 2), ('ETH', '15m', 2), ('SOL', '15m', 3), ('XRP', '15m', 4),
        ('BTC', '1h', 2), ('ETH', '1h', 2), ('SOL', '1h', 3), ('XRP', '1h', 4)
    ]
    strategies = [
        'kinetic_sniper', 'momentum', 'mid_arb', 'otm', 'l2_spread_scalper', 'vol_mean_reversion',
        'mean_reversion_obi', 'spread_compression', 'divergence_imbalance', 'obi_acceleration',
        'volatility_compression_breakout', 'absorption_pattern', 'cross_market_spillover',
        'session_based_edge', 'settlement_convergence', 'liquidity_vacuum',
        'micro_pullback_continuation', 'synthetic_arbitrage'
    ]
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for sym, tf, _ in markets:
        market_key = f"{sym}_{tf}"
        optimizations[market_key] = {}
        for strat in strategies:
            cur.execute('''SELECT parameters, win_rate, pnl_percent FROM optimization_logs 
                           WHERE symbol=? AND timeframe=? AND strategy=? 
                           ORDER BY pnl_percent DESC LIMIT 1''', (sym, tf, strat))
            row = cur.fetchone()
            if row:
                params_str, wr, pnl = row
                try:
                    p = json.loads(params_str)
                    p['wr'] = round(wr, 1)
                    optimizations[market_key][strat] = p
                    print(f"   ✅ Skompilowano {sym} {tf} | {strat} | Rekordowy PnL: +{pnl:.2f}%")
                except json.JSONDecodeError: pass
    conn.close()
    return optimizations

def get_latest_session_id(db_path="data/polymarket.db"):
    if not os.path.exists(db_path): return None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT session_id FROM market_logs_v11 WHERE session_id IS NOT NULL ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception: return None

def run_post_mortem_analysis(db_path="data/polymarket.db", all_history=False):
    print("\n" + "=" * 80)
    print(" 🕵️ POST-MORTEM ANALYSIS (WYNIKI HISTORYCZNE TRADINGU) ")
    print("=" * 80)
    if not os.path.exists(db_path):
        print("Brak pliku bazy danych. Pomijam analizę Post-Mortem.")
        return
    try:
        latest_session = get_latest_session_id(db_path) if not all_history else None
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_logs_v10'")
        if not cur.fetchone():
            print("Tabela trade_logs_v10 jest pusta. Pomijam analizę.")
            conn.close()
            return
        if latest_session:
            print(f"🔍 Analiza wyizolowana stricte dla ostatniej sesji: {latest_session}")
            try: df_trades = pd.read_sql_query(f"SELECT * FROM trade_logs_v10 WHERE session_id='{latest_session}'", conn)
            except: df_trades = pd.read_sql_query("SELECT * FROM trade_logs_v10", conn)
        else:
            print(f"🌍 Analiza wykorzystująca PEŁNĄ bazę historyczną.")
            df_trades = pd.read_sql_query("SELECT * FROM trade_logs_v10", conn)
        conn.close()
        if df_trades.empty:
            print("Nie znaleziono historii zrealizowanych transakcji dla tego przebiegu.")
            return
        total_pnl = df_trades['pnl'].sum()
        total_trades = len(df_trades)
        win_rate = (len(df_trades[df_trades['pnl'] > 0]) / total_trades) * 100 if total_trades > 0 else 0.0
        print(f"📊 PnL Sesji: {total_pnl:>+10.2f}$ | Ilość transakcji: {total_trades} | Średnie WR: {win_rate:.1f}%\n")
        print(f"{'RYNEK':<12} | {'STRATEGIA':<16} | {'TRADES':<6} | {'WR %':<6} | {'PNL ($)':<12}")
        print("-" * 65)
        grouped = df_trades.groupby(['timeframe', 'strategy'])
        for (market, strategy), group in grouped:
            trades_count = len(group)
            winning_trades = len(group[group['pnl'] > 0])
            wr = (winning_trades / trades_count) * 100 if trades_count > 0 else 0.0
            pnl_sum = group['pnl'].sum()
            color_start = "\033[32m" if pnl_sum > 0 else "\033[31m"
            color_end = "\033[0m"
            print(f"{market:<12} | {strategy:<16} | {trades_count:<6} | {wr:>5.1f}% | {color_start}{pnl_sum:>+10.2f}${color_end}")
    except Exception as e:
        print(f"Błąd podczas generowania analizy Post-Mortem: {e}")

# ==========================================
# 1. LEVEL 2 DATA PREPARATION
# ==========================================
def load_and_prepare_data(db_path="data/polymarket.db", all_history=False):
    if not os.path.exists(db_path): return pd.DataFrame()
    latest_session = get_latest_session_id(db_path) if not all_history else None
    conn = sqlite3.connect(db_path)
    try:
        if latest_session:
            print(f"⏳ Pobieranie ticków Level 2 wyłącznie dla sesji: {latest_session}...")
            query = f"SELECT * FROM market_logs_v11 WHERE session_id='{latest_session}' AND (buy_up > 0 OR buy_down > 0) ORDER BY fetched_at ASC"
            df = pd.read_sql_query(query, conn)
        else:
            print(f"🌍 Pobieranie ticków Level 2 dla pełnej historii...")
            df = pd.read_sql_query("SELECT * FROM market_logs_v11 WHERE buy_up > 0 OR buy_down > 0 ORDER BY fetched_at ASC", conn)
    except Exception:
        df = pd.DataFrame()
    finally:
        conn.close()
    if df.empty: return df
    df['fetched_at'] = pd.to_datetime(df['fetched_at'], format='ISO8601')
    timeframe_parts = df['timeframe'].str.split('_', n=1, expand=True)
    df['symbol'] = timeframe_parts[0]
    df['interval_label'] = timeframe_parts[1]
    df['session_hour'] = df['fetched_at'].dt.hour
    df['session_minute'] = df['fetched_at'].dt.minute
    df['up_spread'] = (df['sell_up'] - df['buy_up']).clip(lower=0.0)
    df['dn_spread'] = (df['sell_down'] - df['buy_down']).clip(lower=0.0)
    df['up_total_vol'] = df['buy_up_vol'] + df['sell_up_vol']
    df['dn_total_vol'] = df['buy_down_vol'] + df['sell_down_vol']
    df['up_vol_imbalance'] = (df['buy_up_vol'] - df['sell_up_vol']) / (df['up_total_vol'].replace(0, np.nan))
    df['dn_vol_imbalance'] = (df['buy_down_vol'] - df['sell_down_vol']) / (df['dn_total_vol'].replace(0, np.nan))
    df['up_vol_imbalance'] = df['up_vol_imbalance'].fillna(0.0)
    df['dn_vol_imbalance'] = df['dn_vol_imbalance'].fillna(0.0)
    max_times = df.groupby('market_id')['fetched_at'].transform('max')
    df['sec_left'] = (max_times - df['fetched_at']).dt.total_seconds()
    min_times = df.groupby('market_id')['fetched_at'].transform('min')
    df['sec_since_start'] = (df['fetched_at'] - min_times).dt.total_seconds()
    df['asset_jump'] = df.groupby('market_id')['live_price'].diff()
    df['up_change'] = df.groupby('market_id')['buy_up'].diff().abs()
    df['dn_change'] = df.groupby('market_id')['buy_down'].diff().abs()
    df['live_pct_change'] = df.groupby('market_id')['live_price'].pct_change().fillna(0.0)
    last_ticks = df.groupby('market_id').last()
    winning_up_markets = last_ticks[last_ticks['live_price'] >= last_ticks['target_price']].index.tolist()
    df['won_up'] = df['market_id'].isin(winning_up_markets)
    return df

def prepare_fast_markets(df_markets):
    markets = []
    ordered = df_markets.sort_values(['fetched_at', 'market_id'])
    for m_id, group in ordered.groupby('market_id', sort=False):
        markets.append({
            'm_id': m_id,
            'symbol': group['symbol'].iloc[0],
            'interval': group['interval_label'].iloc[0],
            'session_id': group['session_id'].iloc[0] if 'session_id' in group.columns else '',
            'session_hour': group['session_hour'].values,
            'target': group['target_price'].iloc[0],
            'won_up': group['won_up'].iloc[0],
            'sec_left': group['sec_left'].values,
            'sec_since_start': group['sec_since_start'].values,
            'live': group['live_price'].values,
            'live_pct_change': group['live_pct_change'].values,
            'up_change': group['up_change'].values,
            'dn_change': group['dn_change'].values,
            'b_up': group['buy_up'].values,
            'b_dn': group['buy_down'].values,
            's_up': group['sell_up'].values,
            's_dn': group['sell_down'].values,
            'buy_up_vol': group['buy_up_vol'].values,
            'sell_up_vol': group['sell_up_vol'].values,
            'buy_down_vol': group['buy_down_vol'].values,
            'sell_down_vol': group['sell_down_vol'].values,
            'up_obi': group['up_obi'].values,
            'dn_obi': group['dn_obi'].values,
            'up_spread': group['up_spread'].values,
            'dn_spread': group['dn_spread'].values,
            'up_total_vol': group['up_total_vol'].values,
            'dn_total_vol': group['dn_total_vol'].values,
            'up_vol_imbalance': group['up_vol_imbalance'].values,
            'dn_vol_imbalance': group['dn_vol_imbalance'].values
        })
    return markets

# ==========================================
# SIMULATION EXIT ENGINES
# ==========================================
def simulate_kinetic_exit(mkt, entry_idx, direction, entry_price, stake, global_sec_rule):
    shares = stake / entry_price
    ticks_down = 0
    last_bid = entry_price
    for tick in range(entry_idx + 1, len(mkt['sec_left'])):
        sec_left = mkt['sec_left'][tick]
        current_bid = mkt['b_up'][tick] if direction == 'UP' else mkt['b_dn'][tick]
        if sec_left <= global_sec_rule and current_bid > entry_price: return (current_bid * shares) - stake
        if current_bid >= entry_price * 1.50: return (current_bid * shares) - stake
        if tick - entry_idx >= 10 and current_bid > entry_price: return (current_bid * shares) - stake
        if current_bid < last_bid and current_bid > entry_price: ticks_down += 1
        elif current_bid > last_bid: ticks_down = 0
        if ticks_down >= 2 and current_bid > entry_price: return (current_bid * shares) - stake
        if current_bid <= entry_price * 0.90:
            exit_tick = min(tick + 10, len(mkt['sec_left']) - 1)
            exit_bid = mkt['b_up'][exit_tick] if direction == 'UP' else mkt['b_dn'][exit_tick]
            return (exit_bid * shares) - stake
        last_bid = current_bid
    won = mkt['won_up'] if direction == 'UP' else not mkt['won_up']
    return (1.0 * shares - stake) if won else -stake

def simulate_simple_exit(mkt, entry_idx, direction, entry_price, stake, global_sec_rule):
    shares = stake / entry_price
    for tick in range(entry_idx + 1, len(mkt['sec_left'])):
        sec_left = mkt['sec_left'][tick]
        current_bid = mkt['b_up'][tick] if direction == 'UP' else mkt['b_dn'][tick]
        if sec_left <= global_sec_rule and current_bid > entry_price: return (current_bid * shares) - stake
        if current_bid >= entry_price * 3.0: return (current_bid * shares) - stake
    won = mkt['won_up'] if direction == 'UP' else not mkt['won_up']
    return (1.0 * shares - stake) if won else -stake

def simulate_micro_exit(mkt, entry_idx, direction, entry_price, stake, params, neutral_fn=None):
    shares = stake / entry_price
    max_hold_ticks = int(params.get('max_hold_ticks', 20))
    tp_mult = params.get('tp_mult', 1.10)
    sl_mult = params.get('sl_mult', 0.85)
    for tick in range(entry_idx + 1, min(len(mkt['sec_left']), entry_idx + max_hold_ticks + 1)):
        current_bid = mkt['b_up'][tick] if direction == 'UP' else mkt['b_dn'][tick]
        if current_bid <= 0:
            continue
        if neutral_fn and neutral_fn(mkt, tick, direction, entry_price, current_bid):
            return (current_bid * shares) - stake
        if current_bid >= entry_price * tp_mult:
            return (current_bid * shares) - stake
        if current_bid <= entry_price * sl_mult:
            return (current_bid * shares) - stake
    final_tick = min(len(mkt['sec_left']) - 1, entry_idx + max_hold_ticks)
    final_bid = mkt['b_up'][final_tick] if direction == 'UP' else mkt['b_dn'][final_tick]
    if final_bid > 0:
        return (final_bid * shares) - stake
    won = mkt['won_up'] if direction == 'UP' else not mkt['won_up']
    return (1.0 * shares - stake) if won else -stake

# ==========================================
# SHARED OPTIMIZATION HELPERS
# ==========================================
def select_optimal_result(best_results):
    if not best_results: return None
    best_results.sort(key=lambda x: x['score'], reverse=True)
    absolute_best = best_results[0]
    valid_results = [r for r in best_results if r['t'] >= 3]
    if not valid_results:
        print(f"   ⚠️ Uwaga: Żaden wariant nie przekroczył 3 transakcji (najlepszy ma {absolute_best['t']} wejść).")
        return absolute_best
    return valid_results[0]

def get_sampling_target(total, default_limit=MONTE_CARLO_LIMIT):
    if total <= 0:
        return 0, None
    if QUICK_MODE:
        if 100_001 <= total <= 1_000_000:
            return QUICK_MONTE_CARLO_LIMIT, "quick_100k"
        if total > 1_000_000:
            return min(default_limit, max(1, int(total * 0.10))), "quick_10pct_capped"
        return total, None
    if total > default_limit:
        return default_limit, "default_limit"
    return total, None

def format_sampling_reason(reason, total, sample_size):
    if reason == "quick_100k":
        return (
            f"Quick mode: siatka badawcza ({total}) mieści się w przedziale 100 001-1 000 000. "
            f"Losuję 100 000 kombinacji parametrów Monte Carlo i testuję je na pełnej historii ticków."
        )
    if reason == "quick_10pct_capped":
        pct = (sample_size / total) * 100 if total else 0
        cap_note = " (cap 1 000 000)" if sample_size == MONTE_CARLO_LIMIT and total * 0.10 > MONTE_CARLO_LIMIT else ""
        return (
            f"Quick mode: siatka badawcza ({total}) przekracza 1 000 000. "
            f"Losuję {sample_size} kombinacji parametrów Monte Carlo ({pct:.2f}% całości){cap_note} "
            f"i testuję je na pełnej historii ticków."
        )
    return f"Siatka badawcza ({total}) przekracza limit. Uruchamiam metodę Monte Carlo (losuję {sample_size} prób)..."

def log_parameter_search_space(sampled_count, total_param_combinations, ticks_count):
    estimated_tick_evals = sampled_count * ticks_count
    print(
        "   📊 Przestrzeń testu | "
        f"kombinacje_parametrow: {sampled_count} (z {total_param_combinations}) | "
        f"ticki_bazy: {ticks_count} | "
        f"szac_koszt_tick_eval: {estimated_tick_evals}"
    )

def apply_monte_carlo(combinations, limit=1000000):
    total = len(combinations)
    sample_size, reason = get_sampling_target(total, default_limit=limit)
    if reason:
        print(f"   ⚠️ {format_sampling_reason(reason, total, sample_size)}")
        sampled = random.sample(combinations, sample_size)
        # Szybkie odzyskiwanie pamięci RAM
        del combinations
        gc.collect()
        return sampled, total
    return combinations, total

def jitter_values(values, integer=False, precision=6, min_value=None, max_value=None):
    if isinstance(values, (int, float)):
        values = [values]
    expanded = set()
    for value in values:
        pct = PARAM_JITTER_MIN_PCT
        variants = []
        while pct <= PARAM_JITTER_MAX_PCT + 1e-9:
            variants.append(value * (1.0 + pct))
            pct += PARAM_JITTER_STEP_PCT
        for variant in variants:
            if integer:
                variant = int(round(variant))
            else:
                variant = round(float(variant), precision)
            if min_value is not None and variant < min_value:
                variant = min_value
            if max_value is not None and variant > max_value:
                variant = max_value
            expanded.add(variant)
    return sorted(expanded)

def build_param_combinations(param_axes, fixed_params=None, limit=MONTE_CARLO_LIMIT):
    fixed_params = fixed_params or {}
    keys = list(param_axes.keys())
    axes = [list(dict.fromkeys(param_axes[key])) for key in keys]
    total = 1
    for axis in axes:
        total *= max(1, len(axis))
    sample_size, reason = get_sampling_target(total, default_limit=limit)
    if not reason:
        combinations = []
        for values in itertools.product(*axes):
            combo = dict(zip(keys, values))
            combo.update(fixed_params)
            combinations.append(combo)
        return combinations, total

    print(f"   ⚠️ {format_sampling_reason(reason, total, sample_size)}")
    sampled = set()
    max_attempts = max(sample_size * 5, 1_000)
    attempts = 0
    while len(sampled) < sample_size and attempts < max_attempts:
        sampled.add(tuple(random.choice(axis) for axis in axes))
        attempts += 1
    combinations = []
    for values in sampled:
        combo = dict(zip(keys, values))
        combo.update(fixed_params)
        combinations.append(combo)
    return combinations, total

def display_results_and_compare(best_result, symbol, interval, strategy_name):
    if not best_result: return
    params = best_result['p']
    print(f"\n   👑 TOP OBECNEGO TESTU: {best_result['pnl']:>+7.2f}$ ({best_result['pnl_proc']:>+6.2f}%) | WR: {best_result['wr']:>5.1f}% | T: {best_result['t']:>3} | SCORE: {best_result.get('score', 0):.2f}")
    param_str = ", ".join([f"{k}={v}" for k, v in params.items() if k not in ['g_sec', 'id', 'max_delta_abs', 'window_ms']])
    print(f"   ⚙️ Parametry: {param_str}")
    print(f"   🔑 ID Bazy: {params['id']}")
    top_3 = get_top_3_historical(symbol, interval, strategy_name)
    print("   🏆 TOP 3 HISTORYCZNE (Wg Zysku i WR):")
    if not top_3:
        print("      Brak rekordów. Ta strategia ustanawia pierwszy wynik w skarbcu Alpha Vault.")
    else:
        for idx, row in enumerate(top_3):
            ts, wr, pnl_proc, params_str, t_count = row
            try:
                params_json = json.loads(params_str)
                hist_id = params_json.get('id', 'No ID')
                hist_clean_params = {k: v for k, v in params_json.items() if k not in ['g_sec', 'id', 'max_delta_abs']}
                hist_param_str = ", ".join([f"{k}={v}" for k, v in hist_clean_params.items()])
            except:
                hist_id = "Brak ID"; hist_param_str = "Brak danych"
            date_str = ts[:10]
            print(f"      {idx+1}. WR: {wr:>5.1f}% | PnL: {pnl_proc:>+6.2f}% | T: {t_count} | ID: {hist_id} (Data: {date_str})")
            print(f"         ⚙️ Parametry: {hist_param_str}")

def execute_with_progress(worker_func, combinations, fast_markets, num_cores):
    if not combinations: return []
    chunk_size = max(1, len(combinations) // (num_cores * 4))
    chunks = [combinations[i:i + chunk_size] for i in range(0, len(combinations), chunk_size)]
    best_results = []
    print(f"   ⏳ Postęp testów ({len(combinations)} pkt): 0%", end="", flush=True)
    total_chunks = len(chunks)
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [executor.submit(worker_func, chunk, fast_markets) for chunk in chunks]
        completed = 0
        last_printed_pct = 0
        for f in as_completed(futures):
            best_results.extend(f.result())
            completed += 1
            pct = int((completed / total_chunks) * 100)
            if pct - last_printed_pct >= 10 or completed == total_chunks:
                step = (pct // 10) * 10
                if step > last_printed_pct:
                    print(f" ➡️ {step}%", end="", flush=True)
                    last_printed_pct = step
    print(" ✅ Gotowe!", flush=True)
    return best_results

# ==========================================
# WORKERS & STRATEGIES (HYPER-DENSE + MONTE CARLO)
# ==========================================

# --- 1. KINETIC SNIPER ---
def worker_kinetic_sniper(param_chunk, fast_markets):
    results = []
    stake = 1.0 
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            for tick in range(len(mkt['sec_left'])):
                if mkt['sec_left'][tick] <= 10: break
                pct_jump = mkt['live_pct_change'][tick]
                entry = None
                if pct_jump >= params['trigger_pct'] and mkt['up_change'][tick] <= params['max_slippage'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                    entry = ('UP', mkt['b_up'][tick])
                elif pct_jump <= -params['trigger_pct'] and mkt['dn_change'][tick] <= params['max_slippage'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                    entry = ('DOWN', mkt['b_dn'][tick])
                if entry:
                    trade_pnl = simulate_kinetic_exit(mkt, tick, entry[0], entry[1], stake, params['g_sec'])
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0: wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count/trades_count)*100, 't': trades_count, 'score': score})
    return results

def test_kinetic_sniper(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_kin_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🚀 KINETIC SNIPER | {symbol} {interval}")
    
    trigger_base_map = {
        "BTC": 0.00075,
        "ETH": 0.00105,
        "SOL": 0.0022,
        "XRP": 0.0027,
    }
    param_axes = {
        'trigger_pct': jitter_values(trigger_base_map.get(symbol, 0.0010), precision=6, min_value=0.0),
        'max_slippage': jitter_values(0.035, precision=4, min_value=0.0001),
        'max_price': jitter_values(0.85, precision=3, min_value=0.05, max_value=0.999),
        'g_sec': jitter_values(3.0, precision=3, min_value=0.1),
        'window_ms': jitter_values(1000, integer=True, min_value=1),
    }

    combinations, original_count = build_param_combinations(param_axes)
    ticks_count = len(df_markets)
    log_parameter_search_space(len(combinations), original_count, ticks_count)
    
    fast_markets = prepare_fast_markets(df_markets)
    num_cores = os.cpu_count() or 4
    
    try:
        best_results = execute_with_progress(worker_kinetic_sniper, combinations, fast_markets, num_cores)
    except Exception as e:
        create_crash_dump("kinetic_sniper", symbol, interval, e)
        return None
        
    best_result = select_optimal_result(best_results)
    if not best_result: return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "kinetic_sniper")
    return best_result

# --- 2. 1-MINUTE MOMENTUM ---
def worker_1min_momentum(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            entry_idx = -1
            entry_type = None
            entry_price = 0.0
            for tick in range(len(mkt['sec_left'])):
                if mkt['sec_left'][tick] > params['win_start']: continue
                if mkt['sec_left'][tick] < params['win_end']: break
                delta = mkt['live'][tick] - mkt['target']
                if delta >= params['delta'] and 0 < mkt['b_up'][tick] <= params['max_p']:
                    entry_idx = tick; entry_type = 'UP'; entry_price = mkt['b_up'][tick]
                    break
                elif delta <= -params['delta'] and 0 < mkt['b_dn'][tick] <= params['max_p']:
                    entry_idx = tick; entry_type = 'DOWN'; entry_price = mkt['b_dn'][tick]
                    break
            if entry_idx != -1:
                trade_pnl = simulate_simple_exit(mkt, entry_idx, entry_type, entry_price, stake, params['g_sec'])
                total_pnl += trade_pnl
                trades_count += 1
                if trade_pnl > 0: wins_count += 1
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count/trades_count)*100, 't': trades_count, 'score': score})
    return results

def test_1min_momentum(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_mom_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🚀 1-MINUTE MOMENTUM | {symbol} {interval}")
    
    delta_base_map = {
        "BTC": 7.5,
        "ETH": 0.5,
        "SOL": 0.05,
        "XRP": 0.0005,
    }
    param_axes = {
        'delta': jitter_values(delta_base_map.get(symbol, 1.0), precision=6, min_value=0.0),
        'max_p': jitter_values(0.725, precision=3, min_value=0.05, max_value=0.999),
        'win_start': jitter_values(150, integer=True, min_value=1),
        'win_end': jitter_values(28, integer=True, min_value=0),
        'g_sec': jitter_values(2.0, precision=3, min_value=0.1),
    }

    combinations, original_count = build_param_combinations(param_axes)
    combinations = [c for c in combinations if c['win_start'] > c['win_end'] + 10]
    ticks_count = len(df_markets)
    log_parameter_search_space(len(combinations), original_count, ticks_count)
    
    fast_markets = prepare_fast_markets(df_markets)
    num_cores = os.cpu_count() or 4
    random.shuffle(combinations) 
    
    try:
        best_results = execute_with_progress(worker_1min_momentum, combinations, fast_markets, num_cores)
    except Exception as e:
        create_crash_dump("momentum", symbol, interval, e)
        return None
        
    best_result = select_optimal_result(best_results)
    if not best_result: return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "momentum")
    return best_result

# --- 3. MID GAME ARB ---
def worker_mid_arb(param_chunk, fast_markets):
    results = []
    stake = 2.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            for tick in range(len(mkt['sec_left'])):
                if params['win_start'] > mkt['sec_left'][tick] > params['win_end']:
                    delta = mkt['live'][tick] - mkt['target']
                    entry = None
                    if delta > params['delta'] and 0 < mkt['b_up'][tick] <= params['max_p']:
                        entry = ('UP', mkt['b_up'][tick])
                    elif delta < -params['delta'] and 0 < mkt['b_dn'][tick] <= params['max_p']:
                        entry = ('DOWN', mkt['b_dn'][tick])
                    if entry:
                        trade_pnl = simulate_simple_exit(mkt, tick, entry[0], entry[1], stake, params['g_sec'])
                        total_pnl += trade_pnl
                        trades_count += 1
                        if trade_pnl > 0: wins_count += 1
                        break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count/trades_count)*100, 't': trades_count, 'score': score})
    return results

def test_mid_game_arb(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_arb_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" ⚖️ MID-GAME ARB | {symbol} {interval}")
    
    delta_base_map = {
        "BTC": 5.0,
        "ETH": 0.3,
        "SOL": 0.02,
        "XRP": 0.0002,
    }
    param_axes = {
        'delta': jitter_values(delta_base_map.get(symbol, 1.0), precision=6, min_value=0.0),
        'max_p': jitter_values(0.625, precision=3, min_value=0.05, max_value=0.999),
        'win_start': jitter_values(1200, integer=True, min_value=1),
        'win_end': jitter_values(180, integer=True, min_value=0),
        'g_sec': jitter_values(2.0, precision=3, min_value=0.1),
    }

    combinations, original_count = build_param_combinations(param_axes)
    combinations = [c for c in combinations if c['win_start'] > c['win_end'] + 60]
    ticks_count = len(df_markets)
    log_parameter_search_space(len(combinations), original_count, ticks_count)
    
    fast_markets = prepare_fast_markets(df_markets)
    num_cores = os.cpu_count() or 4
    random.shuffle(combinations)
    
    try:
        best_results = execute_with_progress(worker_mid_arb, combinations, fast_markets, num_cores)
    except Exception as e:
        create_crash_dump("mid_arb", symbol, interval, e)
        return None
        
    best_result = select_optimal_result(best_results)
    if not best_result: return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "mid_arb")
    return best_result

# --- 4. OTM BARGAIN ---
def worker_otm(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            for tick in range(len(mkt['sec_left'])):
                if params['win_start'] >= mkt['sec_left'][tick] >= params['win_end']:
                    delta_abs = abs(mkt['live'][tick] - mkt['target'])
                    entry = None
                    if delta_abs < params['max_delta_abs']:
                        if 0 < mkt['b_up'][tick] <= params['max_p']: entry = ('UP', mkt['b_up'][tick])
                        elif 0 < mkt['b_dn'][tick] <= params['max_p']: entry = ('DOWN', mkt['b_dn'][tick])
                        if entry:
                            trade_pnl = simulate_simple_exit(mkt, tick, entry[0], entry[1], stake, params['g_sec'])
                            total_pnl += trade_pnl
                            trades_count += 1
                            if trade_pnl > 0: wins_count += 1
                            break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count/trades_count)*100, 't': trades_count, 'score': score})
    return results

def test_otm_bargain(df_markets, symbol, interval):
    if not validate_market_rules("otm", symbol, interval):
        print(f"\n   ⛔ OTM BARGAIN BLOCKED dla {symbol} {interval} (Reguły Bezpieczeństwa)")
        return None

    strat_id = f"{symbol.lower()}_{interval}_otm_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🎟️ OTM BARGAIN | {symbol} {interval}")
    
    if symbol == "BTC": max_d = 120.0
    elif symbol == "ETH": max_d = 6.0
    elif symbol == "SOL": max_d = 0.3
    elif symbol == "XRP": max_d = 0.003
    else: max_d = 10.0
    
    param_axes = {
        'win_start': jitter_values(90, integer=True, min_value=1),
        'win_end': jitter_values(35, integer=True, min_value=0),
        'max_p': jitter_values(0.135, precision=3, min_value=0.001, max_value=0.999),
        'g_sec': jitter_values(2.0, precision=3, min_value=0.1),
        'max_delta_abs': jitter_values(max_d, precision=6, min_value=0.0),
    }

    combinations, original_count = build_param_combinations(param_axes)
    combinations = [c for c in combinations if c['win_start'] > c['win_end']]
    ticks_count = len(df_markets)
    log_parameter_search_space(len(combinations), original_count, ticks_count)
    
    fast_markets = prepare_fast_markets(df_markets)
    num_cores = os.cpu_count() or 4
    random.shuffle(combinations)
    
    try:
        best_results = execute_with_progress(worker_otm, combinations, fast_markets, num_cores)
    except Exception as e:
        create_crash_dump("otm", symbol, interval, e)
        return None
        
    best_result = select_optimal_result(best_results)
    if not best_result: return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "otm")
    return best_result

# --- 5. L2 SPREAD SCALPER ---
def worker_l2_spread_scalper(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        spread_threshold = params['min_spread_threshold']
        skew_allowance = params['skew_allowance']
        max_hold_sec = params['max_hold_sec']
        for mkt in fast_markets:
            for tick in range(len(mkt['sec_left'])):
                up_spread = mkt['s_up'][tick] - mkt['b_up'][tick]
                dn_spread = mkt['s_dn'][tick] - mkt['b_dn'][tick]
                up_skew = abs(mkt['up_vol_imbalance'][tick])
                dn_skew = abs(mkt['dn_vol_imbalance'][tick])
                entry = None
                if up_spread >= spread_threshold and up_skew <= skew_allowance and mkt['b_up'][tick] > 0.0:
                    entry = ('UP', mkt['b_up'][tick], up_spread)
                elif dn_spread >= spread_threshold and dn_skew <= skew_allowance and mkt['b_dn'][tick] > 0.0:
                    entry = ('DOWN', mkt['b_dn'][tick], dn_spread)
                if not entry:
                    continue

                direction, entry_price, entry_spread = entry
                shares = stake / entry_price
                exit_pnl = None
                entry_sec_left = mkt['sec_left'][tick]
                trades_count += 1

                for future_tick in range(tick + 1, len(mkt['sec_left'])):
                    held_sec = max(0.0, entry_sec_left - mkt['sec_left'][future_tick])
                    current_bid = mkt['b_up'][future_tick] if direction == 'UP' else mkt['b_dn'][future_tick]
                    current_spread = mkt['up_spread'][future_tick] if direction == 'UP' else mkt['dn_spread'][future_tick]
                    if current_bid <= 0:
                        continue
                    if current_spread <= entry_spread * 0.5:
                        exit_pnl = (current_bid * shares) - stake
                        break
                    if current_bid >= entry_price * 1.05:
                        exit_pnl = (current_bid * shares) - stake
                        break
                    if current_bid <= entry_price * 0.95:
                        exit_pnl = (current_bid * shares) - stake
                        break
                    if held_sec >= max_hold_sec:
                        exit_pnl = (current_bid * shares) - stake
                        break

                if exit_pnl is None:
                    final_bid = mkt['b_up'][-1] if direction == 'UP' else mkt['b_dn'][-1]
                    if final_bid > 0:
                        exit_pnl = (final_bid * shares) - stake
                    else:
                        won = mkt['won_up'] if direction == 'UP' else not mkt['won_up']
                        exit_pnl = (1.0 * shares - stake) if won else -stake

                total_pnl += exit_pnl
                if exit_pnl > 0:
                    wins_count += 1
                break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count/trades_count)*100, 't': trades_count, 'score': score})
    return results

def test_l2_spread_scalper(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_l2scalp_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" ⚡ L2 SPREAD SCALPER | {symbol} {interval}")
    
    param_axes = {
        'min_spread_threshold': jitter_values(0.055, precision=4, min_value=0.0001),
        'skew_allowance': jitter_values(0.0625, precision=4, min_value=0.0001),
        'max_hold_sec': jitter_values(12.5, precision=3, min_value=0.1),
    }
    combinations, original_count = build_param_combinations(param_axes)
    ticks_count = len(df_markets)
    log_parameter_search_space(len(combinations), original_count, ticks_count)
    
    fast_markets = prepare_fast_markets(df_markets)
    num_cores = os.cpu_count() or 4
    
    try:
        best_results = execute_with_progress(worker_l2_spread_scalper, combinations, fast_markets, num_cores)
    except Exception as e:
        create_crash_dump("l2_spread_scalper", symbol, interval, e)
        return None
        
    best_result = select_optimal_result(best_results)
    if not best_result: return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "l2_spread_scalper")
    return best_result

# --- 6. VOLATILITY MEAN REVERSION ---
def worker_vol_mean_reversion(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        drop_threshold = params['clob_panic_drop']
        rev_multiplier = params['reversion_tp_multiplier']
        vol_multiplier = params['binance_vol_multiplier']
        for mkt in fast_markets:
            total_vol = mkt['up_total_vol'] + mkt['dn_total_vol']
            for tick in range(5, len(mkt['sec_left'])):
                recent_avg_vol = float(np.mean(total_vol[max(0, tick - 5):tick]))
                current_vol = total_vol[tick]
                volume_spike = current_vol >= recent_avg_vol * vol_multiplier if recent_avg_vol > 0 else current_vol > 0
                if mkt['live_pct_change'][tick] <= -drop_threshold and volume_spike:
                    entry_price = mkt['b_up'][tick]
                    if entry_price <= 0:
                        continue
                    trades_count += 1
                    reverted = False
                    for future_tick in range(tick + 1, min(tick + 50, len(mkt['sec_left']))):
                        if mkt['b_up'][future_tick] >= entry_price * rev_multiplier:
                            reverted = True
                            break
                    if reverted:
                        wins_count += 1
                        total_pnl += (entry_price * rev_multiplier) - entry_price
                    else:
                        total_pnl -= entry_price
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count/trades_count)*100, 't': trades_count, 'score': score})
    return results

def test_vol_mean_reversion(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_volrev_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 📉 VOL MEAN REVERSION | {symbol} {interval}")
    
    param_axes = {
        'binance_vol_multiplier': jitter_values(3.0, precision=3, min_value=0.01),
        'clob_panic_drop': jitter_values(0.048, precision=4, min_value=0.0001),
        'reversion_tp_multiplier': jitter_values(1.275, precision=4, min_value=1.001),
    }
    combinations, original_count = build_param_combinations(param_axes)
    ticks_count = len(df_markets)
    log_parameter_search_space(len(combinations), original_count, ticks_count)
    
    fast_markets = prepare_fast_markets(df_markets)
    num_cores = os.cpu_count() or 4
    
    try:
        best_results = execute_with_progress(worker_vol_mean_reversion, combinations, fast_markets, num_cores)
    except Exception as e:
        create_crash_dump("vol_mean_reversion", symbol, interval, e)
        return None
        
    best_result = select_optimal_result(best_results)
    if not best_result: return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "vol_mean_reversion")
    return best_result

# --- 7. MEAN REVERSION PO EKSTREMACH OBI ---
def worker_mean_reversion_obi(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        def neutral_exit(mkt, tick, direction, entry_price, current_bid):
            return abs(mkt['up_obi'][tick]) <= params['neutral_band'] and abs(mkt['dn_obi'][tick]) <= params['neutral_band']
        for mkt in fast_markets:
            for tick in range(1, len(mkt['sec_left'])):
                entry = None
                if mkt['up_obi'][tick] >= params['obi_extreme'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                    entry = ('DOWN', mkt['b_dn'][tick])
                elif mkt['up_obi'][tick] <= -params['obi_extreme'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                    entry = ('UP', mkt['b_up'][tick])
                elif mkt['dn_obi'][tick] >= params['obi_extreme'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                    entry = ('UP', mkt['b_up'][tick])
                elif mkt['dn_obi'][tick] <= -params['obi_extreme'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                    entry = ('DOWN', mkt['b_dn'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params, neutral_exit)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_mean_reversion_obi(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_obi_mr_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🔄 MEAN REVERSION OBI | {symbol} {interval}")
    param_axes = {
        'obi_extreme': jitter_values([0.90], precision=3, min_value=0.55),
        'neutral_band': jitter_values([0.20], precision=3, min_value=0.05),
        'max_price': jitter_values([0.45], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([18], integer=True, min_value=4),
        'tp_mult': jitter_values([1.12], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.88], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_mean_reversion_obi, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "mean_reversion_obi")
    return best_result

# --- 8. SPREAD COMPRESSION STRATEGY ---
def worker_spread_compression(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            up_spreads = mkt['up_spread'][mkt['up_spread'] > 0]
            dn_spreads = mkt['dn_spread'][mkt['dn_spread'] > 0]
            if len(up_spreads) == 0 and len(dn_spreads) == 0:
                continue
            up_pctl = np.percentile(up_spreads, params['spread_percentile']) if len(up_spreads) else 0.0
            dn_pctl = np.percentile(dn_spreads, params['spread_percentile']) if len(dn_spreads) else 0.0
            up_median = np.median(up_spreads) if len(up_spreads) else 0.0
            dn_median = np.median(dn_spreads) if len(dn_spreads) else 0.0

            def neutral_exit(mkt2, tick, direction, entry_price, current_bid):
                current_spread = mkt2['up_spread'][tick] if direction == 'UP' else mkt2['dn_spread'][tick]
                median_spread = up_median if direction == 'UP' else dn_median
                return current_spread <= median_spread * params['median_revert_mult']

            for tick in range(len(mkt['sec_left'])):
                entry = None
                up_ratio = mkt['buy_up_vol'][tick] / max(mkt['sell_up_vol'][tick], 1e-9)
                dn_ratio = mkt['buy_down_vol'][tick] / max(mkt['sell_down_vol'][tick], 1e-9)
                if mkt['up_spread'][tick] >= up_pctl and up_ratio >= params['vol_ratio'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                    entry = ('UP', mkt['b_up'][tick])
                elif mkt['dn_spread'][tick] >= dn_pctl and dn_ratio >= params['vol_ratio'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                    entry = ('DOWN', mkt['b_dn'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params, neutral_exit)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_spread_compression(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_spread_cmp_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🗜️ SPREAD COMPRESSION | {symbol} {interval}")
    param_axes = {
        'spread_percentile': jitter_values([90], integer=True, min_value=70, max_value=100),
        'vol_ratio': jitter_values([1.80], precision=3, min_value=1.05),
        'median_revert_mult': jitter_values([1.10], precision=3, min_value=0.5),
        'max_price': jitter_values([0.75], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([16], integer=True, min_value=4),
        'tp_mult': jitter_values([1.10], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.90], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_spread_compression, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "spread_compression")
    return best_result

# --- 9. DIVERGENCE UP VS DOWN IMBALANCE ---
def worker_divergence_imbalance(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            for tick in range(len(mkt['sec_left'])):
                entry = None
                if mkt['up_obi'][tick] >= params['obi_threshold'] and (mkt['up_obi'][tick] - mkt['dn_obi'][tick]) >= params['divergence_gap'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                    entry = ('DOWN', mkt['b_dn'][tick])
                elif mkt['dn_obi'][tick] >= params['obi_threshold'] and (mkt['dn_obi'][tick] - mkt['up_obi'][tick]) >= params['divergence_gap'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                    entry = ('UP', mkt['b_up'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_divergence_imbalance(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_div_obi_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" ↔️ DIVERGENCE UP/DOWN IMBALANCE | {symbol} {interval}")
    param_axes = {
        'obi_threshold': jitter_values([0.65], precision=3, min_value=0.15),
        'divergence_gap': jitter_values([0.50], precision=3, min_value=0.10),
        'max_price': jitter_values([0.70], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([14], integer=True, min_value=4),
        'tp_mult': jitter_values([1.12], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.88], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_divergence_imbalance, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "divergence_imbalance")
    return best_result

# --- 10. MOMENTUM OBI ACCELERATION ---
def worker_obi_acceleration(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            for tick in range(1, len(mkt['sec_left'])):
                entry = None
                up_delta = mkt['up_obi'][tick] - mkt['up_obi'][tick - 1]
                dn_delta = mkt['dn_obi'][tick] - mkt['dn_obi'][tick - 1]
                up_vol_ratio = mkt['buy_up_vol'][tick] / max(mkt['buy_up_vol'][tick - 1], 1e-9)
                dn_vol_ratio = mkt['buy_down_vol'][tick] / max(mkt['buy_down_vol'][tick - 1], 1e-9)
                if up_delta >= params['delta_obi'] and up_vol_ratio >= params['vol_accel'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                    entry = ('UP', mkt['b_up'][tick])
                elif dn_delta >= params['delta_obi'] and dn_vol_ratio >= params['vol_accel'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                    entry = ('DOWN', mkt['b_dn'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_obi_acceleration(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_obi_acc_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🚄 MOMENTUM OBI ACCELERATION | {symbol} {interval}")
    param_axes = {
        'delta_obi': jitter_values([0.18], precision=4, min_value=0.02),
        'vol_accel': jitter_values([1.25], precision=3, min_value=1.01),
        'max_price': jitter_values([0.82], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([15], integer=True, min_value=4),
        'tp_mult': jitter_values([1.15], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.90], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_obi_acceleration, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "obi_acceleration")
    return best_result

# --- 11. VOLATILITY COMPRESSION -> BREAKOUT ---
def worker_volatility_compression_breakout(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        lookback = int(params['lookback_ticks'])
        for mkt in fast_markets:
            for tick in range(lookback, len(mkt['sec_left'])):
                price_window = mkt['live'][tick - lookback:tick]
                if len(price_window) < lookback:
                    continue
                compression = (price_window.max() - price_window.min()) / max(abs(price_window.mean()), 1e-9)
                entry = None
                if compression <= params['compression_pct']:
                    recent_high = price_window.max()
                    recent_low = price_window.min()
                    avg_buy_vol = np.mean(mkt['buy_up_vol'][tick - lookback:tick])
                    avg_sell_vol = np.mean(mkt['buy_down_vol'][tick - lookback:tick])
                    if mkt['live'][tick] > recent_high and mkt['buy_up_vol'][tick] >= avg_buy_vol * params['vol_build_mult'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                        entry = ('UP', mkt['b_up'][tick])
                    elif mkt['live'][tick] < recent_low and mkt['buy_down_vol'][tick] >= avg_sell_vol * params['vol_build_mult'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                        entry = ('DOWN', mkt['b_dn'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_volatility_compression_breakout(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_vcb_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 📦 VOLATILITY COMPRESSION BREAKOUT | {symbol} {interval}")
    param_axes = {
        'lookback_ticks': jitter_values([10], integer=True, min_value=4),
        'compression_pct': jitter_values([0.0008], precision=6, min_value=0.00005),
        'vol_build_mult': jitter_values([1.30], precision=3, min_value=1.01),
        'max_price': jitter_values([0.85], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([18], integer=True, min_value=4),
        'tp_mult': jitter_values([1.16], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.90], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_volatility_compression_breakout, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "volatility_compression_breakout")
    return best_result

# --- 12. ABSORPTION PATTERN ---
def worker_absorption_pattern(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        lookback = int(params['lookback_ticks'])
        for mkt in fast_markets:
            for tick in range(lookback, len(mkt['sec_left'])):
                recent_move = abs(mkt['live'][tick] - mkt['live'][tick - lookback]) / max(abs(mkt['live'][tick - lookback]), 1e-9)
                entry = None
                if recent_move <= params['price_stall_pct']:
                    up_ratio = mkt['buy_up_vol'][tick] / max(mkt['sell_up_vol'][tick], 1e-9)
                    dn_ratio = mkt['buy_down_vol'][tick] / max(mkt['sell_down_vol'][tick], 1e-9)
                    if up_ratio >= params['absorption_ratio'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                        entry = ('DOWN', mkt['b_dn'][tick])
                    elif dn_ratio >= params['absorption_ratio'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                        entry = ('UP', mkt['b_up'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_absorption_pattern(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_absorb_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🧽 ABSORPTION PATTERN | {symbol} {interval}")
    param_axes = {
        'lookback_ticks': jitter_values([8], integer=True, min_value=3),
        'price_stall_pct': jitter_values([0.0006], precision=6, min_value=0.00005),
        'absorption_ratio': jitter_values([2.0], precision=3, min_value=1.05),
        'max_price': jitter_values([0.75], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([16], integer=True, min_value=4),
        'tp_mult': jitter_values([1.12], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.90], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_absorption_pattern, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "absorption_pattern")
    return best_result

# --- 13. CROSS-MARKET SENTIMENT SPILLOVER (SEKWENCYJNY PO MARKET_ID) ---
def worker_cross_market_spillover(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        prev_sentiments = []
        for mkt in fast_markets:
            prior_edge = np.mean(prev_sentiments[-params['memory_markets']:]) if prev_sentiments else 0.0
            for tick in range(len(mkt['sec_left'])):
                entry = None
                local_edge = mkt['up_obi'][tick] - mkt['dn_obi'][tick]
                if prior_edge >= params['peer_signal'] and local_edge <= params['lag_tolerance'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                    entry = ('UP', mkt['b_up'][tick])
                elif prior_edge <= -params['peer_signal'] and local_edge >= -params['lag_tolerance'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                    entry = ('DOWN', mkt['b_dn'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
            prev_sentiments.append(float(np.nanmean(mkt['up_obi'] - mkt['dn_obi'])))
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_cross_market_spillover(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_spill_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🌊 CROSS-MARKET SENTIMENT SPILLOVER | {symbol} {interval}")
    param_axes = {
        'peer_signal': jitter_values([0.30], precision=3, min_value=0.05),
        'lag_tolerance': jitter_values([0.10], precision=3, min_value=0.01),
        'memory_markets': jitter_values([3], integer=True, min_value=1),
        'max_price': jitter_values([0.80], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([14], integer=True, min_value=4),
        'tp_mult': jitter_values([1.10], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.90], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_cross_market_spillover, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "cross_market_spillover")
    return best_result

# --- 14. SESSION-BASED EDGE ---
def worker_session_based_edge(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            for tick in range(1, len(mkt['sec_left'])):
                hour = int(mkt['session_hour'][tick])
                if not (params['hour_start'] <= hour <= params['hour_end']):
                    continue
                entry = None
                total_vol = mkt['up_total_vol'][tick] + mkt['dn_total_vol'][tick]
                low_liq = total_vol <= params['liquidity_cutoff']
                if low_liq:
                    if mkt['up_obi'][tick] >= params['obi_extreme'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                        entry = ('DOWN', mkt['b_dn'][tick])
                    elif mkt['up_obi'][tick] <= -params['obi_extreme'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                        entry = ('UP', mkt['b_up'][tick])
                else:
                    if (mkt['up_obi'][tick] - mkt['up_obi'][tick - 1]) >= params['delta_obi'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                        entry = ('UP', mkt['b_up'][tick])
                    elif (mkt['dn_obi'][tick] - mkt['dn_obi'][tick - 1]) >= params['delta_obi'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                        entry = ('DOWN', mkt['b_dn'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_session_based_edge(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_session_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🕒 SESSION-BASED EDGE | {symbol} {interval}")
    param_axes = {
        'hour_start': jitter_values([13], integer=True, min_value=0, max_value=23),
        'hour_end': jitter_values([20], integer=True, min_value=0, max_value=23),
        'liquidity_cutoff': jitter_values([500.0], precision=3, min_value=10.0),
        'obi_extreme': jitter_values([0.70], precision=3, min_value=0.10),
        'delta_obi': jitter_values([0.14], precision=3, min_value=0.02),
        'max_price': jitter_values([0.80], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([14], integer=True, min_value=4),
        'tp_mult': jitter_values([1.10], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.90], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    combinations = [c for c in combinations if c['hour_start'] <= c['hour_end']]
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_session_based_edge, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "session_based_edge")
    return best_result

# --- 15. SETTLEMENT CONVERGENCE STRATEGY ---
def worker_settlement_convergence(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            for tick in range(len(mkt['sec_left'])):
                if mkt['sec_left'][tick] > params['enter_sec_left']:
                    continue
                entry = None
                if mkt['b_up'][tick] >= params['prob_threshold'] and mkt['up_obi'][tick] >= params['obi_confirm']:
                    entry = ('UP', mkt['b_up'][tick])
                elif mkt['b_dn'][tick] >= params['prob_threshold'] and mkt['dn_obi'][tick] >= params['obi_confirm']:
                    entry = ('DOWN', mkt['b_dn'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_settlement_convergence(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_settle_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🎯 SETTLEMENT CONVERGENCE | {symbol} {interval}")
    param_axes = {
        'enter_sec_left': jitter_values([45], integer=True, min_value=2),
        'prob_threshold': jitter_values([0.85], precision=3, min_value=0.50),
        'obi_confirm': jitter_values([0.25], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([12], integer=True, min_value=2),
        'tp_mult': jitter_values([1.08], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.93], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_settlement_convergence, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "settlement_convergence")
    return best_result

# --- 16. LIQUIDITY VACUUM STRATEGY ---
def worker_liquidity_vacuum(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            for tick in range(len(mkt['sec_left'])):
                entry = None
                if mkt['sell_up_vol'][tick] <= params['min_counter_vol'] and mkt['buy_up_vol'][tick] >= params['dominant_vol'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                    entry = ('UP', mkt['b_up'][tick])
                elif mkt['sell_down_vol'][tick] <= params['min_counter_vol'] and mkt['buy_down_vol'][tick] >= params['dominant_vol'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                    entry = ('DOWN', mkt['b_dn'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_liquidity_vacuum(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_vacuum_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🕳️ LIQUIDITY VACUUM | {symbol} {interval}")
    param_axes = {
        'min_counter_vol': jitter_values([40.0], precision=3, min_value=1.0),
        'dominant_vol': jitter_values([180.0], precision=3, min_value=5.0),
        'max_price': jitter_values([0.82], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([12], integer=True, min_value=3),
        'tp_mult': jitter_values([1.10], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.92], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_liquidity_vacuum, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "liquidity_vacuum")
    return best_result

# --- 17. MICRO PULLBACK CONTINUATION ---
def worker_micro_pullback_continuation(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        lookback = int(params['lookback_ticks'])
        for mkt in fast_markets:
            for tick in range(lookback + 1, len(mkt['sec_left'])):
                entry = None
                prev_up_impulse = (mkt['b_up'][tick - 1] - mkt['b_up'][tick - lookback]) if mkt['b_up'][tick - lookback] > 0 else 0
                prev_dn_impulse = (mkt['b_dn'][tick - 1] - mkt['b_dn'][tick - lookback]) if mkt['b_dn'][tick - lookback] > 0 else 0
                up_pullback = (mkt['b_up'][tick - 1] - mkt['b_up'][tick]) if mkt['b_up'][tick] > 0 else 0
                dn_pullback = (mkt['b_dn'][tick - 1] - mkt['b_dn'][tick]) if mkt['b_dn'][tick] > 0 else 0
                if prev_up_impulse >= params['impulse_ticks'] and up_pullback >= params['pullback_ticks'] and mkt['up_obi'][tick] >= params['obi_floor'] and 0 < mkt['b_up'][tick] <= params['max_price']:
                    entry = ('UP', mkt['b_up'][tick])
                elif prev_dn_impulse >= params['impulse_ticks'] and dn_pullback >= params['pullback_ticks'] and mkt['dn_obi'][tick] >= params['obi_floor'] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                    entry = ('DOWN', mkt['b_dn'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_micro_pullback_continuation(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_pullback_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" ↩️ MICRO PULLBACK CONTINUATION | {symbol} {interval}")
    param_axes = {
        'lookback_ticks': jitter_values([6], integer=True, min_value=2),
        'impulse_ticks': jitter_values([0.06], precision=4, min_value=0.005),
        'pullback_ticks': jitter_values([0.02], precision=4, min_value=0.002),
        'obi_floor': jitter_values([0.15], precision=3, min_value=0.02),
        'max_price': jitter_values([0.82], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([12], integer=True, min_value=3),
        'tp_mult': jitter_values([1.10], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.92], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_micro_pullback_continuation, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "micro_pullback_continuation")
    return best_result

# --- 18. SYNTHETIC ARBITRAGE UP/DOWN SPREAD ---
def worker_synthetic_arbitrage(param_chunk, fast_markets):
    results = []
    stake = 1.0
    for params in param_chunk:
        total_pnl, trades_count, wins_count = 0.0, 0, 0
        for mkt in fast_markets:
            def neutral_exit(mkt2, tick, direction, entry_price, current_bid):
                synthetic_sum = mkt2['b_up'][tick] + mkt2['b_dn'][tick]
                return abs(synthetic_sum - 1.0) <= params['revert_band']
            for tick in range(len(mkt['sec_left'])):
                synthetic_sum = mkt['b_up'][tick] + mkt['b_dn'][tick]
                entry = None
                if synthetic_sum <= 1.0 - params['arb_gap']:
                    if mkt['b_up'][tick] <= mkt['b_dn'][tick] and 0 < mkt['b_up'][tick] <= params['max_price']:
                        entry = ('UP', mkt['b_up'][tick])
                    elif 0 < mkt['b_dn'][tick] <= params['max_price']:
                        entry = ('DOWN', mkt['b_dn'][tick])
                elif synthetic_sum >= 1.0 + params['arb_gap']:
                    if mkt['b_up'][tick] >= mkt['b_dn'][tick] and 0 < mkt['b_dn'][tick] <= params['max_price']:
                        entry = ('DOWN', mkt['b_dn'][tick])
                    elif 0 < mkt['b_up'][tick] <= params['max_price']:
                        entry = ('UP', mkt['b_up'][tick])
                if entry:
                    trade_pnl = simulate_micro_exit(mkt, tick, entry[0], entry[1], stake, params, neutral_exit)
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0:
                        wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            score = calculate_composite_score(total_pnl, trades_count)
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count / trades_count) * 100, 't': trades_count, 'score': score})
    return results

def test_synthetic_arbitrage(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_synarb_{uuid.uuid4().hex[:8]}"
    print(f"\n" + "=" * 80)
    print(f" 🧮 SYNTHETIC ARBITRAGE UP/DOWN | {symbol} {interval}")
    param_axes = {
        'arb_gap': jitter_values([0.03], precision=4, min_value=0.002),
        'revert_band': jitter_values([0.01], precision=4, min_value=0.001),
        'max_price': jitter_values([0.85], precision=3, min_value=0.05),
        'max_hold_ticks': jitter_values([10], integer=True, min_value=2),
        'tp_mult': jitter_values([1.08], precision=3, min_value=1.01),
        'sl_mult': jitter_values([0.94], precision=3, min_value=0.50),
    }
    combinations, original_count = build_param_combinations(param_axes)
    log_parameter_search_space(len(combinations), original_count, len(df_markets))
    fast_markets = prepare_fast_markets(df_markets)
    best_results = execute_with_progress(worker_synthetic_arbitrage, combinations, fast_markets, os.cpu_count() or 4)
    best_result = select_optimal_result(best_results)
    if not best_result:
        return None
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "synthetic_arbitrage")
    return best_result

# ==========================================
# MAIN ORCHESTRATION & JSON GENERATOR
# ==========================================
def generate_tracked_configs_output(optimizations):
    print("\n" + "=" * 80)
    print(" 📋 PODSUMOWANIE I NADPISYWANIE PLIKU tracked_configs.json")
    print("=" * 80)
    tracked_configs = []
    markets = [
        ('XRP', '15m', 4), ('BTC', '15m', 2), ('ETH', '15m', 2), ('SOL', '15m', 3),
        ('BTC', '5m', 2), ('ETH', '5m', 2), ('SOL', '5m', 3), ('XRP', '5m', 4),
        ('XRP', '1h', 4), ('BTC', '1h', 2), ('ETH', '1h', 2), ('SOL', '1h', 3)
    ]
    for sym, tf, decimals in markets:
        market_key = f"{sym}_{tf}"
        strats = optimizations.get(market_key, {})
        interval_s = int(tf.replace('m', '').replace('h', '')) * (60 if 'm' in tf else 3600)
        cfg = {
            "symbol": sym,
            "pair": f"{sym}USDT",
            "timeframe": tf,
            "interval": interval_s,
            "decimals": decimals,
            "offset": 0.0,
        }
        if 'kinetic_sniper' in strats:
            ks = strats['kinetic_sniper']
            cfg['kinetic_sniper'] = {
                "window_ms": ks.get('window_ms', 1000),
                "trigger_pct": ks.get('trigger_pct', 0.0),
                "max_price": ks.get('max_price', 0.85),
                "max_slippage": ks.get('max_slippage', 0.025),
                "id": ks.get('id', ''),
                "wr": ks.get('wr', 0.0)
            }
        for strategy_name in [
            'momentum', 'mid_arb', 'otm', 'l2_spread_scalper', 'vol_mean_reversion',
            'mean_reversion_obi', 'spread_compression', 'divergence_imbalance', 'obi_acceleration',
            'volatility_compression_breakout', 'absorption_pattern', 'cross_market_spillover',
            'session_based_edge', 'settlement_convergence', 'liquidity_vacuum',
            'micro_pullback_continuation', 'synthetic_arbitrage'
        ]:
            if strategy_name in strats:
                cfg[strategy_name] = {k: v for k, v in strats[strategy_name].items() if k not in ['g_sec', 'max_delta_abs']}
        tracked_configs.append(cfg)
        
    config_file = "tracked_configs.json"
    try:
        with open(config_file, "w", encoding="utf-8") as f:
            json.dump(tracked_configs, f, indent=4)
        print(f"✅ Poprawnie zapisano zoptymalizowane parametry do {config_file}.")
        save_terminal_summary_txt()
    except Exception as e:
        print(f"❌ Błąd zapisu pliku konfiguracyjnego: {e}")

def build_strategy_test_plan(unique_markets):
    strategy_plan = [
        ("kinetic_sniper", test_kinetic_sniper),
        ("momentum", test_1min_momentum),
        ("mid_arb", test_mid_game_arb),
        ("otm", test_otm_bargain),
        ("l2_spread_scalper", test_l2_spread_scalper),
        ("vol_mean_reversion", test_vol_mean_reversion),
        ("mean_reversion_obi", test_mean_reversion_obi),
        ("spread_compression", test_spread_compression),
        ("divergence_imbalance", test_divergence_imbalance),
        ("obi_acceleration", test_obi_acceleration),
        ("volatility_compression_breakout", test_volatility_compression_breakout),
        ("absorption_pattern", test_absorption_pattern),
        ("cross_market_spillover", test_cross_market_spillover),
        ("session_based_edge", test_session_based_edge),
        ("settlement_convergence", test_settlement_convergence),
        ("liquidity_vacuum", test_liquidity_vacuum),
        ("micro_pullback_continuation", test_micro_pullback_continuation),
        ("synthetic_arbitrage", test_synthetic_arbitrage),
    ]
    plan = []
    for market in unique_markets:
        if "_" not in market:
            continue
        symbol, interval = market.split('_')
        for strategy_name, strategy_func in strategy_plan:
            if strategy_name == "otm" and not validate_market_rules("otm", symbol, interval):
                continue
            plan.append((market, symbol, interval, strategy_name, strategy_func))
    return plan

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    
    parser = argparse.ArgumentParser(description="Watcher v10.29 Alpha Vault Backtester (HYPER-DENSE + MONTE CARLO)")
    parser.add_argument('--fast-track', action='store_true', help="Pomiń testowanie i zbuduj tracked_configs z najlepszych historycznych parametrów")
    parser.add_argument('--all-history', action='store_true', help="Użyj pełnej bazy historycznej danych L2 z SQLite")
    parser.add_argument('--quick', action='store_true', help="Uruchom przyspieszone próbkowanie Monte Carlo dla dużych siatek parametrów")
    args = parser.parse_args()
    QUICK_MODE = args.quick

    init_history_db()

    if args.fast_track:
        optimizations = compile_best_from_vault()
        generate_tracked_configs_output(optimizations)
    else:
        if args.quick:
            print("⚡ Quick mode aktywny: duże siatki parametrów będą próbkowane w trybie przyspieszonym.")
        run_post_mortem_analysis(all_history=args.all_history)
        print("\n⏳ Wczytywanie bazy danych SQLite i formatowanie macierzy Numpy dla HYPER-DENSE GRID SEARCH...")
        time_s = time.time()
        historical_data = load_and_prepare_data(all_history=args.all_history)
        print(f"✅ Pomyślnie wektoryzowano dane L2 w {time.time()-time_s:.2f} sekund.")
        
        if historical_data.empty:
            print("Nie znaleziono w logach arkusza zleceń żadnych zdarzeń rynkowych nadających się do optymalizacji.")
        else:
            unique_markets = historical_data['timeframe'].unique()
            test_plan = build_strategy_test_plan(unique_markets)
            total_tests = len(test_plan)
            test_counter = 0
            optimizations = {}
            for market in unique_markets:
                if "_" not in market: continue
                symbol, interval = market.split('_')
                df_interval = historical_data[historical_data['timeframe'] == market].copy()
                if df_interval.empty: continue
                
                optimizations[market] = {}
                market_tests = [item for item in test_plan if item[0] == market]
                for _, _, _, strategy_name, strategy_func in market_tests:
                    test_counter += 1
                    print(f"\n📍 Test {test_counter} z {total_tests} | {symbol} {interval} | {strategy_name}")
                    best_result = strategy_func(df_interval, symbol, interval)
                    if best_result:
                        save_optimization_result(symbol, interval, strategy_name, best_result)
                        optimizations[market][strategy_name] = best_result['p']
                        optimizations[market][strategy_name]['wr'] = round(best_result['wr'], 1)
                    
            generate_tracked_configs_output(optimizations)
