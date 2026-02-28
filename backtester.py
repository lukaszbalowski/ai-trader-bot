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
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

PARAM_JITTER_PCTS = (0.10, 0.15)
MONTE_CARLO_LIMIT = 1_000_000

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

def apply_monte_carlo(combinations, limit=1000000):
    total = len(combinations)
    if total > limit:
        print(f"   ⚠️ Siatka badawcza ({total}) przekracza limit. Uruchamiam metodę Monte Carlo (losuję 1 000 000 prób)...")
        sampled = random.sample(combinations, limit)
        # Szybkie odzyskiwanie pamięci RAM
        del combinations
        gc.collect()
        return sampled, total
    return combinations, total

def jitter_values(values, integer=False, precision=6, min_value=None, max_value=None):
    expanded = set()
    for value in values:
        variants = [value]
        for pct in PARAM_JITTER_PCTS:
            variants.append(value * (1.0 - pct))
            variants.append(value * (1.0 + pct))
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
    if total <= limit:
        combinations = []
        for values in itertools.product(*axes):
            combo = dict(zip(keys, values))
            combo.update(fixed_params)
            combinations.append(combo)
        return combinations, total

    print(f"   ⚠️ Siatka badawcza ({total}) przekracza limit. Uruchamiam metodę Monte Carlo (losuję 1 000 000 prób)...")
    sampled = set()
    max_attempts = limit * 5
    attempts = 0
    while len(sampled) < limit and attempts < max_attempts:
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
    
    if symbol == "BTC": trig_list = [x/10000.0 for x in range(1, 15, 1)] 
    elif symbol == "ETH": trig_list = [x/10000.0 for x in range(2, 20, 1)] 
    elif symbol == "SOL": trig_list = [x/10000.0 for x in range(5, 40, 2)] 
    elif symbol == "XRP": trig_list = [x/10000.0 for x in range(5, 50, 2)] 
    else: trig_list = [0.0010]
    
    combinations = [
        {'trigger_pct': tp, 'max_slippage': sl, 'max_price': mp, 'g_sec': 3.0, 'window_ms': 1000}
        for tp in trig_list
        for sl in [0.02, 0.03, 0.04, 0.05] 
        for mp in [x/100.0 for x in range(70, 100, 1)]
    ]
    
    ticks_count = len(df_markets)
    combinations, original_count = apply_monte_carlo(combinations)
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {ticks_count} ticków danych L2")
    
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
    
    if symbol == "BTC": d_list = [x/1.0 for x in range(1, 15)] 
    elif symbol == "ETH": d_list = [x/10.0 for x in range(1, 10)] 
    elif symbol == "SOL": d_list = [x/100.0 for x in range(1, 10)] 
    elif symbol == "XRP": d_list = [x/10000.0 for x in range(1, 10)] 
    else: d_list = [1.0]
    
    combinations = [
        {'delta': d, 'max_p': m_p, 'win_start': ws, 'win_end': we, 'g_sec': 2.0}
        for d in d_list 
        for m_p in [x / 100.0 for x in range(50, 96, 1)] 
        for ws in range(240, 59, -2) # Krok co 2s chroni przed budową listy > 10 milionów przed Monte Carlo
        for we in range(50, 4, -2) 
        if ws > we + 10
    ]
    
    ticks_count = len(df_markets)
    combinations, original_count = apply_monte_carlo(combinations)
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {ticks_count} ticków danych L2")
    
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
    
    if symbol == "BTC": d_list = [x/1.0 for x in range(1, 10)]
    elif symbol == "ETH": d_list = [x/10.0 for x in range(1, 6)]
    elif symbol == "SOL": d_list = [x/100.0 for x in range(1, 4)]
    elif symbol == "XRP": d_list = [x/10000.0 for x in range(1, 4)]
    else: d_list = [1.0]
    
    combinations = [
        {'delta': d, 'max_p': mp, 'win_start': ws, 'win_end': we, 'g_sec': 2.0}
        for d in d_list 
        for mp in [x/100.0 for x in range(45, 81, 1)] 
        for ws in range(1800, 599, -10) 
        for we in range(240, 119, -2)  
        if ws > we + 60
    ]
    
    ticks_count = len(df_markets)
    combinations, original_count = apply_monte_carlo(combinations)
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {ticks_count} ticków danych L2")
    
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
    
    combinations = [
        {'win_start': ws, 'win_end': we, 'max_p': p, 'g_sec': 2.0, 'max_delta_abs': max_d}
        for ws in range(120, 60, -1) 
        for we in range(50, 20, -1)  
        for p in [x/100.0 for x in range(2, 26, 1)] 
        if ws > we
    ]
    
    ticks_count = len(df_markets)
    combinations, original_count = apply_monte_carlo(combinations)
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {ticks_count} ticków danych L2")
    
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
        for mkt in fast_markets:
            for tick in range(len(mkt['sec_left'])):
                up_spread = mkt['s_up'][tick] - mkt['b_up'][tick]
                dn_spread = mkt['s_dn'][tick] - mkt['b_dn'][tick]
                if up_spread >= spread_threshold and mkt['b_up'][tick] > 0.0:
                    trades_count += 1
                    if mkt['won_up'] or (tick + 10 < len(mkt['s_up']) and mkt['b_up'][tick+10] > mkt['b_up'][tick]):
                        wins_count += 1
                        total_pnl += (up_spread * 0.8) * stake
                    else:
                        total_pnl -= 0.10 * stake
                    break
                elif dn_spread >= spread_threshold and mkt['b_dn'][tick] > 0.0:
                    trades_count += 1
                    if not mkt['won_up'] or (tick + 10 < len(mkt['s_dn']) and mkt['b_dn'][tick+10] > mkt['b_dn'][tick]):
                        wins_count += 1
                        total_pnl += (dn_spread * 0.8) * stake
                    else:
                        total_pnl -= 0.10 * stake
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
    
    combinations = [
        {'min_spread_threshold': sp, 'skew_allowance': sk, 'max_hold_sec': mh}
        for sp in [x/100.0 for x in range(1, 11)] 
        for sk in [0.02, 0.05, 0.08, 0.10] 
        for mh in [5.0, 10.0, 15.0, 20.0]
    ]
    
    ticks_count = len(df_markets)
    combinations, original_count = apply_monte_carlo(combinations)
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {ticks_count} ticków danych L2")
    
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
        for mkt in fast_markets:
            for tick in range(1, len(mkt['sec_left'])):
                if mkt['live_pct_change'][tick] <= -drop_threshold:
                    entry_price = mkt['b_up'][tick]
                    if entry_price <= 0: continue
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
    
    combinations = [
        {'binance_vol_multiplier': vm, 'clob_panic_drop': pd, 'reversion_tp_multiplier': rtp}
        for vm in [2.0, 3.0, 4.0]
        for pd in [0.01, 0.02, 0.03, 0.05, 0.08, 0.10] 
        for rtp in [1.1, 1.2, 1.3, 1.5] 
    ]
    
    ticks_count = len(df_markets)
    combinations, original_count = apply_monte_carlo(combinations)
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {ticks_count} ticków danych L2")
    
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    print(f"   📊 Rozmiar siatki badawczej: {len(combinations)} kombinacji (z {original_count}) | {len(df_markets)} ticków danych L2")
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
    args = parser.parse_args()

    init_history_db()

    if args.fast_track:
        optimizations = compile_best_from_vault()
        generate_tracked_configs_output(optimizations)
    else:
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
