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
    strategies = ['kinetic_sniper', 'momentum', 'mid_arb', 'otm', 'l2_spread_scalper', 'vol_mean_reversion']
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
    for m_id, group in df_markets.groupby('market_id'):
        markets.append({
            'm_id': m_id,
            'target': group['target_price'].iloc[0],
            'won_up': group['won_up'].iloc[0],
            'sec_left': group['sec_left'].values,
            'live': group['live_price'].values,
            'live_pct_change': group['live_pct_change'].values,
            'up_change': group['up_change'].values,
            'dn_change': group['dn_change'].values,
            'b_up': group['buy_up'].values,
            'b_dn': group['buy_down'].values,
            's_up': group['sell_up'].values,
            's_dn': group['sell_down'].values
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
        current_bid = mkt['s_up'][tick] if direction == 'UP' else mkt['s_dn'][tick]
        if sec_left <= global_sec_rule and current_bid > entry_price: return (current_bid * shares) - stake
        if current_bid >= entry_price * 1.50: return (current_bid * shares) - stake
        if tick - entry_idx >= 10 and current_bid > entry_price: return (current_bid * shares) - stake
        if current_bid < last_bid and current_bid > entry_price: ticks_down += 1
        elif current_bid > last_bid: ticks_down = 0
        if ticks_down >= 2 and current_bid > entry_price: return (current_bid * shares) - stake
        if current_bid <= entry_price * 0.90:
            exit_tick = min(tick + 10, len(mkt['sec_left']) - 1)
            exit_bid = mkt['s_up'][exit_tick] if direction == 'UP' else mkt['s_dn'][exit_tick]
            return (exit_bid * shares) - stake
        last_bid = current_bid
    won = mkt['won_up'] if direction == 'UP' else not mkt['won_up']
    return (1.0 * shares - stake) if won else -stake

def simulate_simple_exit(mkt, entry_idx, direction, entry_price, stake, global_sec_rule):
    shares = stake / entry_price
    for tick in range(entry_idx + 1, len(mkt['sec_left'])):
        sec_left = mkt['sec_left'][tick]
        current_bid = mkt['s_up'][tick] if direction == 'UP' else mkt['s_dn'][tick]
        if sec_left <= global_sec_rule and current_bid > entry_price: return (current_bid * shares) - stake
        if current_bid >= entry_price * 3.0: return (current_bid * shares) - stake
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
        for strategy_name in ['momentum', 'mid_arb', 'otm', 'l2_spread_scalper', 'vol_mean_reversion']:
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
            optimizations = {}
            for market in unique_markets:
                if "_" not in market: continue
                symbol, interval = market.split('_')
                df_interval = historical_data[historical_data['timeframe'] == market].copy()
                if df_interval.empty: continue
                
                optimizations[market] = {}
                
                best_kin = test_kinetic_sniper(df_interval, symbol, interval)
                if best_kin:
                    save_optimization_result(symbol, interval, "kinetic_sniper", best_kin)
                    optimizations[market]['kinetic_sniper'] = best_kin['p']
                    optimizations[market]['kinetic_sniper']['wr'] = round(best_kin['wr'], 1)
                    
                best_mom = test_1min_momentum(df_interval, symbol, interval)
                if best_mom:
                    save_optimization_result(symbol, interval, "momentum", best_mom)
                    optimizations[market]['momentum'] = best_mom['p']
                    optimizations[market]['momentum']['wr'] = round(best_mom['wr'], 1)
                    
                best_arb = test_mid_game_arb(df_interval, symbol, interval)
                if best_arb:
                    save_optimization_result(symbol, interval, "mid_arb", best_arb)
                    optimizations[market]['mid_arb'] = best_arb['p']
                    optimizations[market]['mid_arb']['wr'] = round(best_arb['wr'], 1)
                    
                best_otm = test_otm_bargain(df_interval, symbol, interval)
                if best_otm:
                    save_optimization_result(symbol, interval, "otm", best_otm)
                    optimizations[market]['otm'] = best_otm['p']
                    optimizations[market]['otm']['wr'] = round(best_otm['wr'], 1)
                    
                best_l2 = test_l2_spread_scalper(df_interval, symbol, interval)
                if best_l2:
                    save_optimization_result(symbol, interval, "l2_spread_scalper", best_l2)
                    optimizations[market]['l2_spread_scalper'] = best_l2['p']
                    optimizations[market]['l2_spread_scalper']['wr'] = round(best_l2['wr'], 1)
                    
                best_vol = test_vol_mean_reversion(df_interval, symbol, interval)
                if best_vol:
                    save_optimization_result(symbol, interval, "vol_mean_reversion", best_vol)
                    optimizations[market]['vol_mean_reversion'] = best_vol['p']
                    optimizations[market]['vol_mean_reversion']['wr'] = round(best_vol['wr'], 1)
                    
            generate_tracked_configs_output(optimizations)