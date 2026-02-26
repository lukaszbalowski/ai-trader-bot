import sqlite3
import pandas as pd
import numpy as np
import random
import time
import itertools
import os
import json
import argparse
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
                       ORDER BY win_rate DESC, pnl_percent DESC LIMIT 3''', (symbol, timeframe, strategy))
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception:
        return []

# ==========================================
# 0B. FAST-TRACK COMPILER
# ==========================================
def compile_best_from_vault(db_path="data/backtest_history.db"):
    print("\n" + "=" * 80)
    print(" 🚀 FAST-TRACK COMPILER: Building best settings from Alpha Vault (By PnL)")
    print("=" * 80)
    
    if not os.path.exists(db_path):
        print("Alpha Vault file (backtest_history.db) not found. Compilation aborted.")
        return {}

    optimizations = {}
    markets = [
        ('BTC', '5m', 2), ('ETH', '5m', 2), ('SOL', '5m', 3), ('XRP', '5m', 4),
        ('BTC', '15m', 2), ('ETH', '15m', 2), ('SOL', '15m', 3), ('XRP', '15m', 4),
        ('BTC', '1h', 2), ('ETH', '1h', 2), ('SOL', '1h', 3), ('XRP', '1h', 4)
    ]
    # ZAKTUALIZOWANE STRATEGIE: lag_sniper zastąpiony przez kinetic_sniper
    strategies = ['kinetic_sniper', 'momentum', 'mid_arb', 'otm']

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
                    print(f"   ✅ Compiled {sym} {tf} | {strat} | Record PnL: +{pnl:.2f}%")
                except json.JSONDecodeError:
                    pass
    conn.close()
    return optimizations

# ==========================================
# 0C. SESSION ID EXTRACTOR AND POST MORTEM
# ==========================================
def get_latest_session_id(db_path="data/polymarket.db"):
    if not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT session_id FROM market_logs_v11 WHERE session_id IS NOT NULL ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None

def run_post_mortem_analysis(db_path="data/polymarket.db", all_history=False):
    print("\n" + "=" * 80)
    print(" 🕵️ POST-MORTEM ANALYSIS (HISTORICAL TRADING RESULTS) ")
    print("=" * 80)
    if not os.path.exists(db_path):
        print("Database file not found. Skipping Post-Mortem analysis.")
        return
    try:
        latest_session = get_latest_session_id(db_path) if not all_history else None
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_logs_v10'")
        if not cur.fetchone():
            print("Table trade_logs_v10 is empty or does not exist. Skipping analysis.")
            conn.close()
            return
            
        if latest_session:
            print(f"🔍 Analysis isolated strictly for the latest session: {latest_session}")
            try:
                df_trades = pd.read_sql_query(f"SELECT * FROM trade_logs_v10 WHERE session_id='{latest_session}'", conn)
            except Exception:
                df_trades = pd.read_sql_query("SELECT * FROM trade_logs_v10", conn)
        else:
            if all_history:
                print(f"🌍 Analysis utilizing FULL historical database.")
            df_trades = pd.read_sql_query("SELECT * FROM trade_logs_v10", conn)
            
        conn.close()
        if df_trades.empty:
            print("No trade history found in database for this run.")
            return
            
        total_pnl = df_trades['pnl'].sum()
        total_trades = len(df_trades)
        win_rate = (len(df_trades[df_trades['pnl'] > 0]) / total_trades) * 100 if total_trades > 0 else 0.0
        print(f"📊 Session PnL: {total_pnl:>+10.2f}$ | Total trades: {total_trades} | Avg WR: {win_rate:.1f}%\n")
        print(f"{'MARKET':<12} | {'STRATEGY':<16} | {'TRADES':<6} | {'WR %':<6} | {'PNL ($)':<12}")
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
        print(f"Error during Post-Mortem analysis generation: {e}")

# ==========================================
# 1. LEVEL 2 DATA PREPARATION
# ==========================================
def load_and_prepare_data(db_path="data/polymarket.db", all_history=False):
    if not os.path.exists(db_path):
        return pd.DataFrame()
        
    latest_session = get_latest_session_id(db_path) if not all_history else None
    conn = sqlite3.connect(db_path)
    try:
        if latest_session:
            print(f"⏳ Fetching Level 2 ticks exclusively for session: {latest_session}...")
            query = f"SELECT * FROM market_logs_v11 WHERE session_id='{latest_session}' AND (buy_up > 0 OR buy_down > 0) ORDER BY fetched_at ASC"
            df = pd.read_sql_query(query, conn)
        else:
            print(f"🌍 Fetching Level 2 ticks for FULL historical data...")
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
    
    # We leave these standard diffs for legacy strats and safety
    df['asset_jump'] = df.groupby('market_id')['live_price'].diff()
    df['up_change'] = df.groupby('market_id')['buy_up'].diff().abs()
    df['dn_change'] = df.groupby('market_id')['buy_down'].diff().abs()
    
    # We add PCT change simulation for Kinetic window
    # Assuming each tick in DB is roughly 1 second apart, we simulate the 'window' jump
    # by taking the percentage difference from the previous row.
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
            'sec_since_start': group['sec_since_start'].values,
            'live': group['live_price'].values,
            'live_pct_change': group['live_pct_change'].values, # NEW: for Kinetic
            'asset_jump': group['asset_jump'].values,
            'up_change': group['up_change'].values,
            'dn_change': group['dn_change'].values,
            'b_up': group['buy_up'].values,
            'b_dn': group['buy_down'].values,
            's_up': group['sell_up'].values,
            's_dn': group['sell_down'].values
        })
    return markets

# ==========================================
# ZAKTUALIZOWANA FUNKCJA WYJŚCIA (Micro-Take-Profit Simulation)
# ==========================================
def simulate_kinetic_exit(mkt, entry_idx, direction, entry_price, stake, global_sec_rule):
    shares = stake / entry_price
    ticks_down = 0
    last_bid = entry_price
    
    for tick in range(entry_idx + 1, len(mkt['sec_left'])):
        sec_left = mkt['sec_left'][tick]
        current_bid = mkt['s_up'][tick] if direction == 'UP' else mkt['s_dn'][tick]
        
        # 1. Securing profits before expiry (Global Rule)
        if sec_left <= global_sec_rule and current_bid > entry_price:
            return (current_bid * shares) - stake
            
        # 2. Zapadnia 1: Hard PNL (+50%)
        if current_bid >= entry_price * 1.50:
             return (current_bid * shares) - stake
             
        # 3. Zapadnia 2: Time-Decay Escape (Max 10s simulated ~ 10 ticks)
        if tick - entry_idx >= 10 and current_bid > entry_price:
            return (current_bid * shares) - stake
            
        # 4. Zapadnia 3: Momentum Reversal (2 ticks down)
        if current_bid < last_bid and current_bid > entry_price:
            ticks_down += 1
        elif current_bid > last_bid:
            ticks_down = 0
            
        if ticks_down >= 2 and current_bid > entry_price:
             return (current_bid * shares) - stake
             
        # Fallback SL 
        if current_bid <= entry_price * 0.90:
            # We assume in backtest that hitting 10% drop means we hold for 10s then exit. 
            # For simplicity in simulation, if it hits -10% we assume it hits the Timeout SL immediately or soon after.
            exit_tick = min(tick + 10, len(mkt['sec_left']) - 1)
            exit_bid = mkt['s_up'][exit_tick] if direction == 'UP' else mkt['s_dn'][exit_tick]
            return (exit_bid * shares) - stake
            
        last_bid = current_bid

    # If held until expiry
    won = mkt['won_up'] if direction == 'UP' else not mkt['won_up']
    return (1.0 * shares - stake) if won else -stake

def simulate_simple_exit(mkt, entry_idx, direction, entry_price, stake, global_sec_rule):
    shares = stake / entry_price
    for tick in range(entry_idx + 1, len(mkt['sec_left'])):
        sec_left = mkt['sec_left'][tick]
        current_bid = mkt['s_up'][tick] if direction == 'UP' else mkt['s_dn'][tick]
        if sec_left <= global_sec_rule and current_bid > entry_price:
            return (current_bid * shares) - stake
            
        # Standard Global TP for non-kinetic
        if current_bid >= entry_price * 3.0:
            return (current_bid * shares) - stake
            
    won = mkt['won_up'] if direction == 'UP' else not mkt['won_up']
    return (1.0 * shares - stake) if won else -stake

# ==========================================
# SHARED PROGRESS BAR & DB CHECK
# ==========================================
def display_results_and_compare(best_result, symbol, interval, strategy_name):
    if not best_result: return
    params = best_result['p']
    print(f"\n   👑 CURRENT TEST TOP: {best_result['pnl']:>+7.2f}$ ({best_result['pnl_proc']:>+6.2f}%) | WR: {best_result['wr']:>5.1f}% | T: {best_result['t']:>3}")
    param_str = ", ".join([f"{k}={v}" for k, v in params.items() if k not in ['g_sec', 'id', 'max_delta_abs', 'window_ms']])
    print(f"   ⚙️ Parameters: {param_str}")
    print(f"   🔑 Database ID: {params['id']}")
    top_3 = get_top_3_historical(symbol, interval, strategy_name)
    print("   🏆 TOP 3 HISTORICAL (By WinRate):")
    if not top_3:
        print("      No historical records found. This strategy sets the first record.")
    else:
        for idx, row in enumerate(top_3):
            ts, wr, pnl_proc, params_str, t_count = row
            try:
                params_json = json.loads(params_str)
                hist_id = params_json.get('id', 'No ID')
                hist_clean_params = {k: v for k, v in params_json.items() if k not in ['g_sec', 'id', 'max_delta_abs']}
                hist_param_str = ", ".join([f"{k}={v}" for k, v in hist_clean_params.items()])
            except:
                hist_id = "No ID"
                hist_param_str = "No data"
            date_str = ts[:10]
            print(f"      {idx+1}. WR: {wr:>5.1f}% | PnL: {pnl_proc:>+6.2f}% | T: {t_count} | ID: {hist_id} (Date: {date_str})")
            print(f"         ⚙️ Parameters: {hist_param_str}")

def execute_with_progress(worker_func, combinations, fast_markets, num_cores):
    chunk_size = max(1, len(combinations) // (num_cores * 4))
    chunks = [combinations[i:i + chunk_size] for i in range(0, len(combinations), chunk_size)]
    best_results = []
    print("   ⏳ Test progress: 0%", end="", flush=True)
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
    print(" ✅ Done!", flush=True)
    return best_results

# ==========================================
# WORKERS & STRATEGIES
# ==========================================
# NOWY WORKER ZASTĘPUJĄCY LAG SNIPER
def worker_kinetic_sniper(param_chunk, fast_markets):
    results = []
    stake = 1.0 # Kinetic sizing
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
                    # Używamy nowej, wysoce zoptymalizowanej funkcji wyjścia dla Kinetic
                    trade_pnl = simulate_kinetic_exit(mkt, tick, entry[0], entry[1], stake, params['g_sec'])
                    total_pnl += trade_pnl
                    trades_count += 1
                    if trade_pnl > 0: wins_count += 1
                    break
        if trades_count > 0:
            pnl_proc = (total_pnl / (trades_count * stake)) * 100
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count/trades_count)*100, 't': trades_count})
    return results

def test_kinetic_sniper(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_kin_{int(time.time())}"
    print(f"\n" + "=" * 80)
    print(f" 🚀 KINETIC SNIPER | {symbol} {interval}")
    
    # Grid kalibracyjny (procentowe momentum)
    if symbol == "BTC": trig_list = [x/10000.0 for x in range(4, 20, 2)] # 0.0004 to 0.0020
    elif symbol == "ETH": trig_list = [x/10000.0 for x in range(8, 30, 2)]
    elif symbol == "SOL": trig_list = [x/10000.0 for x in range(15, 55, 5)]
    elif symbol == "XRP": trig_list = [x/10000.0 for x in range(20, 65, 5)]
    else: trig_list = [0.0010]
    
    combinations = [
        {'trigger_pct': tp, 'max_slippage': sl, 'max_price': mp, 'g_sec': 3.0, 'window_ms': 1000}
        for tp in trig_list
        for sl in [0.02, 0.025, 0.03] 
        for mp in [0.85, 0.90, 0.95]
    ]
    ticks_count = len(df_markets)
    print(f"   📊 Data Volume: {len(combinations)} grid combinations | {ticks_count} L2 book ticks")
    fast_markets = prepare_fast_markets(df_markets)
    num_cores = os.cpu_count() or 4
    best_results = execute_with_progress(worker_kinetic_sniper, combinations, fast_markets, num_cores)
    
    if not best_results: return None
    
    valid_results = [r for r in best_results if r['t'] >= 10]
    if not valid_results:
        print("   ⚠️ Warning: No configuration reached the minimum of 10 trades. Falling back to the best available.")
        valid_results = best_results

    valid_results.sort(key=lambda x: x['pnl'], reverse=True)
    best_result = valid_results[0]
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "kinetic_sniper")
    return best_result

# Omitted other unchanged strats for code brevity, they remain identical:
# worker_1min_momentum, test_1min_momentum
# worker_mid_arb, test_mid_game_arb
# worker_otm, test_otm_bargain

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
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count/trades_count)*100, 't': trades_count})
    return results

def test_1min_momentum(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_mom_{int(time.time())}"
    print(f"\n" + "=" * 80)
    print(f" 🚀 1-MINUTE MOMENTUM | {symbol} {interval}")
    
    if symbol == "BTC": d_list = list(range(15, 41))
    elif symbol == "ETH": d_list = [x/10.0 for x in range(5, 26)]
    elif symbol == "SOL": d_list = [x/100.0 for x in range(2, 16)]
    elif symbol == "XRP": d_list = [x/10000.0 for x in range(2, 16)]
    else: d_list = [10.0]
    
    combinations = [
        {'delta': d, 'max_p': m_p, 'win_start': ws, 'win_end': we, 'g_sec': 2.0}
        for d in d_list for m_p in [x / 100.0 for x in range(65, 86)]
        for ws in range(60, 91, 2) for we in range(30, 56, 2)
        if ws > we + 5
    ]
    ticks_count = len(df_markets)
    print(f"   📊 Data Volume: {len(combinations)} grid combinations | {ticks_count} L2 book ticks")
    fast_markets = prepare_fast_markets(df_markets)
    num_cores = os.cpu_count() or 4
    random.shuffle(combinations)
    best_results = execute_with_progress(worker_1min_momentum, combinations, fast_markets, num_cores)
    
    if not best_results: return None
    
    valid_results = [r for r in best_results if r['t'] >= 10]
    if not valid_results:
        valid_results = best_results

    valid_results.sort(key=lambda x: x['pnl'], reverse=True)
    best_result = valid_results[0]
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "momentum")
    return best_result

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
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count/trades_count)*100, 't': trades_count})
    return results

def test_mid_game_arb(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_arb_{int(time.time())}"
    print(f"\n" + "=" * 80)
    print(f" ⚖️ MID-GAME ARB | {symbol} {interval}")
    
    if symbol == "BTC": d_list = list(range(5, 21))
    elif symbol == "ETH": d_list = [x/10.0 for x in range(3, 11)]
    elif symbol == "SOL": d_list = [x/100.0 for x in range(1, 6)]
    elif symbol == "XRP": d_list = [x/10000.0 for x in range(1, 6)]
    else: d_list = [5.0]
    
    combinations = [
        {'delta': d, 'max_p': mp, 'win_start': ws, 'win_end': we, 'g_sec': 2.0}
        for d in d_list for mp in [x/100.0 for x in range(45, 66, 2)]
        for ws in range(120, 201, 10) for we in range(30, 91, 5)
        if ws > we + 10
    ]
    ticks_count = len(df_markets)
    print(f"   📊 Data Volume: {len(combinations)} grid combinations | {ticks_count} L2 book ticks")
    fast_markets = prepare_fast_markets(df_markets)
    num_cores = os.cpu_count() or 4
    best_results = execute_with_progress(worker_mid_arb, combinations, fast_markets, num_cores)
    
    if not best_results: return None
    
    valid_results = [r for r in best_results if r['t'] >= 10]
    if not valid_results:
        valid_results = best_results

    valid_results.sort(key=lambda x: x['pnl'], reverse=True)
    best_result = valid_results[0]
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "mid_arb")
    return best_result

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
            results.append({'p': params, 'pnl': total_pnl, 'pnl_proc': pnl_proc, 'wr': (wins_count/trades_count)*100, 't': trades_count})
    return results

def test_otm_bargain(df_markets, symbol, interval):
    strat_id = f"{symbol.lower()}_{interval}_otm_{int(time.time())}"
    print(f"\n" + "=" * 80)
    print(f" 🎟️ OTM BARGAIN | {symbol} {interval}")
    
    if symbol == "BTC": max_d = 40.0
    elif symbol == "ETH": max_d = 2.0
    elif symbol == "SOL": max_d = 0.1
    elif symbol == "XRP": max_d = 0.001
    else: max_d = 10.0
    
    combinations = [
        {'win_start': ws, 'win_end': we, 'max_p': p, 'g_sec': 2.0, 'max_delta_abs': max_d}
        for ws in range(50, 91, 2) for we in range(20, 56, 2)
        for p in [x/100.0 for x in range(2, 10)]
        if ws > we
    ]
    ticks_count = len(df_markets)
    print(f"   📊 Data Volume: {len(combinations)} grid combinations | {ticks_count} L2 book ticks")
    fast_markets = prepare_fast_markets(df_markets)
    num_cores = os.cpu_count() or 4
    best_results = execute_with_progress(worker_otm, combinations, fast_markets, num_cores)
    
    if not best_results: return None
    
    valid_results = [r for r in best_results if r['t'] >= 10]
    if not valid_results:
        valid_results = best_results

    valid_results.sort(key=lambda x: x['pnl'], reverse=True)
    best_result = valid_results[0]
    best_result['p']['id'] = strat_id
    display_results_and_compare(best_result, symbol, interval, "otm")
    return best_result

# ==========================================
# MAIN ORCHESTRATION & ZAKTUALIZOWANY GENERATOR JSON
# ==========================================
def generate_tracked_configs_output(optimizations):
    print("\n" + "=" * 80)
    print(" 📋 SUMMARY AND OVERWRITE OF tracked_configs.json")
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
        
        # AKTUALIZACJA: Wstrzyknięcie struktury dla Kinetic Snipera zamiast Lag Snipera
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
        
        if 'momentum' in strats:
            cfg['momentum'] = {k: v for k, v in strats['momentum'].items() if k not in ['g_sec', 'max_delta_abs']}
        if 'mid_arb' in strats:
            cfg['mid_arb'] = {k: v for k, v in strats['mid_arb'].items() if k not in ['g_sec', 'max_delta_abs']}
        if 'otm' in strats:
            cfg['otm'] = {k: v for k, v in strats['otm'].items() if k not in ['g_sec', 'max_delta_abs']}
            
        tracked_configs.append(cfg)
        
    config_file = "tracked_configs.json"
    try:
        with open(config_file, "w", encoding="utf-8") as f:
            json.dump(tracked_configs, f, indent=4)
        print(f"✅ Successfully saved new optimized configuration to {config_file}.")
        print("   Restart the bot (main.py) to use the new parameters.")
    except Exception as e:
        print(f"❌ Configuration file save error: {e}")

def export_trades_analysis(db_path="data/polymarket.db"):
    # (Unchanged logic for brevity, keeps existing CSV export function intact)
    print("\n" + "=" * 80)
    print(" 🤖 AI EXPORT: ALL TRADES FROM THE LAST 24 HOURS (WITH ORDERBOOKS)")
    print("=" * 80)
    
    if not os.path.exists(db_path):
        print("Database not found. Make sure the bot has traded.")
        return
        
    conn = sqlite3.connect(db_path)
    
    try:
        df_trades = pd.read_sql_query("SELECT * FROM trade_logs_v10", conn)
    except Exception as e:
        print(f"Error reading trade_logs_v10: {e}")
        conn.close()
        return

    if df_trades.empty:
        print("No trades found in the database.")
        conn.close()
        return
        
    try:
        df_trades['entry_time_dt'] = pd.to_datetime(df_trades['entry_time'], format='ISO8601', utc=True)
    except Exception:
        df_trades['entry_time_dt'] = pd.to_datetime(df_trades['entry_time'])
    
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=24)
    if df_trades['entry_time_dt'].dt.tz is None:
        cutoff = cutoff.tz_localize(None)
        
    df_trades_24h = df_trades[df_trades['entry_time_dt'] >= cutoff].copy()
    
    if df_trades_24h.empty:
        print("No trades from the last 24 hours.")
        conn.close()
        return
    
    print(f"Found {len(df_trades_24h)} trades from the last 24h. Fetching orderbooks...")
    
    try:
        df_markets = pd.read_sql_query("SELECT * FROM market_logs_v11", conn)
        try:
            df_markets['fetched_at_dt'] = pd.to_datetime(df_markets['fetched_at'], format='ISO8601', utc=True)
        except Exception:
            df_markets['fetched_at_dt'] = pd.to_datetime(df_markets['fetched_at'])
    except Exception as e:
        print("Could not read market_logs_v11. Orderbooks will be empty.")
        df_markets = pd.DataFrame()
        
    conn.close()
    
    export_rows = []
    
    for _, trade in df_trades_24h.iterrows():
        m_id = trade['market_id']
        entry_t = trade['entry_time_dt']
        
        try:
            exit_t = pd.to_datetime(trade['exit_time'], format='ISO8601', utc=True)
            if exit_t.tzinfo is None and entry_t.tzinfo is not None:
                exit_t = exit_t.tz_localize('UTC')
        except Exception:
            try:
                exit_t = pd.to_datetime(trade['exit_time'])
            except:
                exit_t = None
            
        entry_ob = {}
        exit_ob = {}
        
        if not df_markets.empty:
            m_ticks = df_markets[df_markets['market_id'] == m_id]
            if not m_ticks.empty:
                entry_tick = m_ticks.iloc[(m_ticks['fetched_at_dt'] - entry_t).abs().argsort()[:1]]
                if not entry_tick.empty:
                    et = entry_tick.iloc[0]
                    entry_ob = {
                        'entry_ob_buy_up': et['buy_up'], 'entry_ob_sell_up': et['sell_up'], 'entry_ob_up_vol': et['buy_up_vol'],
                        'entry_ob_buy_dn': et['buy_down'], 'entry_ob_sell_dn': et['sell_down'], 'entry_ob_dn_vol': et['buy_down_vol']
                    }
                    
                if exit_t is not pd.NaT and exit_t:
                    exit_tick = m_ticks.iloc[(m_ticks['fetched_at_dt'] - exit_t).abs().argsort()[:1]]
                    if not exit_tick.empty:
                        xt = exit_tick.iloc[0]
                        exit_ob = {
                            'exit_ob_buy_up': xt['buy_up'], 'exit_ob_sell_up': xt['sell_up'], 'exit_ob_up_vol': xt['buy_up_vol'],
                            'exit_ob_buy_dn': xt['buy_down'], 'exit_ob_sell_dn': xt['sell_down'], 'exit_ob_dn_vol': xt['buy_down_vol']
                        }
                        
        row = trade.drop('entry_time_dt').to_dict()
        row.update(entry_ob)
        row.update(exit_ob)
        export_rows.append(row)
        
    df_export = pd.DataFrame(export_rows)
    export_path = "data/ai_trades_analysis.csv"
    df_export.to_csv(export_path, index=False)
    print(f"✅ Data exported successfully to {export_path}")

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    
    parser = argparse.ArgumentParser(description="Watcher v10.29 Alpha Vault Backtester")
    parser.add_argument('--fast-track', action='store_true', help="Skips testing and builds tracked_configs.json from historical vault bests")
    parser.add_argument('--all-history', action='store_true', help="Force backtester to use all historical data instead of just the latest session")
    parser.add_argument('--trades', action='store_true', help="Exports all trades from the last 24 hours with entry and exit orderbooks for AI analysis")
    args = parser.parse_args()

    init_history_db()

    if args.trades:
        export_trades_analysis()
    elif args.fast_track:
        optimizations = compile_best_from_vault()
        generate_tracked_configs_output(optimizations)
    else:
        run_post_mortem_analysis(all_history=args.all_history)
        print("\n⏳ Loading SQLite database and formatting Numpy data for simulation...")
        time_s = time.time()
        historical_data = load_and_prepare_data(all_history=args.all_history)
        print(f"✅ Data loaded and vectorized in {time.time()-time_s:.2f} seconds.")
        
        if historical_data.empty:
            print("No Level 2 market logs found in the database to perform new grid optimization.")
        else:
            unique_markets = historical_data['timeframe'].unique()
            optimizations = {}
            
            for market in unique_markets:
                if "_" not in market: continue
                symbol, interval = market.split('_')
                
                df_interval = historical_data[historical_data['timeframe'] == market].copy()
                if df_interval.empty: continue
                
                optimizations[market] = {}
                
                # AKTUALIZACJA: Wywołanie nowej strategii w pętli optymalizatora
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
                    
            generate_tracked_configs_output(optimizations)