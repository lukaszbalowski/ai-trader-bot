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
# 0A. ALPHA VAULT (HISTORIA OPTYMALIZACJI)
# ==========================================
def init_history_db(sciezka="data/backtest_history.db"):
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(sciezka)
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

def save_optimization_result(symbol, timeframe, strategy, result_obj, sciezka="data/backtest_history.db"):
    conn = sqlite3.connect(sciezka)
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

def get_top_3_historical(symbol, timeframe, strategy, sciezka="data/backtest_history.db"):
    if not os.path.exists(sciezka):
        return []
    try:
        conn = sqlite3.connect(sciezka)
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
def compile_best_from_vault(sciezka="data/backtest_history.db"):
    """Szybka kompilacja TRACKED_CONFIGS na podstawie najwyższego PnL z bazy historycznej."""
    print("\n" + "=" * 80)
    print(" 🚀 FAST-TRACK COMPILER: Budowanie najlepszych ustawień z Alpha Vault (Wg PnL)")
    print("=" * 80)
    
    if not os.path.exists(sciezka):
        print("Brak pliku Alpha Vault (backtest_history.db). Nie można skompilować.")
        return {}

    optymalizacje = {}
    rynki = [
        ('BTC', '5m', 2), ('ETH', '5m', 2), ('SOL', '5m', 3), ('XRP', '5m', 4),
        ('BTC', '15m', 2), ('ETH', '15m', 2), ('SOL', '15m', 3), ('XRP', '15m', 4),
        ('BTC', '1h', 2), ('ETH', '1h', 2), ('SOL', '1h', 3), ('XRP', '1h', 4)
    ]
    strategie = ['lag_sniper', 'momentum', 'mid_arb', 'otm']

    conn = sqlite3.connect(sciezka)
    cur = conn.cursor()

    for sym, tf, _ in rynki:
        rynek_klucz = f"{sym}_{tf}"
        optymalizacje[rynek_klucz] = {}
        for strat in strategie:
            cur.execute('''SELECT parameters, win_rate, pnl_percent FROM optimization_logs 
                           WHERE symbol=? AND timeframe=? AND strategy=? 
                           ORDER BY pnl_percent DESC LIMIT 1''', (sym, tf, strat))
            row = cur.fetchone()
            if row:
                params_str, wr, pnl = row
                try:
                    p = json.loads(params_str)
                    p['wr'] = round(wr, 1)
                    optymalizacje[rynek_klucz][strat] = p
                    print(f"   ✅ Skompilowano {sym} {tf} | {strat} | Rekordowy PnL: +{pnl:.2f}%")
                except json.JSONDecodeError:
                    pass
    conn.close()
    return optymalizacje

# ==========================================
# 0C. EKSTRAKTOR ID SESJI I POST MORTEM
# ==========================================
def get_latest_session_id(sciezka_bazy="data/polymarket.db"):
    if not os.path.exists(sciezka_bazy):
        return None
    try:
        conn = sqlite3.connect(sciezka_bazy)
        cur = conn.cursor()
        cur.execute("SELECT session_id FROM market_logs_v11 WHERE session_id IS NOT NULL ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None

def wykonaj_analize_post_mortem(sciezka_bazy="data/polymarket.db"):
    print("\n" + "=" * 80)
    print(" 🕵️ ANALIZA POST-MORTEM (WYNIKI HISTORYCZNE TRANSAKCJI) ")
    print("=" * 80)
    if not os.path.exists(sciezka_bazy):
        print("Brak pliku bazy danych. Pomięcie analizy Post-Mortem.")
        return
    try:
        latest_session = get_latest_session_id(sciezka_bazy)
        conn = sqlite3.connect(sciezka_bazy)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_logs_v10'")
        if not cur.fetchone():
            print("Tabela trade_logs_v10 jest pusta lub nie istnieje. Pomięcie analizy.")
            conn.close()
            return
        if latest_session:
            print(f"🔍 Wyizolowano analizę wyłącznie dla ostatniej sesji: {latest_session}")
            try:
                df_trades = pd.read_sql_query(f"SELECT * FROM trade_logs_v10 WHERE session_id='{latest_session}'", conn)
            except Exception:
                df_trades = pd.read_sql_query("SELECT * FROM trade_logs_v10", conn)
        else:
            df_trades = pd.read_sql_query("SELECT * FROM trade_logs_v10", conn)
        conn.close()
        if df_trades.empty:
            print("Brak historii transakcji w bazie dla tej sesji.")
            return
        total_pnl = df_trades['pnl'].sum()
        total_trades = len(df_trades)
        win_rate = (len(df_trades[df_trades['pnl'] > 0]) / total_trades) * 100 if total_trades > 0 else 0.0
        print(f"📊 PnL Sesji: {total_pnl:>+10.2f}$ | Łącznie zagrań: {total_trades} | Średni WR: {win_rate:.1f}%\n")
        print(f"{'RYNEK':<12} | {'STRATEGIA':<16} | {'ZAGRAŃ':<6} | {'WR %':<6} | {'PNL ($)':<12}")
        print("-" * 65)
        grouped = df_trades.groupby(['timeframe', 'strategy'])
        for (rynek, strategia), group in grouped:
            zagrań = len(group)
            zyskownych = len(group[group['pnl'] > 0])
            wr = (zyskownych / zagrań) * 100 if zagrań > 0 else 0.0
            pnl_sum = group['pnl'].sum()
            color_start = "\033[32m" if pnl_sum > 0 else "\033[31m"
            color_end = "\033[0m"
            print(f"{rynek:<12} | {strategia:<16} | {zagrań:<6} | {wr:>5.1f}% | {color_start}{pnl_sum:>+10.2f}${color_end}")
    except Exception as e:
        print(f"Błąd podczas generowania analizy Post-Mortem: {e}")

# ==========================================
# 1. PRZYGOTOWANIE DANYCH LEVEL 2
# ==========================================
def wczytaj_i_przygotuj_dane(sciezka_bazy="data/polymarket.db"):
    if not os.path.exists(sciezka_bazy):
        return pd.DataFrame()
    latest_session = get_latest_session_id(sciezka_bazy)
    conn = sqlite3.connect(sciezka_bazy)
    try:
        if latest_session:
            print(f"⏳ Pobieranie ticków poziomu 2 wyłącznie dla sesji: {latest_session}...")
            query = f"SELECT * FROM market_logs_v11 WHERE session_id='{latest_session}' AND (buy_up > 0 OR buy_down > 0) ORDER BY fetched_at ASC"
            df = pd.read_sql_query(query, conn)
        else:
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
    df['skok_btc'] = df.groupby('market_id')['live_price'].diff()
    df['zmiana_up'] = df.groupby('market_id')['buy_up'].diff().abs()
    df['zmiana_down'] = df.groupby('market_id')['buy_down'].diff().abs()
    ostatnie_tiki = df.groupby('market_id').last()
    wygrane_rynki_up = ostatnie_tiki[ostatnie_tiki['live_price'] >= ostatnie_tiki['target_price']].index.tolist()
    df['won_up'] = df['market_id'].isin(wygrane_rynki_up)
    return df

def prepare_fast_markets(df_rynki):
    markets = []
    for m_id, group in df_rynki.groupby('market_id'):
        markets.append({
            'm_id': m_id,
            'target': group['target_price'].iloc[0],
            'won_up': group['won_up'].iloc[0],
            'sec_left': group['sec_left'].values,
            'sec_since_start': group['sec_since_start'].values,
            'live': group['live_price'].values,
            'skok_btc': group['skok_btc'].values,
            'zmiana_up': group['zmiana_up'].values,
            'zmiana_down': group['zmiana_down'].values,
            'b_up': group['buy_up'].values,
            'b_dn': group['buy_down'].values,
            's_up': group['sell_up'].values,
            's_dn': group['sell_down'].values
        })
    return markets

def symuluj_wyjscie_proste(r, idx_wejscia, kierunek, cena_zakupu, stawka, global_sec_rule):
    udzialy = stawka / cena_zakupu
    for j in range(idx_wejscia + 1, len(r['sec_left'])):
        sec_left = r['sec_left'][j]
        current_bid = r['s_up'][j] if kierunek == 'UP' else r['s_dn'][j]
        if sec_left <= global_sec_rule and current_bid > cena_zakupu:
            return (current_bid * udzialy) - stawka
    wygrana = r['won_up'] if kierunek == 'UP' else not r['won_up']
    return (1.0 * udzialy - stawka) if wygrana else -stawka

# ==========================================
# WSPÓŁDZIELONA FUNKCJA PROGRESS BAR & DB CHECK
# ==========================================
def display_results_and_compare(w, symbol, interwal, strategy_name):
    if not w: return
    p = w['p']
    print(f"\n   👑 TOP OBECNEGO TESTU: {w['pnl']:>+7.2f}$ ({w['pnl_proc']:>+6.2f}%) | WR: {w['wr']:>5.1f}% | T: {w['t']:>3}")
    param_str = ", ".join([f"{k}={v}" for k, v in p.items() if k not in ['g_sec', 'id', 'max_delta_abs']])
    print(f"   ⚙️ Parametry: {param_str}")
    print(f"   🔑 Identyfikator w bazie: {p['id']}")
    top_3 = get_top_3_historical(symbol, interwal, strategy_name)
    print("   🏆 TOP 3 HISTORYCZNE (Wg WinRate):")
    if not top_3:
        print("      Brak historycznych zapisów. Ta strategia ustanawia pierwszy rekord.")
    else:
        for idx, row in enumerate(top_3):
            ts, wr, pnl_proc, params_str, t_count = row
            try:
                params_json = json.loads(params_str)
                hist_id = params_json.get('id', 'Brak ID')
                hist_clean_params = {k: v for k, v in params_json.items() if k not in ['g_sec', 'id', 'max_delta_abs']}
                hist_param_str = ", ".join([f"{k}={v}" for k, v in hist_clean_params.items()])
            except:
                hist_id = "Brak ID"
                hist_param_str = "Brak danych"
            date_str = ts[:10]
            print(f"      {idx+1}. WR: {wr:>5.1f}% | PnL: {pnl_proc:>+6.2f}% | T: {t_count} | ID: {hist_id} (Data: {date_str})")
            print(f"         ⚙️ Parametry: {hist_param_str}")

def execute_with_progress(worker_func, kombinacje, fast_markets, num_cores):
    chunk_size = max(1, len(kombinacje) // (num_cores * 4))
    chunks = [kombinacje[i:i + chunk_size] for i in range(0, len(kombinacje), chunk_size)]
    najlepsze = []
    print("   ⏳ Postęp testu: 0%", end="", flush=True)
    total_chunks = len(chunks)
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [executor.submit(worker_func, chunk, fast_markets) for chunk in chunks]
        completed = 0
        last_printed_pct = 0
        for f in as_completed(futures):
            najlepsze.extend(f.result())
            completed += 1
            pct = int((completed / total_chunks) * 100)
            if pct - last_printed_pct >= 10 or completed == total_chunks:
                step = (pct // 10) * 10
                if step > last_printed_pct:
                    print(f" ➡️ {step}%", end="", flush=True)
                    last_printed_pct = step
    print(" ✅ Gotowe!", flush=True)
    return najlepsze

# ==========================================
# WORKERS & STRATEGIES
# ==========================================
def worker_lag_sniper(param_chunk, fast_markets):
    wyniki = []
    stawka = 2.0
    for p in param_chunk:
        calkowity_pnl, transakcje, wygrane = 0.0, 0, 0
        for r in fast_markets:
            for j in range(len(r['sec_left'])):
                if r['sec_left'][j] <= 10: break
                prog = p['prog_koncowka'] if r['sec_left'][j] <= p['czas_koncowki'] else p['prog_bazowy']
                wejscie = None
                if r['skok_btc'][j] >= prog and r['zmiana_up'][j] <= p['lag_tol'] and 0 < r['b_up'][j] <= p['max_cena']:
                    wejscie = ('UP', r['b_up'][j])
                elif r['skok_btc'][j] <= -prog and r['zmiana_down'][j] <= p['lag_tol'] and 0 < r['b_dn'][j] <= p['max_cena']:
                    wejscie = ('DOWN', r['b_dn'][j])
                if wejscie:
                    pnl_transakcji = symuluj_wyjscie_proste(r, j, wejscie[0], wejscie[1], stawka, p['g_sec'])
                    calkowity_pnl += pnl_transakcji
                    transakcje += 1
                    if pnl_transakcji > 0: wygrane += 1
                    break
        if transakcje > 0:
            pnl_proc = (calkowity_pnl / (transakcje * stawka)) * 100
            wyniki.append({'p': p, 'pnl': calkowity_pnl, 'pnl_proc': pnl_proc, 'wr': (wygrane/transakcje)*100, 't': transakcje})
    return wyniki

def testuj_lag_sniper(df_rynki, symbol, interwal):
    strat_id = f"{symbol.lower()}_{interwal}_ls_{int(time.time())}"
    print(f"\n" + "=" * 80)
    print(f" 🎯 LAG SNIPER | {symbol} {interwal}")
    
    if symbol == "BTC": pb_list, pk_list = list(range(15, 36)), list(range(5, 21))
    elif symbol == "ETH": pb_list, pk_list = [x/10.0 for x in range(10, 31, 2)], [x/10.0 for x in range(5, 16, 2)]
    elif symbol == "SOL": pb_list, pk_list = [x/100.0 for x in range(5, 21)], [x/100.0 for x in range(2, 11)]
    elif symbol == "XRP": pb_list, pk_list = [x/10000.0 for x in range(5, 21)], [x/10000.0 for x in range(2, 11)]
    else: pb_list, pk_list = [10.0], [5.0]
    
    kombinacje = [
        {'prog_bazowy': pb, 'prog_koncowka': pk, 'czas_koncowki': ck, 'lag_tol': lt, 'max_cena': mc, 'g_sec': 2.0}
        for pb in pb_list for pk in pk_list for ck in range(30, 91, 5)
        for lt in [0.05, 0.10, 0.15] for mc in [0.92, 0.94, 0.96, 0.98]
        if pk <= pb
    ]
    ticks_count = len(df_rynki)
    print(f"   📊 Wolumen Danych: {len(kombinacje)} kombinacji siatki | {ticks_count} ticków z arkusza L2")
    fast_markets = prepare_fast_markets(df_rynki)
    num_cores = os.cpu_count() or 4
    najlepsze = execute_with_progress(worker_lag_sniper, kombinacje, fast_markets, num_cores)
    if not najlepsze: return None
    najlepsze.sort(key=lambda x: x['pnl'], reverse=True)
    w = najlepsze[0]
    w['p']['id'] = strat_id
    display_results_and_compare(w, symbol, interwal, "lag_sniper")
    return w

def worker_1min_momentum(param_chunk, fast_markets):
    wyniki = []
    stawka = 1.0
    for p in param_chunk:
        calkowity_pnl, transakcje, wygrane = 0.0, 0, 0
        for r in fast_markets:
            idx_wejscia = -1
            wejscie_typ = None
            wejscie_cena = 0.0
            for j in range(len(r['sec_left'])):
                if r['sec_left'][j] > p['win_start']: continue
                if r['sec_left'][j] < p['win_end']: break
                delta = r['live'][j] - r['target']
                if delta >= p['delta'] and 0 < r['b_up'][j] <= p['max_p']:
                    idx_wejscia = j; wejscie_typ = 'UP'; wejscie_cena = r['b_up'][j]
                    break
                elif delta <= -p['delta'] and 0 < r['b_dn'][j] <= p['max_p']:
                    idx_wejscia = j; wejscie_typ = 'DOWN'; wejscie_cena = r['b_dn'][j]
                    break
            if idx_wejscia != -1:
                pnl_t = symuluj_wyjscie_proste(r, idx_wejscia, wejscie_typ, wejscie_cena, stawka, p['g_sec'])
                calkowity_pnl += pnl_t
                transakcje += 1
                if pnl_t > 0: wygrane += 1
        if transakcje > 0:
            pnl_proc = (calkowity_pnl / (transakcje * stawka)) * 100
            wyniki.append({'p': p, 'pnl': calkowity_pnl, 'pnl_proc': pnl_proc, 'wr': (wygrane/transakcje)*100, 't': transakcje})
    return wyniki

def testuj_1min_momentum(df_rynki, symbol, interwal):
    strat_id = f"{symbol.lower()}_{interwal}_mom_{int(time.time())}"
    print(f"\n" + "=" * 80)
    print(f" 🚀 1-MINUTE MOMENTUM | {symbol} {interwal}")
    
    if symbol == "BTC": d_list = list(range(15, 41))
    elif symbol == "ETH": d_list = [x/10.0 for x in range(5, 26)]
    elif symbol == "SOL": d_list = [x/100.0 for x in range(2, 16)]
    elif symbol == "XRP": d_list = [x/10000.0 for x in range(2, 16)]
    else: d_list = [10.0]
    
    kombinacje = [
        {'delta': d, 'max_p': m_p, 'win_start': ws, 'win_end': we, 'g_sec': 2.0}
        for d in d_list for m_p in [x / 100.0 for x in range(65, 86)]
        for ws in range(60, 91, 2) for we in range(30, 56, 2)
        if ws > we + 5
    ]
    ticks_count = len(df_rynki)
    print(f"   📊 Wolumen Danych: {len(kombinacje)} kombinacji siatki | {ticks_count} ticków z arkusza L2")
    fast_markets = prepare_fast_markets(df_rynki)
    num_cores = os.cpu_count() or 4
    random.shuffle(kombinacje)
    najlepsze = execute_with_progress(worker_1min_momentum, kombinacje, fast_markets, num_cores)
    if not najlepsze: return None
    najlepsze.sort(key=lambda x: x['pnl'], reverse=True)
    w = najlepsze[0]
    w['p']['id'] = strat_id
    display_results_and_compare(w, symbol, interwal, "momentum")
    return w

def worker_mid_arb(param_chunk, fast_markets):
    wyniki = []
    stawka = 2.0
    for p in param_chunk:
        calkowity_pnl, transakcje, wygrane = 0.0, 0, 0
        for r in fast_markets:
            for j in range(len(r['sec_left'])):
                if p['win_start'] > r['sec_left'][j] > p['win_end']:
                    delta = r['live'][j] - r['target']
                    wejscie = None
                    if delta > p['delta'] and 0 < r['b_up'][j] <= p['max_p']:
                        wejscie = ('UP', r['b_up'][j])
                    elif delta < -p['delta'] and 0 < r['b_dn'][j] <= p['max_p']:
                        wejscie = ('DOWN', r['b_dn'][j])
                    if wejscie:
                        pnl_transakcji = symuluj_wyjscie_proste(r, j, wejscie[0], wejscie[1], stawka, p['g_sec'])
                        calkowity_pnl += pnl_transakcji
                        transakcje += 1
                        if pnl_transakcji > 0: wygrane += 1
                        break
        if transakcje > 0:
            pnl_proc = (calkowity_pnl / (transakcje * stawka)) * 100
            wyniki.append({'p': p, 'pnl': calkowity_pnl, 'pnl_proc': pnl_proc, 'wr': (wygrane/transakcje)*100, 't': transakcje})
    return wyniki

def testuj_mid_game_arb(df_rynki, symbol, interwal):
    strat_id = f"{symbol.lower()}_{interwal}_arb_{int(time.time())}"
    print(f"\n" + "=" * 80)
    print(f" ⚖️ MID-GAME ARB | {symbol} {interwal}")
    
    if symbol == "BTC": d_list = list(range(5, 21))
    elif symbol == "ETH": d_list = [x/10.0 for x in range(3, 11)]
    elif symbol == "SOL": d_list = [x/100.0 for x in range(1, 6)]
    elif symbol == "XRP": d_list = [x/10000.0 for x in range(1, 6)]
    else: d_list = [5.0]
    
    kombinacje = [
        {'delta': d, 'max_p': mp, 'win_start': ws, 'win_end': we, 'g_sec': 2.0}
        for d in d_list for mp in [x/100.0 for x in range(45, 66, 2)]
        for ws in range(120, 201, 10) for we in range(30, 91, 5)
        if ws > we + 10
    ]
    ticks_count = len(df_rynki)
    print(f"   📊 Wolumen Danych: {len(kombinacje)} kombinacji siatki | {ticks_count} ticków z arkusza L2")
    fast_markets = prepare_fast_markets(df_rynki)
    num_cores = os.cpu_count() or 4
    najlepsze = execute_with_progress(worker_mid_arb, kombinacje, fast_markets, num_cores)
    if not najlepsze: return None
    najlepsze.sort(key=lambda x: x['pnl'], reverse=True)
    w = najlepsze[0]
    w['p']['id'] = strat_id
    display_results_and_compare(w, symbol, interwal, "mid_arb")
    return w

def worker_otm(param_chunk, fast_markets):
    wyniki = []
    stawka = 1.0
    for p in param_chunk:
        calkowity_pnl, transakcje, wygrane = 0.0, 0, 0
        for r in fast_markets:
            for j in range(len(r['sec_left'])):
                if p['win_start'] >= r['sec_left'][j] >= p['win_end']:
                    delta_abs = abs(r['live'][j] - r['target'])
                    wejscie = None
                    if delta_abs < p['max_delta_abs']:
                        if 0 < r['b_up'][j] <= p['max_p']: wejscie = ('UP', r['b_up'][j])
                        elif 0 < r['b_dn'][j] <= p['max_p']: wejscie = ('DOWN', r['b_dn'][j])
                        if wejscie:
                            pnl_t = symuluj_wyjscie_proste(r, j, wejscie[0], wejscie[1], stawka, p['g_sec'])
                            calkowity_pnl += pnl_t
                            transakcje += 1
                            if pnl_t > 0: wygrane += 1
                            break
        if transakcje > 0:
            pnl_proc = (calkowity_pnl / (transakcje * stawka)) * 100
            wyniki.append({'p': p, 'pnl': calkowity_pnl, 'pnl_proc': pnl_proc, 'wr': (wygrane/transakcje)*100, 't': transakcje})
    return wyniki

def testuj_otm_bargain(df_rynki, symbol, interwal):
    strat_id = f"{symbol.lower()}_{interwal}_otm_{int(time.time())}"
    print(f"\n" + "=" * 80)
    print(f" 🎟️ OTM BARGAIN | {symbol} {interwal}")
    
    if symbol == "BTC": max_d = 40.0
    elif symbol == "ETH": max_d = 2.0
    elif symbol == "SOL": max_d = 0.1
    elif symbol == "XRP": max_d = 0.001
    else: max_d = 10.0
    
    kombinacje = [
        {'win_start': ws, 'win_end': we, 'max_p': p, 'g_sec': 2.0, 'max_delta_abs': max_d}
        for ws in range(50, 91, 2) for we in range(20, 56, 2)
        for p in [x/100.0 for x in range(2, 10)]
        if ws > we
    ]
    ticks_count = len(df_rynki)
    print(f"   📊 Wolumen Danych: {len(kombinacje)} kombinacji siatki | {ticks_count} ticków z arkusza L2")
    fast_markets = prepare_fast_markets(df_rynki)
    num_cores = os.cpu_count() or 4
    najlepsze = execute_with_progress(worker_otm, kombinacje, fast_markets, num_cores)
    if not najlepsze: return None
    najlepsze.sort(key=lambda x: x['pnl'], reverse=True)
    w = najlepsze[0]
    w['p']['id'] = strat_id
    display_results_and_compare(w, symbol, interwal, "otm")
    return w

# ==========================================
# GŁÓWNA ORKIESTRACJA
# ==========================================
def generate_tracked_configs_output(optymalizacje):
    print("\n" + "=" * 80)
    print(" 📋 ZESTAWIENIE DO PLIKU main.py (Gotowe do skopiowania)")
    print("=" * 80)
    
    tracked_configs = []
    
    # Stała, zdefiniowana przez architekta kolejność i atrybuty
    rynki = [
        ('XRP', '15m', 4), ('BTC', '15m', 2), ('ETH', '15m', 2), ('SOL', '15m', 3),
        ('BTC', '5m', 2), ('ETH', '5m', 2), ('SOL', '5m', 3), ('XRP', '5m', 4),
        ('XRP', '1h', 4), ('BTC', '1h', 2), ('ETH', '1h', 2), ('SOL', '1h', 3)
    ]
    
    for sym, tf, decimals in rynki:
        rynek_klucz = f"{sym}_{tf}"
        strats = optymalizacje.get(rynek_klucz, {})
        
        interval_s = int(tf.replace('m', '').replace('h', '')) * (60 if 'm' in tf else 3600)
        
        cfg = {
            "symbol": sym,
            "pair": f"{sym}USDT",
            "timeframe": tf,
            "interval": interval_s,
            "decimals": decimals,
            "offset": 0.0,
        }
        
        if 'lag_sniper' in strats:
            cfg['lag_sniper'] = {k: v for k, v in strats['lag_sniper'].items() if k not in ['g_sec', 'max_delta_abs']}
        if 'momentum' in strats:
            cfg['momentum'] = {k: v for k, v in strats['momentum'].items() if k not in ['g_sec', 'max_delta_abs']}
        if 'mid_arb' in strats:
            cfg['mid_arb'] = {k: v for k, v in strats['mid_arb'].items() if k not in ['g_sec', 'max_delta_abs']}
        if 'otm' in strats:
            cfg['otm'] = {k: v for k, v in strats['otm'].items() if k not in ['g_sec', 'max_delta_abs']}
            
        tracked_configs.append(cfg)
        
    config_str = json.dumps(tracked_configs, indent=4)
    config_str = config_str.replace("null", "None").replace("true", "True").replace("false", "False")
    print(f"TRACKED_CONFIGS = {config_str}")

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    
    parser = argparse.ArgumentParser(description="Watcher v10.27 Alpha Vault Backtester")
    parser.add_argument('--fast-track', action='store_true', help="Pomija testowanie i buduje TRACKED_CONFIGS z najlepszych wyników historycznych bazy")
    args = parser.parse_args()

    init_history_db()

    if args.fast_track:
        optymalizacje = compile_best_from_vault()
        generate_tracked_configs_output(optymalizacje)
    else:
        wykonaj_analize_post_mortem()
        print("\n⏳ Wczytywanie bazy danych SQLite (v11) i formatowanie danych Numpy do symulacji...")
        time_s = time.time()
        dane_historyczne = wczytaj_i_przygotuj_dane()
        print(f"✅ Załadowano i wektoryzowano dane w {time.time()-time_s:.2f} sekundy.")
        
        if dane_historyczne.empty:
            print("Brak logów rynkowych poziomu 2 w ostatniej sesji do przeprowadzenia nowej optymalizacji siatkowej.")
        else:
            unikalne_rynki = dane_historyczne['timeframe'].unique()
            optymalizacje = {}
            
            for rynek in unikalne_rynki:
                if "_" not in rynek: continue
                symbol, interwal = rynek.split('_')
                
                d_int = dane_historyczne[dane_historyczne['timeframe'] == rynek].copy()
                if d_int.empty: continue
                
                optymalizacje[rynek] = {}
                
                best_lag = testuj_lag_sniper(d_int, symbol, interwal)
                if best_lag:
                    save_optimization_result(symbol, interwal, "lag_sniper", best_lag)
                    optymalizacje[rynek]['lag_sniper'] = best_lag['p']
                    optymalizacje[rynek]['lag_sniper']['wr'] = round(best_lag['wr'], 1)
                    
                best_mom = testuj_1min_momentum(d_int, symbol, interwal)
                if best_mom:
                    save_optimization_result(symbol, interwal, "momentum", best_mom)
                    optymalizacje[rynek]['momentum'] = best_mom['p']
                    optymalizacje[rynek]['momentum']['wr'] = round(best_mom['wr'], 1)
                    
                best_arb = testuj_mid_game_arb(d_int, symbol, interwal)
                if best_arb:
                    save_optimization_result(symbol, interwal, "mid_arb", best_arb)
                    optymalizacje[rynek]['mid_arb'] = best_arb['p']
                    optymalizacje[rynek]['mid_arb']['wr'] = round(best_arb['wr'], 1)
                    
                best_otm = testuj_otm_bargain(d_int, symbol, interwal)
                if best_otm:
                    save_optimization_result(symbol, interwal, "otm", best_otm)
                    optymalizacje[rynek]['otm'] = best_otm['p']
                    optymalizacje[rynek]['otm']['wr'] = round(best_otm['wr'], 1)
                    
            generate_tracked_configs_output(optymalizacje)