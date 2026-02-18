import sqlite3
import pandas as pd
import numpy as np
import random
import time
import itertools
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

# ==========================================
# 1. WSPÓLNE PRZYGOTOWANIE DANYCH
# ==========================================
def wczytaj_i_przygotuj_dane(sciezka_bazy="bazy/polymarket.db"):
    conn = sqlite3.connect(sciezka_bazy)
    df = pd.read_sql_query("SELECT * FROM market_logs_v10 WHERE buy_up > 0 OR buy_down > 0 ORDER BY fetched_at ASC", conn)
    conn.close()
    
    if df.empty: return df

    df['fetched_at'] = pd.to_datetime(df['fetched_at'])
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

# ==========================================
# 2. PATH-DEPENDENT SIMULATOR (Uproszczony - Hold with Safety)
# ==========================================
def symuluj_wyjscie_proste(r, idx_wejscia, kierunek, cena_zakupu, stawka, global_sec_rule):
    udzialy = stawka / cena_zakupu
    
    # Tylko Reguła 2 sekund - chronimy zyski na samym końcu
    for j in range(idx_wejscia + 1, len(r['sec_left'])):
        sec_left = r['sec_left'][j]
        current_bid = r['s_up'][j] if kierunek == 'UP' else r['s_dn'][j]
        
        if sec_left <= global_sec_rule and current_bid > cena_zakupu:
            return (current_bid * udzialy) - stawka
            
    # W innym przypadku twarde rozliczenie Oracle
    wygrana = r['won_up'] if kierunek == 'UP' else not r['won_up']
    return (1.0 * udzialy - stawka) if wygrana else -stawka

# ==========================================
# 3. LAG SNIPER (Wektorowo-Ścieżkowy)
# ==========================================
def worker_lag_sniper(param_chunk, fast_markets):
    wyniki = []
    stawka = 2.0
    for p in param_chunk:
        calkowity_pnl, transakcje, wygrane = 0.0, 0, 0
        for r in fast_markets:
            for j in range(len(r['sec_left'])):
                if r['sec_left'][j] <= 10: break 
                
                # Zależność od czasu - powrót do logiki z pliku Word
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
                    break # Jeden strzał na rynek
        if transakcje > 0:
            wyniki.append({'p': p, 'pnl': calkowity_pnl, 'wr': (wygrane/transakcje)*100, 't': transakcje})
    return wyniki

def testuj_lag_sniper(df_rynki, interwal):
    print(f"\n" + "="*80)
    print(f" 🎯 LAG SNIPER (Interwał {interwal}) | Wektorowo z Safety Cashout")
    print("="*80)
    
    kombinacje = [
        {'prog_bazowy': pb, 'prog_koncowka': pk, 'czas_koncowki': ck, 'lag_tol': lt, 'max_cena': mc, 'g_sec': 2.0}
        for pb in [20.0, 25.0, 30.0, 35.0]
        for pk in [10.0, 15.0, 20.0]
        for ck in [30.0, 45.0, 60.0, 90.0]
        for lt in [0.05, 0.10, 0.15]
        for mc in [0.90, 0.94, 0.96, 0.98]
        if pk <= pb
    ]
    
    fast_markets = prepare_fast_markets(df_rynki)
    num_cores = os.cpu_count() or 4
    print(f"⚙️ Testuję {len(kombinacje)} wariantów na {len(fast_markets)} rynkach (Wielowątkowo na {num_cores} rdzeniach).")

    chunks = [kombinacje[i:i + len(kombinacje)//num_cores + 1] for i in range(0, len(kombinacje), len(kombinacje)//num_cores + 1)]
    najlepsze = []
    
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [executor.submit(worker_lag_sniper, chunk, fast_markets) for chunk in chunks]
        for f in as_completed(futures): najlepsze.extend(f.result())

    if not najlepsze: return
    najlepsze.sort(key=lambda x: x['pnl'], reverse=True)
    print("\n 👑 TOP 10 (LAG SNIPER) ")
    for i, w in enumerate(najlepsze[:10], 1):
        p = w['p']
        print(f"[{i:02d}] PnL: {w['pnl']:>+7.2f}$ | WR: {w['wr']:>5.1f}% | T: {w['t']:>3} || "
              f"Skok: ${p['prog_bazowy']} (do ${p['prog_koncowka']} w {p['czas_koncowki']}s) | Lag: {p['lag_tol']*100:.0f}¢ | MaxCena: {p['max_cena']*100:.0f}¢")

# ==========================================
# 4. 1-MINUTE MOMENTUM
# ==========================================
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
            wyniki.append({'p': p, 'pnl': calkowity_pnl, 'wr': (wygrane/transakcje)*100, 't': transakcje})
    return wyniki

def testuj_1min_momentum(df_rynki, interwal):
    print(f"\n" + "="*80)
    print(f" 🚀 1-MINUTE MOMENTUM (Interwał {interwal}) | Grid Search")
    print("="*80)
    
    kombinacje = [
        {'delta': d, 'max_p': m_p, 'win_start': ws, 'win_end': we, 'g_sec': 2.0}
        for d in range(20, 36)                        # Delta co $1 (20-35)
        for m_p in [x / 100.0 for x in range(70, 86)] # Max cena co 1¢ (70-85)
        for ws in range(70, 91, 2)                    # Start okna
        for we in range(40, 56, 2)                    # Koniec okna
        if ws > we + 5                                
    ]
    
    fast_markets = prepare_fast_markets(df_rynki)
    num_cores = os.cpu_count() or 4
    print(f"⚙️ Testuję {len(kombinacje):,} wariantów brute-force na {len(fast_markets)} rynkach.")
    
    random.shuffle(kombinacje) 
    chunks = [kombinacje[i:i + len(kombinacje)//num_cores + 1] for i in range(0, len(kombinacje), len(kombinacje)//num_cores + 1)]
    najlepsze = []
    
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [executor.submit(worker_1min_momentum, chunk, fast_markets) for chunk in chunks]
        zrealizowane = 0
        for f in as_completed(futures):
            zrealizowane += len(chunks[0])
            najlepsze.extend(f.result())
            print(f"   [Worker] Zakończono paczkę. Postęp: ~{zrealizowane}/{len(kombinacje)}...")

    if not najlepsze: return
    najlepsze.sort(key=lambda x: x['pnl'], reverse=True)
    print("\n 👑 TOP 10 (1-MINUTE MOMENTUM) ")
    for i, w in enumerate(najlepsze[:10], 1):
        p = w['p']
        print(f"[{i:02d}] PnL: {w['pnl']:>+7.2f}$ | WR: {w['wr']:>5.1f}% | T: {w['t']:>3} || "
              f"Delta: >${p['delta']} | MaxCena: {p['max_p']*100:.0f}¢ | Okno: {p['win_start']}s->{p['win_end']}s | Cashout: {p['g_sec']}s")

# ==========================================
# 5. MID-GAME VALUE ARBITRAGE
# ==========================================
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
            wyniki.append({'p': p, 'pnl': calkowity_pnl, 'wr': (wygrane/transakcje)*100, 't': transakcje})
    return wyniki

def testuj_mid_game_arb(df_rynki, interwal):
    print(f"\n" + "="*80)
    print(f" ⚖️ MID-GAME VALUE ARBITRAGE (Interwał {interwal})")
    print("="*80)
    
    kombinacje = [
        {'delta': d, 'max_p': mp, 'win_start': ws, 'win_end': we, 'g_sec': 2.0} 
        for d in [5.0, 8.0, 10.0, 12.0, 15.0] 
        for mp in [0.50, 0.55, 0.60]
        for ws in [180, 150, 120]
        for we in [60, 45]
    ]
    fast_markets = prepare_fast_markets(df_rynki)
    najlepsze = worker_mid_arb(kombinacje, fast_markets) 
    
    if not najlepsze: return
    najlepsze.sort(key=lambda x: x['pnl'], reverse=True)
    print("\n 👑 TOP 5 WYNIKÓW (MID-GAME ARB)")
    for i, w in enumerate(najlepsze[:5], 1):
        p = w['p']
        print(f"[{i:02d}] PnL: {w['pnl']:>+7.2f}$ | WR: {w['wr']:>5.1f}% | T: {w['t']:>3} || Delta: >${p['delta']} | MaxCena: {p['max_p']*100:.0f}¢ | Okno: {p['win_start']}s->{p['win_end']}s")

# ==========================================
# 6. OTM BARGAIN (TANIE LOSY)
# ==========================================
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
                    if delta_abs < 40.0:
                        if 0 < r['b_up'][j] <= p['max_p']: wejscie = ('UP', r['b_up'][j])
                        elif 0 < r['b_dn'][j] <= p['max_p']: wejscie = ('DOWN', r['b_dn'][j])
                        
                    if wejscie:
                        pnl_t = symuluj_wyjscie_proste(r, j, wejscie[0], wejscie[1], stawka, p['g_sec'])
                        calkowity_pnl += pnl_t
                        transakcje += 1
                        if pnl_t > 0: wygrane += 1
                        break
        if transakcje > 0:
            wyniki.append({'p': p, 'pnl': calkowity_pnl, 'wr': (wygrane/transakcje)*100, 't': transakcje})
    return wyniki

def testuj_otm_bargain(df_rynki, interwal):
    print(f"\n" + "="*80)
    print(f" 🎟️ OTM BARGAIN / WYPRZEDAŻ (Interwał {interwal})")
    print("="*80)
    
    kombinacje = [
        {'win_start': ws, 'win_end': we, 'max_p': p, 'g_sec': 2.0} 
        for ws in range(60, 91, 5) 
        for we in range(30, 56, 5) 
        for p in [0.03, 0.04, 0.05, 0.06, 0.07]
        if ws > we
    ]
    
    fast_markets = prepare_fast_markets(df_rynki)
    num_cores = os.cpu_count() or 4
    chunks = [kombinacje[i:i + len(kombinacje)//num_cores + 1] for i in range(0, len(kombinacje), len(kombinacje)//num_cores + 1)]
    najlepsze = []
    
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [executor.submit(worker_otm, chunk, fast_markets) for chunk in chunks]
        for f in as_completed(futures): najlepsze.extend(f.result())

    if not najlepsze: return
    najlepsze.sort(key=lambda x: x['pnl'], reverse=True)
    print("\n 👑 TOP 10 (OTM BARGAIN)")
    for i, w in enumerate(najlepsze[:10], 1):
        p = w['p']
        print(f"[{i:02d}] PnL: {w['pnl']:>+7.2f}$ | WR: {w['wr']:>5.1f}% | T: {w['t']:>3} || Okno: {p['win_start']}s->{p['win_end']}s | MaxCena: {p['max_p']*100:.0f}¢")

# ==========================================
# GŁÓWNY PANEL ORKIESTRATORA
# ==========================================
if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    
    print("⏳ Wczytywanie bazy danych SQLite i formatowanie danych Numpy...")
    time_s = time.time()
    dane_historyczne = wczytaj_i_przygotuj_dane()
    print(f"✅ Załadowano i wektoryzowano dane w {time.time()-time_s:.2f} sekundy.")
    
    if dane_historyczne.empty:
        print("Baza danych jest pusta.")
    else:
        for interwal in ['5m', '15m']:
            d_int = dane_historyczne[dane_historyczne['timeframe'] == interwal].copy()
            if d_int.empty: 
                continue
            
            testuj_lag_sniper(d_int, interwal)
            testuj_1min_momentum(d_int, interwal)
            testuj_mid_game_arb(d_int, interwal)
            testuj_otm_bargain(d_int, interwal)
            
        print("\n✅ Analiza HFT klasy Enterprise Zakończona Pomyślnie!")