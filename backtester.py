import sqlite3
import pandas as pd
import numpy as np
import random
import time
import itertools

# ==========================================
# 1. WSPÓLNE PRZYGOTOWANIE DANYCH
# ==========================================
def wczytaj_i_przygotuj_dane(sciezka_bazy="data/polymarket.db"):
    """Wczytuje logi rynkowe i przygotowuje zunifikowane metryki czasowe i cenowe."""
    conn = sqlite3.connect(sciezka_bazy)
    df = pd.read_sql_query("SELECT * FROM market_logs_v10 ORDER BY fetched_at ASC", conn)
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

# ==========================================
# 2. STRATEGIA 1: PRECISION LAG SNIPER
# ==========================================
def testuj_lag_sniper(df_rynki, interwal):
    print(f"\n" + "="*80)
    print(f" 🎯 STRATEGIA 1: LAG SNIPER (Interwał {interwal}) | Tryb: Grid Search")
    print("="*80)
    
    param_grid = {
        'prog_bazowy': [15.0, 20.0, 25.0, 30.0, 35.0, 40.0],
        'prog_koncowka': [10.0, 15.0, 20.0, 25.0, 30.0],
        'czas_koncowki': [30.0, 45.0, 60.0, 75.0, 90.0],
        'lag_tol': [0.05, 0.10, 0.15],
        'max_cena': [0.90, 0.92, 0.94, 0.96, 0.98]
    }
    klucze = list(param_grid.keys())
    kombinacje = list(itertools.product(*param_grid.values()))
    
    num_variants = len(kombinacje)
    num_logs = len(df_rynki)
    total_ops = num_variants * num_logs
    
    print(f"⚙️ Testuję {num_variants} wariantów na {num_logs:,} informacjach rynkowych.")
    print(f"⚙️ Łączna liczba kombinacji do zweryfikowania: {total_ops:,}")

    wyniki_testow = []
    start_time = time.time()
    
    for i, wartosci in enumerate(kombinacje):
        p = dict(zip(klucze, wartosci))
        
        if p['prog_koncowka'] > p['prog_bazowy']: 
            continue
            
        aktywny_prog = np.where(df_rynki['sec_left'] <= p['czas_koncowki'], p['prog_koncowka'], p['prog_bazowy'])
        
        maska_up = (df_rynki['skok_btc'] >= aktywny_prog) & \
                   (df_rynki['zmiana_up'] < p['lag_tol']) & \
                   (df_rynki['buy_up'] > 0) & \
                   (df_rynki['buy_up'] <= p['max_cena']) & \
                   (df_rynki['sec_left'] > 10)
                   
        maska_down = (df_rynki['skok_btc'] <= -aktywny_prog) & \
                     (df_rynki['zmiana_down'] < p['lag_tol']) & \
                     (df_rynki['buy_down'] > 0) & \
                     (df_rynki['buy_down'] <= p['max_cena']) & \
                     (df_rynki['sec_left'] > 10)
                     
        sygnaly = df_rynki[maska_up | maska_down].drop_duplicates(subset=['market_id'], keep='first').copy()
        
        if not sygnaly.empty:
            sygnaly['is_up'] = maska_up.loc[sygnaly.index]
            sygnaly['wygrana'] = (sygnaly['is_up'] & sygnaly['won_up']) | (~sygnaly['is_up'] & ~sygnaly['won_up'])
            sygnaly['cena_wejscia'] = np.where(sygnaly['is_up'], sygnaly['buy_up'], sygnaly['buy_down'])
            sygnaly['pnl'] = np.where(sygnaly['wygrana'], (1.0 / sygnaly['cena_wejscia']) - 1.0, -1.0)
            
            wyniki_testow.append({
                'parametry': p, 
                'pnl': sygnaly['pnl'].sum(), 
                'win_rate': sygnaly['wygrana'].mean() * 100, 
                'transakcje': len(sygnaly)
            })
            
        if (i+1) % 5000 == 0:
            print(f"[{i+1}/{num_variants}] Przetestowano... ({(time.time() - start_time):.1f}s)")
        
    if not wyniki_testow:
        print(" Brak wejść dla Lag Sniper.")
        return

    wyniki_testow.sort(key=lambda x: x['pnl'], reverse=True)
    print("\n 👑 TOP 20 (LAG SNIPER) ")
    for i, w in enumerate(wyniki_testow[:20], 1):
        p = w['parametry']
        print(f"[{i:02d}] PnL: {w['pnl']:>+7.2f}$ | WR: {w['win_rate']:>5.1f}% | Transakcji: {w['transakcje']:>3} || "
              f"Skok: {p['prog_bazowy']}$ (do {p['prog_koncowka']}$ w {p['czas_koncowki']}s) | Lag: {p['lag_tol']*100:.0f}¢ | MaxCena: {p['max_cena']*100:.0f}¢")

# ==========================================
# 3. STRATEGIA 2: STRADDLE & CUT (WEJŚCIE)
# ==========================================
def symuluj_straddle(tiki_rynku, p):
    idx_wejscia = -1
    cena_docelowa = tiki_rynku['target_price'].iloc[0]
    
    # Szukanie punktu wejścia
    for row in tiki_rynku.itertuples():
        if row.sec_since_start > p['start_win']: 
            break
        adj_delta = abs(row.live_price - cena_docelowa)
        if adj_delta <= p['max_delta']:
            if p['min_p'] <= row.buy_up <= p['max_p'] and p['min_p'] <= row.buy_down <= p['max_p']:
                idx_wejscia, cena_wejscia_up, cena_wejscia_down = row.Index, row.buy_up, row.buy_down
                break
                
    if idx_wejscia == -1: 
        return 0.0, False 
        
    udzialy_up, udzialy_down = 1.0 / cena_wejscia_up, 1.0 / cena_wejscia_down
    up_aktywne, down_aktywne = True, True
    pnl_calkowity = 0.0
    up_timer_start, down_timer_start = None, None
    
    # Symulacja zarządzania pozycją (wyjścia)
    for row in tiki_rynku.loc[idx_wejscia+1:].itertuples():
        if not up_aktywne and not down_aktywne: 
            break
            
        if up_aktywne:
            if row.sell_up >= p['tp']:
                pnl_calkowity += (row.sell_up * udzialy_up) - 1.0
                up_aktywne = False
            elif row.sell_up <= p['cl_act']:
                if up_timer_start is None: 
                    up_timer_start = row.sec_since_start
                elif (row.sec_since_start - up_timer_start) >= p['cl_timer']:
                    pnl_calkowity += (row.sell_up * udzialy_up) - 1.0
                    up_aktywne = False
            elif row.sell_up >= 0.50: 
                up_timer_start = None 
                
        if down_aktywne:
            if row.sell_down >= p['tp']:
                pnl_calkowity += (row.sell_down * udzialy_down) - 1.0
                down_aktywne = False
            elif row.sell_down <= p['cl_act']:
                if down_timer_start is None: 
                    down_timer_start = row.sec_since_start
                elif (row.sec_since_start - down_timer_start) >= p['cl_timer']:
                    pnl_calkowity += (row.sell_down * udzialy_down) - 1.0
                    down_aktywne = False
            elif row.sell_down >= 0.50: 
                down_timer_start = None 

    # Końcowe rozliczenie
    wygrana_up = tiki_rynku['won_up'].iloc[0]
    if up_aktywne: 
        pnl_calkowity += (1.0 * udzialy_up - 1.0) if wygrana_up else -1.0
    if down_aktywne: 
        pnl_calkowity += (1.0 * udzialy_down - 1.0) if not wygrana_up else -1.0
        
    return pnl_calkowity, True

def testuj_straddle_random_search(df_rynki, interwal, ilosc_probek=10000):
    print(f"\n" + "="*80)
    print(f" ⚖️ STRATEGIA 2: STRADDLE & CUT (Interwał {interwal}) | Tryb: Monte Carlo")
    print("="*80)
    
    num_logs = len(df_rynki)
    total_ops = ilosc_probek * num_logs
    
    print(f"⚙️ Testuję {ilosc_probek} wariantów na {num_logs:,} informacjach rynkowych.")
    print(f"⚙️ Łączna liczba kombinacji do zweryfikowania: {total_ops:,}")

    najlepsze_wyniki = []
    lista_rynkow = [df_rynku for _, df_rynku in df_rynki.groupby('market_id')]
    start_time = time.time()
    
    for i in range(ilosc_probek):
        p = {
            'start_win': random.randint(20, 60), 
            'max_delta': random.randint(10, 30),
            'min_p': random.randint(40, 50) / 100.0, 
            'max_p': random.randint(50, 60) / 100.0,
            'tp': random.randint(80, 95) / 100.0, 
            'cl_act': random.randint(20, 40) / 100.0, 
            'cl_timer': random.randint(20, 40)
        }
        
        calkowity_pnl, ilosc_transakcji, zyskownych = 0.0, 0, 0
        
        for tiki_rynku in lista_rynkow:
            pnl_rynku, bylo_wejscie = symuluj_straddle(tiki_rynku, p)
            if bylo_wejscie:
                calkowity_pnl += pnl_rynku
                ilosc_transakcji += 1
                if pnl_rynku > 0: 
                    zyskownych += 1
                    
        if ilosc_transakcji > 0:
            najlepsze_wyniki.append({
                'parametry': p, 
                'pnl': calkowity_pnl, 
                'win_rate': (zyskownych / ilosc_transakcji) * 100, 
                'transakcje': ilosc_transakcji
            })
            
        if (i+1) % 2500 == 0:
            print(f"[{i+1}/{ilosc_probek}] Przetestowano... ({(time.time() - start_time):.1f}s)")
            
    if not najlepsze_wyniki:
        print(" Brak wejść dla Straddle.")
        return
        
    najlepsze_wyniki.sort(key=lambda x: x['pnl'], reverse=True)
    print("\n 👑 TOP 20 (STRADDLE & CUT) ")
    for i, w in enumerate(najlepsze_wyniki[:20], 1):
        p = w['parametry']
        print(f"[{i:02d}] PnL: {w['pnl']:>+7.2f}$ | WR: {w['win_rate']:>5.1f}% | T: {w['transakcje']} || "
              f"Wejście: Czas do {p['start_win']}s | Delta: ${p['max_delta']} | Wyjście: TP {p['tp']*100:.0f}¢ | CL po {p['cl_timer']}s")

# ==========================================
# 4. STRATEGIA 3: 1-MINUTE MOMENTUM
# ==========================================
def testuj_1min_momentum(df_rynki, interwal, ilosc_probek=10000):
    print(f"\n" + "="*80)
    print(f" 🚀 STRATEGIA 3: 1-MINUTE MOMENTUM (Interwał {interwal}) | Tryb: Monte Carlo")
    print("="*80)
    
    num_logs = len(df_rynki)
    total_ops = ilosc_probek * num_logs
    
    print(f"⚙️ Testuję {ilosc_probek} wariantów na {num_logs:,} informacjach rynkowych.")
    print(f"⚙️ Łączna liczba kombinacji do zweryfikowania: {total_ops:,}")
    
    wyniki_testow = []
    start_time = time.time()
    
    for i in range(ilosc_probek):
        p = {
            'delta_threshold': random.randint(20, 40),
            'win_start': random.randint(55, 90),
            'win_end': random.randint(40, 54),
            'max_price': random.randint(70, 90) / 100.0
        }
        
        maska_czasu = (df_rynki['sec_left'] <= p['win_start']) & (df_rynki['sec_left'] >= p['win_end'])
        
        maska_up = maska_czasu & \
                   ((df_rynki['live_price'] - df_rynki['target_price']) >= p['delta_threshold']) & \
                   (df_rynki['buy_up'] > 0) & \
                   (df_rynki['buy_up'] <= p['max_price'])
                   
        maska_down = maska_czasu & \
                     ((df_rynki['target_price'] - df_rynki['live_price']) >= p['delta_threshold']) & \
                     (df_rynki['buy_down'] > 0) & \
                     (df_rynki['buy_down'] <= p['max_price'])
                     
        sygnaly = df_rynki[maska_up | maska_down].drop_duplicates(subset=['market_id'], keep='first').copy()
        
        if not sygnaly.empty:
            sygnaly['is_up'] = maska_up.loc[sygnaly.index]
            sygnaly['wygrana'] = (sygnaly['is_up'] & sygnaly['won_up']) | (~sygnaly['is_up'] & ~sygnaly['won_up'])
            sygnaly['cena_wejscia'] = np.where(sygnaly['is_up'], sygnaly['buy_up'], sygnaly['buy_down'])
            sygnaly['pnl'] = np.where(sygnaly['wygrana'], (1.0 / sygnaly['cena_wejscia']) - 1.0, -1.0)
            
            wyniki_testow.append({
                'parametry': p, 
                'pnl': sygnaly['pnl'].sum(), 
                'win_rate': sygnaly['wygrana'].mean() * 100, 
                'transakcje': len(sygnaly)
            })
            
        if (i+1) % 2500 == 0:
            print(f"[{i+1}/{ilosc_probek}] Przetestowano... ({(time.time() - start_time):.1f}s)")
            
    if not wyniki_testow:
        print(" Brak wejść dla Momentum.")
        return

    wyniki_testow.sort(key=lambda x: x['pnl'], reverse=True)
    print("\n 👑 TOP 10 (1-MINUTE MOMENTUM) ")
    for i, w in enumerate(wyniki_testow[:10], 1):
        p = w['parametry']
        print(f"[{i:02d}] PnL: {w['pnl']:>+7.2f}$ | WR: {w['win_rate']:>5.1f}% | T: {w['transakcje']:>3} || "
              f"Okno: {p['win_start']}s -> {p['win_end']}s | Delta: >${p['delta_threshold']} | MaxCena: {p['max_price']*100:.0f}¢")

# ==========================================
# 5. STRATEGIA 4: DEEP SNIPE
# ==========================================
def testuj_deep_snipe(df_rynki, interwal):
    print(f"\n" + "="*80)
    print(f" 🎯 STRATEGIA 4: DEEP SNIPE (Interwał {interwal}) | Tryb: Grid Search")
    print("="*80)
    
    param_grid = {
        'win_start': list(range(30, 41)),       
        'win_end': list(range(20, 30)),         
        'delta_threshold': list(range(50, 91)), 
        'max_price': [0.95, 0.96, 0.97, 0.98]   
    }
    klucze = list(param_grid.keys())
    kombinacje = list(itertools.product(*param_grid.values()))
    
    num_variants = len(kombinacje)
    num_logs = len(df_rynki)
    total_ops = num_variants * num_logs
    
    print(f"⚙️ Testuję {num_variants} wariantów na {num_logs:,} informacjach rynkowych.")
    print(f"⚙️ Łączna liczba kombinacji do zweryfikowania: {total_ops:,}")

    wyniki_testow = []
    start_time = time.time()
    
    for i, wartosci in enumerate(kombinacje):
        p = dict(zip(klucze, wartosci))
        
        maska_czasu = (df_rynki['sec_left'] <= p['win_start']) & (df_rynki['sec_left'] >= p['win_end'])
        
        maska_up = maska_czasu & \
                   ((df_rynki['live_price'] - df_rynki['target_price']) >= p['delta_threshold']) & \
                   (df_rynki['buy_up'] > 0) & \
                   (df_rynki['buy_up'] <= p['max_price'])
                   
        maska_down = maska_czasu & \
                     ((df_rynki['target_price'] - df_rynki['live_price']) >= p['delta_threshold']) & \
                     (df_rynki['buy_down'] > 0) & \
                     (df_rynki['buy_down'] <= p['max_price'])
                     
        sygnaly = df_rynki[maska_up | maska_down].drop_duplicates(subset=['market_id'], keep='first').copy()
        
        if not sygnaly.empty:
            sygnaly['is_up'] = maska_up.loc[sygnaly.index]
            sygnaly['wygrana'] = (sygnaly['is_up'] & sygnaly['won_up']) | (~sygnaly['is_up'] & ~sygnaly['won_up'])
            sygnaly['cena_wejscia'] = np.where(sygnaly['is_up'], sygnaly['buy_up'], sygnaly['buy_down'])
            
            stawka = 10.0
            sygnaly['pnl'] = np.where(sygnaly['wygrana'], stawka * (1.0 / sygnaly['cena_wejscia']) - stawka, -stawka)
            
            wyniki_testow.append({
                'parametry': p, 
                'pnl': sygnaly['pnl'].sum(), 
                'win_rate': sygnaly['wygrana'].mean() * 100, 
                'transakcje': len(sygnaly)
            })
            
        if (i+1) % 5000 == 0:
            print(f"[{i+1}/{num_variants}] Przetestowano... ({(time.time() - start_time):.1f}s)")
            
    if not wyniki_testow:
        print(" Brak wejść dla Deep Snipe.")
        return

    wyniki_testow.sort(key=lambda x: x['pnl'], reverse=True)
    print("\n 👑 TOP 20 (DEEP SNIPE) ")
    for i, w in enumerate(wyniki_testow[:20], 1):
        p = w['parametry']
        print(f"[{i:02d}] PnL: {w['pnl']:>+7.2f}$ | WR: {w['win_rate']:>5.1f}% | T: {w['transakcje']:>3} || "
              f"Okno: {p['win_start']}s -> {p['win_end']}s | Delta: >${p['delta_threshold']} | MaxCena: {p['max_price']*100:.0f}¢")

# ==========================================
# 6. STRATEGIA 5: 60-SECOND POWER SNIPE
# ==========================================
def testuj_power_snipe(df_rynki, interwal):
    print(f"\n" + "="*85)
    print(f" ⚡ STRATEGIA 5: POWER SNIPE & SAFETY CASHOUT (Interwał {interwal})")
    print("="*85)
    
    param_grid = {
        'win_start': list(range(55, 81)),        
        'win_end': list(range(45, 55)),          
        'is_open_window': [True, False],         
        'delta_threshold': list(range(75, 111, 5)), 
        'max_price': [0.95, 0.96, 0.97, 0.98]    
    }
    klucze = list(param_grid.keys())
    kombinacje = list(itertools.product(*param_grid.values()))
    
    num_variants = len(kombinacje)
    num_logs = len(df_rynki)
    total_ops = num_variants * num_logs
    
    print(f"⚙️ Testuję {num_variants} wariantów na {num_logs:,} informacjach rynkowych.")
    print(f"⚙️ Łączna liczba kombinacji do zweryfikowania: {total_ops:,}")
    
    rynki = [d for _, d in df_rynki.groupby('market_id')]
    wyniki_testow = []
    
    for i, wartosci in enumerate(kombinacje):
        p = dict(zip(klucze, wartosci))
        calkowity_pnl, transakcje, wygrane = 0.0, 0, 0
        
        for tiki in rynki:
            target = tiki['target_price'].iloc[0]
            won_up = tiki['won_up'].iloc[0]
            wejscie = None
            
            for row in tiki.itertuples():
                if p['is_open_window']:
                    aktywne_okno = (row.sec_left <= p['win_start'] and row.sec_left >= 2.0)
                else:
                    aktywne_okno = (row.sec_left <= p['win_start'] and row.sec_left >= p['win_end'])
                
                if aktywne_okno:
                    delta = row.live_price - target
                    if delta >= p['delta_threshold'] and 0 < row.buy_up <= p['max_price']:
                        wejscie = {'typ': 'UP', 'cena': row.buy_up, 'idx': row.Index}
                        break
                    elif delta <= -p['delta_threshold'] and 0 < row.buy_down <= p['max_price']:
                        wejscie = {'typ': 'DOWN', 'cena': row.buy_down, 'idx': row.Index}
                        break
                        
            if wejscie:
                transakcje += 1
                stawka = 2.0 
                udzialy = stawka / wejscie['cena']
                pnl_rynku = -stawka 
                cashout_wykonany = False
                
                for r in tiki.loc[wejscie['idx']+1:].itertuples():
                    if 1.5 <= r.sec_left <= 2.5: 
                        current_bid = r.sell_up if wejscie['typ'] == 'UP' else r.sell_down
                        if current_bid > wejscie['cena']:
                            pnl_rynku = (current_bid * udzialy) - stawka
                            cashout_wykonany = True
                            wygrane += 1
                            break
                            
                if not cashout_wykonany:
                    is_winner = (wejscie['typ'] == 'UP' and won_up) or (wejscie['typ'] == 'DOWN' and not won_up)
                    if is_winner:
                        pnl_rynku = (1.0 * udzialy) - stawka
                        wygrane += 1
                        
                calkowity_pnl += pnl_rynku
                
        if transakcje > 0:
            wyniki_testow.append({
                'p': p, 
                'pnl': calkowity_pnl, 
                'wr': (wygrane / transakcje) * 100, 
                't': transakcje
            })

    if not wyniki_testow:
        print(" Brak wejść dla Power Snipe.")
        return

    wyniki_testow.sort(key=lambda x: x['pnl'], reverse=True)
    print("\n 👑 TOP 20 (POWER SNIPE)")
    for i, w in enumerate(wyniki_testow[:20], 1):
        p = w['p']
        tryb = "OTWARTY (do 2s)" if p['is_open_window'] else f"STAŁY ({p['win_start']}-{p['win_end']}s)"
        print(f"[{i:02d}] PnL: {w['pnl']:>+7.2f}$ | WR: {w['wr']:>5.1f}% | T: {w['t']:>3} || "
              f"Tryb: {tryb} | Delta: >${p['delta_threshold']} | MaxCena: {p['max_price']*100:.0f}¢")

# ==========================================
# 7. STRATEGIA 6: STRADDLE EARLY EXIT
# ==========================================
def testuj_straddle_early_exit(df_rynki, interwal):
    print(f"\n" + "="*85)
    print(f" 🛡️ STRATEGIA 6: STRADDLE EARLY EXIT (Interwał {interwal}) | Tryb: Grid Search")
    print("="*85)

    tp_levels = [round(x * 0.01, 2) for x in range(80, 96)]
    num_variants = len(tp_levels)
    num_logs = len(df_rynki)
    total_ops = num_variants * num_logs
    
    print(f"⚙️ Testuję {num_variants} wariantów na {num_logs:,} informacjach rynkowych.")
    print(f"⚙️ Łączna liczba kombinacji do zweryfikowania: {total_ops:,}")

    p_wejscie = {
        'start_win': 45, 
        'max_delta': 20.0, 
        'min_p': 0.45, 
        'max_p': 0.55,
        'cl_act': 0.30, 
        'cl_timer': 30 if interwal == '5m' else 60
    }
    
    wyniki_testow = []
    rynki = [d for _, d in df_rynki.groupby('market_id')]
    
    for tp in tp_levels:
        calkowity_pnl, transakcje, wygrane = 0.0, 0, 0
        p = p_wejscie.copy()
        p['tp'] = tp
        
        for tiki in rynki:
            pnl_rynku, bylo_wejscie = symuluj_straddle(tiki, p)
            if bylo_wejscie:
                calkowity_pnl += pnl_rynku
                transakcje += 1
                if pnl_rynku > 0: 
                    wygrane += 1
                    
        if transakcje > 0:
            wyniki_testow.append({
                'tp': tp, 
                'pnl': calkowity_pnl, 
                'wr': (wygrane / transakcje) * 100, 
                't': transakcje
            })

    if not wyniki_testow:
        print(" Brak wejść dla Straddle Early Exit.")
        return

    wyniki_testow.sort(key=lambda x: x['pnl'], reverse=True)
    print("\n 👑 TOP WYNIKI TAKE PROFIT ")
    for i, w in enumerate(wyniki_testow, 1):
        print(f"[{i:02d}] PnL: {w['pnl']:>+7.2f}$ | WR: {w['wr']:>5.1f}% | T: {w['t']} || TP: {w['tp']*100:.0f}¢")

# ==========================================
# 8. GŁÓWNY PANEL ORKIESTRATORA
# ==========================================
if __name__ == "__main__":
    print("⏳ Wczytywanie i wektoryzacja bazy danych dla 6 strategii...")
    dane_historyczne = wczytaj_i_przygotuj_dane()
    
    if dane_historyczne.empty:
        print("Baza danych jest pusta.")
    else:
        for interwal in ['5m', '15m']:
            d_int = dane_historyczne[dane_historyczne['timeframe'] == interwal].copy()
            if d_int.empty: 
                continue
            
            
            testuj_lag_sniper(d_int, interwal)
            testuj_straddle_random_search(d_int, interwal, ilosc_probek=10000)
            testuj_1min_momentum(d_int, interwal, ilosc_probek=10000)
            testuj_deep_snipe(d_int, interwal)
            testuj_power_snipe(d_int, interwal)
            
            
            testuj_straddle_early_exit(d_int, interwal)
            
        print("\n✅ Backtest zakończony!")