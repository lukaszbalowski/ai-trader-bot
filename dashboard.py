import sys

def render_dashboard(configs, state, active_markets, trades, live_data, balance, init_balance, logs, errors, trade_history):
    CLR = "\033[2J\033[H"
    RST = "\033[0m"
    BLD = "\033[1m"
    GRN = "\033[32m"
    RED = "\033[31m"
    YLW = "\033[33m"
    CYN = "\033[36m"
    MAG = "\033[35m"
    
    out = [CLR]
    out.append(f"{BLD}{MAG}=== WATCHER v10.27 (INTEGRATED POSITIONS & ID POOL) ==={RST}")
    
    # 1. Globalny PnL %
    net_pnl = balance - init_balance
    net_pnl_pct = (net_pnl / init_balance) * 100 if init_balance else 0.0
    pnl_color = GRN if net_pnl >= 0 else RED
    out.append(f"Zysk/Strata (Netto): {pnl_color}${net_pnl:+.2f} ({net_pnl_pct:+.2f}%){RST} | Saldo: ${balance:.2f} (Start: ${init_balance:.2f})")
    out.append(f"{BLD}{CYN}--- RYNKI (LIVE) ---{RST}")
    
    for cfg in configs:
        tk = f"{cfg['symbol']}_{cfg['timeframe']}"
        live_p = state['binance_live_price'].get(cfg['pair'], 0.0)
        adj_p = live_p + cfg['offset']
        
        # 2. PnL Zrealizowany (dla konkretnego rynku)
        realized_pnl = sum(t['pnl'] for t in trade_history if f"{t['symbol']}_{t['timeframe']}" == tk)
        realized_pct = (realized_pnl / init_balance) * 100 if init_balance else 0.0
        session_color = GRN if realized_pnl > 0 else (RED if realized_pnl < 0 else RST)
        session_str = f" | Sesja: {session_color}${realized_pnl:+.2f} ({realized_pct:+.2f}%){RST}" if realized_pnl != 0 else ""
        
        if tk in active_markets:
            m_id = active_markets[tk]['m_id']
            target = active_markets[tk]['target']
            timing = state.get(f'timing_{m_id}', {})
            sec_left = timing.get('sec_left', 0)
            
            is_ver = timing.get('m_data', {}).get('verified', False)
            ver_txt = f"{GRN}[VERIFIED]{RST}" if is_ver else f"{YLW}[UNVERIFIED]{RST}"
            
            m_data = live_data.get(m_id, {})
            b_up = m_data.get('UP_BID', 0.0)
            b_dn = m_data.get('DOWN_BID', 0.0)
            
            diff = adj_p - target
            diff_trend = f"{GRN}↑{RST}" if diff >= 0 else f"{RED}↓{RST}"
            
            market_trades = [t for t in trades if t['market_id'] == m_id]
            floating_str = ""
            
            if market_trades:
                market_pnl = 0.0
                market_invested = 0.0
                for t in market_trades:
                    c_bid = m_data.get(f"{t['direction']}_BID", 0.0)
                    market_pnl += (c_bid * t['shares']) - t['invested']
                    market_invested += t['invested']
                
                # 3. PnL Pływający % (Floating PnL dla aktywnych pozycji)
                float_pct = (market_pnl / market_invested) * 100 if market_invested else 0.0
                float_color = GRN if market_pnl > 0 else (RED if market_pnl < 0 else YLW)
                floating_str = f" | Float PnL: {float_color}${market_pnl:+.2f} ({float_pct:+.2f}%){RST}"
            
            out.append(f"{BLD}{cfg['symbol']} {cfg['timeframe']}{RST} {ver_txt} | Meta: {sec_left:05.1f}s | Baza: ${target:,.2f}{session_str}")
            out.append(f"   ├─ Live: ${adj_p:,.2f} | D: {diff_trend} ${abs(diff):.2f}{floating_str}")
            
            if market_trades:
                out.append(f"   ├─ L2 -> UP: {b_up*100:04.1f}c | DOWN: {b_dn*100:04.1f}c")
                out.append(f"   └─ {BLD}{MAG}[AKTYWNE POZYCJE]{RST}")
                for idx, t in enumerate(market_trades):
                    prefix = "    └─" if idx == len(market_trades) - 1 else "    ├─"
                    c_bid = m_data.get(f"{t['direction']}_BID", 0.0)
                    c_val = c_bid * t['shares']
                    pnl = c_val - t['invested']
                    pnl_pct = (pnl / t['invested']) * 100 if t['invested'] else 0.0
                    t_color = GRN if pnl > 0 else (RED if pnl < 0 else YLW)
                    
                    # Wyświetlanie ID z puli 0-99
                    out.append(f"   {prefix} [ID: {t['short_id']:02d}] {t['strategy']} ({t['direction']}) | Wkład: ${t['invested']:.2f} | PnL: {t_color}${pnl:+.2f} ({pnl_pct:+.2f}%){RST}")
            else:
                out.append(f"   └─ L2 -> UP: {b_up*100:04.1f}c | DOWN: {b_dn*100:04.1f}c")
                
        else:
            out.append(f"{BLD}{cfg['symbol']} {cfg['timeframe']}{RST} | Oczekiwanie... (Live: ${live_p:,.2f}){session_str}")
            
    if errors:
        out.append(f"\n{BLD}{RED}--- ALARMY / BŁĘDY ---{RST}")
        for err in errors:
            out.append(f"{RED}{err}{RST}")
            
    out.append(f"\n{BLD}{YLW}--- LOGI SYSTEMOWE ---{RST}")
    for l in logs[-5:]:
        out.append(l)
        
    out.append(f"\n{BLD}[Klawiszologia] {RST}'q' + Enter -> Awaryjna wyprzedaż i zamknięcie")
    sys.stdout.write("\n".join(out) + "\n")
    sys.stdout.flush()