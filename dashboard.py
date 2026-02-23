import sys
import time
import datetime

def render_dashboard(configs, state, active_markets, trades, live_data, balance, init_balance, logs, errors, trade_history):
    if 'session_start' not in state:
        state['session_start'] = time.time()
        
    CLR = "\033[2J\033[H"
    RST = "\033[0m"
    BLD = "\033[1m"
    GRN = "\033[32m"
    RED = "\033[31m"
    YLW = "\033[33m"
    CYN = "\033[36m"
    MAG = "\033[35m"

    out = [CLR]
    out.append(f"{BLD}{MAG}=== WATCHER v10.30 (CLEAN-BASE / MICRO-PROTECTION) ==={RST}")
    out.append(f"{BLD}{CYN}--- MARKETS (LIVE) ---{RST}")

    for cfg in configs:
        tk = f"{cfg['symbol']}_{cfg['timeframe']}"
        live_p = state['binance_live_price'].get(cfg['pair'], 0.0)
        adj_p = live_p + cfg['offset']
        
        realized_pnl = sum(t['pnl'] for t in trade_history if f"{t['symbol']}_{t['timeframe']}" == tk)
        realized_pct = (realized_pnl / init_balance) * 100 if init_balance else 0.0
        session_color = GRN if realized_pnl > 0 else (RED if realized_pnl < 0 else RST)
        session_str = f" | Session: {session_color}${realized_pnl:+.2f} ({realized_pct:+.2f}%){RST}" if realized_pnl != 0 else ""

        strat_ids = []
        strat_map = {'lag_sniper': 'LS', 'momentum': 'MOM', 'mid_arb': 'ARB', 'otm': 'OTM'}
        for s_key, s_abbr in strat_map.items():
            if s_key in cfg:
                s_id = cfg[s_key].get('id', 'None')
                strat_ids.append(f"{s_abbr}: {s_id}")
        ids_str = f"\n   ├─ IDs: {', '.join(strat_ids)}" if strat_ids else ""

        if tk in active_markets:
            m_id = active_markets[tk]['m_id']
            target = active_markets[tk]['target']
            timing = state.get(f'timing_{m_id}', {})
            sec_left = timing.get('sec_left', 0)
            sec_since_start = timing.get('sec_since_start', 0)
            is_pre_warming = timing.get('is_pre_warming', False)
            
            is_base_fetched = timing.get('m_data', {}).get('base_fetched', False)
            is_clean = timing.get('m_data', {}).get('is_clean', False)
            
            if is_pre_warming:
                ver_txt = f"{MAG}[🔥 PRE-WARM]{RST} {GRN}[BASE OK]{RST}" if is_base_fetched else f"{MAG}[🔥 PRE-WARM]{RST} {YLW}[FETCHING...]{RST}"
                meta_str = f"Starts in: {abs(sec_since_start):04.1f}s"
            else:
                if is_base_fetched:
                    ver_txt = f"{GRN}[BASE OK]{RST}" if is_clean else f"{YLW}[MID-JOIN: OTM ONLY]{RST}"
                else:
                    ver_txt = f"{YLW}[NO BASE]{RST}"
                meta_str = f"End: {sec_left:05.1f}s"
            
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
                float_pct = (market_pnl / market_invested) * 100 if market_invested else 0.0
                float_color = GRN if market_pnl > 0 else (RED if market_pnl < 0 else YLW)
                floating_str = f" | Float PnL: {float_color}${market_pnl:+.2f} ({float_pct:+.2f}%){RST}"

            out.append(f"{BLD}{cfg['symbol']} {cfg['timeframe']}{RST} {ver_txt} | {meta_str} | Strike: ${target:,.2f}{session_str}{ids_str}")
            out.append(f"   ├─ Live: ${adj_p:,.2f} | D: {diff_trend} ${abs(diff):.2f}{floating_str}")

            if market_trades:
                out.append(f"   ├─ L2 -> UP: {b_up*100:04.1f}c | DOWN: {b_dn*100:04.1f}c")
                out.append(f"   └─ {BLD}{MAG}[ACTIVE POSITIONS]{RST}")
                for idx, t in enumerate(market_trades):
                    prefix = "    └─" if idx == len(market_trades) - 1 else "    ├─"
                    c_bid = m_data.get(f"{t['direction']}_BID", 0.0)
                    c_val = c_bid * t['shares']
                    pnl = c_val - t['invested']
                    pnl_pct = (pnl / t['invested']) * 100 if t['invested'] else 0.0
                    t_color = GRN if pnl > 0 else (RED if pnl < 0 else YLW)
                    
                    # Wizualne wskazanie, czy pozycja ma nałożony licznik SL dla Lag Snipera
                    sl_warn = f" {RED}[SL: {int(10 - (time.time() - t['sl_countdown']))}s]{RST}" if 'sl_countdown' in t else ""
                    
                    out.append(f"   {prefix} [ID: {t['short_id']:02d}] {t['strategy']} ({t['direction']}) | Invested: ${t['invested']:.2f} | PnL: {t_color}${pnl:+.2f} ({pnl_pct:+.2f}%){RST}{sl_warn}")
            else:
                out.append(f"   └─ L2 -> UP: {b_up*100:04.1f}c | DOWN: {b_dn*100:04.1f}c")
        else:
            out.append(f"{BLD}{cfg['symbol']} {cfg['timeframe']}{RST} | Waiting for new market... (Live: ${live_p:,.2f}){session_str}{ids_str}")

    total_floating_value = 0.0
    total_invested_value = 0.0
    
    for t in trades:
        m_id = t['market_id']
        m_data = live_data.get(m_id, {})
        c_bid = m_data.get(f"{t['direction']}_BID", 0.0)
        total_floating_value += c_bid * t['shares']
        total_invested_value += t['invested']
    
    current_equity = balance + total_floating_value
    net_pnl = current_equity - init_balance
    net_pnl_pct = (net_pnl / init_balance) * 100 if init_balance else 0.0
    pnl_color = GRN if net_pnl >= 0 else RED
    
    start_ts = state.get('session_start', time.time())
    duration_sec = int(time.time() - start_ts)
    duration_str = str(datetime.timedelta(seconds=duration_sec))
    session_id_str = state.get('session_id', 'No ID')

    out.append(f"\n{BLD}{CYN}--- PORTFOLIO STATUS ---{RST}")
    out.append(f"Start: ${init_balance:.2f} | Session time: {duration_str} | Session ID: {session_id_str}")
    out.append(f"Invested in positions: ${total_invested_value:.2f} | Floating options value: ${total_floating_value:.2f}")
    out.append(f"Total portfolio value: {BLD}${current_equity:.2f}{RST} (Cash balance: ${balance:.2f})")
    out.append(f"Global PnL (Realized + Float): {pnl_color}${net_pnl:+.2f} ({net_pnl_pct:+.2f}%){RST}")

    if errors:
        out.append(f"\n{BLD}{RED}--- ALARMS / ERRORS ---{RST}")
        for err in errors:
            out.append(f"{RED}{err}{RST}")

    out.append(f"\n{BLD}{YLW}--- SYSTEM LOGS ---{RST}")
    for l in logs[-5:]:
        out.append(l)

    out.append(f"\n{BLD}[Controls] {RST}'q' + Enter -> Emergency sell & exit | 'd' + Enter -> PANIC DUMP (Save to file)")
    sys.stdout.write("\n".join(out) + "\n")
    sys.stdout.flush()