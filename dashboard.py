import sys
from datetime import datetime

# Klasyczne kolory ANSI do formatowania konsoli
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RESET = '\033[0m'
BOLD = '\033[1m'

def render_dashboard(tracked_configs, local_state, active_markets, paper_trades, live_market_data, portfolio_balance, initial_balance, recent_logs, active_errors, trade_history):
    lines = []
    
    # Przesunięcie kursora na samą górę i wyczyszczenie ekranu
    lines.append('\033[H\033[J')
    
    lines.append(f"{BOLD}{CYAN}=== 🚀 WATCHER v10.20 LIVE HFT TERMINAL ==={RESET}   Czas lokalny: {datetime.now().strftime('%H:%M:%S')}")
    lines.append("")

    total_current_value = 0.0

    # Rysowanie ramek dla każdego z rynków
    for config in tracked_configs:
        symbol = config['symbol']
        tf = config['timeframe']
        pair = config['pair']
        decimals = config['decimals']
        
        timeframe_key = f"{symbol}_{tf}"
        active_m_info = active_markets.get(timeframe_key)
        
        market_id = None
        sec_left = 0
        m_data = None
        
        if active_m_info:
            market_id = active_m_info['m_id']
            timing = local_state.get(f"timing_{market_id}")
            if timing:
                sec_left = timing['sec_left']
                m_data = timing['m_data']
                
        header = f" {BOLD}{symbol} {tf}{RESET} (Zostało: {sec_left:.0f}s) "
        
        if not market_id or not m_data:
            lines.append(f"╔════{header}════════════════════════════════════════════════════════════════")
            lines.append(f"║ ⏳ Oczekiwanie na inicjalizację rynku...")
            lines.append("╚═════════════════════════════════════════════════════════════════════════════════════")
            continue

        live_p = local_state['binance_live_price'].get(pair, 0.0)
        adjusted_live_p = live_p + config['offset']
        base_p = m_data['price']
        
        verified_tag = f"{GREEN}VERIFIED{RESET}" if m_data['verified'] else f"{YELLOW}FALLBACK{RESET}"
        
        delta = adjusted_live_p - base_p
        if delta >= 0:
            status_str = f"{GREEN}ABOVE (+{delta:.{decimals}f}){RESET}"
        else:
            status_str = f"{RED}BELOW ({delta:.{decimals}f}){RESET}"
            
        # Obliczanie historycznego, zrealizowanego PnL dla tego konkretnego rynku
        realized_pnl = sum(t.get('pnl', 0.0) for t in trade_history if t.get('symbol') == symbol and t.get('timeframe') == tf)
        pnl_hist_color = GREEN if realized_pnl > 0 else (RED if realized_pnl < 0 else RESET)
            
        lines.append(f"╔════{header}════════════════════════════════════════════════════════════════")
        lines.append(f"║ BAZA: ${base_p:,.{decimals}f} [{verified_tag}]  |  LIVE: ${adjusted_live_p:,.{decimals}f}  |  STATUS: {status_str}")
        lines.append(f"║ 📊 Zrealizowany PnL (Sesja): {pnl_hist_color}${realized_pnl:+.2f}{RESET}")
        
        market_trades = [t for t in paper_trades if t['market_id'] == market_id]
        if market_trades:
            lines.append(f"║ {BOLD}Otwarte pozycje ({len(market_trades)}):{RESET}")
            for t in market_trades:
                current_bid = live_market_data.get(market_id, {}).get(f"{t['direction']}_BID", 0.0)
                val = t['shares'] * current_bid
                pnl = val - t['invested']
                
                pnl_color = GREEN if pnl > 0 else (RED if pnl < 0 else RESET)
                icon = "📈" if pnl >= 0 else "📉"
                
                lines.append(f"║  ↳ {t['strategy']} ({t['direction']}) | Wkład: ${t['invested']:.2f} | Wycena: ${val:.2f} | Unrealized PnL: {pnl_color}{icon} ${pnl:+.2f}{RESET}")
        else:
            lines.append(f"║ Brak otwartych pozycji.")
            
        lines.append("╚═════════════════════════════════════════════════════════════════════════════════════")

    # Globalna kalkulacja portfela
    for t in paper_trades:
        cbid = live_market_data.get(t['market_id'], {}).get(f"{t['direction']}_BID", 0.0)
        total_current_value += t['shares'] * cbid

    total_invested = sum(t['invested'] for t in paper_trades)
    total_equity = portfolio_balance + total_current_value
    net_pnl = total_equity - initial_balance
    net_color = GREEN if net_pnl >= 0 else (RED if net_pnl < 0 else RESET)

    lines.append("")
    lines.append(f"💼 {BOLD}PODSUMOWANIE PORTFELA{RESET}")
    lines.append("─────────────────────────────────────────────────────────────────────────────────────")
    lines.append(f" Gotówka (Konto)  : ${portfolio_balance:.2f}")
    lines.append(f" Zainwestowane    : ${total_invested:.2f}")
    lines.append(f" Aktualna Wycena  : ${total_current_value:.2f}")
    lines.append(f" Całkowite Equity : ${total_equity:.2f}  |  TOTAL PnL: {net_color}${net_pnl:+.2f}{RESET}")
    lines.append("─────────────────────────────────────────────────────────────────────────────────────")
    
    # 🔴 MODUŁ WYŚWIETLANIA BŁĘDÓW (Jeśli istnieją)
    if active_errors:
        lines.append("")
        lines.append(f"🚨 {BOLD}{RED}WYKRYTO BŁĘDY SYSTEMOWE (Zrzut zapisano w data/error_dumps.log){RESET}")
        for err in active_errors:
            lines.append(f" {RED}{err}{RESET}")

    lines.append("")
    lines.append(f"📝 {BOLD}OSTATNIA AKTYWNOŚĆ (Historia Systemowa){RESET}")
    for l in recent_logs:
        lines.append(f" {l}")

    sys.stdout.write('\n'.join(lines) + '\n')
    sys.stdout.flush()