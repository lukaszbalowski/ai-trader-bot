import asyncio
import websockets
import aiohttp
import aiosqlite
import json
import time
import sys
import os
import re
import argparse
from datetime import datetime
import pytz
from playwright.async_api import async_playwright

# --- STAŁE I KONFIGURACJA ---
CHECK_INTERVAL = 1
FIXED_OFFSET = -35.0

TRACKED_CONFIGS = [
    {"symbol": "BTC", "pair": "BTCUSDT", "timeframe": "15m", "interval": 900},
    {"symbol": "BTC", "pair": "BTCUSDT", "timeframe": "5m", "interval": 300}
]

# --- GLOBALNY STAN (W RAM) ---
LOCAL_STATE = {
    'binance_live_price': 0.0,
    'polymarket_books': {}, 
    'prev_btc': 0.0
}

LOCKED_PRICES = {}
MARKET_CACHE = {}
PAPER_TRADES = []
TRADE_HISTORY = []
EXECUTED_STRAT = {}
ACTIVE_MARKETS = {}
LIVE_MARKET_DATA = {}

INITIAL_BALANCE = 0.0
PORTFOLIO_BALANCE = 0.0

MARKET_LOGS_BUFFER = []
TRADE_LOGS_BUFFER = []
LAST_FLUSH_TS = 0

WS_SUBSCRIPTION_QUEUE = asyncio.Queue()

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

# ==========================================
# 1. BAZA DANYCH (ASYNCHRONICZNA)
# ==========================================
async def init_db():
    async with aiosqlite.connect('data/polymarket.db') as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS market_logs_v10 (
            id INTEGER PRIMARY KEY AUTOINCREMENT, timeframe TEXT, market_id TEXT,
            target_price REAL, live_price REAL, buy_up REAL, sell_up REAL,
            buy_down REAL, sell_down REAL, fetched_at TEXT)''')
        
        await db.execute('''CREATE TABLE IF NOT EXISTS trade_logs_v10 (
            trade_id TEXT PRIMARY KEY, market_id TEXT, timeframe TEXT, strategy TEXT, direction TEXT,
            invested REAL, entry_price REAL, entry_time TEXT, exit_price REAL, exit_time TEXT, pnl REAL,
            reason TEXT)''')
        await db.commit()

async def flush_to_db():
    global MARKET_LOGS_BUFFER, TRADE_LOGS_BUFFER
    if not MARKET_LOGS_BUFFER and not TRADE_LOGS_BUFFER: 
        return
    
    try:
        async with aiosqlite.connect('data/polymarket.db') as db:
            if MARKET_LOGS_BUFFER:
                await db.executemany('''INSERT INTO market_logs_v10 
                    (timeframe, market_id, target_price, live_price, buy_up, sell_up, buy_down, sell_down, fetched_at) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', MARKET_LOGS_BUFFER)
            if TRADE_LOGS_BUFFER:
                await db.executemany('''INSERT INTO trade_logs_v10 
                    (trade_id, market_id, timeframe, strategy, direction, invested, entry_price, entry_time, exit_price, exit_time, pnl, reason) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', TRADE_LOGS_BUFFER)
            await db.commit()
        
        log(f"BULK SAVE: Zapisano do bazy {len(MARKET_LOGS_BUFFER)} tików i {len(TRADE_LOGS_BUFFER)} transakcji.")
        MARKET_LOGS_BUFFER.clear()
        TRADE_LOGS_BUFFER.clear()
    except Exception as e:
        log(f"Błąd zapisu async DB: {e}")

async def async_smart_flush_worker():
    global LAST_FLUSH_TS
    while True:
        await asyncio.sleep(1)
        now_ts = int(time.time())
        current_5m_block = (now_ts // 300) * 300
        if (now_ts % 300) >= 5 and current_5m_block > LAST_FLUSH_TS:
            await flush_to_db()
            LAST_FLUSH_TS = current_5m_block

# ==========================================
# 2. ZARZĄDZANIE PORTFELEM I UI
# ==========================================
def print_portfolio_status():
    print("\n" + "="*60)
    print(" BIEŻĄCY STATUS PORTFELA (MARK-TO-MARKET)")
    print("="*60)
    print(f" Gotówka (Konto): ${PORTFOLIO_BALANCE:.2f}")
    
    current_value = 0.0
    unrealized_pnl = 0.0
    
    if not PAPER_TRADES:
        print(" Brak otwartych pozycji.")
    else:
        print(" Otwarte pozycje:")
        for i, t in enumerate(PAPER_TRADES):
            live_bid = LIVE_MARKET_DATA.get(t['market_id'], {}).get(f"{t['direction']}_BID", 0.0)
            val = t['shares'] * live_bid
            pnl = val - t['invested']
            current_value += val
            unrealized_pnl += pnl
            icon = "📈" if pnl >= 0 else "📉"
            print(f" [{i+1}] {t['strategy']} ({t['direction']}) | Wkład: ${t['invested']:.2f} | Wycena: ${val:.2f} | PnL: {icon} ${pnl:+.2f}")
    
    total_equity = PORTFOLIO_BALANCE + current_value
    net_pnl = total_equity - INITIAL_BALANCE
    print("-" * 60)
    print(f" Wartość zainwestowana: ${sum(t['invested'] for t in PAPER_TRADES):.2f}")
    print(f" Aktualna wycena pozycji: ${current_value:.2f} (Unrealized PnL: ${unrealized_pnl:+.2f})")
    print(f" Całkowite Equity: ${total_equity:.2f}")
    print(f" TOTAL PnL (Netto): ${net_pnl:+.2f}")
    print("="*60 + "\n")

async def liquidate_all_and_quit():
    log("\n🚨 ROZPOCZYNAM PROCEDURĘ AWARYJNEJ LIKWIDACJI...")
    if PAPER_TRADES:
        log(f" Znaleziono {len(PAPER_TRADES)} otwartych pozycji. Trwa rynkowa wyprzedaż...")
        for trade in PAPER_TRADES[:]:
            live_bid = LIVE_MARKET_DATA.get(trade['market_id'], {}).get(f"{trade['direction']}_BID", 0.0)
            close_trade(trade, live_bid, "ZAMKNIĘCIE AWARYJNE (Likwidacja)")
    else:
        log(" Brak otwartych pozycji do likwidacji.")
    
    await flush_to_db()
    print_portfolio_status()
    os._exit(0)

def handle_stdin():
    cmd = sys.stdin.readline().strip().lower()
    if cmd == 'p':
        print_portfolio_status()
    elif cmd == 'q':
        asyncio.create_task(liquidate_all_and_quit())

# ==========================================
# 3. LOGIKA HANDLOWA (CORE)
# ==========================================
def execute_trade(market_id, timeframe, strategy, direction, size_usd, price):
    global PORTFOLIO_BALANCE
    
    if price <= 0: 
        return
    if size_usd > PORTFOLIO_BALANCE:
        log(f"❌ Odrzucono zakup '{strategy}'. Brak środków.")
        return
        
    PORTFOLIO_BALANCE -= size_usd
    shares = size_usd / price
    trade = {
        'id': f"{market_id}_{len(PAPER_TRADES)}_{int(time.time())}",
        'market_id': market_id, 
        'timeframe': timeframe, 
        'strategy': strategy,
        'direction': direction, 
        'entry_price': price, 
        'entry_time': datetime.now().isoformat(),
        'shares': shares, 
        'invested': size_usd, 
        'timer_start': None
    }
    
    PAPER_TRADES.append(trade)
    print("\a")
    print(f"\n✅ [ZAKUP] {strategy} ({timeframe}) | {direction} | Stawka: ${size_usd:.2f} | Cena: {price*100:.1f}¢ | Saldo: ${PORTFOLIO_BALANCE:.2f}\n")

def close_trade(trade, close_price, reason):
    global PORTFOLIO_BALANCE
    
    return_value = close_price * trade['shares']
    pnl = return_value - trade['invested']
    exit_time = datetime.now().isoformat()
    
    PORTFOLIO_BALANCE += return_value
    TRADE_HISTORY.append({'pnl': pnl, 'reason': reason, **trade})
    PAPER_TRADES.remove(trade)
    
    icon = "💰" if pnl > 0 else "🩸"
    print(f"\n{icon} [SPRZEDAŻ] {trade['strategy']} [{trade['timeframe']}] ({trade['direction']}) | {reason} | Wyjście: {close_price*100:.1f}¢ | PnL: ${pnl:+.2f} | Saldo: ${PORTFOLIO_BALANCE:.2f}\n")
    
    TRADE_LOGS_BUFFER.append((
        trade['id'], trade['market_id'], trade['timeframe'],
        trade['strategy'], trade['direction'], trade['invested'], 
        trade['entry_price'], trade['entry_time'],
        close_price, exit_time, pnl, reason
    ))

def resolve_market(market_id, final_btc_price, target_price):
    is_up_winner = final_btc_price >= target_price
    for trade in PAPER_TRADES[:]:
        if trade['market_id'] == market_id:
            close_price = 1.0 if (trade['direction'] == 'UP' and is_up_winner) or (trade['direction'] == 'DOWN' and not is_up_winner) else 0.0
            close_trade(trade, close_price, "Rozliczenie Wyroczni (Koniec Rynku)")

async def get_target_via_visual_async(slug):
    url = f"https://polymarket.com/event/{slug}"
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
            page = await browser.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(4)
            content = await page.content()
            await browser.close()
            match = re.search(r'Price to beat.*?\$(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', content, re.IGNORECASE | re.DOTALL)
            if match: 
                return float(match.group(1).replace(',', ''))
    except: 
        pass
    return None

# ==========================================
# 4. WEBSOCKETY (EVENT-DRIVEN NASŁUCH)
# ==========================================
async def binance_ws_listener():
    url = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log("[WS] Podłączono do Binance (Ultra-Low Latency)")
                async for msg in ws:
                    data = json.loads(msg)
                    new_price = float(data['c'])
                    
                    if new_price != LOCAL_STATE['binance_live_price']:
                        LOCAL_STATE['prev_btc'] = LOCAL_STATE['binance_live_price']
                        LOCAL_STATE['binance_live_price'] = new_price
                        await evaluate_strategies("BINANCE_TICK")
        except Exception as e:
            log(f"[WS BŁĄD] Utracono Binance: {e}. Reconnect za 2s...")
            await asyncio.sleep(2)

def extract_best_prices(token_id):
    book = LOCAL_STATE['polymarket_books'].get(token_id, {'bids': {}, 'asks': {}})
    
    valid_bids = [p for p, s in book['bids'].items() if s > 0 and 0.005 < p < 0.995]
    best_bid = max(valid_bids) if valid_bids else 0.0
    
    valid_asks = [p for p, s in book['asks'].items() if s > 0 and 0.005 < p < 0.995]
    best_ask = min(valid_asks) if valid_asks else 0.0
    
    return best_ask, best_bid

async def polymarket_ws_listener():
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    subscribed_tokens = set()
    
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log("[WS] Podłączono do Polymarket CLOB (Asynchronous)")
                
                if subscribed_tokens:
                    await ws.send(json.dumps({"assets_ids": list(subscribed_tokens), "type": "market"}))

                async def process_queue(websocket):
                    try:
                        while True:
                            new_tokens = await WS_SUBSCRIPTION_QUEUE.get()
                            
                            while not WS_SUBSCRIPTION_QUEUE.empty():
                                new_tokens.extend(WS_SUBSCRIPTION_QUEUE.get_nowait())
                                
                            tokens_to_add = list(set([t for t in new_tokens if t not in subscribed_tokens]))
                            
                            if tokens_to_add:
                                subscribed_tokens.update(tokens_to_add)
                                log(f"[WS] Wykryto nowy rynek (+{len(tokens_to_add)} tokenów). Wymuszam twardy restart strumienia dla zrzutu arkusza...")
                                await websocket.close()
                                break
                    except asyncio.CancelledError:
                        pass
                
                queue_task = asyncio.create_task(process_queue(ws))

                try:
                    async for msg in ws:
                        if not msg or (not msg.startswith('{') and not msg.startswith('[')): 
                            continue
                        
                        try:
                            parsed_msg = json.loads(msg)
                        except json.JSONDecodeError:
                            continue
                            
                        if not isinstance(parsed_msg, list):
                            parsed_msg = [parsed_msg]
                            
                        for data in parsed_msg:
                            event_type = data.get('event_type', '')
                            
                            if event_type == 'book' or 'bids' in data or 'asks' in data:
                                t_id = data.get('asset_id')
                                if t_id:
                                    LOCAL_STATE['polymarket_books'][t_id] = {'bids': {}, 'asks': {}}
                                    for bid in data.get('bids', []):
                                        LOCAL_STATE['polymarket_books'][t_id]['bids'][float(bid['price'])] = float(bid['size'])
                                    for ask in data.get('asks', []):
                                        LOCAL_STATE['polymarket_books'][t_id]['asks'][float(ask['price'])] = float(ask['size'])
                            
                            if event_type == 'price_change' or 'price_changes' in data:
                                for change in data.get('price_changes', []):
                                    t_id = change.get('asset_id')
                                    if not t_id: continue
                                    
                                    if t_id not in LOCAL_STATE['polymarket_books']:
                                        LOCAL_STATE['polymarket_books'][t_id] = {'bids': {}, 'asks': {}}
                                        
                                    price = float(change.get('price'))
                                    size = float(change.get('size', 0))
                                    side = change.get('side', '').upper()
                                    
                                    if side == 'BUY':
                                        LOCAL_STATE['polymarket_books'][t_id]['bids'][price] = size
                                    elif side == 'SELL':
                                        LOCAL_STATE['polymarket_books'][t_id]['asks'][price] = size
                                        
                        await evaluate_strategies("CLOB_TICK")
                finally:
                    queue_task.cancel()

        except Exception as e:
            await asyncio.sleep(0.1)

# ==========================================
# 5. MENEDŻER RYNKÓW (Menedżer Stanu)
# ==========================================
async def fetch_and_track_markets():
    tz_et = pytz.timezone('America/New_York')
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                now_et = datetime.now(tz_et)
                live_p = LOCAL_STATE['binance_live_price']
                
                if live_p == 0.0:
                    await asyncio.sleep(1)
                    continue

                for config in TRACKED_CONFIGS:
                    interval_s = config['interval']
                    base_ts = (int(now_et.timestamp()) // interval_s) * interval_s
                    start_time = datetime.fromtimestamp(base_ts, tz_et)
                    time_str = start_time.strftime('%-I:%M%p').lower()
                    sec_since_start = (now_et - start_time).total_seconds()
                    sec_left = interval_s - sec_since_start
                    
                    slug = f"{config['symbol'].lower()}-updown-{config['timeframe']}-{base_ts}"
                    
                    if slug not in MARKET_CACHE:
                        async with session.get(f"https://gamma-api.polymarket.com/events?slug={slug}") as resp:
                            if resp.status == 200:
                                res = await resp.json()
                                if res and 'markets' in res[0]:
                                    for m in res[0]['markets']:
                                        if m.get('active') and time_str in m.get('question', '').lower():
                                            outcomes = eval(m.get('outcomes', '[]'))
                                            clob_ids = eval(m.get('clobTokenIds', '[]'))
                                            up_id = clob_ids[outcomes.index("Up")]
                                            dn_id = clob_ids[outcomes.index("Down")]
                                            
                                            MARKET_CACHE[slug] = {'id': str(m.get('id')), 'up_id': up_id, 'dn_id': dn_id}
                                            await WS_SUBSCRIPTION_QUEUE.put([up_id, dn_id])
                                            break
                    
                    if slug not in MARKET_CACHE: 
                        continue
                    
                    m_id = MARKET_CACHE[slug]['id']
                    timeframe = config['timeframe']
                    
                    if m_id not in LOCKED_PRICES:
                        LOCKED_PRICES[m_id] = {'price': live_p, 'verified': False, 'last_retry': 0, 'prev_up': 0, 'prev_dn': 0}
                    
                    m_data = LOCKED_PRICES[m_id]
                    
                    if timeframe not in ACTIVE_MARKETS:
                        ACTIVE_MARKETS[timeframe] = {'m_id': m_id, 'target': m_data['price']}
                    elif ACTIVE_MARKETS[timeframe]['m_id'] != m_id:
                        old_m_id = ACTIVE_MARKETS[timeframe]['m_id']
                        old_target = ACTIVE_MARKETS[timeframe]['target']
                        adjusted_final_price = live_p + FIXED_OFFSET
                        
                        log(f"🔔 RYNEK ZAMKNIĘTY [{timeframe}]. Baza=${old_target:.2f} vs Adjusted Binance=${adjusted_final_price:.2f}")
                        resolve_market(old_m_id, adjusted_final_price, old_target)
                        
                        ACTIVE_MARKETS[timeframe] = {'m_id': m_id, 'target': m_data['price']}
                    else:
                        ACTIVE_MARKETS[timeframe]['target'] = m_data['price']

                    if sec_left >= interval_s - 2: 
                        continue

                    if not m_data['verified'] and sec_since_start >= 30:
                        if (time.time() - m_data['last_retry']) > 60:
                            m_data['last_retry'] = time.time()
                            asyncio.create_task(verify_price_visual(slug, m_id))

                    LOCAL_STATE[f'timing_{m_id}'] = {
                        'sec_left': sec_left,
                        'sec_since_start': sec_since_start,
                        'interval_s': interval_s,
                        'timeframe': timeframe,
                        'm_data': m_data
                    }
                    
                    up_id = MARKET_CACHE[slug]['up_id']
                    dn_id = MARKET_CACHE[slug]['dn_id']
                    b_up, s_up = extract_best_prices(up_id)
                    b_dn, s_dn = extract_best_prices(dn_id)
                    
                    LIVE_MARKET_DATA[m_id] = {'UP_BID': s_up, 'DOWN_BID': s_dn}
                    adjusted_live_p = live_p + FIXED_OFFSET
                    
                    MARKET_LOGS_BUFFER.append((
                        config['timeframe'], m_id, m_data['price'], 
                        adjusted_live_p, b_up, s_up, b_dn, s_dn, datetime.now().isoformat()
                    ))

                    pref = " [VERIFIED]" if m_data['verified'] else " [FALLBACK]"
                    status = " ABOVE" if adjusted_live_p >= m_data['price'] else " BELOW"
                    sign = "+" if FIXED_OFFSET >= 0 else "-"
                    offset_str = f"{sign}${abs(FIXED_OFFSET):.2f}"
                    adj_delta = adjusted_live_p - m_data['price']
                    
                    log(f"{pref} [{config['symbol']} {config['timeframe']} - zostało {sec_left:.0f}s] BAZA: ${m_data['price']:,.2f} | Live: ${adjusted_live_p:,.2f} ({offset_str}) ({status} o ${abs(adj_delta):.2f})")
                    log(f"   UP -> Kup: {b_up*100:.1f}¢ | Sprzedaj: {s_up*100:.1f}¢")
                    log(f"   DOWN -> Kup: {b_dn*100:.1f}¢ | Sprzedaj: {s_dn*100:.1f}¢")

                    await evaluate_strategies("TIME_TICK", market_filter=m_id)

            except Exception as e:
                pass 
            
            await asyncio.sleep(CHECK_INTERVAL)

async def verify_price_visual(slug, m_id):
    v = await get_target_via_visual_async(slug)
    if v and m_id in LOCKED_PRICES:
        LOCKED_PRICES[m_id]['price'] = v
        LOCKED_PRICES[m_id]['verified'] = True
        log(f"👁️ [VERIFIED] Playwright pobrał nową bazę: {v}")

# ==========================================
# 6. SILNIK STRATEGII (ZOPTYMALIZOWANY)
# ==========================================
async def evaluate_strategies(trigger_source, market_filter=None):
    live_p = LOCAL_STATE['binance_live_price']
    adjusted_live_p = live_p + FIXED_OFFSET
    
    for slug, cache in MARKET_CACHE.items():
        m_id = cache['id']
        if market_filter and m_id != market_filter: 
            continue
            
        if m_id not in EXECUTED_STRAT: 
            EXECUTED_STRAT[m_id] = []
            
        if f'timing_{m_id}' not in LOCAL_STATE: 
            continue

        timing = LOCAL_STATE[f'timing_{m_id}']
        sec_left = timing['sec_left']
        sec_since_start = timing['sec_since_start']
        m_data = timing['m_data']
        timeframe = timing['timeframe']
        
        adj_delta = adjusted_live_p - m_data['price']
        b_up, s_up = extract_best_prices(cache['up_id'])
        b_dn, s_dn = extract_best_prices(cache['dn_id'])

        # ==========================================
        # ZARZĄDZANIE OTWARTYMI POZYCJAMI (Wyjścia)
        # ==========================================
        for trade in PAPER_TRADES[:]:
            if trade['market_id'] != m_id: 
                continue
                
            current_bid = s_up if trade['direction'] == 'UP' else s_dn
            
            # STRATEGIA 7: 2-Second Safety Cashout
            if 1.0 <= sec_left <= 2.5:
                if current_bid > trade['entry_price']:
                    close_trade(trade, current_bid, "Safety Cashout (Zysk przed końcem)")
                    continue
            
            # STRATEGIA 6: Zoptymalizowany Straddle Take Profit / Cut Loss
            if trade['strategy'] == "Straddle":
                tp_level = 0.95 if trade['timeframe'] == '15m' else 0.93
                if current_bid >= tp_level:
                    close_trade(trade, current_bid, f"Straddle Take Profit (Cel >{int(tp_level*100)}¢)")
                    continue
                
                cl_act = 0.38 if trade['timeframe'] == '15m' else 0.35
                wait_limit = 27 if trade['timeframe'] == '15m' else 23
                
                if current_bid <= cl_act and trade['timer_start'] is None:
                    trade['timer_start'] = time.time()
                
                if trade['timer_start'] is not None:
                    if current_bid >= 0.50:
                        trade['timer_start'] = None
                    elif (time.time() - trade['timer_start']) >= wait_limit:
                        close_trade(trade, current_bid, f"Straddle Cut-Loss ({wait_limit}s bez powrotu)")
                        continue

        # ==========================================
        # STRATEGIA 5: POWER SNIPE (Zoptymalizowany Tryb Otwarty)
        # ==========================================
        power_snipe_flag = f"power_snipe_{m_id}"
        if 2.0 <= sec_left <= 80.0 and power_snipe_flag not in EXECUTED_STRAT[m_id]:
            max_price_ps = 0.95 if timeframe == '15m' else 0.96
            if adj_delta >= 75.0 and 0 < b_up <= max_price_ps:
                execute_trade(m_id, timeframe, "Power Snipe", "UP", 2.0, b_up)
                EXECUTED_STRAT[m_id].append(power_snipe_flag)
            elif adj_delta <= -75.0 and 0 < b_dn <= max_price_ps:
                execute_trade(m_id, timeframe, "Power Snipe", "DOWN", 2.0, b_dn)
                EXECUTED_STRAT[m_id].append(power_snipe_flag)

        # ==========================================
        # STRATEGIA 1: LAG SNIPER (Zoptymalizowane Progi)
        # ==========================================
        if trigger_source == "BINANCE_TICK":
            btc_jump = live_p - LOCAL_STATE['prev_btc']
            up_change = b_up - m_data['prev_up']
            dn_change = b_dn - m_data['prev_dn']
            
            if timeframe == '15m':
                jump_threshold = 20.0 if sec_left > 30 else 10.0
                lag_tol = 0.05
                max_price_ls = 0.90
            else:
                jump_threshold = 15.0 # Zoptymalizowany stały próg dla 5m
                lag_tol = 0.05
                max_price_ls = 0.92
            
            if 10 < sec_left < timing['interval_s'] - 5:
                # Usunięto filtr Out-of-the-money (adj_delta) na podstawie wyników backtestów
                if btc_jump >= jump_threshold and abs(up_change) <= lag_tol and 0 < b_up <= max_price_ls:
                    execute_trade(m_id, timeframe, "Lag Sniper", "UP", 2.0, b_up)
                elif btc_jump <= -jump_threshold and abs(dn_change) <= lag_tol and 0 < b_dn <= max_price_ls:
                    execute_trade(m_id, timeframe, "Lag Sniper", "DOWN", 2.0, b_dn)
            
            m_data['prev_up'], m_data['prev_dn'] = b_up, b_dn

        # ==========================================
        # STRATEGIA 2: STRADDLE & CUT (Zoptymalizowane Wejście)
        # ==========================================
        start_win_straddle = 53 if timeframe == '15m' else 36
        max_delta_straddle = 28.0 if timeframe == '15m' else 27.0
        
        if sec_since_start <= start_win_straddle and 'straddle' not in EXECUTED_STRAT[m_id]:
            if abs(adj_delta) <= max_delta_straddle:
                if 0.45 <= b_up <= 0.55 and 0.45 <= b_dn <= 0.55:
                    execute_trade(m_id, timeframe, "Straddle", "UP", 1.0, b_up)
                    execute_trade(m_id, timeframe, "Straddle", "DOWN", 1.0, b_dn)
                    EXECUTED_STRAT[m_id].append('straddle')

        # ==========================================
        # STRATEGIA 3: 1-MINUTE MOMENTUM (Zoptymalizowane)
        # ==========================================
        if timeframe == '15m':
            win_start_mom, win_end_mom = 55, 52
            req_delta_mom = 20.0
            max_price_mom = 0.72
        else:
            win_start_mom, win_end_mom = 74, 48
            req_delta_mom = 33.0
            max_price_mom = 0.82

        if win_end_mom <= sec_left <= win_start_mom and 'momentum' not in EXECUTED_STRAT[m_id]:
            if adj_delta >= req_delta_mom and 0 < b_up <= max_price_mom:
                execute_trade(m_id, timeframe, "1-Min Momentum", "UP", 1.0, b_up)
                EXECUTED_STRAT[m_id].append('momentum')
            elif adj_delta <= -req_delta_mom and 0 < b_dn <= max_price_mom:
                execute_trade(m_id, timeframe, "1-Min Momentum", "DOWN", 1.0, b_dn)
                EXECUTED_STRAT[m_id].append('momentum')

        # ==========================================
        # STRATEGIA 4: DEEP SNIPE (Zoptymalizowane)
        # ==========================================
        if 20 <= sec_left <= 38 and 'deep_snipe' not in EXECUTED_STRAT[m_id]:
            if adj_delta >= 50.0 and 0 < b_up <= 0.95:
                execute_trade(m_id, timeframe, "Deep Snipe", "UP", 10.0, b_up)
                EXECUTED_STRAT[m_id].append('deep_snipe')
            elif adj_delta <= -50.0 and 0 < b_dn <= 0.95:
                execute_trade(m_id, timeframe, "Deep Snipe", "DOWN", 10.0, b_dn)
                EXECUTED_STRAT[m_id].append('deep_snipe')

# ==========================================
# GŁÓWNA ORKIESTRACJA (MAIN LOOP)
# ==========================================
async def main():
    global INITIAL_BALANCE, PORTFOLIO_BALANCE, LAST_FLUSH_TS
    
    parser = argparse.ArgumentParser(description="HFT Polymarket Paper Trader (Async/WS)")
    parser.add_argument('--portfolio', type=float, default=100.0, help="Początkowa wartość portfela w USD")
    args = parser.parse_args()
    
    INITIAL_BALANCE = args.portfolio
    PORTFOLIO_BALANCE = args.portfolio
    LAST_FLUSH_TS = (int(time.time()) // 300) * 300
    
    await init_db()
    
    log(f"🚀 Watcher 10.15 (ZOPTYMALIZOWANY QUANT TRADER) online!")
    log(f"💰 Początkowy kapitał: ${PORTFOLIO_BALANCE:.2f}")
    log(f"🔧 Sztywna korekta (Offset): {FIXED_OFFSET:+.2f}")
    log("⌨️  Wpisz 'p' + Enter aby zobaczyć portfel. 'q' + Enter aby zamknąć.")
    
    loop = asyncio.get_event_loop()
    try:
        loop.add_reader(sys.stdin.fileno(), handle_stdin)
    except NotImplementedError:
        log("UWAGA: Uruchomiono w środowisku bez asynchronicznej obsługi klawiatury.")

    await asyncio.gather(
        binance_ws_listener(),         
        polymarket_ws_listener(),      
        fetch_and_track_markets(),     
        async_smart_flush_worker()     
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nPrzerwano przez użytkownika. Zamykam...")
        os._exit(0)