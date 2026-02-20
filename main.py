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
import traceback
from datetime import datetime
import pytz
from playwright.async_api import async_playwright

from dashboard import render_dashboard

# ==========================================
# CONSTANTS & MULTI-COIN CONFIGURATION
# ==========================================
CHECK_INTERVAL = 1
GLOBAL_SEC_RULE = 4.0

DRAWDOWN_LIMIT = 0.30          
MAX_MARKET_EXPOSURE = 0.15     
BURST_LIMIT_TRADES = 5         
BURST_LIMIT_SEC = 10           

SANITY_THRESHOLDS = {
    'BTC': 0.04, 'ETH': 0.05, 'SOL': 0.08, 'XRP': 0.15
}

FULL_NAMES = {
    'BTC': 'bitcoin', 'ETH': 'ethereum', 'SOL': 'solana', 'XRP': 'xrp'
}

# 12 Rynków: 4x 15m, 4x 5m, 4x 1h
TRACKED_CONFIGS = [
    # --- RYNKI 15 MINUTOWE ---
    {"symbol": "XRP", "pair": "XRPUSDT", "timeframe": "15m", "interval": 900, "decimals": 4, "offset": 0.0, "lag_sniper": {"prog_bazowy": 0.0013, "prog_koncowka": 0.0002, "czas_koncowki": 90, "lag_tol": 0.1, "max_cena": 0.98, "wr": 85.7}, "momentum": {"delta": 0.0002, "max_p": 0.77, "win_start": 64, "win_end": 30, "wr": 100.0}, "mid_arb": {"delta": 0.0002, "max_p": 0.53, "win_start": 190, "win_end": 30, "wr": 83.3}, "otm": {"win_start": 50, "win_end": 22, "max_p": 0.06, "wr": 80.0}},
    {"symbol": "BTC", "pair": "BTCUSDT", "timeframe": "15m", "interval": 900, "decimals": 2, "offset": 0.0, "lag_sniper": {"prog_bazowy": 29, "prog_koncowka": 6, "czas_koncowki": 80, "lag_tol": 0.1, "max_cena": 0.96, "wr": 75.9}, "momentum": {"delta": 15, "max_p": 0.83, "win_start": 76, "win_end": 36, "wr": 76.9}, "mid_arb": {"delta": 7, "max_p": 0.57, "win_start": 190, "win_end": 65, "wr": 71.4}, "otm": {"win_start": 70, "win_end": 46, "max_p": 0.03, "wr": 50.0}},
    {"symbol": "ETH", "pair": "ETHUSDT", "timeframe": "15m", "interval": 900, "decimals": 2, "offset": 0.0, "lag_sniper": {"prog_bazowy": 1.6, "prog_koncowka": 0.5, "czas_koncowki": 90, "lag_tol": 0.15, "max_cena": 0.98, "wr": 81.8}, "momentum": {"delta": 0.7, "max_p": 0.85, "win_start": 76, "win_end": 30, "wr": 100.0}, "mid_arb": {"delta": 0.5, "max_p": 0.59, "win_start": 190, "win_end": 90, "wr": 100.0}, "otm": {"win_start": 50, "win_end": 20, "max_p": 0.04, "wr": 38.5}},
    {"symbol": "SOL", "pair": "SOLUSDT", "timeframe": "15m", "interval": 900, "decimals": 3, "offset": 0.0, "lag_sniper": {"prog_bazowy": 0.05, "prog_koncowka": 0.02, "czas_koncowki": 50, "lag_tol": 0.05, "max_cena": 0.98, "wr": 69.2}, "momentum": {"delta": 0.08, "max_p": 0.85, "win_start": 68, "win_end": 46, "wr": 100.0}, "mid_arb": {"delta": 0.01, "max_p": 0.45, "win_start": 120, "win_end": 30, "wr": 50.0}, "otm": {"win_start": 74, "win_end": 50, "max_p": 0.02, "wr": 25.0}},
    
    # --- RYNKI 5 MINUTOWE (Dopasowane delty i progi) ---
    {"symbol": "BTC", "pair": "BTCUSDT", "timeframe": "5m", "interval": 300, "decimals": 2, "offset": 0.0, "lag_sniper": {"prog_bazowy": 16, "prog_koncowka": 8, "czas_koncowki": 90, "lag_tol": 0.15, "max_cena": 0.92, "wr": 68.8}, "momentum": {"delta": 16, "max_p": 0.65, "win_start": 70, "win_end": 50, "wr": 69.4}, "mid_arb": {"delta": 12, "max_p": 0.49, "win_start": 120, "win_end": 30, "wr": 52.5}, "otm": {"win_start": 58, "win_end": 24, "max_p": 0.06, "wr": 15.0}},
    {"symbol": "ETH", "pair": "ETHUSDT", "timeframe": "5m", "interval": 300, "decimals": 2, "offset": 0.0, "lag_sniper": {"prog_bazowy": 1.0, "prog_koncowka": 0.3, "czas_koncowki": 90, "lag_tol": 0.15, "max_cena": 0.92, "wr": 65.0}, "momentum": {"delta": 1.0, "max_p": 0.65, "win_start": 70, "win_end": 50, "wr": 65.0}, "mid_arb": {"delta": 0.6, "max_p": 0.49, "win_start": 120, "win_end": 30, "wr": 50.0}, "otm": {"win_start": 58, "win_end": 24, "max_p": 0.06, "wr": 15.0}},
    {"symbol": "SOL", "pair": "SOLUSDT", "timeframe": "5m", "interval": 300, "decimals": 3, "offset": 0.0, "lag_sniper": {"prog_bazowy": 0.04, "prog_koncowka": 0.015, "czas_koncowki": 90, "lag_tol": 0.05, "max_cena": 0.92, "wr": 62.0}, "momentum": {"delta": 0.05, "max_p": 0.65, "win_start": 70, "win_end": 50, "wr": 65.0}, "mid_arb": {"delta": 0.02, "max_p": 0.49, "win_start": 120, "win_end": 30, "wr": 50.0}, "otm": {"win_start": 58, "win_end": 24, "max_p": 0.06, "wr": 15.0}},
    {"symbol": "XRP", "pair": "XRPUSDT", "timeframe": "5m", "interval": 300, "decimals": 4, "offset": 0.0, "lag_sniper": {"prog_bazowy": 0.0008, "prog_koncowka": 0.0001, "czas_koncowki": 90, "lag_tol": 0.1, "max_cena": 0.92, "wr": 70.0}, "momentum": {"delta": 0.0005, "max_p": 0.65, "win_start": 70, "win_end": 50, "wr": 70.0}, "mid_arb": {"delta": 0.0003, "max_p": 0.49, "win_start": 120, "win_end": 30, "wr": 55.0}, "otm": {"win_start": 58, "win_end": 24, "max_p": 0.06, "wr": 20.0}},
    
    # --- RYNKI 1-GODZINNE ---
    {"symbol": "XRP", "pair": "XRPUSDT", "timeframe": "1h", "interval": 3600, "decimals": 4, "offset": 0.0, "lag_sniper": {"prog_bazowy": 0.0013, "prog_koncowka": 0.0002, "czas_koncowki": 90, "lag_tol": 0.1, "max_cena": 0.98, "wr": 85.7}, "momentum": {"delta": 0.0002, "max_p": 0.77, "win_start": 64, "win_end": 30, "wr": 100.0}, "mid_arb": {"delta": 0.0002, "max_p": 0.53, "win_start": 190, "win_end": 30, "wr": 83.3}, "otm": {"win_start": 50, "win_end": 22, "max_p": 0.06, "wr": 80.0}},
    {"symbol": "BTC", "pair": "BTCUSDT", "timeframe": "1h", "interval": 3600, "decimals": 2, "offset": 0.0, "lag_sniper": {"prog_bazowy": 29, "prog_koncowka": 6, "czas_koncowki": 80, "lag_tol": 0.1, "max_cena": 0.96, "wr": 75.9}, "momentum": {"delta": 15, "max_p": 0.83, "win_start": 76, "win_end": 36, "wr": 76.9}, "mid_arb": {"delta": 7, "max_p": 0.57, "win_start": 190, "win_end": 65, "wr": 71.4}, "otm": {"win_start": 70, "win_end": 46, "max_p": 0.03, "wr": 50.0}},
    {"symbol": "ETH", "pair": "ETHUSDT", "timeframe": "1h", "interval": 3600, "decimals": 2, "offset": 0.0, "lag_sniper": {"prog_bazowy": 1.6, "prog_koncowka": 0.5, "czas_koncowki": 90, "lag_tol": 0.15, "max_cena": 0.98, "wr": 81.8}, "momentum": {"delta": 0.7, "max_p": 0.85, "win_start": 76, "win_end": 30, "wr": 100.0}, "mid_arb": {"delta": 0.5, "max_p": 0.59, "win_start": 190, "win_end": 90, "wr": 100.0}, "otm": {"win_start": 50, "win_end": 20, "max_p": 0.04, "wr": 38.5}},
    {"symbol": "SOL", "pair": "SOLUSDT", "timeframe": "1h", "interval": 3600, "decimals": 3, "offset": 0.0, "lag_sniper": {"prog_bazowy": 0.05, "prog_koncowka": 0.02, "czas_koncowki": 50, "lag_tol": 0.05, "max_cena": 0.98, "wr": 69.2}, "momentum": {"delta": 0.08, "max_p": 0.85, "win_start": 68, "win_end": 46, "wr": 100.0}, "mid_arb": {"delta": 0.01, "max_p": 0.45, "win_start": 120, "win_end": 30, "wr": 50.0}, "otm": {"win_start": 74, "win_end": 50, "max_p": 0.02, "wr": 25.0}}
]

LOCAL_STATE = {
    'binance_live_price': {cfg['pair']: 0.0 for cfg in TRACKED_CONFIGS},
    'prev_price': {cfg['pair']: 0.0 for cfg in TRACKED_CONFIGS},
    'polymarket_books': {}
}

# --- System puli ID transakcji ---
AVAILABLE_TRADE_IDS = list(range(100)) # ID od 0 do 99

LOCKED_PRICES = {}
MARKET_CACHE = {}
PAPER_TRADES = []
TRADE_HISTORY = []
EXECUTED_STRAT = {}
ACTIVE_MARKETS = {}
LIVE_MARKET_DATA = {}
TRADE_TIMESTAMPS = {}

INITIAL_BALANCE = 0.0
PORTFOLIO_BALANCE = 0.0

MARKET_LOGS_BUFFER = []
TRADE_LOGS_BUFFER = []
LAST_FLUSH_TS = 0

RECENT_LOGS = []
ACTIVE_ERRORS = []

WS_SUBSCRIPTION_QUEUE = asyncio.Queue()
VERIFICATION_QUEUE = asyncio.PriorityQueue()

# ==========================================
# 0. SYSTEM LOGOWANIA BŁĘDÓW & HELPERS
# ==========================================
def get_verification_priority(symbol, timeframe):
    # Rynki 5m są absolutnie najważniejsze bez względu na symbol waluty
    if timeframe == '5m': return 1
    if symbol == 'BTC' and timeframe == '15m': return 2
    if timeframe == '15m': return 3
    if timeframe == '1h': return 4
    return 5

def log(msg):
    timestamped = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    RECENT_LOGS.append(timestamped)
    if len(RECENT_LOGS) > 10:
        RECENT_LOGS.pop(0)

def log_error(context_msg, e):
    ts = datetime.now().strftime('%H:%M:%S')
    err_name = type(e).__name__
    err_msg = str(e).replace('\n', ' ')[:80]
    
    ACTIVE_ERRORS.insert(0, f"[{ts}] {context_msg} | {err_name}: {err_msg}")
    if len(ACTIVE_ERRORS) > 3:
        ACTIVE_ERRORS.pop()
        
    try:
        os.makedirs("data", exist_ok=True)
        with open("data/error_dumps.log", "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"CZAS: {datetime.now().isoformat()}\n")
            f.write(f"MODUŁ: {context_msg}\n")
            f.write(f"TYP BŁĘDU: {err_name}\n")
            f.write(f"WIADOMOŚĆ:\n{str(e)}\n\n")
            f.write(f"TRACEBACK:\n{traceback.format_exc()}\n")
            f.write(f"{'='*60}\n")
    except Exception:
        pass

# ==========================================
# 1. DATABASE (ASYNC)
# ==========================================
async def init_db():
    async with aiosqlite.connect('data/polymarket.db') as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS market_logs_v11 (
            id INTEGER PRIMARY KEY AUTOINCREMENT, timeframe TEXT, market_id TEXT,
            target_price REAL, live_price REAL, 
            buy_up REAL, buy_up_vol REAL, sell_up REAL, sell_up_vol REAL, up_obi REAL,
            buy_down REAL, buy_down_vol REAL, sell_down REAL, sell_down_vol REAL, dn_obi REAL, 
            fetched_at TEXT)''')
            
        await db.execute('''CREATE TABLE IF NOT EXISTS trade_logs_v10 (
            trade_id TEXT PRIMARY KEY, market_id TEXT, timeframe TEXT, strategy TEXT, direction TEXT,
            invested REAL, entry_price REAL, entry_time TEXT, exit_price REAL, exit_time TEXT, pnl REAL,
            reason TEXT)''')
        await db.commit()

async def flush_to_db():
    global MARKET_LOGS_BUFFER, TRADE_LOGS_BUFFER
    if not MARKET_LOGS_BUFFER and not TRADE_LOGS_BUFFER: return
    try:
        async with aiosqlite.connect('data/polymarket.db') as db:
            if MARKET_LOGS_BUFFER:
                await db.executemany('''INSERT INTO market_logs_v11
                    (timeframe, market_id, target_price, live_price, buy_up, buy_up_vol, sell_up, sell_up_vol, up_obi, buy_down, buy_down_vol, sell_down, sell_down_vol, dn_obi, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', MARKET_LOGS_BUFFER)
            if TRADE_LOGS_BUFFER:
                await db.executemany('''INSERT INTO trade_logs_v10
                    (trade_id, market_id, timeframe, strategy, direction, invested, entry_price, entry_time, exit_price, exit_time, pnl, reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', TRADE_LOGS_BUFFER)
            await db.commit()
        log(f"💾 BULK SAVE: DB Sync ({len(MARKET_LOGS_BUFFER)} ticks LEVEL 2)")
        MARKET_LOGS_BUFFER.clear()
        TRADE_LOGS_BUFFER.clear()
    except Exception as e:
        log_error("Baza Danych (flush_to_db)", e)

async def async_smart_flush_worker():
    global LAST_FLUSH_TS
    while True:
        try:
            await asyncio.sleep(1)
            now_ts = int(time.time())
            current_5m_block = (now_ts // 300) * 300
            if (now_ts % 300) >= 5 and current_5m_block > LAST_FLUSH_TS:
                await flush_to_db()
                LAST_FLUSH_TS = current_5m_block
        except Exception as e:
            log_error("Smart Flush Worker", e)

# ==========================================
# 2. UI WORKER & EMERGENCY LIQUIDATION
# ==========================================
async def ui_updater_worker():
    while True:
        try:
            total_invested = sum(t['invested'] for t in PAPER_TRADES)
            current_equity = PORTFOLIO_BALANCE + total_invested
            if current_equity <= INITIAL_BALANCE * (1.0 - DRAWDOWN_LIMIT):
                log(f"🛑 CRITICAL: Drawdown Limit osiągnięty! Kapitał spadł poniżej {(1.0 - DRAWDOWN_LIMIT)*100}%")
                asyncio.create_task(liquidate_all_and_quit())

            render_dashboard(
                TRACKED_CONFIGS, LOCAL_STATE, ACTIVE_MARKETS, PAPER_TRADES, 
                LIVE_MARKET_DATA, PORTFOLIO_BALANCE, INITIAL_BALANCE, RECENT_LOGS, ACTIVE_ERRORS, TRADE_HISTORY
            )
        except Exception as e:
            log_error("Dashboard Renderer", e)
        await asyncio.sleep(1)

async def liquidate_all_and_quit():
    log("🚨 AWARYJNA LIKWIDACJA. Sprzedaję pozycje i zatrzymuję system...")
    if PAPER_TRADES:
        for trade in PAPER_TRADES[:]:
            live_bid = LIVE_MARKET_DATA.get(trade['market_id'], {}).get(f"{trade['direction']}_BID", 0.0)
            close_trade(trade, live_bid, "EMERGENCY CLOSE (Liquidation/Drawdown)")
        await flush_to_db()
    render_dashboard(TRACKED_CONFIGS, LOCAL_STATE, ACTIVE_MARKETS, PAPER_TRADES, LIVE_MARKET_DATA, PORTFOLIO_BALANCE, INITIAL_BALANCE, RECENT_LOGS, ACTIVE_ERRORS, TRADE_HISTORY)
    os._exit(1)

def handle_stdin():
    cmd = sys.stdin.readline().strip().lower()
    if cmd == 'q': asyncio.create_task(liquidate_all_and_quit())

# ==========================================
# 3. TRADING LOGIC
# ==========================================
def calculate_dynamic_size(base_stake, win_rate, market_id):
    target_stake = PORTFOLIO_BALANCE * 0.02 * (win_rate / 100.0)
    size_usd = max(base_stake, target_stake) 
    
    market_invested = sum(t['invested'] for t in PAPER_TRADES if t['market_id'] == market_id)
    if market_invested + size_usd > INITIAL_BALANCE * MAX_MARKET_EXPOSURE:
        size_usd = (INITIAL_BALANCE * MAX_MARKET_EXPOSURE) - market_invested
        if size_usd < base_stake:
            return 0.0 
    return size_usd

def execute_trade(market_id, timeframe, strategy, direction, base_stake, price, symbol, win_rate):
    global PORTFOLIO_BALANCE
    
    if not AVAILABLE_TRADE_IDS:
        log_error("ID Pool Exhausted", Exception("Brakuje wolnych identyfikatorów (0-99). Zawieś nowe transakcje."))
        return
    
    now = time.time()
    if market_id not in TRADE_TIMESTAMPS: TRADE_TIMESTAMPS[market_id] = []
    TRADE_TIMESTAMPS[market_id] = [ts for ts in TRADE_TIMESTAMPS[market_id] if now - ts <= BURST_LIMIT_SEC]
    if len(TRADE_TIMESTAMPS[market_id]) >= BURST_LIMIT_TRADES:
        log_error("Burst Guard", Exception(f"Zablokowano wejście na {symbol} - Zbyt duża częstotliwość transakcji."))
        return

    size_usd = calculate_dynamic_size(base_stake, win_rate, market_id)
    if price <= 0 or size_usd < base_stake or size_usd > PORTFOLIO_BALANCE: 
        return
    
    short_id = AVAILABLE_TRADE_IDS.pop(0)
    
    PORTFOLIO_BALANCE -= size_usd
    shares = size_usd / price
    trade = {
        'id': f"{market_id}_{len(PAPER_TRADES)}_{int(time.time())}",
        'short_id': short_id,
        'market_id': market_id, 'timeframe': timeframe, 'symbol': symbol,
        'strategy': strategy, 'direction': direction, 
        'entry_price': price, 'entry_time': datetime.now().isoformat(),
        'shares': shares, 'invested': size_usd
    }
    PAPER_TRADES.append(trade)
    TRADE_TIMESTAMPS[market_id].append(now)
    
    sys.stdout.write('\a')
    sys.stdout.flush()
    log(f"✅ [ID: {short_id:02d}] ZAKUP {symbol} {strategy} ({timeframe}) | {direction} | Wkład: ${size_usd:.2f} | Cena: {price*100:.1f}¢")

def close_trade(trade, close_price, reason):
    global PORTFOLIO_BALANCE
    return_value = close_price * trade['shares']
    pnl = return_value - trade['invested']
    PORTFOLIO_BALANCE += return_value
    TRADE_HISTORY.append({'pnl': pnl, 'reason': reason, **trade})
    
    PAPER_TRADES.remove(trade)
    
    AVAILABLE_TRADE_IDS.append(trade['short_id'])
    AVAILABLE_TRADE_IDS.sort()
    
    icon = "💰" if pnl > 0 else "🩸"
    log(f"{icon} [ID: {trade['short_id']:02d}] SPRZEDAŻ {trade['symbol']} {trade['strategy']} [{trade['timeframe']}] ({trade['direction']}) | {reason} | PnL: ${pnl:+.2f}")
    
    TRADE_LOGS_BUFFER.append((
        trade['id'], trade['market_id'], f"{trade['symbol']}_{trade['timeframe']}",
        trade['strategy'], trade['direction'], trade['invested'], 
        trade['entry_price'], trade['entry_time'], close_price, datetime.now().isoformat(), pnl, reason
    ))

def resolve_market(market_id, final_asset_price, target_price):
    is_up_winner = final_asset_price >= target_price
    for trade in PAPER_TRADES[:]:
        if trade['market_id'] == market_id:
            close_price = 1.0 if (trade['direction'] == 'UP' and is_up_winner) or (trade['direction'] == 'DOWN' and not is_up_winner) else 0.0
            close_trade(trade, close_price, "Rozliczenie Wyroczni")

def extract_orderbook_metrics(token_id):
    book = LOCAL_STATE['polymarket_books'].get(token_id, {'bids': {}, 'asks': {}})
    valid_bids = {p: s for p, s in book['bids'].items() if s > 0 and 0.005 < p < 0.995}
    valid_asks = {p: s for p, s in book['asks'].items() if s > 0 and 0.005 < p < 0.995}
    
    best_bid = max(valid_bids.keys()) if valid_bids else 0.0
    best_bid_vol = valid_bids.get(best_bid, 0.0) if best_bid else 0.0
    
    best_ask = min(valid_asks.keys()) if valid_asks else 0.0
    best_ask_vol = valid_asks.get(best_ask, 0.0) if best_ask else 0.0
    
    top_bids = sorted(valid_bids.keys(), reverse=True)[:5]
    bid_vol_sum = sum(valid_bids[p] for p in top_bids)
    
    top_asks = sorted(valid_asks.keys())[:5]
    ask_vol_sum = sum(valid_asks[p] for p in top_asks)
    
    obi = 0.0
    if bid_vol_sum + ask_vol_sum > 0:
        obi = (bid_vol_sum - ask_vol_sum) / (bid_vol_sum + ask_vol_sum)
        
    return best_ask, best_ask_vol, best_bid, best_bid_vol, obi

# ==========================================
# 4. WEBSOCKETS (BINANCE & CLOB)
# ==========================================
async def binance_ws_listener():
    streams = list(set([cfg['pair'].lower() + "@ticker" for cfg in TRACKED_CONFIGS]))
    stream_url = "/".join(streams)
    url = f"wss://stream.binance.com:9443/stream?streams={stream_url}"
    
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log(f"[WS] Podłączono do Binance (Tokens: {len(streams)})")
                async for msg in ws:
                    data = json.loads(msg)
                    if 'data' in data and 's' in data['data']:
                        pair = data['data']['s']
                        new_price = float(data['data']['c'])
                        
                        if new_price != LOCAL_STATE['binance_live_price'].get(pair, 0.0):
                            LOCAL_STATE['prev_price'][pair] = LOCAL_STATE['binance_live_price'].get(pair, 0.0)
                            LOCAL_STATE['binance_live_price'][pair] = new_price
                            
                        await evaluate_strategies("BINANCE_TICK", pair_filter=pair)
        except Exception as e:
            log_error("Binance WebSocket", e)
            await asyncio.sleep(2)

async def polymarket_ws_listener():
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    subscribed_tokens = set()
    
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log("[WS] Podłączono do Polymarket CLOB")
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
                                log(f"🔄 Restart strumienia L2 (Nowy rynek)...")
                                await websocket.close()
                                break
                    except asyncio.CancelledError: pass
                    
                queue_task = asyncio.create_task(process_queue(ws))
                
                try:
                    async for msg in ws:
                        if not msg.startswith(('{', '[')): continue
                        try: parsed_msg = json.loads(msg)
                        except: continue
                        
                        if not isinstance(parsed_msg, list): parsed_msg = [parsed_msg]
                        
                        for data in parsed_msg:
                            event_type = data.get('event_type', '')
                            
                            if event_type == 'book' or 'bids' in data or 'asks' in data:
                                t_id = data.get('asset_id')
                                if t_id:
                                    LOCAL_STATE['polymarket_books'][t_id] = {'bids': {}, 'asks': {}}
                                    for bid in data.get('bids', []): LOCAL_STATE['polymarket_books'][t_id]['bids'][float(bid['price'])] = float(bid['size'])
                                    for ask in data.get('asks', []): LOCAL_STATE['polymarket_books'][t_id]['asks'][float(ask['price'])] = float(ask['size'])
                                    
                            if event_type == 'price_change' or 'price_changes' in data:
                                for change in data.get('price_changes', []):
                                    t_id = change.get('asset_id')
                                    if not t_id: continue
                                    if t_id not in LOCAL_STATE['polymarket_books']: LOCAL_STATE['polymarket_books'][t_id] = {'bids': {}, 'asks': {}}
                                    price = float(change.get('price'))
                                    size = float(change.get('size', 0))
                                    side = 'bids' if change.get('side', '').upper() == 'BUY' else 'asks'
                                    LOCAL_STATE['polymarket_books'][t_id][side][price] = size
                        await evaluate_strategies("CLOB_TICK")
                finally: queue_task.cancel()
        except Exception as e:
            log_error("Polymarket CLOB", e)
            await asyncio.sleep(0.1)

# ==========================================
# 5. MARKET STATE MANAGER & DYNAMIC SLUG ROUTER
# ==========================================
async def playwright_verification_worker():
    log("🕵️ Playwright Worker online. (Oczekuje na priorytetyzację...)")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
            while True:
                priority, req_ts, slug, m_id = await VERIFICATION_QUEUE.get()
                try:
                    url = f"https://polymarket.com/event/{slug}"
                    context = await browser.new_context(user_agent="Mozilla/5.0")
                    page = await context.new_page()
                    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    await asyncio.sleep(4)
                    content = await page.content()
                    await context.close()
                    
                    match = re.search(r'Price to beat.*?\$(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', content, re.IGNORECASE | re.DOTALL)
                    if match:
                        v = float(match.group(1).replace(',', ''))
                        if m_id in LOCKED_PRICES:
                            LOCKED_PRICES[m_id]['price'] = v
                            LOCKED_PRICES[m_id]['verified'] = True
                            log(f"👁️ [VERIFIED] Pobrana baza: ${v} dla {slug} (Priorytet: P{priority})")
                except Exception as e:
                    log_error(f"Playwright Worker ({slug})", e)
                finally:
                    VERIFICATION_QUEUE.task_done()
                    await asyncio.sleep(3)
    except Exception as e:
        log_error("Fatal Playwright Crash", e)

async def fetch_and_track_markets():
    tz_et = pytz.timezone('America/New_York')
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                now_et = datetime.now(tz_et)
                for config in TRACKED_CONFIGS:
                    pair = config['pair']
                    live_p = LOCAL_STATE['binance_live_price'].get(pair, 0.0)
                    if live_p == 0.0: continue
                    
                    interval_s = config['interval']
                    base_ts = (int(now_et.timestamp()) // interval_s) * interval_s
                    start_time = datetime.fromtimestamp(base_ts, tz_et)
                    
                    sec_since_start = (now_et - start_time).total_seconds()
                    sec_left = interval_s - sec_since_start
                    
                    slug_standard = f"{config['symbol'].lower()}-updown-{config['timeframe']}-{base_ts}"
                    
                    month_name = start_time.strftime('%B').lower()
                    day = start_time.strftime('%-d')
                    hour_str = start_time.strftime('%-I%p').lower()
                    coin_name = FULL_NAMES.get(config['symbol'], config['symbol'].lower())
                    slug_seo = f"{coin_name}-up-or-down-{month_name}-{day}-{hour_str}-et"
                    
                    candidate_slugs = [slug_standard, slug_seo]
                    active_slug = None
                    
                    for candidate in candidate_slugs:
                        if candidate in MARKET_CACHE:
                            active_slug = candidate
                            break
                            
                        async with session.get(f"https://gamma-api.polymarket.com/events?slug={candidate}") as resp:
                            if resp.status == 200:
                                res = await resp.json()
                                if res and 'markets' in res[0]:
                                    for m in res[0]['markets']:
                                        q_text = m.get('question', '').lower().replace(' ', '')
                                        time_str = start_time.strftime('%-I:%M%p').lower()
                                        time_str_alt = start_time.strftime('%-I%p').lower()
                                        
                                        if m.get('active') and (time_str in q_text or time_str_alt in q_text):
                                            outcomes = eval(m.get('outcomes', '[]'))
                                            clob_ids = eval(m.get('clobTokenIds', '[]'))
                                            MARKET_CACHE[candidate] = {'id': str(m.get('id')), 'up_id': clob_ids[outcomes.index("Up")], 'dn_id': clob_ids[outcomes.index("Down")], 'config': config}
                                            await WS_SUBSCRIPTION_QUEUE.put([clob_ids[0], clob_ids[1]])
                                            active_slug = candidate
                                            break
                        if active_slug:
                            break
                                            
                    if not active_slug: continue
                    slug = active_slug
                    
                    m_id = MARKET_CACHE[slug]['id']
                    timeframe_key = f"{config['symbol']}_{config['timeframe']}"
                    
                    if m_id not in LOCKED_PRICES:
                        LOCKED_PRICES[m_id] = {'price': live_p, 'verified': False, 'last_retry': 0, 'prev_up': 0, 'prev_dn': 0}
                    
                    m_data = LOCKED_PRICES[m_id]
                    
                    if timeframe_key not in ACTIVE_MARKETS:
                        ACTIVE_MARKETS[timeframe_key] = {'m_id': m_id, 'target': m_data['price']}
                    elif ACTIVE_MARKETS[timeframe_key]['m_id'] != m_id:
                        old_m_id = ACTIVE_MARKETS[timeframe_key]['m_id']
                        old_target = ACTIVE_MARKETS[timeframe_key]['target']
                        adjusted_final_price = live_p + config['offset']
                        log(f"🔔 RYNEK ZAMKNIĘTY [{timeframe_key}]. Rozliczanie...")
                        resolve_market(old_m_id, adjusted_final_price, old_target)
                        ACTIVE_MARKETS[timeframe_key] = {'m_id': m_id, 'target': m_data['price']}
                        LOCAL_STATE.pop(f'timing_{old_m_id}', None)
                        LIVE_MARKET_DATA.pop(old_m_id, None)
                        LOCKED_PRICES.pop(old_m_id, None)
                    else:
                        ACTIVE_MARKETS[timeframe_key]['target'] = m_data['price']
                        
                    if sec_left >= interval_s - 2: continue
                    
                    if not m_data['verified'] and sec_since_start >= 30:
                        if (time.time() - m_data['last_retry']) > 60:
                            m_data['last_retry'] = time.time()
                            priority_level = get_verification_priority(config['symbol'], config['timeframe'])
                            VERIFICATION_QUEUE.put_nowait((priority_level, time.time(), slug, m_id))
                            
                    LOCAL_STATE[f'timing_{m_id}'] = {
                        'sec_left': sec_left, 'sec_since_start': sec_since_start,
                        'interval_s': interval_s, 'timeframe': config['timeframe'],
                        'symbol': config['symbol'], 'pair': pair,
                        'm_data': m_data, 'config': config
                    }
                    
                    up_id = MARKET_CACHE[slug]['up_id']
                    dn_id = MARKET_CACHE[slug]['dn_id']
                    s_up, s_up_vol, b_up, b_up_vol, up_obi = extract_orderbook_metrics(up_id)
                    s_dn, s_dn_vol, b_dn, b_dn_vol, dn_obi = extract_orderbook_metrics(dn_id)
                    
                    LIVE_MARKET_DATA[m_id] = {'UP_BID': s_up, 'DOWN_BID': s_dn}
                    adjusted_live_p = live_p + config['offset']
                    
                    MARKET_LOGS_BUFFER.append((
                        f"{config['symbol']}_{config['timeframe']}", m_id, m_data['price'],
                        adjusted_live_p,
                        b_up, b_up_vol, s_up, s_up_vol, up_obi,
                        b_dn, b_dn_vol, s_dn, s_dn_vol, dn_obi,
                        datetime.now().isoformat()
                    ))
            except Exception as e:
                log_error("Market State Manager", e)
            await asyncio.sleep(CHECK_INTERVAL)

# ==========================================
# 6. STRATEGY ENGINE
# ==========================================
async def evaluate_strategies(trigger_source, pair_filter=None):
    try:
        for slug, cache in MARKET_CACHE.items():
            m_id = cache['id']
            if f'timing_{m_id}' not in LOCAL_STATE: continue
            timing = LOCAL_STATE[f'timing_{m_id}']
            pair = timing['pair']
            
            if trigger_source == "BINANCE_TICK" and pair_filter and pair != pair_filter: continue
            
            if m_id not in EXECUTED_STRAT: EXECUTED_STRAT[m_id] = []
            
            config = timing['config']
            symbol = timing['symbol']
            timeframe = timing['timeframe']
            sec_left = timing['sec_left']
            m_data = timing['m_data']
            
            is_verified = m_data.get('verified', False)
            live_p = LOCAL_STATE['binance_live_price'].get(pair, 0.0)
            prev_p = LOCAL_STATE['prev_price'].get(pair, 0.0)
            
            if live_p == 0.0: continue
            
            adjusted_live_p = live_p + config['offset']
            adj_delta = adjusted_live_p - m_data['price']
            
            sanity_limit = SANITY_THRESHOLDS.get(symbol, 0.05)
            if m_data['price'] > 0 and abs(adj_delta) / m_data['price'] > sanity_limit:
                continue 
            
            s_up, _, b_up, _, _ = extract_orderbook_metrics(cache['up_id'])
            s_dn, _, b_dn, _, _ = extract_orderbook_metrics(cache['dn_id'])
            
            for trade in PAPER_TRADES[:]:
                if trade['market_id'] != m_id: continue
                current_bid = s_up if trade['direction'] == 'UP' else s_dn
                entry_p = trade['entry_price']
                cashout_sec = 2.0 if trade['strategy'] == "1-Min Mom" else GLOBAL_SEC_RULE
                
                if 0 < sec_left <= cashout_sec:
                    if current_bid > entry_p:
                        close_trade(trade, current_bid, f"Zabezpieczenie Zysków przed końcem ({cashout_sec}s)")
                    continue
                    
            if is_verified:
                m_cfg = config.get('mid_arb', {})
                mid_arb_flag = f"mid_arb_{m_id}"
                if m_cfg and m_cfg['win_end'] < sec_left < m_cfg['win_start'] and mid_arb_flag not in EXECUTED_STRAT[m_id]:
                    if adj_delta > m_cfg['delta'] and 0 < b_up <= m_cfg['max_p']:
                        execute_trade(m_id, timeframe, "Mid-Game Arb", "UP", 2.0, b_up, symbol, m_cfg.get('wr', 50.0))
                        EXECUTED_STRAT[m_id].append(mid_arb_flag)
                    elif adj_delta < -m_cfg['delta'] and 0 < b_dn <= m_cfg['max_p']:
                        execute_trade(m_id, timeframe, "Mid-Game Arb", "DOWN", 2.0, b_dn, symbol, m_cfg.get('wr', 50.0))
                        EXECUTED_STRAT[m_id].append(mid_arb_flag)
                        
                otm_cfg = config.get('otm', {})
                otm_flag = f"otm_{m_id}"
                if otm_cfg and otm_cfg['win_end'] <= sec_left <= otm_cfg['win_start'] and otm_flag not in EXECUTED_STRAT[m_id]:
                    if abs(adj_delta) < 40.0:
                        if 0 < b_up <= otm_cfg['max_p']:
                            execute_trade(m_id, timeframe, "OTM Bargain", "UP", 1.0, b_up, symbol, otm_cfg.get('wr', 50.0))
                            EXECUTED_STRAT[m_id].append(otm_flag)
                        elif 0 < b_dn <= otm_cfg['max_p']:
                            execute_trade(m_id, timeframe, "OTM Bargain", "DOWN", 1.0, b_dn, symbol, otm_cfg.get('wr', 50.0))
                            EXECUTED_STRAT[m_id].append(otm_flag)
                            
                mom_cfg = config.get('momentum', {})
                if mom_cfg and mom_cfg['win_end'] <= sec_left <= mom_cfg['win_start'] and 'momentum' not in EXECUTED_STRAT[m_id]:
                    if adj_delta >= mom_cfg['delta'] and 0 < b_up <= mom_cfg['max_p']:
                        execute_trade(m_id, timeframe, "1-Min Mom", "UP", 1.0, b_up, symbol, mom_cfg.get('wr', 50.0))
                        EXECUTED_STRAT[m_id].append('momentum')
                    elif adj_delta <= -mom_cfg['delta'] and 0 < b_dn <= mom_cfg['max_p']:
                        execute_trade(m_id, timeframe, "1-Min Mom", "DOWN", 1.0, b_dn, symbol, mom_cfg.get('wr', 50.0))
                        EXECUTED_STRAT[m_id].append('momentum')
                        
            if trigger_source == "BINANCE_TICK" and is_verified:
                asset_jump = live_p - prev_p 
                up_change = b_up - m_data['prev_up']
                dn_change = b_dn - m_data['prev_dn']
                
                sniper_cfg = config.get('lag_sniper', {})
                if sniper_cfg:
                    prog = sniper_cfg['prog_koncowka'] if sec_left <= sniper_cfg['czas_koncowki'] else sniper_cfg['prog_bazowy']
                    if 10 < sec_left < timing['interval_s'] - 5:
                        if asset_jump >= prog and abs(up_change) <= sniper_cfg['lag_tol'] and 0 < b_up <= sniper_cfg['max_cena']:
                            execute_trade(m_id, timeframe, "Lag Sniper", "UP", 2.0, b_up, symbol, sniper_cfg.get('wr', 50.0))
                        elif asset_jump <= -prog and abs(dn_change) <= sniper_cfg['lag_tol'] and 0 < b_dn <= sniper_cfg['max_cena']:
                            execute_trade(m_id, timeframe, "Lag Sniper", "DOWN", 2.0, b_dn, symbol, sniper_cfg.get('wr', 50.0))
                            
                m_data['prev_up'], m_data['prev_dn'] = b_up, b_dn
    except Exception as e:
        log_error("Strategy Engine", e)

# ==========================================
# MAIN ORCHESTRATION LOOP
# ==========================================
async def main():
    global INITIAL_BALANCE, PORTFOLIO_BALANCE, LAST_FLUSH_TS
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--portfolio', type=float, default=100.0)
    args = parser.parse_args()
    
    INITIAL_BALANCE = args.portfolio
    PORTFOLIO_BALANCE = args.portfolio
    LAST_FLUSH_TS = (int(time.time()) // 300) * 300
    
    await init_db()
    
    loop = asyncio.get_event_loop()
    try:
        loop.add_reader(sys.stdin.fileno(), handle_stdin)
    except NotImplementedError:
        pass
        
    await asyncio.gather(
        binance_ws_listener(),
        polymarket_ws_listener(),
        fetch_and_track_markets(),
        playwright_verification_worker(),
        ui_updater_worker(),
        async_smart_flush_worker()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        os._exit(0)