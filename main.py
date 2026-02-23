import asyncio
import websockets
import aiohttp
import aiosqlite
import json
import time
import sys
import os
import argparse
import traceback
import uuid
import string
from datetime import datetime, timedelta
import pytz

from py_clob_client.client import ClobClient
from dashboard import render_dashboard

# ==========================================
# CONSTANTS & MULTI-COIN CONFIGURATION
# ==========================================
CHECK_INTERVAL = 1

# --- RISK & MONEY MANAGEMENT ---
DRAWDOWN_LIMIT = 0.30          
MAX_MARKET_EXPOSURE = 0.15     
BURST_LIMIT_TRADES = 5         
BURST_LIMIT_SEC = 10           

# --- NEW MICRO-PROTECTION CONSTANTS ---
PROFIT_SECURE_SEC = 3.0        # Secure profit if <= 3 seconds left
TAKE_PROFIT_MULTIPLIER = 3.0   # 3.0x entry price = 200% PnL Profit
LAG_SNIPER_SL_DROP = 0.90      # -10% drop activates countdown
LAG_SNIPER_TIMEOUT = 10.0      # 10 seconds to recover or exit

SANITY_THRESHOLDS = {
    'BTC': 0.04, 'ETH': 0.05, 'SOL': 0.08, 'XRP': 0.15
}

FULL_NAMES = {
    'BTC': 'bitcoin', 'ETH': 'ethereum', 'SOL': 'solana', 'XRP': 'xrp'
}

SESSION_ID = f"sess_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# ==========================================
# LOAD TRACKED CONFIGS FROM JSON
# ==========================================
CONFIG_FILE = 'tracked_configs.json'
try:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        TRACKED_CONFIGS = json.load(f)
except Exception as e:
    print(f"❌ Critical Error: Unable to load {CONFIG_FILE}. Ensure the file exists.\nDetails: {e}")
    sys.exit(1)

# Dynamically assign UI keys (a, b, c...) to markets for manual controls
MARKET_KEYS = list(string.ascii_lowercase)
for idx, cfg in enumerate(TRACKED_CONFIGS):
    cfg['ui_key'] = MARKET_KEYS[idx] if idx < len(MARKET_KEYS) else str(idx)

# ==========================================
# LOCAL STATE INITIALIZATION
# ==========================================
LOCAL_STATE = {
    'binance_live_price': {cfg['pair']: 0.0 for cfg in TRACKED_CONFIGS},
    'prev_price': {cfg['pair']: 0.0 for cfg in TRACKED_CONFIGS},
    'polymarket_books': {},
    'session_id': SESSION_ID,
    'paused_markets': set()
}

AVAILABLE_TRADE_IDS = list(range(100))

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

# Initialize professional CLOB client
clob_client = ClobClient("https://clob.polymarket.com")

# ==========================================
# 0. SYSTEM LOGGING & HELPERS
# ==========================================
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
            f.write(f"TIME: {datetime.now().isoformat()}\n")
            f.write(f"MODULE: {context_msg}\n")
            f.write(f"ERROR TYPE: {err_name}\n")
            f.write(f"MESSAGE:\n{str(e)}\n\n")
            f.write(f"TRACEBACK:\n{traceback.format_exc()}\n")
            f.write(f"{'='*60}\n")
    except Exception:
        pass

def perform_tech_dump():
    """Dumps full memory state to a file for later analysis."""
    try:
        os.makedirs("data", exist_ok=True)
        filename = f"data/tech_dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # We convert sets to lists so JSON can serialize them
        dump_data = {
            "timestamp": datetime.now().isoformat(),
            "session_id": SESSION_ID,
            "portfolio_balance": PORTFOLIO_BALANCE,
            "paused_markets": list(LOCAL_STATE['paused_markets']),
            "locked_prices": LOCKED_PRICES,
            "market_cache": MARKET_CACHE,
            "active_markets": ACTIVE_MARKETS,
            "live_market_data": LIVE_MARKET_DATA,
            "paper_trades": PAPER_TRADES,
            "binance_live": LOCAL_STATE['binance_live_price']
        }
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(dump_data, f, indent=4)
            
        sys.stdout.write('\a') 
        sys.stdout.flush()
        log(f"\033[1m\033[32m💾 PANIC DUMP SUCCESS: Memory state saved to {filename}\033[0m")
    except Exception as e:
        log_error("Tech Dump Error", e)

# ==========================================
# 0.1 MANUAL CONTROLS HELPERS
# ==========================================
def get_market_tf_key_by_ui(ui_key):
    for cfg in TRACKED_CONFIGS:
        if cfg['ui_key'] == ui_key:
            return f"{cfg['symbol']}_{cfg['timeframe']}"
    return None

def close_manual_trade(short_id):
    for trade in PAPER_TRADES[:]:
        if trade['short_id'] == short_id:
            live_bid = LIVE_MARKET_DATA.get(trade['market_id'], {}).get(f"{trade['direction']}_BID", 0.0)
            close_trade(trade, live_bid, "MANUAL OVERRIDE CLOSE")
            log(f"⚡ [MANUAL] Option ID {short_id:02d} explicitly closed.")
            return
    log(f"⚠️ [MANUAL] Open Option ID {short_id:02d} not found.")

def close_market_trades(ui_key, reason="MANUAL MARKET CLOSE"):
    tf_key = get_market_tf_key_by_ui(ui_key)
    if not tf_key:
        log(f"⚠️ [MANUAL] Invalid market key '{ui_key}'.")
        return
        
    closed_count = 0
    for trade in PAPER_TRADES[:]:
        if f"{trade['symbol']}_{trade['timeframe']}" == tf_key:
            live_bid = LIVE_MARKET_DATA.get(trade['market_id'], {}).get(f"{trade['direction']}_BID", 0.0)
            close_trade(trade, live_bid, reason)
            closed_count += 1
            
    if closed_count > 0:
        log(f"⚡ [MANUAL] Dumped {closed_count} positions for market [{ui_key}] {tf_key}.")
    else:
        log(f"ℹ️ [MANUAL] No open positions found for market [{ui_key}] {tf_key}.")

def stop_market(ui_key):
    tf_key = get_market_tf_key_by_ui(ui_key)
    if tf_key:
        LOCAL_STATE['paused_markets'].add(tf_key)
        close_market_trades(ui_key, reason="MARKET STOP (EMERGENCY LIQUIDATION)")
        log(f"🛑 [MANUAL] Market [{ui_key}] {tf_key} operations PAUSED.")

def restart_market(ui_key):
    tf_key = get_market_tf_key_by_ui(ui_key)
    if tf_key and tf_key in LOCAL_STATE['paused_markets']:
        LOCAL_STATE['paused_markets'].remove(tf_key)
        log(f"▶️ [MANUAL] Market [{ui_key}] {tf_key} RESUMED.")

def handle_stdin():
    cmd = sys.stdin.readline().strip().lower().replace(" ", "")
    
    if cmd == 'q': 
        asyncio.create_task(liquidate_all_and_quit())
    elif cmd == 'd':
        perform_tech_dump()
    elif cmd.startswith('o') and cmd[1:].isdigit():
        close_manual_trade(int(cmd[1:]))
    elif cmd.startswith('ms') and len(cmd) == 3:
        stop_market(cmd[2])
    elif cmd.startswith('mr') and len(cmd) == 3:
        restart_market(cmd[2])
    elif cmd.startswith('m') and len(cmd) == 2:
        close_market_trades(cmd[1])

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
            fetched_at TEXT, session_id TEXT)''')
        try:
            await db.execute("ALTER TABLE market_logs_v11 ADD COLUMN session_id TEXT")
        except Exception: pass
        
        await db.execute('''CREATE TABLE IF NOT EXISTS trade_logs_v10 (
            trade_id TEXT PRIMARY KEY, market_id TEXT, timeframe TEXT, strategy TEXT, direction TEXT,
            invested REAL, entry_price REAL, entry_time TEXT, exit_price REAL, exit_time TEXT, pnl REAL,
            reason TEXT, session_id TEXT)''')
        try:
            await db.execute("ALTER TABLE trade_logs_v10 ADD COLUMN session_id TEXT")
        except Exception: pass
        await db.commit()

async def flush_to_db():
    global MARKET_LOGS_BUFFER, TRADE_LOGS_BUFFER
    if not MARKET_LOGS_BUFFER and not TRADE_LOGS_BUFFER: return
    try:
        async with aiosqlite.connect('data/polymarket.db') as db:
            if MARKET_LOGS_BUFFER:
                await db.executemany('''INSERT INTO market_logs_v11
                    (timeframe, market_id, target_price, live_price, buy_up, buy_up_vol, sell_up, sell_up_vol, up_obi, buy_down, buy_down_vol, sell_down, sell_down_vol, dn_obi, fetched_at, session_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', MARKET_LOGS_BUFFER)
            if TRADE_LOGS_BUFFER:
                await db.executemany('''INSERT INTO trade_logs_v10
                    (trade_id, market_id, timeframe, strategy, direction, invested, entry_price, entry_time, exit_price, exit_time, pnl, reason, session_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', TRADE_LOGS_BUFFER)
            await db.commit()
        log(f"💾 BULK SAVE: DB Sync ({len(MARKET_LOGS_BUFFER)} Level 2 ticks)")
        MARKET_LOGS_BUFFER.clear()
        TRADE_LOGS_BUFFER.clear()
    except Exception as e:
        log_error("Database (flush_to_db)", e)

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
                log(f"🛑 CRITICAL: Drawdown Limit reached! Capital dropped below {(1.0 - DRAWDOWN_LIMIT)*100}%")
                asyncio.create_task(liquidate_all_and_quit())
            render_dashboard(
                TRACKED_CONFIGS, LOCAL_STATE, ACTIVE_MARKETS, PAPER_TRADES, 
                LIVE_MARKET_DATA, PORTFOLIO_BALANCE, INITIAL_BALANCE, RECENT_LOGS, ACTIVE_ERRORS, TRADE_HISTORY
            )
        except Exception as e:
            log_error("Dashboard Renderer", e)
        await asyncio.sleep(1)

async def liquidate_all_and_quit():
    log("🚨 EMERGENCY LIQUIDATION. Selling positions and stopping system...")
    perform_tech_dump() 
    if PAPER_TRADES:
        for trade in PAPER_TRADES[:]:
            live_bid = LIVE_MARKET_DATA.get(trade['market_id'], {}).get(f"{trade['direction']}_BID", 0.0)
            close_trade(trade, live_bid, "EMERGENCY CLOSE (Liquidation/Drawdown)")
        await flush_to_db()
    render_dashboard(TRACKED_CONFIGS, LOCAL_STATE, ACTIVE_MARKETS, PAPER_TRADES, LIVE_MARKET_DATA, PORTFOLIO_BALANCE, INITIAL_BALANCE, RECENT_LOGS, ACTIVE_ERRORS, TRADE_HISTORY)
    os._exit(1)

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

def execute_trade(market_id, timeframe, strategy, direction, base_stake, price, symbol, win_rate, strat_id=""):
    global PORTFOLIO_BALANCE
    if not AVAILABLE_TRADE_IDS:
        log_error("ID Pool Exhausted", Exception("No free IDs available."))
        return
    now = time.time()
    if market_id not in TRADE_TIMESTAMPS: TRADE_TIMESTAMPS[market_id] = []
    TRADE_TIMESTAMPS[market_id] = [ts for ts in TRADE_TIMESTAMPS[market_id] if now - ts <= BURST_LIMIT_SEC]
    if len(TRADE_TIMESTAMPS[market_id]) >= BURST_LIMIT_TRADES:
        return
    size_usd = calculate_dynamic_size(base_stake, win_rate, market_id)
    if price <= 0 or size_usd < base_stake or size_usd > PORTFOLIO_BALANCE: 
        return
    short_id = AVAILABLE_TRADE_IDS.pop(0)
    PORTFOLIO_BALANCE -= size_usd
    shares = size_usd / price
    unique_suffix = uuid.uuid4().hex[:8]
    trade_id = f"{market_id}_{len(PAPER_TRADES)}_{int(time.time() * 1000)}_{unique_suffix}"
    trade = {
        'id': trade_id, 'short_id': short_id, 'strat_id': strat_id,
        'market_id': market_id, 'timeframe': timeframe, 'symbol': symbol,
        'strategy': strategy, 'direction': direction, 
        'entry_price': price, 'entry_time': datetime.now().isoformat(),
        'shares': shares, 'invested': size_usd
    }
    PAPER_TRADES.append(trade)
    TRADE_TIMESTAMPS[market_id].append(now)
    sys.stdout.write('\a'); sys.stdout.flush()
    log(f"✅ [ID: {short_id:02d}] BUY {symbol} {strategy} ({timeframe}) | {direction} | Invested: ${size_usd:.2f} | Price: {price*100:.1f}¢")

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
    log(f"{icon} [ID: {trade['short_id']:02d}] SELL {trade['symbol']} {trade['strategy']} [{trade['timeframe']}] ({trade['direction']}) | {reason} | PnL: ${pnl:+.2f}")
    TRADE_LOGS_BUFFER.append((
        trade['id'], trade['market_id'], f"{trade['symbol']}_{trade['timeframe']}",
        trade['strategy'], trade['direction'], trade['invested'], 
        trade['entry_price'], trade['entry_time'], close_price, datetime.now().isoformat(), pnl, reason, SESSION_ID
    ))

def resolve_market(market_id, final_asset_price, target_price):
    is_up_winner = final_asset_price >= target_price
    for trade in PAPER_TRADES[:]:
        if trade['market_id'] == market_id:
            close_price = 1.0 if (trade['direction'] == 'UP' and is_up_winner) or (trade['direction'] == 'DOWN' and not is_up_winner) else 0.0
            close_trade(trade, close_price, "Oracle Settlement")

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
                log(f"[WS] Connected to Binance")
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
                log("[WS] Connected to Polymarket CLOB")
                if subscribed_tokens:
                    await ws.send(json.dumps({
                        "assets_ids": list(subscribed_tokens), 
                        "type": "market",
                        "custom_feature_enabled": True
                    }))
                async def process_queue(websocket):
                    try:
                        while True:
                            new_tokens = await WS_SUBSCRIPTION_QUEUE.get()
                            while not WS_SUBSCRIPTION_QUEUE.empty():
                                new_tokens.extend(WS_SUBSCRIPTION_QUEUE.get_nowait())
                            tokens_to_add = list(set([t for t in new_tokens if t not in subscribed_tokens]))
                            if tokens_to_add:
                                subscribed_tokens.update(tokens_to_add)
                                log(f"🔄 Restarting L2 stream (Added {len(tokens_to_add)} new tokens)...")
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
            if "no close frame received or sent" not in str(e):
                log_error("Polymarket CLOB", e)
            await asyncio.sleep(0.1)

# ==========================================
# 5. MARKET STATE MANAGER (LOCAL ORACLE)
# ==========================================
async def fetch_and_track_markets():
    tz_et = pytz.timezone('America/New_York')
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                now_et = datetime.now(tz_et)
                now_ts = int(now_et.timestamp())
                
                for config in TRACKED_CONFIGS:
                    pair = config['pair']
                    live_p = LOCAL_STATE['binance_live_price'].get(pair, 0.0)
                    if live_p == 0.0: continue
                    
                    interval_s = config['interval']
                    current_base_ts = (now_ts // interval_s) * interval_s
                    sec_since_start = now_ts - current_base_ts
                    sec_left = interval_s - sec_since_start
                    
                    if sec_left <= 15:
                        target_base_ts = current_base_ts + interval_s
                        is_pre_warming = True
                    else:
                        target_base_ts = current_base_ts
                        is_pre_warming = False
                        
                    start_time = datetime.fromtimestamp(target_base_ts, tz_et)
                    
                    slug_standard = f"{config['symbol'].lower()}-updown-{config['timeframe']}-{target_base_ts}"
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
                                if res and len(res) > 0 and 'markets' in res[0]:
                                    for m in res[0]['markets']:
                                        q_text = m.get('question', '').lower().replace(' ', '')
                                        time_str = start_time.strftime('%-I:%M%p').lower()
                                        time_str_alt = start_time.strftime('%-I%p').lower()
                                        
                                        if m.get('active') and (time_str in q_text or time_str_alt in q_text):
                                            outcomes = eval(m.get('outcomes', '[]'))
                                            clob_ids = eval(m.get('clobTokenIds', '[]'))
                                            
                                            MARKET_CACHE[candidate] = {
                                                'id': str(m.get('id')), 
                                                'up_id': clob_ids[outcomes.index("Up")], 
                                                'dn_id': clob_ids[outcomes.index("Down")], 
                                                'config': config
                                            }
                                            
                                            await WS_SUBSCRIPTION_QUEUE.put([clob_ids[0], clob_ids[1]])
                                            active_slug = candidate
                                            if is_pre_warming:
                                                log(f"🔥 [PRE-WARMING] Subscribed to market {config['symbol']} {config['timeframe']} before its opening!")
                                            break
                        if active_slug:
                            break
                                            
                    if not active_slug: continue
                    slug = active_slug
                    m_id = MARKET_CACHE[slug]['id']
                    
                    if m_id not in LOCKED_PRICES:
                        is_clean_start = is_pre_warming or (sec_since_start <= 15)
                        LOCKED_PRICES[m_id] = {
                            'price': live_p, 
                            'base_fetched': False, 
                            'last_retry': 0, 'prev_up': 0, 'prev_dn': 0,
                            'is_clean': is_clean_start
                        }
                    
                    m_data = LOCKED_PRICES[m_id]

                    if not m_data['base_fetched'] and not is_pre_warming:
                        m_data['price'] = live_p
                        m_data['base_fetched'] = True
                        if m_data.get('is_clean', False):
                            log(f"⚡ [LOCAL ORACLE] Base definitively frozen at start: ${m_data['price']} for {slug}")
                        else:
                            log(f"⚠️ [LOCAL ORACLE] Mid-interval join. Base set to ${m_data['price']}. Trading paused (except OTM) for {slug}")

                    timeframe_key = f"{config['symbol']}_{config['timeframe']}"
                    
                    if timeframe_key not in ACTIVE_MARKETS:
                        ACTIVE_MARKETS[timeframe_key] = {'m_id': m_id, 'target': m_data['price']}
                    elif ACTIVE_MARKETS[timeframe_key]['m_id'] != m_id:
                        old_m_id = ACTIVE_MARKETS[timeframe_key]['m_id']
                        old_target = ACTIVE_MARKETS[timeframe_key]['target']
                        adjusted_final_price = live_p + config['offset']
                        if not is_pre_warming:
                            log(f"🔔 MARKET CLOSED [{timeframe_key}]. Resolving...")
                            resolve_market(old_m_id, adjusted_final_price, old_target)
                            ACTIVE_MARKETS[timeframe_key] = {'m_id': m_id, 'target': m_data['price']}
                            LOCAL_STATE.pop(f'timing_{old_m_id}', None)
                            LIVE_MARKET_DATA.pop(old_m_id, None)
                            LOCKED_PRICES.pop(old_m_id, None)
                    else:
                        ACTIVE_MARKETS[timeframe_key]['target'] = m_data['price']
                        
                    local_sec_since_start = now_ts - target_base_ts
                    local_sec_left = interval_s - local_sec_since_start
                    
                    LOCAL_STATE[f'timing_{m_id}'] = {
                        'sec_left': local_sec_left, 'sec_since_start': local_sec_since_start,
                        'interval_s': interval_s, 'timeframe': config['timeframe'],
                        'symbol': config['symbol'], 'pair': pair,
                        'm_data': m_data, 'config': config,
                        'is_pre_warming': is_pre_warming
                    }
                    
                    up_id = MARKET_CACHE[slug]['up_id']
                    dn_id = MARKET_CACHE[slug]['dn_id']
                    s_up, s_up_vol, b_up, b_up_vol, up_obi = extract_orderbook_metrics(up_id)
                    s_dn, s_dn_vol, b_dn, b_dn_vol, dn_obi = extract_orderbook_metrics(dn_id)
                    
                    LIVE_MARKET_DATA[m_id] = {'UP_BID': s_up, 'DOWN_BID': s_dn}
                    adjusted_live_p = live_p + config['offset']
                    
                    if not is_pre_warming:
                        MARKET_LOGS_BUFFER.append((
                            f"{config['symbol']}_{config['timeframe']}", m_id, m_data['price'],
                            adjusted_live_p, b_up, b_up_vol, s_up, s_up_vol, up_obi,
                            b_dn, b_dn_vol, s_dn, s_dn_vol, dn_obi,
                            datetime.now().isoformat(), SESSION_ID
                        ))
            except Exception as e:
                log_error("Market State Manager", e)
            await asyncio.sleep(CHECK_INTERVAL)

# ==========================================
# 6. STRATEGY ENGINE & TRADE MANAGEMENT
# ==========================================
async def evaluate_strategies(trigger_source, pair_filter=None):
    try:
        for slug, cache in MARKET_CACHE.items():
            m_id = cache['id']
            if f'timing_{m_id}' not in LOCAL_STATE: continue
            timing = LOCAL_STATE[f'timing_{m_id}']
            
            if timing.get('is_pre_warming', False): continue

            pair = timing['pair']
            
            if trigger_source == "BINANCE_TICK" and pair_filter and pair != pair_filter: continue
            
            if m_id not in EXECUTED_STRAT: EXECUTED_STRAT[m_id] = []
            
            config = timing['config']
            symbol = timing['symbol']
            timeframe = timing['timeframe']
            sec_left = timing['sec_left']
            m_data = timing['m_data']
            
            timeframe_key = f"{symbol}_{timeframe}"
            
            is_base_fetched = m_data.get('base_fetched', False)
            is_clean = m_data.get('is_clean', False)
            
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
            
            # =====================================================================
            # MICRO-PROTECTION ENGINE: Stop Losses, Take Profits & Time Exits
            # =====================================================================
            for trade in PAPER_TRADES[:]:
                if trade['market_id'] != m_id: continue
                current_bid = s_up if trade['direction'] == 'UP' else s_dn
                entry_p = trade['entry_price']
                
                if current_bid <= 0.0: 
                    continue 
                
                if current_bid >= entry_p * TAKE_PROFIT_MULTIPLIER and trade['strategy'] != "OTM Bargain":
                    close_trade(trade, current_bid, "Global Take Profit (+200%)")
                    continue
                
                if 0 < sec_left <= PROFIT_SECURE_SEC:
                    if current_bid > entry_p:
                        close_trade(trade, current_bid, f"Securing profits before expiry ({sec_left:.1f}s left)")
                    continue 

                if trade['strategy'] == "Lag Sniper":
                    if current_bid >= entry_p:
                        if 'sl_countdown' in trade:
                            del trade['sl_countdown']
                            log(f"🔄 [ID: {trade['short_id']:02d}] Price recovered to entry. SL Countdown canceled.")
                    elif current_bid <= entry_p * LAG_SNIPER_SL_DROP and 'sl_countdown' not in trade:
                        trade['sl_countdown'] = time.time()
                        log(f"⚠️ [ID: {trade['short_id']:02d}] Lag Sniper -10% drop. 10s countdown started.")
                    
                    if 'sl_countdown' in trade and (time.time() - trade['sl_countdown'] >= LAG_SNIPER_TIMEOUT):
                        close_trade(trade, current_bid, f"Lag Sniper Timeout SL ({LAG_SNIPER_TIMEOUT}s)")
                        continue

            # =====================================================================
            # SIGNAL GENERATION
            # =====================================================================        
            if is_base_fetched and timeframe_key not in LOCAL_STATE['paused_markets']:
                
                # 1. Mid-Game Arb (Wymaga czystej bazy is_clean)
                m_cfg = config.get('mid_arb', {})
                mid_arb_flag = f"mid_arb_{m_id}"
                if m_cfg and is_clean and m_cfg.get('win_end', 0) < sec_left < m_cfg.get('win_start', 0) and mid_arb_flag not in EXECUTED_STRAT[m_id]:
                    if adj_delta > m_cfg.get('delta', 0) and 0 < b_up <= m_cfg.get('max_p', 0):
                        execute_trade(m_id, timeframe, "Mid-Game Arb", "UP", 2.0, b_up, symbol, m_cfg.get('wr', 50.0), m_cfg.get('id', ''))
                        EXECUTED_STRAT[m_id].append(mid_arb_flag)
                    elif adj_delta < -m_cfg.get('delta', 0) and 0 < b_dn <= m_cfg.get('max_p', 0):
                        execute_trade(m_id, timeframe, "Mid-Game Arb", "DOWN", 2.0, b_dn, symbol, m_cfg.get('wr', 50.0), m_cfg.get('id', ''))
                        EXECUTED_STRAT[m_id].append(mid_arb_flag)
                        
                # 2. OTM Bargain (NIE wymaga czystej bazy - działa zawsze)
                otm_cfg = config.get('otm', {})
                otm_flag = f"otm_{m_id}"
                if otm_cfg and otm_cfg.get('wr', 0.0) > 0.0 and otm_cfg.get('win_end', 0) <= sec_left <= otm_cfg.get('win_start', 0) and otm_flag not in EXECUTED_STRAT[m_id]:
                    if abs(adj_delta) < 40.0:
                        if 0 < b_up <= otm_cfg.get('max_p', 0):
                            execute_trade(m_id, timeframe, "OTM Bargain", "UP", 1.0, b_up, symbol, otm_cfg.get('wr', 50.0), otm_cfg.get('id', ''))
                            EXECUTED_STRAT[m_id].append(otm_flag)
                        elif 0 < b_dn <= otm_cfg.get('max_p', 0):
                            execute_trade(m_id, timeframe, "OTM Bargain", "DOWN", 1.0, b_dn, symbol, otm_cfg.get('wr', 50.0), otm_cfg.get('id', ''))
                            EXECUTED_STRAT[m_id].append(otm_flag)
                            
                # 3. Momentum (Wymaga czystej bazy is_clean)
                mom_cfg = config.get('momentum', {})
                if mom_cfg and is_clean and mom_cfg.get('win_end', 0) <= sec_left <= mom_cfg.get('win_start', 0) and 'momentum' not in EXECUTED_STRAT[m_id]:
                    if adj_delta >= mom_cfg.get('delta', 0) and 0 < b_up <= mom_cfg.get('max_p', 0):
                        execute_trade(m_id, timeframe, "1-Min Mom", "UP", 1.0, b_up, symbol, mom_cfg.get('wr', 50.0), mom_cfg.get('id', ''))
                        EXECUTED_STRAT[m_id].append('momentum')
                    elif adj_delta <= -mom_cfg.get('delta', 0) and 0 < b_dn <= mom_cfg.get('max_p', 0):
                        execute_trade(m_id, timeframe, "1-Min Mom", "DOWN", 1.0, b_dn, symbol, mom_cfg.get('wr', 50.0), mom_cfg.get('id', ''))
                        EXECUTED_STRAT[m_id].append('momentum')
                        
            # 4. Lag Sniper (Wymaga czystej bazy is_clean)
            if trigger_source == "BINANCE_TICK" and is_base_fetched and is_clean:
                asset_jump = live_p - prev_p 
                up_change = b_up - m_data['prev_up']
                dn_change = b_dn - m_data['prev_dn']
                
                sniper_cfg = config.get('lag_sniper', {})
                if sniper_cfg and timeframe_key not in LOCAL_STATE['paused_markets']:
                    end_time = sniper_cfg.get('end_time', sniper_cfg.get('czas_koncowki', 0))
                    end_threshold = sniper_cfg.get('end_threshold', sniper_cfg.get('prog_koncowka', 0))
                    base_threshold = sniper_cfg.get('base_threshold', sniper_cfg.get('prog_bazowy', 0))
                    lag_tolerance = sniper_cfg.get('lag_tolerance', sniper_cfg.get('lag_tol', 0))
                    max_price = sniper_cfg.get('max_price', sniper_cfg.get('max_cena', 0))

                    prog = end_threshold if sec_left <= end_time else base_threshold
                    if 10 < sec_left < timing['interval_s'] - 5:
                        if asset_jump >= prog and abs(up_change) <= lag_tolerance and 0 < b_up <= max_price:
                            execute_trade(m_id, timeframe, "Lag Sniper", "UP", 2.0, b_up, symbol, sniper_cfg.get('wr', 50.0), sniper_cfg.get('id', ''))
                        elif asset_jump <= -prog and abs(dn_change) <= lag_tolerance and 0 < b_dn <= max_price:
                            execute_trade(m_id, timeframe, "Lag Sniper", "DOWN", 2.0, b_dn, symbol, sniper_cfg.get('wr', 50.0), sniper_cfg.get('id', ''))
                            
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
    
    log(f"🚀 LOCAL ORACLE SYSTEM INITIALIZATION. Session ID: {SESSION_ID}")
    
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
        ui_updater_worker(),
        async_smart_flush_worker()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        os._exit(0)