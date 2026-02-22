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
from datetime import datetime
import pytz

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

# --- UNIKALNY IDENTYFIKATOR SESJI ---
SESSION_ID = f"sess_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# ==========================================
# TRACKED CONFIGS
# ==========================================
TRACKED_CONFIGS = [
    {
        "symbol": "XRP",
        "pair": "XRPUSDT",
        "timeframe": "15m",
        "interval": 900,
        "decimals": 4,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 0.0007,
            "prog_koncowka": 0.0003,
            "czas_koncowki": 70,
            "lag_tol": 0.05,
            "max_cena": 0.92,
            "id": "xrp_15m_ls_1771765596",
            "wr": 68.3
        },
        "momentum": {
            "delta": 0.0002,
            "max_p": 0.83,
            "win_start": 60,
            "win_end": 34,
            "id": "xrp_15m_mom_1771765637",
            "wr": 93.1
        },
        "mid_arb": {
            "delta": 0.0003,
            "max_p": 0.55,
            "win_start": 170,
            "win_end": 30,
            "id": "xrp_15m_arb_1771765674",
            "wr": 78.3
        },
        "otm": {
            "win_start": 60,
            "win_end": 22,
            "max_p": 0.07,
            "id": "xrp_15m_otm_1771765680",
            "wr": 50.0
        }
    },
    {
        "symbol": "BTC",
        "pair": "BTCUSDT",
        "timeframe": "15m",
        "interval": 900,
        "decimals": 2,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 35,
            "prog_koncowka": 5,
            "czas_koncowki": 80,
            "lag_tol": 0.05,
            "max_cena": 0.92,
            "id": "btc_15m_ls_1771765682",
            "wr": 65.8
        },
        "momentum": {
            "delta": 15,
            "max_p": 0.67,
            "win_start": 72,
            "win_end": 36,
            "id": "btc_15m_mom_1771765765",
            "wr": 69.2
        },
        "mid_arb": {
            "delta": 5,
            "max_p": 0.45,
            "win_start": 200,
            "win_end": 30,
            "id": "btc_15m_arb_1771765834",
            "wr": 81.0
        },
        "otm": {
            "win_start": 58,
            "win_end": 26,
            "max_p": 0.02,
            "id": "btc_15m_otm_1771765852",
            "wr": 52.6
        }
    },
    {
        "symbol": "ETH",
        "pair": "ETHUSDT",
        "timeframe": "15m",
        "interval": 900,
        "decimals": 2,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 1.2,
            "prog_koncowka": 0.5,
            "czas_koncowki": 75,
            "lag_tol": 0.05,
            "max_cena": 0.92,
            "id": "eth_15m_ls_1771765854",
            "wr": 63.2
        },
        "momentum": {
            "delta": 0.8,
            "max_p": 0.76,
            "win_start": 68,
            "win_end": 48,
            "id": "eth_15m_mom_1771765874",
            "wr": 100.0
        },
        "mid_arb": {
            "delta": 0.4,
            "max_p": 0.59,
            "win_start": 200,
            "win_end": 50,
            "id": "eth_15m_arb_1771765930",
            "wr": 81.0
        },
        "otm": {
            "win_start": 58,
            "win_end": 20,
            "max_p": 0.04,
            "id": "eth_15m_otm_1771765940",
            "wr": 45.7
        }
    },
    {
        "symbol": "SOL",
        "pair": "SOLUSDT",
        "timeframe": "15m",
        "interval": 900,
        "decimals": 3,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 0.08,
            "prog_koncowka": 0.02,
            "czas_koncowki": 75,
            "lag_tol": 0.15,
            "max_cena": 0.98,
            "id": "sol_15m_ls_1771765942",
            "wr": 84.4
        },
        "momentum": {
            "delta": 0.02,
            "max_p": 0.85,
            "win_start": 60,
            "win_end": 38,
            "id": "sol_15m_mom_1771765986",
            "wr": 100.0
        },
        "mid_arb": {
            "delta": 0.02,
            "max_p": 0.65,
            "win_start": 160,
            "win_end": 30,
            "id": "sol_15m_arb_1771766028",
            "wr": 92.9
        },
        "otm": {
            "win_start": 78,
            "win_end": 20,
            "max_p": 0.06,
            "id": "sol_15m_otm_1771766034",
            "wr": 50.0
        }
    },
    {
        "symbol": "BTC",
        "pair": "BTCUSDT",
        "timeframe": "5m",
        "interval": 300,
        "decimals": 2,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 35,
            "prog_koncowka": 5,
            "czas_koncowki": 90,
            "lag_tol": 0.1,
            "max_cena": 0.92,
            "id": "btc_5m_ls_1771766037",
            "wr": 69.2
        },
        "momentum": {
            "delta": 15,
            "max_p": 0.75,
            "win_start": 78,
            "win_end": 30,
            "id": "btc_5m_mom_1771766131",
            "wr": 82.4
        },
        "mid_arb": {
            "delta": 5,
            "max_p": 0.45,
            "win_start": 120,
            "win_end": 35,
            "id": "btc_5m_arb_1771766209",
            "wr": 74.7
        },
        "otm": {
            "win_start": 90,
            "win_end": 22,
            "max_p": 0.06,
            "id": "btc_5m_otm_1771766235",
            "wr": 47.9
        }
    },
    {
        "symbol": "ETH",
        "pair": "ETHUSDT",
        "timeframe": "5m",
        "interval": 300,
        "decimals": 2,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 1.0,
            "prog_koncowka": 0.5,
            "czas_koncowki": 55,
            "lag_tol": 0.1,
            "max_cena": 0.92,
            "id": "eth_5m_ls_1771766238",
            "wr": 69.8
        },
        "momentum": {
            "delta": 0.5,
            "max_p": 0.8,
            "win_start": 82,
            "win_end": 30,
            "id": "eth_5m_mom_1771766259",
            "wr": 86.0
        },
        "mid_arb": {
            "delta": 0.3,
            "max_p": 0.53,
            "win_start": 190,
            "win_end": 30,
            "id": "eth_5m_arb_1771766321",
            "wr": 69.8
        },
        "otm": {
            "win_start": 50,
            "win_end": 22,
            "max_p": 0.04,
            "id": "eth_5m_otm_1771766333",
            "wr": 37.3
        }
    },
    {
        "symbol": "SOL",
        "pair": "SOLUSDT",
        "timeframe": "5m",
        "interval": 300,
        "decimals": 3,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 0.05,
            "prog_koncowka": 0.02,
            "czas_koncowki": 85,
            "lag_tol": 0.05,
            "max_cena": 0.92,
            "id": "sol_5m_ls_1771766336",
            "wr": 75.9
        },
        "momentum": {
            "delta": 0.02,
            "max_p": 0.75,
            "win_start": 86,
            "win_end": 30,
            "id": "sol_5m_mom_1771766380",
            "wr": 81.0
        },
        "mid_arb": {
            "delta": 0.01,
            "max_p": 0.45,
            "win_start": 190,
            "win_end": 30,
            "id": "sol_5m_arb_1771766421",
            "wr": 70.5
        },
        "otm": {
            "win_start": 52,
            "win_end": 20,
            "max_p": 0.03,
            "id": "sol_5m_otm_1771766429",
            "wr": 44.4
        }
    },
    {
        "symbol": "XRP",
        "pair": "XRPUSDT",
        "timeframe": "5m",
        "interval": 300,
        "decimals": 4,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 0.0015,
            "prog_koncowka": 0.0002,
            "czas_koncowki": 70,
            "lag_tol": 0.15,
            "max_cena": 0.92,
            "id": "xrp_5m_ls_1771766432",
            "wr": 76.7
        },
        "momentum": {
            "delta": 0.0002,
            "max_p": 0.69,
            "win_start": 78,
            "win_end": 30,
            "id": "xrp_5m_mom_1771766473",
            "wr": 88.3
        },
        "mid_arb": {
            "delta": 0.0001,
            "max_p": 0.51,
            "win_start": 140,
            "win_end": 30,
            "id": "xrp_5m_arb_1771766516",
            "wr": 75.8
        },
        "otm": {
            "win_start": 60,
            "win_end": 22,
            "max_p": 0.09,
            "id": "xrp_5m_otm_1771766523",
            "wr": 57.1
        }
    },
    {
        "symbol": "XRP",
        "pair": "XRPUSDT",
        "timeframe": "1h",
        "interval": 3600,
        "decimals": 4,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 0.0006,
            "prog_koncowka": 0.0003,
            "czas_koncowki": 50,
            "lag_tol": 0.15,
            "max_cena": 0.96,
            "id": "xrp_1h_ls_1771766526",
            "wr": 70.4
        },
        "momentum": {
            "delta": 0.0005,
            "max_p": 0.8,
            "win_start": 70,
            "win_end": 40,
            "id": "xrp_1h_mom_1771766562",
            "wr": 100.0
        },
        "mid_arb": {
            "delta": 0.0002,
            "max_p": 0.61,
            "win_start": 200,
            "win_end": 30,
            "id": "xrp_1h_arb_1771766598",
            "wr": 100.0
        },
        "otm": {
            "win_start": 56,
            "win_end": 20,
            "max_p": 0.03,
            "id": "xrp_1h_otm_1771766602",
            "wr": 0.0
        }
    },
    {
        "symbol": "BTC",
        "pair": "BTCUSDT",
        "timeframe": "1h",
        "interval": 3600,
        "decimals": 2,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 26,
            "prog_koncowka": 5,
            "czas_koncowki": 30,
            "lag_tol": 0.05,
            "max_cena": 0.98,
            "id": "btc_1h_ls_1771766604",
            "wr": 74.1
        },
        "momentum": {
            "delta": 18,
            "max_p": 0.83,
            "win_start": 84,
            "win_end": 32,
            "id": "btc_1h_mom_1771766666",
            "wr": 100.0
        },
        "mid_arb": {
            "delta": 18,
            "max_p": 0.45,
            "win_start": 120,
            "win_end": 30,
            "id": "btc_1h_arb_1771766732",
            "wr": 100.0
        },
        "otm": {
            "win_start": 50,
            "win_end": 20,
            "max_p": 0.02,
            "id": "btc_1h_otm_1771766747",
            "wr": 50.0
        }
    },
    {
        "symbol": "ETH",
        "pair": "ETHUSDT",
        "timeframe": "1h",
        "interval": 3600,
        "decimals": 2,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 1.6,
            "prog_koncowka": 0.7,
            "czas_koncowki": 75,
            "lag_tol": 0.15,
            "max_cena": 0.98,
            "id": "eth_1h_ls_1771766749",
            "wr": 83.3
        },
        "momentum": {
            "delta": 0.8,
            "max_p": 0.71,
            "win_start": 70,
            "win_end": 42,
            "id": "eth_1h_mom_1771766767",
            "wr": 100.0
        },
        "mid_arb": {
            "delta": 0.3,
            "max_p": 0.55,
            "win_start": 190,
            "win_end": 30,
            "id": "eth_1h_arb_1771766821",
            "wr": 50.0
        },
        "otm": {
            "win_start": 86,
            "win_end": 50,
            "max_p": 0.04,
            "id": "eth_1h_otm_1771766828",
            "wr": 60.0
        }
    },
    {
        "symbol": "SOL",
        "pair": "SOLUSDT",
        "timeframe": "1h",
        "interval": 3600,
        "decimals": 3,
        "offset": 0.0,
        "lag_sniper": {
            "prog_bazowy": 0.07,
            "prog_koncowka": 0.02,
            "czas_koncowki": 30,
            "lag_tol": 0.15,
            "max_cena": 0.92,
            "id": "sol_1h_ls_1771766830",
            "wr": 85.0
        },
        "momentum": {
            "delta": 0.02,
            "max_p": 0.83,
            "win_start": 70,
            "win_end": 46,
            "id": "sol_1h_mom_1771766872",
            "wr": 100.0
        },
        "mid_arb": {
            "delta": 0.04,
            "max_p": 0.61,
            "win_start": 170,
            "win_end": 30,
            "id": "sol_1h_arb_1771766907",
            "wr": 100.0
        },
        "otm": {
            "win_start": 50,
            "win_end": 20,
            "max_p": 0.05,
            "id": "sol_1h_otm_1771766912",
            "wr": 20.0
        }
    }
]

LOCAL_STATE = {
    'binance_live_price': {cfg['pair']: 0.0 for cfg in TRACKED_CONFIGS},
    'prev_price': {cfg['pair']: 0.0 for cfg in TRACKED_CONFIGS},
    'polymarket_books': {},
    'session_id': SESSION_ID
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

# ==========================================
# 0. SYSTEM LOGOWANIA BŁĘDÓW & HELPERS
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
            fetched_at TEXT, session_id TEXT)''')
        try:
            await db.execute("ALTER TABLE market_logs_v11 ADD COLUMN session_id TEXT")
        except Exception:
            pass
        await db.execute('''CREATE TABLE IF NOT EXISTS trade_logs_v10 (
            trade_id TEXT PRIMARY KEY, market_id TEXT, timeframe TEXT, strategy TEXT, direction TEXT,
            invested REAL, entry_price REAL, entry_time TEXT, exit_price REAL, exit_time TEXT, pnl REAL,
            reason TEXT, session_id TEXT)''')
        try:
            await db.execute("ALTER TABLE trade_logs_v10 ADD COLUMN session_id TEXT")
        except Exception:
            pass
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

def execute_trade(market_id, timeframe, strategy, direction, base_stake, price, symbol, win_rate, strat_id=""):
    global PORTFOLIO_BALANCE
    if not AVAILABLE_TRADE_IDS:
        log_error("ID Pool Exhausted", Exception("Brakuje wolnych identyfikatorów."))
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
        trade['entry_price'], trade['entry_time'], close_price, datetime.now().isoformat(), pnl, reason, SESSION_ID
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
                log(f"[WS] Podłączono do Binance")
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
# 5. MARKET STATE MANAGER (BINANCE API RESOLUTION)
# ==========================================
async def get_binance_strike_price(session, pair, base_ts):
    """
    Kluczowa funkcja rozwiązująca problem Polymarketu. 
    Zamiast scrapować teksty, pyta Binance o dokładną historyczną cenę Open 
    ze świecy startowej dla danego rynku. Szybko, lekko i w 100% stabilnie.
    """
    url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval=1m&startTime={int(base_ts * 1000)}&limit=1"
    try:
        async with session.get(url, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data and len(data) > 0:
                    # data[0][1] to zawsze Open Price z danej minuty na Binance
                    return float(data[0][1]) 
    except Exception:
        pass
    return None

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
                                            
                                            MARKET_CACHE[candidate] = {
                                                'id': str(m.get('id')), 
                                                'up_id': clob_ids[outcomes.index("Up")], 
                                                'dn_id': clob_ids[outcomes.index("Down")], 
                                                'config': config
                                            }
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
                    
                    # === NOWA WERYFIKACJA OPIARTA O BINANCE ===
                    if not m_data['verified']:
                        strike_p = await get_binance_strike_price(session, pair, base_ts)
                        if strike_p is not None:
                            m_data['price'] = strike_p
                            m_data['verified'] = True
                            log(f"⚡ [BINANCE API VERIFIED] Precyzyjna baza ze świecy startowej: ${strike_p:.2f} dla {slug}")
                        else:
                            # Próba co 10 sekund, jeśli świeca z Binance jeszcze się nie wygenerowała
                            if (time.time() - m_data['last_retry']) > 10:
                                m_data['last_retry'] = time.time()
                                log(f"⏳ [AWAITING BINANCE] Oczekiwanie na świecę historyczną dla {slug}...")
                            
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
                        adjusted_live_p, b_up, b_up_vol, s_up, s_up_vol, up_obi,
                        b_dn, b_dn_vol, s_dn, s_dn_vol, dn_obi,
                        datetime.now().isoformat(), SESSION_ID
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
                        execute_trade(m_id, timeframe, "Mid-Game Arb", "UP", 2.0, b_up, symbol, m_cfg.get('wr', 50.0), m_cfg.get('id', ''))
                        EXECUTED_STRAT[m_id].append(mid_arb_flag)
                    elif adj_delta < -m_cfg['delta'] and 0 < b_dn <= m_cfg['max_p']:
                        execute_trade(m_id, timeframe, "Mid-Game Arb", "DOWN", 2.0, b_dn, symbol, m_cfg.get('wr', 50.0), m_cfg.get('id', ''))
                        EXECUTED_STRAT[m_id].append(mid_arb_flag)
                        
                otm_cfg = config.get('otm', {})
                otm_flag = f"otm_{m_id}"
                if otm_cfg and otm_cfg.get('wr', 0.0) > 0.0 and otm_cfg['win_end'] <= sec_left <= otm_cfg['win_start'] and otm_flag not in EXECUTED_STRAT[m_id]:
                    if abs(adj_delta) < 40.0:
                        if 0 < b_up <= otm_cfg['max_p']:
                            execute_trade(m_id, timeframe, "OTM Bargain", "UP", 1.0, b_up, symbol, otm_cfg.get('wr', 50.0), otm_cfg.get('id', ''))
                            EXECUTED_STRAT[m_id].append(otm_flag)
                        elif 0 < b_dn <= otm_cfg['max_p']:
                            execute_trade(m_id, timeframe, "OTM Bargain", "DOWN", 1.0, b_dn, symbol, otm_cfg.get('wr', 50.0), otm_cfg.get('id', ''))
                            EXECUTED_STRAT[m_id].append(otm_flag)
                            
                mom_cfg = config.get('momentum', {})
                if mom_cfg and mom_cfg['win_end'] <= sec_left <= mom_cfg['win_start'] and 'momentum' not in EXECUTED_STRAT[m_id]:
                    if adj_delta >= mom_cfg['delta'] and 0 < b_up <= mom_cfg['max_p']:
                        execute_trade(m_id, timeframe, "1-Min Mom", "UP", 1.0, b_up, symbol, mom_cfg.get('wr', 50.0), mom_cfg.get('id', ''))
                        EXECUTED_STRAT[m_id].append('momentum')
                    elif adj_delta <= -mom_cfg['delta'] and 0 < b_dn <= mom_cfg['max_p']:
                        execute_trade(m_id, timeframe, "1-Min Mom", "DOWN", 1.0, b_dn, symbol, mom_cfg.get('wr', 50.0), mom_cfg.get('id', ''))
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
                            execute_trade(m_id, timeframe, "Lag Sniper", "UP", 2.0, b_up, symbol, sniper_cfg.get('wr', 50.0), sniper_cfg.get('id', ''))
                        elif asset_jump <= -prog and abs(dn_change) <= sniper_cfg['lag_tol'] and 0 < b_dn <= sniper_cfg['max_cena']:
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
    
    log(f"🚀 INICJALIZACJA SYSTEMU API-ONLY. ID Sesji: {SESSION_ID}")
    
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