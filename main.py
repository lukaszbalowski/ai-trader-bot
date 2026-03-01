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
from math import floor
from collections import deque
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType, BalanceAllowanceParams, AssetType
from dashboard import render_dashboard
from runtime_state import CommandBus, RuntimeModeManager, RuntimeStateStore

# ==========================================
# ENV & CREDENTIALS
# ==========================================
load_dotenv()

HOST = "https://clob.polymarket.com"
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
PROXY_WALLET_ADDRESS = os.getenv("PROXY_WALLET_ADDRESS")
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE", "1"))

TRADING_MODE = 'paper' 
ASYNC_CLOB_CLIENT = None
MODE_MANAGER = RuntimeModeManager()
STATE_STORE = None
COMMAND_BUS = None
DISABLED_STRATEGIES = set()
STRATEGY_KEYS = ('kinetic_sniper', 'momentum', 'mid_arb', 'otm')

# ==========================================
# CONSTANTS & MULTI-COIN CONFIGURATION
# ==========================================
CHECK_INTERVAL = 1

# --- RISK & MONEY MANAGEMENT ---
DRAWDOWN_LIMIT = 0.30          
MAX_MARKET_EXPOSURE = 0.15     
BURST_LIMIT_TRADES = 5         
BURST_LIMIT_SEC = 10           

# --- MICRO-PROTECTION CONSTANTS ---
PROFIT_SECURE_SEC = 3.0        
TAKE_PROFIT_MULTIPLIER = 3.0   
KINETIC_SNIPER_SL_DROP = 0.93
KINETIC_SNIPER_TIMEOUT = 5.0
KINETIC_SNIPER_TP1_WINDOW = 5.0
KINETIC_SNIPER_TP2_WINDOW = 15.0
KINETIC_SNIPER_MAX_HOLD = 15.0
MOMENTUM_TP1_WINDOW = 5.0
MOMENTUM_MAX_HOLD = 15.0
MOMENTUM_SL_DROP = 0.95
LOSS_RECOVERY_TIMEOUT = 10.0
POSITION_DUST_SHARES = 0.001
MIN_MARKET_SELL_SHARES = 0.01
CLOSE_RECONCILE_GRACE_SEC = 4.0
CLOSE_PENDING_TIMEOUT = 20.0
ORPHAN_SCAN_INTERVAL = 15.0
SIGNAL_SKIP_DEDUPE_SEC = 5.0

SANITY_THRESHOLDS = {
    'BTC': 0.04, 'ETH': 0.05, 'SOL': 0.08, 'XRP': 0.15
}

FULL_NAMES = {
    'BTC': 'bitcoin', 'ETH': 'ethereum', 'SOL': 'solana', 'XRP': 'xrp'
}

SESSION_ID = f"sess_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
MAIN_EXPORT_DIR = os.path.join("data", "main")
SESSION_EXPORT_DIR = os.path.join(MAIN_EXPORT_DIR, "session_logs")
TECH_DUMP_DIR = os.path.join(MAIN_EXPORT_DIR, "tech_dumps")
SESSION_LOG_PATH = os.path.join(SESSION_EXPORT_DIR, f"{SESSION_ID}.log")

OBSERVED_MARKET_TEMPLATES = [
    {'symbol': 'BTC', 'pair': 'BTCUSDT', 'timeframe': '5m', 'interval': 300, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'ETH', 'pair': 'ETHUSDT', 'timeframe': '5m', 'interval': 300, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'SOL', 'pair': 'SOLUSDT', 'timeframe': '5m', 'interval': 300, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'XRP', 'pair': 'XRPUSDT', 'timeframe': '5m', 'interval': 300, 'decimals': 4, 'offset': 0.0},
    {'symbol': 'BTC', 'pair': 'BTCUSDT', 'timeframe': '15m', 'interval': 900, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'ETH', 'pair': 'ETHUSDT', 'timeframe': '15m', 'interval': 900, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'SOL', 'pair': 'SOLUSDT', 'timeframe': '15m', 'interval': 900, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'XRP', 'pair': 'XRPUSDT', 'timeframe': '15m', 'interval': 900, 'decimals': 4, 'offset': 0.0},
    {'symbol': 'BTC', 'pair': 'BTCUSDT', 'timeframe': '1h', 'interval': 3600, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'ETH', 'pair': 'ETHUSDT', 'timeframe': '1h', 'interval': 3600, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'SOL', 'pair': 'SOLUSDT', 'timeframe': '1h', 'interval': 3600, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'XRP', 'pair': 'XRPUSDT', 'timeframe': '1h', 'interval': 3600, 'decimals': 4, 'offset': 0.0},
    {'symbol': 'BTC', 'pair': 'BTCUSDT', 'timeframe': '4h', 'interval': 14400, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'ETH', 'pair': 'ETHUSDT', 'timeframe': '4h', 'interval': 14400, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'SOL', 'pair': 'SOLUSDT', 'timeframe': '4h', 'interval': 14400, 'decimals': 2, 'offset': 0.0},
    {'symbol': 'XRP', 'pair': 'XRPUSDT', 'timeframe': '4h', 'interval': 14400, 'decimals': 4, 'offset': 0.0},
]


def market_key(cfg):
    return f"{cfg['symbol']}_{cfg['timeframe']}"


def is_strategy_enabled(cfg, strategy_key):
    if not cfg.get(strategy_key):
        return False
    return (market_key(cfg), strategy_key) not in DISABLED_STRATEGIES


def has_tradeable_strategies(cfg):
    return any(is_strategy_enabled(cfg, name) for name in STRATEGY_KEYS)


def is_market_trading_enabled(cfg):
    return MODE_MANAGER.execution_enabled and has_tradeable_strategies(cfg)


def build_observed_configs(tracked_configs):
    tracked_by_key = {market_key(cfg): cfg for cfg in tracked_configs}
    observed_configs = []
    for template in OBSERVED_MARKET_TEMPLATES:
        cfg = dict(template)
        cfg.update(tracked_by_key.get(market_key(template), {}))
        observed_configs.append(cfg)
    return observed_configs

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

OBSERVED_CONFIGS = build_observed_configs(TRACKED_CONFIGS)

MARKET_KEYS = list(string.ascii_lowercase)
for idx, cfg in enumerate(OBSERVED_CONFIGS):
    cfg['ui_key'] = MARKET_KEYS[idx] if idx < len(MARKET_KEYS) else str(idx)

# ==========================================
# LOCAL STATE INITIALIZATION (SAFE START)
# ==========================================
initial_paused_markets = set([market_key(cfg) for cfg in OBSERVED_CONFIGS])

LOCAL_STATE = {
    'binance_live_price': {cfg['pair']: 0.0 for cfg in OBSERVED_CONFIGS},
    'price_history': {cfg['pair']: deque() for cfg in OBSERVED_CONFIGS},
    'prev_price': {cfg['pair']: 0.0 for cfg in OBSERVED_CONFIGS},
    'polymarket_books': {},
    'recent_logs': [],
    'session_id': SESSION_ID,
    'paused_markets': initial_paused_markets,
    'market_status': {
        market_key(cfg): "[paused] [no strategies]" if not has_tradeable_strategies(cfg) else "[paused]"
        for cfg in OBSERVED_CONFIGS
    },
    'service_status': {
        'binance_ws': {'status': 'starting', 'details': 'Waiting for Binance stream'},
        'polymarket_market_ws': {'status': 'starting', 'details': 'Waiting for market stream'},
        'polymarket_user_ws': {'status': 'starting', 'details': 'Waiting for user stream'},
        'database': {'status': 'starting', 'details': 'SQLite not initialized yet'},
        'dashboard_api': {'status': 'starting', 'details': 'Dashboard server booting'}
    }
}

AVAILABLE_TRADE_IDS = list(range(100))
LOCKED_PRICES = {}
MARKET_CACHE = {} 
PAPER_TRADES = []
TRADE_HISTORY = []
EXECUTED_STRAT = {}
ACTIVE_MARKETS = {}
PRE_WARMING_MARKETS = {}
LIVE_MARKET_DATA = {}
TRADE_TIMESTAMPS = {}
MANUALLY_PAUSED_MARKETS = set()

INITIAL_BALANCE = 0.0
PORTFOLIO_BALANCE = 0.0

MARKET_LOGS_BUFFER = []
TRADE_LOGS_BUFFER = []
SIGNAL_SKIP_LOGS_BUFFER = []
LAST_FLUSH_TS = 0
RECENT_LOGS = []
ACTIVE_ERRORS = []
SESSION_LOG_LINES = []
SESSION_ERROR_LOGS = []
SIGNAL_SKIP_DEDUPE = {}

WS_SUBSCRIPTION_QUEUE = asyncio.Queue()


def set_service_status(name, status, details=""):
    LOCAL_STATE.setdefault('service_status', {})
    LOCAL_STATE['service_status'][name] = {'status': status, 'details': details}


def get_config_by_market_key(tf_key):
    return next((cfg for cfg in OBSERVED_CONFIGS if market_key(cfg) == tf_key), None)


def get_market_tf_key_by_ui(ui_key):
    for cfg in OBSERVED_CONFIGS:
        if cfg['ui_key'] == ui_key:
            return f"{cfg['symbol']}_{cfg['timeframe']}"
    return None


def normalize_market_selector(selector):
    if selector in {market_key(cfg) for cfg in OBSERVED_CONFIGS}:
        return selector
    return get_market_tf_key_by_ui(selector)


def get_ui_key_by_market_tf_key(tf_key):
    cfg = get_config_by_market_key(tf_key)
    return cfg.get('ui_key') if cfg else None


def market_pause_reason(cfg):
    if not has_tradeable_strategies(cfg):
        return "[paused] [no strategies]"
    if not MODE_MANAGER.execution_enabled:
        return "[paused] [observe-only]"
    return "[paused]"


def record_signal_skip(
    *,
    market_id,
    timeframe,
    strategy_key,
    trigger_source,
    reason_code,
    detail="",
    sec_left=0.0,
    live_price=0.0,
    target_price=0.0,
    delta=0.0,
    buy_up=0.0,
    buy_down=0.0,
):
    now = time.time()
    dedupe_key = (market_id, timeframe, strategy_key, trigger_source)
    dedupe_entry = SIGNAL_SKIP_DEDUPE.get(dedupe_key)
    if dedupe_entry and dedupe_entry["reason_code"] == reason_code and (now - dedupe_entry["ts"]) < SIGNAL_SKIP_DEDUPE_SEC:
        return
    SIGNAL_SKIP_DEDUPE[dedupe_key] = {"reason_code": reason_code, "ts": now}
    SIGNAL_SKIP_LOGS_BUFFER.append(
        (
            timeframe,
            market_id,
            strategy_key,
            trigger_source,
            reason_code,
            detail[:240],
            float(sec_left),
            float(live_price),
            float(target_price),
            float(delta),
            float(buy_up),
            float(buy_down),
            datetime.now().isoformat(),
            SESSION_ID,
        )
    )


def refresh_market_runtime_flags():
    for cfg in OBSERVED_CONFIGS:
        tf_key = market_key(cfg)
        current_status = LOCAL_STATE['market_status'].get(tf_key, "[paused]")

        if not has_tradeable_strategies(cfg):
            LOCAL_STATE['paused_markets'].add(tf_key)
            LOCAL_STATE['market_status'][tf_key] = "[paused] [no strategies]"
            continue

        if not MODE_MANAGER.execution_enabled:
            LOCAL_STATE['paused_markets'].add(tf_key)
            LOCAL_STATE['market_status'][tf_key] = "[paused] [observe-only]"
            continue

        if tf_key in MANUALLY_PAUSED_MARKETS:
            LOCAL_STATE['paused_markets'].add(tf_key)
            LOCAL_STATE['market_status'][tf_key] = "[paused] [manual]"
            continue

        if tf_key in ACTIVE_MARKETS:
            LOCAL_STATE['paused_markets'].discard(tf_key)
            if "waiting for next" in current_status:
                LOCAL_STATE['market_status'][tf_key] = current_status
            else:
                LOCAL_STATE['market_status'][tf_key] = "[running]"
            continue

        if tf_key in PRE_WARMING_MARKETS:
            LOCAL_STATE['paused_markets'].add(tf_key)
            LOCAL_STATE['market_status'][tf_key] = "[paused] [pre-warm]"
            continue

        LOCAL_STATE['paused_markets'].add(tf_key)
        if "waiting for next" not in current_status:
            LOCAL_STATE['market_status'][tf_key] = "[paused] [searching next]"


def switch_operation_mode(target_mode):
    MODE_MANAGER.set_operation_mode(target_mode)
    refresh_market_runtime_flags()
    log(f"🎛️ [MODE] Operation mode switched to {MODE_MANAGER.operation_mode}.")
    return f"Operation mode switched to {MODE_MANAGER.operation_mode}"

# ==========================================
# DATABASE EVENT WORKER (ZERO-BLOCKING)
# ==========================================
class TradeDatabaseWorker:
    def __init__(self):
        self.db_queue = asyncio.Queue()

    async def start_worker(self):
        log("👷 [DB Worker] Alpha Vault background updater started.")
        while True:
            try:
                task = await self.db_queue.get()
                operation = task.get("operation")
                data = task.get("data")

                async with aiosqlite.connect('data/polymarket.db') as db:
                    if operation == "UPDATE_MARKET_CLOSE":
                        await db.execute("UPDATE trade_logs_v10 SET market_closed_price = ? WHERE market_id = ?", 
                                        (data['closed_price'], data['market_id']))
                    elif operation == "UPDATE_SETTLEMENT":
                        await db.execute("UPDATE trade_logs_v10 SET settlement_value = ?, reason = ? WHERE market_id = ?", 
                                        (data['value'], f"RESOLVED: {data['status']}", data['market_id']))
                    await db.commit()
                
                self.db_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log_error("DB Worker Error", e)

DB_WORKER = TradeDatabaseWorker()

# ==========================================
# ASYNC CLOB WRAPPER
# ==========================================
class AsyncClobClient:
    def __init__(self, client: ClobClient):
        self.client = client

    async def init_creds(self):
        return await asyncio.to_thread(self.client.create_or_derive_api_creds)

    async def get_actual_balance(self, token_id: str) -> float:
        params = BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL,
            token_id=token_id,
            signature_type=SIGNATURE_TYPE
        )
        res = await asyncio.to_thread(self.client.get_balance_allowance, params)
        raw_balance = float(res.get("balance", 0.0))
        return raw_balance / 1_000_000.0

    async def get_collateral_balance(self) -> float:
        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            signature_type=SIGNATURE_TYPE
        )
        res = await asyncio.to_thread(self.client.get_balance_allowance, params)
        raw_balance = float(res.get("balance", 0.0))
        return raw_balance / 1_000_000.0

    async def execute_market_order(self, args: MarketOrderArgs):
        order = await asyncio.to_thread(self.client.create_market_order, args)
        return await asyncio.to_thread(self.client.post_order, order, OrderType.FAK)

# ==========================================
# LIVE EXECUTION WORKERS
# ==========================================
async def sync_live_balance():
    global PORTFOLIO_BALANCE
    if TRADING_MODE == 'live' and ASYNC_CLOB_CLIENT:
        try:
            await asyncio.sleep(2.0) 
            balance = await ASYNC_CLOB_CLIENT.get_collateral_balance()
            if balance > 0:
                PORTFOLIO_BALANCE = balance
                log(f"💰 [LIVE] Wallet Cash Balance Auto-Synced: ${PORTFOLIO_BALANCE:.2f}")
        except Exception as e:
            log_error("Failed to sync live balance", e)

def rollback_failed_trade(trade_id):
    global PORTFOLIO_BALANCE
    for t in PAPER_TRADES[:]:
        if t['id'] == trade_id:
            PORTFOLIO_BALANCE += t['invested']
            PAPER_TRADES.remove(t)
            break

def get_trade_by_id(trade_id):
    for trade in PAPER_TRADES:
        if trade['id'] == trade_id:
            return trade
    return None

def get_trade_token_id(trade):
    token_id = trade.get('token_id')
    if token_id:
        return token_id
    market_id = trade.get('market_id')
    if market_id in MARKET_CACHE:
        return MARKET_CACHE[market_id]['up_id'] if trade['direction'] == 'UP' else MARKET_CACHE[market_id]['dn_id']
    return None

def has_open_trades_for_market(market_id):
    return any(trade.get('market_id') == market_id for trade in PAPER_TRADES)

def cleanup_market_state_if_unused(market_id):
    if has_open_trades_for_market(market_id):
        return
    if any(state.get('m_id') == market_id for state in ACTIVE_MARKETS.values()):
        return
    if any(state.get('m_id') == market_id for state in PRE_WARMING_MARKETS.values()):
        return
    LOCAL_STATE.pop(f'timing_{market_id}', None)
    LIVE_MARKET_DATA.pop(market_id, None)
    LOCKED_PRICES.pop(market_id, None)
    MARKET_CACHE.pop(market_id, None)

def clear_pending_close_state(trade):
    trade.pop('closing_pending', None)
    trade.pop('pending_close_price', None)
    trade.pop('pending_close_reason', None)
    trade.pop('close_requested_at', None)
    trade.pop('close_requested_ts', None)
    trade.pop('close_submitted_ts', None)
    trade.pop('close_clob_order_id', None)
    trade.pop('close_confirmed', None)
    trade.pop('close_execution_price', None)
    trade.pop('close_confirmation_ts', None)
    trade.pop('pending_close_initial_shares', None)
    trade.pop('pending_close_reported_shares', None)
    trade.pop('pending_close_reported_price', None)
    trade.pop('pending_close_reported_value', None)

def normalize_market_sell_shares(shares):
    if shares <= 0:
        return 0.0
    return floor(shares * 100) / 100.0

def mark_trade_close_blocked(trade, reason):
    clear_pending_close_state(trade)
    trade['close_blocked'] = True
    trade['close_blocked_reason'] = reason
    log(f"🧱 [LIVE] Close blocked for {trade['id']}. {reason} Waiting for resolution.")

def clear_trade_close_blocked(trade):
    trade.pop('close_blocked', None)
    trade.pop('close_blocked_reason', None)

def can_trade_submit_close(trade):
    return normalize_market_sell_shares(trade.get('shares', 0.0)) >= MIN_MARKET_SELL_SHARES

def get_trade_sell_price(trade):
    if trade['market_id'] in LIVE_MARKET_DATA:
        price = LIVE_MARKET_DATA[trade['market_id']].get(f"{trade['direction']}_SELL", 0.0)
        if price > 0:
            return price
    token_id = get_trade_token_id(trade)
    if not token_id:
        return 0.0
    _, _, best_bid, _, _ = extract_orderbook_metrics(token_id)
    return best_bid

def create_recovered_trade(market_id, token_id, direction, shares, symbol, timeframe):
    if not AVAILABLE_TRADE_IDS:
        log_error("Orphan Recovery", Exception("No free IDs available for recovered position."))
        return None
    short_id = AVAILABLE_TRADE_IDS.pop(0)
    market_sell = 0.0
    if market_id in LIVE_MARKET_DATA:
        market_sell = LIVE_MARKET_DATA[market_id].get(f"{direction}_SELL", 0.0)
    if market_sell <= 0:
        _, _, market_sell, _, _ = extract_orderbook_metrics(token_id)
    if market_sell <= 0:
        market_sell = 0.5
    invested = shares * market_sell
    trade = {
        'id': f"recovered_{market_id}_{direction.lower()}_{uuid.uuid4().hex[:8]}",
        'short_id': short_id,
        'strat_id': 'recovered_orphan',
        'market_id': market_id,
        'timeframe': timeframe,
        'symbol': symbol,
        'strategy': 'Recovered Orphan',
        'direction': direction,
        'entry_price': market_sell,
        'entry_time': datetime.now().isoformat(),
        'entry_time_ts': time.time(),
        'last_bid_price': 0.0,
        'ticks_down': 0,
        'shares': shares,
        'invested': invested,
        'clob_order_id': '',
        'token_id': token_id,
        'recovered_orphan': True
    }
    PAPER_TRADES.append(trade)
    log(f"🧲 [LIVE] Recovered orphan position {trade['id']} | {symbol} {timeframe} {direction} | {shares:.4f} shares.")
    return trade

def rollback_failed_close(trade_id, details=""):
    trade = get_trade_by_id(trade_id)
    if not trade or not trade.get('closing_pending'):
        return
    clear_pending_close_state(trade)
    suffix = f" Details: {details}" if details else ""
    log(f"🔄 [LIVE] Close rollback for {trade_id}. Position restored for monitoring.{suffix}")

def apply_partial_close_fill(trade, remaining_shares, execution_price, reason):
    global PORTFOLIO_BALANCE
    original_shares = trade.get('pending_close_initial_shares', trade['shares'])
    if original_shares <= 0:
        rollback_failed_close(trade['id'], "Invalid original share count during partial close.")
        return
    sold_shares = max(original_shares - remaining_shares, 0.0)
    if sold_shares <= POSITION_DUST_SHARES:
        rollback_failed_close(trade['id'], "Partial close reconciliation found no sold shares.")
        return
    cost_per_share = trade['invested'] / original_shares if original_shares > 0 else trade['entry_price']
    realized_value = execution_price * sold_shares
    realized_cost = cost_per_share * sold_shares
    pnl = realized_value - realized_cost
    trade['shares'] = remaining_shares
    trade['invested'] = max(trade['invested'] - realized_cost, 0.0)
    clear_trade_close_blocked(trade)
    clear_pending_close_state(trade)
    PORTFOLIO_BALANCE += realized_value
    log(f"🪓 [LIVE] Partial close for {trade['id']}. Sold {sold_shares:.4f} shares, remaining {remaining_shares:.4f}. PnL: ${pnl:+.2f}")
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(sync_live_balance())
    except Exception:
        pass

def finalize_trade_close(trade, close_price, reason):
    global PORTFOLIO_BALANCE
    if trade not in PAPER_TRADES:
        return
    return_value = close_price * trade['shares']
    pnl = return_value - trade['invested']
    PORTFOLIO_BALANCE += return_value
    history_trade = {k: v for k, v in trade.items() if k != 'closing_pending'}
    history_trade.pop('close_blocked', None)
    history_trade.pop('close_blocked_reason', None)
    TRADE_HISTORY.append({'pnl': pnl, 'reason': reason, **history_trade})
    PAPER_TRADES.remove(trade)
    AVAILABLE_TRADE_IDS.append(trade['short_id'])
    AVAILABLE_TRADE_IDS.sort()

    icon = "💰" if pnl > 0 else "🩸"
    log(f"{icon} [ID: {trade['short_id']:02d}] SELL {trade['symbol']} {trade['strategy']} [{trade['timeframe']}] ({trade['direction']}) | {reason} | PnL: ${pnl:+.2f}")

    if TRADING_MODE == 'live':
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(sync_live_balance())
        except Exception:
            pass

    TRADE_LOGS_BUFFER.append((
        trade['id'], trade['market_id'], f"{trade['symbol']}_{trade['timeframe']}",
        trade['strategy'], trade['direction'], trade['invested'],
        trade['entry_price'], trade['entry_time'], close_price, datetime.now().isoformat(), pnl, reason, SESSION_ID
    ))
    cleanup_market_state_if_unused(trade['market_id'])

def handle_market_resolved_event(data):
    market_id = str(data.get('id') or data.get('market_id') or '')
    winning_asset_id = str(data.get('winning_asset_id') or '')
    if not market_id or not winning_asset_id:
        return

    for trade in PAPER_TRADES[:]:
        if str(trade.get('market_id')) != market_id:
            continue

        token_id = get_trade_token_id(trade)
        if not token_id:
            continue

        close_price = 1.0 if str(token_id) == winning_asset_id else 0.0
        outcome = "WON" if close_price > 0 else "LOST"
        close_trade(trade, close_price, f"RESOLVED: {outcome} (Market WS)")

async def live_buy_worker(trade_id, token_id, size_usd, async_client: AsyncClobClient):
    buy_args = MarketOrderArgs(token_id=token_id, amount=size_usd, side="BUY")
    try:
        log(f"⚡ [LIVE] Dispatching FAK BUY Order for {trade_id}...")
        buy_res = await async_client.execute_market_order(buy_args)
        
        if not buy_res.get("success"):
            error_msg = str(buy_res).lower()
            if "no orders found to match" in error_msg:
                log(f"⚠️ [LIVE] Liquidity sniped by faster bot for {trade_id}. FAK order dropped gracefully.")
            else:
                log(f"⚠️ [LIVE] BUY Order rejected for {trade_id}: {buy_res.get('error_message', 'Unknown Error')}")
            
            rollback_failed_trade(trade_id)
            return

        raw_taking = float(buy_res.get("takingAmount", 0)) 
        raw_making = float(buy_res.get("makingAmount", 0)) 
        
        shares_bought = raw_taking / 1_000_000.0 if raw_taking > 1000 else raw_taking
        usd_spent = raw_making / 1_000_000.0 if raw_making > 1000 else raw_making

        if shares_bought <= 0 or usd_spent <= 0:
            log(f"⚠️ [LIVE] BUY FAK matched 0 shares (Dry Book) for {trade_id}. Order cancelled.")
            rollback_failed_trade(trade_id)
            return

        global PORTFOLIO_BALANCE
        for trade in PAPER_TRADES:
            if trade['id'] == trade_id:
                refund = trade['invested'] - usd_spent
                trade['invested'] = usd_spent
                trade['shares'] = shares_bought
                trade['entry_price'] = usd_spent / shares_bought if shares_bought > 0 else trade['entry_price']
                trade['clob_order_id'] = buy_res.get("orderID", "")
                trade['token_id'] = token_id 
                
                if refund > 0.01:
                    PORTFOLIO_BALANCE += refund
                    log(f"🔄 [LIVE] Partial Fill Sync! Refunded to portfolio: ${refund:.2f}")
                break
                
        log(f"✅ [LIVE] BUY matched for {trade_id}. Bought: {shares_bought:.4f} shares for ${usd_spent:.4f}")
        asyncio.create_task(sync_live_balance())

    except Exception as e:
        err_str = str(e).lower()
        if "request exception" in err_str or "timeout" in err_str:
            log(f"🔌 [LIVE] Network timeout / API dropped request for {trade_id}. Rolling back safely.")
        elif "no orders found" in err_str:
             log(f"⚠️ [LIVE] Liquidity sniped by faster bot for {trade_id}. FAK order dropped gracefully.")
        else:
            log_error(f"Live BUY Fatal Exception {trade_id}", e)
            
        rollback_failed_trade(trade_id)

async def live_sell_worker(trade_id, token_id, expected_gross_shares, async_client: AsyncClobClient):
    try:
        net_shares = 0.0
        min_threshold = expected_gross_shares * 0.85
        log(f"⚡ [LIVE] Awaiting net balance sync for {trade_id}...")
        
        for attempt in range(10):
            await asyncio.sleep(2.0)
            net_shares = await async_client.get_actual_balance(token_id)
            if net_shares >= min_threshold:
                log(f"✅ [LIVE] Net balance synchronized: {net_shares:.6f} shares (Attempt {attempt+1})")
                break
                
        sell_amount = min(net_shares, expected_gross_shares)
        if sell_amount <= 0:
            log_error(f"LIVE SELL Error {trade_id}", Exception("Insufficient net balance to sell."))
            rollback_failed_close(trade_id, "No synced balance available for SELL.")
            return

        normalized_sell_amount = normalize_market_sell_shares(sell_amount)
        if normalized_sell_amount < MIN_MARKET_SELL_SHARES:
            trade = get_trade_by_id(trade_id)
            reason = (
                f"Position size {sell_amount:.6f} shares is below the minimum sellable size "
                f"({MIN_MARKET_SELL_SHARES:.2f}) after SDK rounding."
            )
            if trade:
                mark_trade_close_blocked(trade, reason)
            else:
                rollback_failed_close(trade_id, reason)
            return

        sell_args = MarketOrderArgs(token_id=token_id, amount=normalized_sell_amount, side="SELL")
        log(f"⚡ [LIVE] Dispatching FAK SELL Order ({normalized_sell_amount:.6f} shares) for {trade_id}...")
        sell_res = await async_client.execute_market_order(sell_args)
        
        if sell_res.get("success"):
            clob_order_id = sell_res.get("orderID", "")
            for trade in PAPER_TRADES:
                if trade['id'] == trade_id:
                    trade['close_clob_order_id'] = clob_order_id
                    trade['close_submitted_ts'] = time.time()
                    clear_trade_close_blocked(trade)
                    break
            
            raw_taking = float(sell_res.get("takingAmount", 0)) 
            raw_making = float(sell_res.get("makingAmount", 0)) 
            
            usd_received = raw_taking / 1_000_000.0 if raw_taking > 1000 else raw_taking
            shares_sold = raw_making / 1_000_000.0 if raw_making > 1000 else raw_making
            
            log(f"✅ [LIVE] SELL FAK dispatched for {trade_id}. Received: ${usd_received:.4f} for {shares_sold:.4f} shares.")
            trade = get_trade_by_id(trade_id)
            if not trade:
                return
            if shares_sold <= 0 or usd_received < 0:
                rollback_failed_close(trade_id, "SELL response returned no filled shares.")
                return
            trade['pending_close_reported_shares'] = shares_sold
            trade['pending_close_reported_value'] = usd_received
            trade['pending_close_reported_price'] = usd_received / shares_sold if shares_sold > 0 else trade.get('pending_close_price', 0.0)
        else:
            error_msg = str(sell_res).lower()
            if "no orders found to match" in error_msg:
                log(f"⚠️ [LIVE] Liquidity sniped by faster bot while selling {trade_id}. FAK order dropped gracefully.")
                rollback_failed_close(trade_id, "Liquidity disappeared before SELL matched.")
            else:
                log(f"⚠️ [LIVE] SELL Order rejected for {trade_id}: {sell_res.get('error_message', 'Unknown Error')}")
                rollback_failed_close(trade_id, sell_res.get('error_message', 'Unknown Error'))
    except Exception as e:
        err_str = str(e).lower()
        if "request exception" in err_str or "timeout" in err_str:
            log(f"🔌 [LIVE] Network timeout / API dropped request for SELL {trade_id}.")
        elif "no orders found" in err_str:
             log(f"⚠️ [LIVE] Liquidity sniped by faster bot while selling {trade_id}. FAK order dropped gracefully.")
        elif "invalid amounts" in err_str:
            trade = get_trade_by_id(trade_id)
            reason = (
                f"Exchange rejected SELL because the final maker/taker amounts rounded to zero "
                f"for {expected_gross_shares:.6f} shares."
            )
            if trade:
                mark_trade_close_blocked(trade, reason)
                return
        else:
            log_error(f"Live SELL Fatal Exception {trade_id}", e)
        rollback_failed_close(trade_id, str(e)[:120])

# ==========================================
# 0. SYSTEM LOGGING & HELPERS
# ==========================================
def log(msg):
    timestamped = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    RECENT_LOGS.append(timestamped)
    if len(RECENT_LOGS) > 10:
        RECENT_LOGS.pop(0)
    LOCAL_STATE['recent_logs'] = RECENT_LOGS[:]
    SESSION_LOG_LINES.append(timestamped)


def build_session_log_text(trigger_reason="scheduled 5m refresh"):
    snapshot = STATE_STORE.snapshot().dict() if STATE_STORE else None
    lines = [
        "Watcher session log",
        f"session_id: {SESSION_ID}",
        f"written_at: {datetime.now().isoformat()}",
        f"trigger: {trigger_reason}",
        "",
    ]

    if snapshot:
        session = snapshot["session"]
        lines.extend(
            [
                "[session]",
                f"startup_mode: {session['startup_mode']}",
                f"operation_mode: {session['operation_mode']}",
                f"execution_enabled: {session['execution_enabled']}",
                f"duration: {session['duration_label']}",
                f"cash_balance: {session['cash_balance']:.4f}",
                f"invested_value: {session['invested_value']:.4f}",
                f"portfolio_value: {session['total_portfolio_value']:.4f}",
                f"pnl_value: {session['pnl_value']:.4f}",
                f"pnl_percent: {session['pnl_percent']:.2f}",
                "",
                "[services]",
            ]
        )
        for service in snapshot["services"]:
            lines.append(f"{service['name']}: {service['status']} | {service['details']}")
        lines.extend(["", "[markets]"])
        for market in snapshot["markets"]:
            lines.append(
                f"{market['title']} | status={market['status']} | market_id={market['market_id'] or '-'} "
                f"| live={market['live_price']:.4f} | delta={market['delta']:.4f} "
                f"| positions={market['open_positions']}"
            )
        lines.extend(["", "[positions]"])
        if snapshot["positions"]:
            for position in snapshot["positions"]:
                lines.append(
                    f"#{position['short_id']:02d} {position['market_title']} | {position['strategy']} | "
                    f"{position['direction']} | entry={position['entry_price']:.4f} "
                    f"| current={position['current_price']:.4f} | value={position['current_value']:.4f} "
                    f"| pnl={position['pnl_value']:.4f} ({position['pnl_percent']:.2f}%) "
                    f"| close_blocked={position['close_blocked']}"
                )
        else:
            lines.append("none")
        lines.extend(["", "[strategies]"])
        for strategy in snapshot["strategies"]:
            lines.append(
                f"{strategy['market_title']} | {strategy['strategy_label']} | enabled={strategy['enabled']} "
                f"| execution_active={strategy['execution_active']} | open_positions={strategy['open_positions']} "
                f"| pnl={strategy['pnl_value']:.4f} ({strategy['pnl_percent']:.2f}%)"
            )
        lines.append("")

    lines.append("[active_errors]")
    if SESSION_ERROR_LOGS:
        lines.extend(SESSION_ERROR_LOGS)
    else:
        lines.append("none")
    lines.extend(["", "[session_events]"])
    if SESSION_LOG_LINES:
        lines.extend(SESSION_LOG_LINES)
    else:
        lines.append("none")
    lines.append("")
    return "\n".join(lines)


def write_session_log(trigger_reason="scheduled 5m refresh"):
    os.makedirs(SESSION_EXPORT_DIR, exist_ok=True)
    with open(SESSION_LOG_PATH, "w", encoding="utf-8") as f:
        f.write(build_session_log_text(trigger_reason))
    return SESSION_LOG_PATH

def log_error(context_msg, e):
    ts = datetime.now().strftime('%H:%M:%S')
    err_name = type(e).__name__
    err_msg = str(e).replace('\n', ' ')[:80]
    error_line = f"[{ts}] {context_msg} | {err_name}: {err_msg}"
    ACTIVE_ERRORS.insert(0, error_line)
    if len(ACTIVE_ERRORS) > 3:
        ACTIVE_ERRORS.pop()
    SESSION_ERROR_LOGS.append(error_line)
    SESSION_ERROR_LOGS.append(traceback.format_exc().rstrip())

def perform_tech_dump():
    try:
        os.makedirs(TECH_DUMP_DIR, exist_ok=True)
        filename = os.path.join(
            TECH_DUMP_DIR,
            f"tech_dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        )
        
        dump_data = {
            "timestamp": datetime.now().isoformat(),
            "session_id": SESSION_ID,
            "portfolio_balance": PORTFOLIO_BALANCE,
            "paused_markets": list(LOCAL_STATE['paused_markets']),
            "market_status": LOCAL_STATE.get('market_status', {}),
            "locked_prices": LOCKED_PRICES,
            "market_cache": MARKET_CACHE,
            "active_markets": ACTIVE_MARKETS,
            "pre_warming_markets": PRE_WARMING_MARKETS,
            "live_market_data": LIVE_MARKET_DATA,
            "paper_trades": PAPER_TRADES,
            "binance_live": LOCAL_STATE['binance_live_price'],
            "recent_logs": RECENT_LOGS,
            "active_errors": ACTIVE_ERRORS
        }
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(dump_data, f, indent=4)
            
        sys.stdout.write('\a') 
        sys.stdout.flush()
        log(f"\033[1m\033[32m💾 PANIC DUMP SUCCESS: Memory state saved to {filename}\033[0m")
    except Exception as e:
        log_error("Tech Dump Error", e)


def dump_session_log():
    path = write_session_log("manual dashboard dump")
    log(f"📝 [SESSION LOG] Snapshot written to {path}")
    return f"Session log saved to {path}"

def close_manual_trade(short_id):
    for trade in PAPER_TRADES[:]:
        if trade['short_id'] == short_id:
            live_bid = LIVE_MARKET_DATA.get(trade['market_id'], {}).get(f"{trade['direction']}_SELL", 0.0)
            close_trade(trade, live_bid, "MANUAL OVERRIDE CLOSE")
            log(f"⚡ [MANUAL] Option ID {short_id:02d} explicitly closed.")
            return f"Closed option ID {short_id:02d}"
    message = f"Open Option ID {short_id:02d} not found."
    log(f"⚠️ [MANUAL] {message}")
    raise KeyError(message)

def close_trade_by_trade_id(trade_id):
    for trade in PAPER_TRADES[:]:
        if trade['id'] == trade_id:
            live_bid = LIVE_MARKET_DATA.get(trade['market_id'], {}).get(f"{trade['direction']}_SELL", 0.0)
            close_trade(trade, live_bid, "MANUAL OVERRIDE CLOSE")
            log(f"⚡ [MANUAL] Trade {trade_id} explicitly closed.")
            return f"Trade {trade_id} closed."
    raise KeyError(f"Trade {trade_id} not found.")

def close_market_trades(selector, reason="MANUAL MARKET CLOSE"):
    tf_key = normalize_market_selector(selector)
    if not tf_key:
        raise KeyError(f"Invalid market key '{selector}'.")
        
    closed_count = 0
    for trade in PAPER_TRADES[:]:
        if f"{trade['symbol']}_{trade['timeframe']}" == tf_key:
            live_bid = LIVE_MARKET_DATA.get(trade['market_id'], {}).get(f"{trade['direction']}_SELL", 0.0)
            close_trade(trade, live_bid, reason)
            closed_count += 1
            
    ui_key = get_ui_key_by_market_tf_key(tf_key) or selector
    if closed_count > 0:
        message = f"Dumped {closed_count} positions for market [{ui_key}] {tf_key}."
        log(f"⚡ [MANUAL] {message}")
        return message

    message = f"No open positions found for market [{ui_key}] {tf_key}."
    log(f"ℹ️ [MANUAL] {message}")
    return message

def stop_market(selector):
    tf_key = normalize_market_selector(selector)
    if not tf_key:
        raise KeyError(f"Invalid market key '{selector}'.")
    MANUALLY_PAUSED_MARKETS.add(tf_key)
    LOCAL_STATE['paused_markets'].add(tf_key)
    LOCAL_STATE['market_status'][tf_key] = "[paused] [manual]"
    close_market_trades(tf_key, reason="MARKET STOP (EMERGENCY LIQUIDATION)")
    message = f"Market {tf_key} operations paused."
    log(f"🛑 [MANUAL] {message}")
    return message

def restart_market(selector):
    tf_key = normalize_market_selector(selector)
    cfg = get_config_by_market_key(tf_key)
    if not tf_key or not cfg:
        raise KeyError(f"Invalid market key '{selector}'.")
    if not has_tradeable_strategies(cfg):
        LOCAL_STATE['paused_markets'].add(tf_key)
        LOCAL_STATE['market_status'][tf_key] = "[paused] [no strategies]"
        message = f"Market {tf_key} remains paused. No strategies assigned."
        log(f"ℹ️ [MANUAL] {message}")
        return message
    if not MODE_MANAGER.execution_enabled:
        LOCAL_STATE['paused_markets'].add(tf_key)
        LOCAL_STATE['market_status'][tf_key] = "[paused] [observe-only]"
        message = f"Market {tf_key} cannot resume while observe-only mode is active."
        log(f"ℹ️ [MANUAL] {message}")
        return message
    MANUALLY_PAUSED_MARKETS.discard(tf_key)
    refresh_market_runtime_flags()
    message = f"Market {tf_key} resumed."
    log(f"▶️ [MANUAL] {message}")
    return message

def set_strategy_enabled(selector, strategy_key, enabled):
    tf_key = normalize_market_selector(selector)
    cfg = get_config_by_market_key(tf_key)
    if not tf_key or not cfg:
        raise KeyError(f"Invalid market key '{selector}'.")
    if strategy_key not in STRATEGY_KEYS:
        raise ValueError(f"Unsupported strategy '{strategy_key}'.")
    if not cfg.get(strategy_key):
        raise ValueError(f"Strategy '{strategy_key}' is not configured for {tf_key}.")

    strategy_ref = (tf_key, strategy_key)
    if enabled:
        DISABLED_STRATEGIES.discard(strategy_ref)
    else:
        DISABLED_STRATEGIES.add(strategy_ref)

    refresh_market_runtime_flags()
    state = "enabled" if enabled else "disabled"
    message = f"Strategy {strategy_key} for {tf_key} is now {state}."
    log(f"🎚️ [MANUAL] {message}")
    return message

def handle_stdin():
    cmd = sys.stdin.readline().strip().lower().replace(" ", "")
    
    try:
        if cmd == 'q': 
            asyncio.create_task(liquidate_all_and_quit())
        elif cmd == 'd':
            perform_tech_dump()
            dump_session_log()
        elif cmd.startswith('o') and cmd[1:].isdigit():
            close_manual_trade(int(cmd[1:]))
        elif cmd.startswith('ms') and len(cmd) == 3:
            stop_market(cmd[2])
        elif cmd.startswith('mr') and len(cmd) == 3:
            restart_market(cmd[2])
        elif cmd.startswith('m') and len(cmd) == 2:
            close_market_trades(cmd[1])
    except (KeyError, ValueError) as e:
        log(f"⚠️ [MANUAL] {e}")

# ==========================================
# 1. DATABASE (ASYNC)
# ==========================================
async def init_db():
    set_service_status('database', 'starting', 'Opening SQLite database')
    async with aiosqlite.connect('data/polymarket.db') as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS market_logs_v11 (
            id INTEGER PRIMARY KEY AUTOINCREMENT, timeframe TEXT, market_id TEXT,
            target_price REAL, live_price REAL, 
            buy_up REAL, buy_up_vol REAL, sell_up REAL, sell_up_vol REAL, up_obi REAL,
            buy_down REAL, buy_down_vol REAL, sell_down REAL, sell_down_vol REAL, dn_obi REAL, 
            fetched_at TEXT, session_id TEXT)''')
        
        try: await db.execute("ALTER TABLE market_logs_v11 ADD COLUMN session_id TEXT")
        except Exception: pass
        
        await db.execute('''CREATE TABLE IF NOT EXISTS trade_logs_v10 (
            trade_id TEXT PRIMARY KEY, market_id TEXT, timeframe TEXT, strategy TEXT, direction TEXT,
            invested REAL, entry_price REAL, entry_time TEXT, exit_price REAL, exit_time TEXT, pnl REAL,
            reason TEXT, session_id TEXT)''')
        await db.execute('''CREATE TABLE IF NOT EXISTS signal_skip_logs_v1 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timeframe TEXT,
            market_id TEXT,
            strategy_key TEXT,
            trigger_source TEXT,
            reason_code TEXT,
            detail TEXT,
            sec_left REAL,
            live_price REAL,
            target_price REAL,
            delta REAL,
            buy_up REAL,
            buy_down REAL,
            logged_at TEXT,
            session_id TEXT
        )''')
        
        new_columns = [
            "session_id TEXT",
            "exact_execution_price REAL",
            "execution_time_ms INTEGER",
            "market_closed_price REAL",
            "settlement_value REAL"
        ]
        for col in new_columns:
            try: await db.execute(f"ALTER TABLE trade_logs_v10 ADD COLUMN {col}")
            except Exception: pass
            
        await db.commit()
    set_service_status('database', 'ready', 'SQLite schema is ready')

async def flush_to_db():
    global MARKET_LOGS_BUFFER, TRADE_LOGS_BUFFER, SIGNAL_SKIP_LOGS_BUFFER
    if not MARKET_LOGS_BUFFER and not TRADE_LOGS_BUFFER and not SIGNAL_SKIP_LOGS_BUFFER:
        return
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
            if SIGNAL_SKIP_LOGS_BUFFER:
                await db.executemany('''INSERT INTO signal_skip_logs_v1
                    (timeframe, market_id, strategy_key, trigger_source, reason_code, detail, sec_left, live_price, target_price, delta, buy_up, buy_down, logged_at, session_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', SIGNAL_SKIP_LOGS_BUFFER)
            await db.commit()
        log(
            f"💾 BULK SAVE: DB Sync ({len(MARKET_LOGS_BUFFER)} Level 2 ticks, "
            f"{len(TRADE_LOGS_BUFFER)} trades, {len(SIGNAL_SKIP_LOGS_BUFFER)} skip logs)"
        )
        MARKET_LOGS_BUFFER.clear()
        TRADE_LOGS_BUFFER.clear()
        SIGNAL_SKIP_LOGS_BUFFER.clear()
    except Exception as e:
        set_service_status('database', 'error', str(e)[:80])
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
                write_session_log("scheduled 5m market refresh")
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
                OBSERVED_CONFIGS, LOCAL_STATE, ACTIVE_MARKETS, PAPER_TRADES,
                LIVE_MARKET_DATA, PORTFOLIO_BALANCE, INITIAL_BALANCE, RECENT_LOGS, ACTIVE_ERRORS, TRADE_HISTORY
            )
        except Exception as e:
            log_error("Dashboard Renderer", e)
        await asyncio.sleep(1)


async def dashboard_api_worker():
    try:
        import uvicorn
        from dashboard_api import create_dashboard_app
    except ImportError as e:
        set_service_status('dashboard_api', 'disabled', 'Install fastapi and uvicorn to enable web dashboard')
        log(f"⚠️ [DASHBOARD] Web dashboard disabled: {e}")
        while True:
            await asyncio.sleep(3600)

    dashboard_host = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    dashboard_port = int(os.getenv("DASHBOARD_PORT", "8000"))
    app = create_dashboard_app(STATE_STORE, COMMAND_BUS, MODE_MANAGER)
    config = uvicorn.Config(
        app,
        host=dashboard_host,
        port=dashboard_port,
        log_level="warning",
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None
    set_service_status('dashboard_api', 'ready', f"Dashboard listening on {dashboard_host}:{dashboard_port}")
    await server.serve()

async def liquidate_all_and_quit():
    log("🚨 EMERGENCY LIQUIDATION. Selling positions and stopping system...")
    perform_tech_dump() 
    write_session_log("emergency liquidation")
    if PAPER_TRADES:
        for trade in PAPER_TRADES[:]:
            live_bid = LIVE_MARKET_DATA.get(trade['market_id'], {}).get(f"{trade['direction']}_SELL", 0.0)
            close_trade(trade, live_bid, "EMERGENCY CLOSE (Liquidation/Drawdown)")
        await flush_to_db()
    render_dashboard(OBSERVED_CONFIGS, LOCAL_STATE, ACTIVE_MARKETS, PAPER_TRADES, LIVE_MARKET_DATA, PORTFOLIO_BALANCE, INITIAL_BALANCE, RECENT_LOGS, ACTIVE_ERRORS, TRADE_HISTORY)
    os._exit(1)

# ==========================================
# 3. TRADING LOGIC
# ==========================================
def calculate_dynamic_size(base_stake, win_rate, market_id):
    return 1.0

def execute_trade(market_id, timeframe, strategy, direction, base_stake, price, symbol, win_rate, strat_id=""):
    global PORTFOLIO_BALANCE
    if not MODE_MANAGER.execution_enabled:
        return False, "execution_disabled"
    if not AVAILABLE_TRADE_IDS:
        log_error("ID Pool Exhausted", Exception("No free IDs available."))
        return False, "id_pool_exhausted"
    now = time.time()
    if market_id not in TRADE_TIMESTAMPS: TRADE_TIMESTAMPS[market_id] = []
    TRADE_TIMESTAMPS[market_id] = [ts for ts in TRADE_TIMESTAMPS[market_id] if now - ts <= BURST_LIMIT_SEC]
    if len(TRADE_TIMESTAMPS[market_id]) >= BURST_LIMIT_TRADES:
        return False, "burst_limit_reached"
        
    size_usd = calculate_dynamic_size(base_stake, win_rate, market_id)
    
    if price <= 0:
        return False, "invalid_price"
    if size_usd <= 0:
        return False, "invalid_size"
    if size_usd > PORTFOLIO_BALANCE:
        return False, "insufficient_balance"
        
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
        'entry_time_ts': time.time(), 'last_bid_price': 0.0, 'ticks_down': 0,
        'shares': shares, 'invested': size_usd, 'clob_order_id': '', 'token_id': ''
    }
    
    PAPER_TRADES.append(trade)
    TRADE_TIMESTAMPS[market_id].append(now)
    sys.stdout.write('\a'); sys.stdout.flush()
    log(f"✅ [ID: {short_id:02d}] BUY {symbol} {strategy} ({timeframe}) | {direction} | Invested: ${size_usd:.2f} | Price: {price*100:.1f}¢")

    if TRADING_MODE == 'live' and ASYNC_CLOB_CLIENT:
        token_id = None
        if market_id in MARKET_CACHE:
            token_id = MARKET_CACHE[market_id]['up_id'] if direction == 'UP' else MARKET_CACHE[market_id]['dn_id']
        if token_id:
            asyncio.create_task(live_buy_worker(trade_id, token_id, size_usd, ASYNC_CLOB_CLIENT))
    return True, None

def close_trade(trade, close_price, reason):
    if trade.get('closing_pending'):
        return

    if TRADING_MODE == 'live' and ASYNC_CLOB_CLIENT and "Oracle Settlement" not in reason and "RESOLVED" not in reason:
        if trade.get('close_blocked'):
            return
        token_id = get_trade_token_id(trade)
        if not token_id:
            log(f"⚠️ [LIVE] Missing token_id for close of {trade['id']}. Position kept open for monitoring.")
            return
        trade['closing_pending'] = True
        trade['pending_close_price'] = close_price
        trade['pending_close_reason'] = reason
        trade['close_requested_at'] = datetime.now().isoformat()
        trade['close_requested_ts'] = time.time()
        trade['pending_close_initial_shares'] = trade['shares']
        trade['close_confirmed'] = False
        asyncio.create_task(live_sell_worker(trade['id'], token_id, trade['shares'], ASYNC_CLOB_CLIENT))
        return

    finalize_trade_close(trade, close_price, reason)

def resolve_market(market_id, final_asset_price, target_price):
    if TRADING_MODE == 'live':
        return
    is_up_winner = final_asset_price >= target_price
    for trade in PAPER_TRADES[:]:
        if trade['market_id'] == market_id:
            close_price = 1.0 if (trade['direction'] == 'UP' and is_up_winner) or (trade['direction'] == 'DOWN' and not is_up_winner) else 0.0
            close_trade(trade, close_price, "Oracle Settlement (Simulated)")

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

def get_symbol_by_market_id(market_id: str):
    if market_id in MARKET_CACHE:
        return MARKET_CACHE[market_id].get('config', {}).get('symbol', '')
    return ""

async def calibrate_market_offset(market_id: str, strike_price: float):
    symbol = get_symbol_by_market_id(market_id)
    if not symbol: return
    pair = f"{symbol}USDT"
    
    binance_spot = LOCAL_STATE['binance_live_price'].get(pair, 0.0)
    
    if binance_spot > 0:
        calculated_offset = binance_spot - strike_price
        
        if market_id in LOCKED_PRICES:
            LOCKED_PRICES[market_id]['offset'] = calculated_offset
            log(f"🎯 [CALIBRATION] Basis Locked! Symbol: {symbol}, Offset: {calculated_offset:.2f} USD")

# ==========================================
# 4. WEBSOCKETS (BINANCE & CLOB)
# ==========================================
async def binance_ws_listener():
    streams = list(set([cfg['pair'].lower() + "@ticker" for cfg in OBSERVED_CONFIGS]))
    stream_url = "/".join(streams)
    url = f"wss://stream.binance.com:9443/stream?streams={stream_url}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log(f"[WS] Connected to Binance")
                set_service_status('binance_ws', 'connected', 'Streaming live prices from Binance')
                async for msg in ws:
                    data = json.loads(msg)
                    if 'data' in data and 's' in data['data']:
                        pair = data['data']['s']
                        new_price = float(data['data']['c'])
                        current_ts = asyncio.get_event_loop().time()
                        
                        history = LOCAL_STATE['price_history'][pair]
                        history.append((current_ts, new_price))
                        while history and (current_ts - history[0][0]) > 1.0: 
                            history.popleft()

                        if new_price != LOCAL_STATE['binance_live_price'].get(pair, 0.0):
                            LOCAL_STATE['prev_price'][pair] = LOCAL_STATE['binance_live_price'].get(pair, 0.0)
                            LOCAL_STATE['binance_live_price'][pair] = new_price
                        await evaluate_strategies("BINANCE_TICK", pair_filter=pair)
        except Exception as e:
            set_service_status('binance_ws', 'error', str(e)[:80])
            log_error("Binance WebSocket", e)
            await asyncio.sleep(2)

async def polymarket_ws_listener():
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    subscribed_tokens = set()
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log("[WS] Connected to Polymarket CLOB Market Stream")
                set_service_status('polymarket_market_ws', 'connected', 'Streaming Polymarket market data')
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

                            if event_type == 'tick_size_change':
                                t_id = data.get('asset_id')
                                if t_id and ASYNC_CLOB_CLIENT:
                                    ASYNC_CLOB_CLIENT.client.clear_tick_size_cache(t_id)

                            if event_type == 'market_resolved':
                                handle_market_resolved_event(data)
                            
                            if event_type == 'price_change' or 'price_changes' in data:
                                for change in data.get('price_changes', []):
                                    t_id = change.get('asset_id')
                                    if not t_id: continue
                                    if t_id not in LOCAL_STATE['polymarket_books']: LOCAL_STATE['polymarket_books'][t_id] = {'bids': {}, 'asks': {}}
                                    price = float(change.get('price'))
                                    size = float(change.get('size', 0))
                                    side = 'bids' if change.get('side', '').upper() == 'BUY' else 'asks'
                                    LOCAL_STATE['polymarket_books'][t_id][side][price] = size
                            
                            if event_type == 'market_status_change' or 'status' in data:
                                m_status = data.get('status')
                                market_id_resp = data.get('market_id') or data.get('asset_id')
                                if m_status == "CLOSED":
                                    last_p = data.get('last_trade_price', 0.0)
                                    await DB_WORKER.db_queue.put({
                                        "operation": "UPDATE_MARKET_CLOSE",
                                        "data": {"market_id": market_id_resp, "closed_price": last_p}
                                    })
                                elif m_status == "RESOLVED":
                                    s_val = data.get('settlement_value', 0.0)
                                    s_str = "WON" if s_val > 0 else "LOST"
                                    await DB_WORKER.db_queue.put({
                                        "operation": "UPDATE_SETTLEMENT",
                                        "data": {"market_id": market_id_resp, "status": s_str, "value": s_val}
                                    })
                                    for trade in PAPER_TRADES[:]:
                                        if trade.get('market_id') == market_id_resp or trade.get('token_id') == market_id_resp:
                                            if trade.get('token_id') == market_id_resp:
                                                close_trade(trade, s_val, f"RESOLVED: {s_str} (Polymarket Oracle)")
                                            elif trade.get('market_id') == market_id_resp:
                                                close_trade(trade, s_val, f"RESOLVED: {s_str} (Fallback Oracle)")
                                    
                        await evaluate_strategies("CLOB_TICK")
                finally: queue_task.cancel()
        except Exception as e:
            if "no close frame received or sent" not in str(e):
                set_service_status('polymarket_market_ws', 'error', str(e)[:80])
                log_error("Polymarket CLOB", e)
            await asyncio.sleep(0.1)

async def user_ws_listener():
    if TRADING_MODE != 'live':
        set_service_status('polymarket_user_ws', 'disabled', 'User stream disabled outside live mode')
        return
        
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    while not ASYNC_CLOB_CLIENT or not ASYNC_CLOB_CLIENT.client.creds:
        await asyncio.sleep(1)
        
    creds = ASYNC_CLOB_CLIENT.client.creds
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log("[WS] Connected to Polymarket CLOB User Stream")
                set_service_status('polymarket_user_ws', 'connected', 'Authenticated user stream is active')
                auth_payload = {
                    "auth": {
                        "apiKey": creds.api_key,
                        "secret": creds.api_secret,
                        "passphrase": creds.api_passphrase
                    },
                    "type": "user"
                }
                await ws.send(json.dumps(auth_payload))
                
                async for msg in ws:
                    if not msg.startswith(('{', '[')): continue
                    try: parsed_msg = json.loads(msg)
                    except: continue
                    
                    if not isinstance(parsed_msg, list): parsed_msg = [parsed_msg]
                    
                    for data in parsed_msg:
                        event_type = data.get('event_type')
                        if event_type == 'trade':
                            status = data.get('status')
                            if status == "MATCHED":
                                maker_orders = data.get('maker_orders', [])
                                if not maker_orders:
                                    continue
                                    
                                exact_price = float(data.get('price', 0))
                                ts_sec = float(data.get('timestamp', 0))
                                taker_order_id = data.get('taker_order_id')
                                
                                for trade in PAPER_TRADES:
                                    if trade.get('clob_order_id') == taker_order_id or any(m.get('order_id') == trade.get('clob_order_id') for m in maker_orders):
                                        log(f"🎯 [USER WS] Trade EXECUTION confirmed for {trade['id']}! Price: {exact_price}")
                                        trade['entry_price'] = exact_price
                                        trade['execution_time_ms'] = int(ts_sec * 1000)
                                        break
                                        
                                    if trade.get('close_clob_order_id') == taker_order_id or any(m.get('order_id') == trade.get('close_clob_order_id') for m in maker_orders):
                                        log(f"🎯 [USER WS] Trade CLOSE confirmed for {trade['id']}! Price: {exact_price}")
                                        trade['close_confirmed'] = True
                                        trade['close_execution_price'] = exact_price
                                        trade['close_confirmation_ts'] = int(ts_sec * 1000)
                                        break
                                        
        except Exception as e:
            if "no close frame received or sent" not in str(e):
                set_service_status('polymarket_user_ws', 'error', str(e)[:80])
                log_error("Polymarket User WS", e)
            await asyncio.sleep(2)

async def live_position_reconciliation_worker():
    if TRADING_MODE != 'live' or not ASYNC_CLOB_CLIENT:
        return

    last_orphan_scan = 0.0
    while True:
        try:
            await asyncio.sleep(2.0)

            for trade in PAPER_TRADES[:]:
                token_id = trade.get('token_id')
                if not token_id:
                    continue

                actual_balance = await ASYNC_CLOB_CLIENT.get_actual_balance(token_id)

                if trade.get('close_blocked') and can_trade_submit_close(trade):
                    clear_trade_close_blocked(trade)
                    log(
                        f"♻️ [LIVE] Close re-enabled for {trade['id']} after trade size recovered to "
                        f"{trade['shares']:.6f} shares."
                    )

                if trade.get('closing_pending'):
                    original_shares = trade.get('pending_close_initial_shares', trade['shares'])
                    elapsed = time.time() - trade.get('close_submitted_ts', trade.get('close_requested_ts', time.time()))
                    confirmed = trade.get('close_confirmed', False)
                    reported_shares = max(trade.get('pending_close_reported_shares', 0.0), 0.0)
                    remaining_from_report = max(original_shares - reported_shares, 0.0)
                    has_trade_fill_report = reported_shares > POSITION_DUST_SHARES

                    if has_trade_fill_report:
                        if confirmed or elapsed >= CLOSE_RECONCILE_GRACE_SEC:
                            execution_price = (
                                trade.get('close_execution_price')
                                or trade.get('pending_close_reported_price')
                                or trade.get('pending_close_price', 0.0)
                            )
                            if remaining_from_report <= POSITION_DUST_SHARES:
                                close_reason = trade.get('pending_close_reason', 'Live SELL Filled')
                                finalize_trade_close(trade, execution_price, close_reason)
                            else:
                                close_reason = trade.get('pending_close_reason', 'Live Partial SELL Filled')
                                apply_partial_close_fill(trade, remaining_from_report, execution_price, close_reason)
                        continue

                    if actual_balance <= POSITION_DUST_SHARES:
                        if confirmed or elapsed >= CLOSE_RECONCILE_GRACE_SEC:
                            execution_price = (
                                trade.get('close_execution_price')
                                or trade.get('pending_close_reported_price')
                                or trade.get('pending_close_price', 0.0)
                            )
                            close_reason = trade.get('pending_close_reason', 'Live SELL Filled')
                            finalize_trade_close(trade, execution_price, close_reason)
                        continue

                    if actual_balance < max(original_shares - POSITION_DUST_SHARES, 0.0):
                        same_token_open = [
                            other for other in PAPER_TRADES
                            if other is not trade and other.get('token_id') == token_id
                        ]
                        if same_token_open:
                            if elapsed >= CLOSE_PENDING_TIMEOUT:
                                rollback_failed_close(
                                    trade['id'],
                                    (
                                        "Shared token balance changed without a per-order fill report; "
                                        "keeping the position open to avoid misallocating shares."
                                    ),
                                )
                            continue

                        if confirmed or elapsed >= CLOSE_RECONCILE_GRACE_SEC:
                            execution_price = (
                                trade.get('close_execution_price')
                                or trade.get('pending_close_reported_price')
                                or trade.get('pending_close_price', 0.0)
                            )
                            close_reason = trade.get('pending_close_reason', 'Live Partial SELL Filled')
                            apply_partial_close_fill(trade, actual_balance, execution_price, close_reason)
                        continue

                    if elapsed >= CLOSE_PENDING_TIMEOUT:
                        rollback_failed_close(trade['id'], f"Close confirmation timeout after {elapsed:.1f}s.")

            now = time.time()
            if now - last_orphan_scan < ORPHAN_SCAN_INTERVAL:
                continue
            last_orphan_scan = now

            tracked_tokens = {trade.get('token_id') for trade in PAPER_TRADES if trade.get('token_id')}
            for market_id, cache in list(MARKET_CACHE.items()):
                market_cfg = cache.get('config', {})
                for direction, token_key in (('UP', 'up_id'), ('DOWN', 'dn_id')):
                    token_id = cache.get(token_key)
                    if not token_id or token_id in tracked_tokens:
                        continue
                    balance = await ASYNC_CLOB_CLIENT.get_actual_balance(token_id)
                    if balance <= POSITION_DUST_SHARES:
                        continue
                    trade = create_recovered_trade(
                        market_id,
                        token_id,
                        direction,
                        balance,
                        market_cfg.get('symbol', ''),
                        market_cfg.get('timeframe', '')
                    )
                    if trade:
                        tracked_tokens.add(token_id)
        except Exception as e:
            log_error("Live Position Reconciliation", e)
            await asyncio.sleep(2)

# ==========================================
# 5. MARKET STATE MANAGER (PARALLEL FULL FIX)
# ==========================================
async def fetch_and_track_markets():
    tz_et = pytz.timezone('America/New_York')
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                now_et = datetime.now(tz_et)
                now_ts = int(now_et.timestamp())
                
                for config in OBSERVED_CONFIGS:
                    trading_enabled = is_market_trading_enabled(config)
                    pair = config['pair']
                    live_p = LOCAL_STATE['binance_live_price'].get(pair, 0.0)
                    if live_p == 0.0: continue
                    
                    interval_s = config['interval']
                    current_base_ts = (now_ts // interval_s) * interval_s
                    sec_since_start = now_ts - current_base_ts
                    sec_left = interval_s - sec_since_start
                    timeframe_key = f"{config['symbol']}_{config['timeframe']}"

                    # --- 1. CLOCK UPDATE & EXPIRATION LOGIC (ACTIVE) ---
                    if timeframe_key in ACTIVE_MARKETS:
                        a_m_id = ACTIVE_MARKETS[timeframe_key]['m_id']
                        a_m_expire_ts = ACTIVE_MARKETS[timeframe_key].get('expire_ts', 0)

                        # CRITICAL ROTATION FIX: Compare absolute timestamp instead of oscillating sec_left
                        if now_ts >= a_m_expire_ts:
                            old_target = ACTIVE_MARKETS[timeframe_key]['target']
                            adjusted_final_price = live_p - LOCKED_PRICES.get(a_m_id, {}).get('offset', 0.0)

                            if TRADING_MODE == 'live':
                                log(f"🔔 MARKET EXPIRED [{timeframe_key}]. Waiting for official settlement.")
                            else:
                                log(f"🔔 MARKET EXPIRED [{timeframe_key}]. Resolving (Simulated)...")
                                resolve_market(a_m_id, adjusted_final_price, old_target)

                            LOCAL_STATE.pop(f'timing_{a_m_id}', None)
                            if not has_open_trades_for_market(a_m_id):
                                LIVE_MARKET_DATA.pop(a_m_id, None)
                                LOCKED_PRICES.pop(a_m_id, None)
                                MARKET_CACHE.pop(a_m_id, None)
                            else:
                                log(f"🧷 Retaining expired market cache for {a_m_id} until live positions are settled.")

                            if timeframe_key in PRE_WARMING_MARKETS:
                                pw_m_id = PRE_WARMING_MARKETS[timeframe_key]['m_id']
                                ACTIVE_MARKETS[timeframe_key] = PRE_WARMING_MARKETS[timeframe_key]
                                del PRE_WARMING_MARKETS[timeframe_key]

                                if f'timing_{pw_m_id}' in LOCAL_STATE:
                                    LOCAL_STATE[f'timing_{pw_m_id}']['is_pre_warming'] = False

                                if (
                                    timeframe_key in LOCAL_STATE['paused_markets']
                                    and trading_enabled
                                    and timeframe_key not in MANUALLY_PAUSED_MARKETS
                                ):
                                    LOCAL_STATE['paused_markets'].remove(timeframe_key)
                                LOCAL_STATE['market_status'][timeframe_key] = "[running]" if trading_enabled else market_pause_reason(config)
                            else:
                                del ACTIVE_MARKETS[timeframe_key]
                                LOCAL_STATE['paused_markets'].add(timeframe_key)
                                LOCAL_STATE['market_status'][timeframe_key] = "[paused] [searching next]" if trading_enabled else market_pause_reason(config)

                    # --- 2. API FETCHING LOGIC ---
                    is_pre_warming_phase = (sec_left <= 60) and (sec_left > 0)
                    target_base_ts = current_base_ts + interval_s if is_pre_warming_phase else current_base_ts

                    needs_active_fetch = (timeframe_key not in ACTIVE_MARKETS) and (sec_left > 0)
                    needs_pw_fetch = is_pre_warming_phase and (timeframe_key not in PRE_WARMING_MARKETS)

                    target_ts_to_fetch = None
                    if needs_active_fetch:
                        target_ts_to_fetch = current_base_ts
                        is_fetching_pw = False
                    elif needs_pw_fetch:
                        target_ts_to_fetch = target_base_ts
                        is_fetching_pw = True

                    if target_ts_to_fetch is not None:
                        start_time = datetime.fromtimestamp(target_ts_to_fetch, tz_et)
                        slug_standard = f"{config['symbol'].lower()}-updown-{config['timeframe']}-{target_ts_to_fetch}"
                        candidate_slugs = [slug_standard]

                        if config['timeframe'] == '1h':
                            month_name = start_time.strftime('%B').lower()
                            day = start_time.strftime('%-d')
                            hour_str = start_time.strftime('%-I%p').lower()
                            coin_name = FULL_NAMES.get(config['symbol'], config['symbol'].lower())
                            candidate_slugs.append(f"{coin_name}-up-or-down-{month_name}-{day}-{hour_str}-et")

                        active_slug = None
                        fetched_m_id = None

                        for candidate in candidate_slugs:
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
                                                fetched_m_id = str(m.get('id'))

                                                MARKET_CACHE[fetched_m_id] = {
                                                    'id': fetched_m_id,
                                                    'up_id': clob_ids[outcomes.index("Up")],
                                                    'dn_id': clob_ids[outcomes.index("Down")],
                                                    'config': config,
                                                    'target_base_ts': target_ts_to_fetch,
                                                    'slug': candidate
                                                }

                                                await WS_SUBSCRIPTION_QUEUE.put([clob_ids[0], clob_ids[1]])
                                                active_slug = candidate
                                                break
                            if active_slug:
                                break

                        if fetched_m_id:
                            is_clean_start = is_fetching_pw or (sec_since_start <= 15)
                            LOCKED_PRICES[fetched_m_id] = {
                                'price': live_p, 'base_fetched': True,
                                'last_retry': 0, 'prev_up': 0, 'prev_dn': 0,
                                'is_clean': is_clean_start, 'offset': 0.0
                            }

                            if is_clean_start:
                                log(f"⚡ [LOCAL ORACLE] Base definitively frozen at start: ${live_p} for {active_slug}")
                                asyncio.create_task(calibrate_market_offset(fetched_m_id, live_p))
                            else:
                                log(f"⚠️ [LOCAL ORACLE] Mid-interval join. Base set to ${live_p}. Trading paused until next interval for {active_slug}")
                                if timeframe_key not in LOCAL_STATE['paused_markets']:
                                     LOCAL_STATE['paused_markets'].add(timeframe_key)

                            if is_fetching_pw:
                                PRE_WARMING_MARKETS[timeframe_key] = {'m_id': fetched_m_id, 'target': live_p, 'expire_ts': target_ts_to_fetch + interval_s}
                                log(f"🔥 [PRE-WARMING] Subscribed to incoming market {config['symbol']} {config['timeframe']} before opening!")
                            else:
                                ACTIVE_MARKETS[timeframe_key] = {'m_id': fetched_m_id, 'target': live_p, 'expire_ts': target_ts_to_fetch + interval_s}
                                if is_clean_start and trading_enabled and timeframe_key not in MANUALLY_PAUSED_MARKETS:
                                    if timeframe_key in LOCAL_STATE['paused_markets']:
                                        LOCAL_STATE['paused_markets'].remove(timeframe_key)
                                    LOCAL_STATE['market_status'][timeframe_key] = "[running]"
                                elif not trading_enabled:
                                    LOCAL_STATE['paused_markets'].add(timeframe_key)
                                    LOCAL_STATE['market_status'][timeframe_key] = market_pause_reason(config)
                                else:
                                    LOCAL_STATE['market_status'][timeframe_key] = "[paused] [waiting for next]"

                    # --- 3. TIMING & STATE UPDATES FOR ENGINE ---
                    for state_dict, is_pw in [(ACTIVE_MARKETS, False), (PRE_WARMING_MARKETS, True)]:
                        if timeframe_key in state_dict:
                            m_id = state_dict[timeframe_key]['m_id']
                            m_data = LOCKED_PRICES.get(m_id, {})

                            # Absolute time tracking based on the market's true expiration timestamp
                            m_expire_ts = state_dict[timeframe_key].get('expire_ts', current_base_ts + interval_s)
                            m_start_ts = m_expire_ts - interval_s
                            m_sec_since_start = now_ts - m_start_ts
                            m_sec_left = interval_s - m_sec_since_start

                            LOCAL_STATE[f'timing_{m_id}'] = {
                                'sec_left': m_sec_left, 'sec_since_start': m_sec_since_start,
                                'interval_s': interval_s, 'timeframe': config['timeframe'],
                                'symbol': config['symbol'], 'pair': pair,
                                'm_data': m_data, 'config': config,
                                'is_pre_warming': is_pw
                            }

                            if not is_pw and m_id in MARKET_CACHE:
                                up_id = MARKET_CACHE[m_id]['up_id']
                                dn_id = MARKET_CACHE[m_id]['dn_id']
                                s_up, s_up_vol, b_up, b_up_vol, up_obi = extract_orderbook_metrics(up_id)
                                s_dn, s_dn_vol, b_dn, b_dn_vol, dn_obi = extract_orderbook_metrics(dn_id)

                                LIVE_MARKET_DATA[m_id] = {'UP_SELL': b_up, 'DOWN_SELL': b_dn}
                                current_offset = m_data.get('offset', 0.0)
                                adjusted_live_p = live_p - current_offset

                                MARKET_LOGS_BUFFER.append((
                                    f"{config['symbol']}_{config['timeframe']}", m_id, m_data.get('price', 0.0),
                                    adjusted_live_p, b_up, b_up_vol, s_up, s_up_vol, up_obi,
                                    b_dn, b_dn_vol, s_dn, s_dn_vol, dn_obi,
                                    datetime.now().isoformat(), SESSION_ID
                                ))

            except Exception as e:
                log_error("Market State Manager", e)
            refresh_market_runtime_flags()
            await asyncio.sleep(CHECK_INTERVAL)

# ==========================================
# 6. STRATEGY ENGINE & TRADE MANAGEMENT
# ==========================================
async def evaluate_strategies(trigger_source, pair_filter=None):
    try:
        for m_id, cache in MARKET_CACHE.items():
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
            is_paused = timeframe_key in LOCAL_STATE['paused_markets']
            
            live_p = LOCAL_STATE['binance_live_price'].get(pair, 0.0)
            
            if live_p == 0.0: continue
            
            current_offset = m_data.get('offset', 0.0)
            synthetic_oracle_price = live_p - current_offset
            adj_delta = synthetic_oracle_price - m_data['price']
            _, _, b_up, _, _ = extract_orderbook_metrics(cache['up_id'])
            _, _, b_dn, _, _ = extract_orderbook_metrics(cache['dn_id'])

            def note_skip(strategy_key, reason_code, detail=""):
                record_signal_skip(
                    market_id=m_id,
                    timeframe=f"{symbol}_{timeframe}",
                    strategy_key=strategy_key,
                    trigger_source=trigger_source,
                    reason_code=reason_code,
                    detail=detail,
                    sec_left=sec_left,
                    live_price=synthetic_oracle_price,
                    target_price=m_data.get('price', 0.0),
                    delta=adj_delta,
                    buy_up=b_up,
                    buy_down=b_dn,
                )
            
            sanity_limit = SANITY_THRESHOLDS.get(symbol, 0.05)
            if m_data['price'] > 0 and abs(adj_delta) / m_data['price'] > sanity_limit:
                for strategy_key in STRATEGY_KEYS:
                    if is_strategy_enabled(config, strategy_key):
                        note_skip(strategy_key, "sanity_limit_exceeded", f"delta_ratio={abs(adj_delta) / m_data['price']:.4f}>{sanity_limit:.4f}")
                continue 
            
            # =====================================================================
            # MICRO-PROTECTION ENGINE
            # =====================================================================
            for trade in PAPER_TRADES[:]:
                if trade['market_id'] != m_id: continue
                if trade.get('closing_pending'): continue
                current_bid = b_up if trade['direction'] == 'UP' else b_dn
                entry_p = trade['entry_price']
                
                if current_bid <= 0.0: 
                    continue 
                
                pnl_ratio = current_bid / entry_p if entry_p > 0 else 0

                if pnl_ratio <= 0.50 and trade['strategy'] != "OTM Bargain":
                    close_trade(trade, current_bid, "Hard Stop Loss (-50% PNL)")
                    continue

                if current_bid < entry_p:
                    if 'loss_countdown' not in trade:
                        trade['loss_countdown'] = time.time()
                        log(f"⚠️ [ID: {trade['short_id']:02d}] {trade['strategy']} below breakeven. {LOSS_RECOVERY_TIMEOUT:.0f}s recovery countdown started.")
                    elif time.time() - trade['loss_countdown'] >= LOSS_RECOVERY_TIMEOUT:
                        close_trade(trade, current_bid, f"Loss Recovery Timeout ({LOSS_RECOVERY_TIMEOUT:.0f}s below breakeven)")
                        continue
                else:
                    trade.pop('loss_countdown', None)

                # Standard Take Profit
                if pnl_ratio >= 2.0 and trade['strategy'] != "OTM Bargain" and trade['strategy'] != "Kinetic Sniper":
                    close_trade(trade, current_bid, "Global Take Profit (+100% PNL)")
                    continue
                
                # Expiry Safe Evac 
                if 0 < sec_left <= PROFIT_SECURE_SEC:
                    if current_bid > entry_p:
                        close_trade(trade, current_bid, f"Securing profits before expiry ({sec_left:.1f}s left)")
                    continue 

                if trade['strategy'] == "1-Min Mom":
                    time_held = time.time() - trade.get('entry_time_ts', time.time())
                    last_bid = trade.get('last_bid_price', 0.0)
                    if current_bid < last_bid and current_bid > entry_p:
                        trade['ticks_down'] = trade.get('ticks_down', 0) + 1
                    elif current_bid > last_bid:
                        trade['ticks_down'] = 0
                    trade['last_bid_price'] = current_bid

                    if time_held <= MOMENTUM_TP1_WINDOW and pnl_ratio >= 1.20:
                        close_trade(trade, current_bid, "Momentum TP (+20% PNL)")
                        continue

                    if time_held >= MOMENTUM_TP1_WINDOW and current_bid > entry_p:
                        close_trade(trade, current_bid, f"Momentum Profit Lock ({time_held:.1f}s)")
                        continue

                    if trade.get('ticks_down', 0) >= 1 and current_bid > entry_p and time_held >= 3.0:
                        close_trade(trade, current_bid, "Momentum Reversal (1 Tick Down)")
                        continue

                    if time_held >= MOMENTUM_MAX_HOLD:
                        close_trade(trade, current_bid, f"Momentum Max Hold ({MOMENTUM_MAX_HOLD:.0f}s)")
                        continue

                    if pnl_ratio <= MOMENTUM_SL_DROP:
                        close_trade(trade, current_bid, "Momentum Early Stop (-5% PNL)")
                        continue

                # Kinetic Sniper Protection
                if trade['strategy'] == "Kinetic Sniper":
                    time_held = time.time() - trade.get('entry_time_ts', time.time())

                    if time_held <= KINETIC_SNIPER_TP1_WINDOW and pnl_ratio >= 1.50:
                        close_trade(trade, current_bid, "Kinetic TP (+50% PNL)")
                        continue

                    if (
                        KINETIC_SNIPER_TP1_WINDOW < time_held <= KINETIC_SNIPER_TP2_WINDOW
                        and current_bid > entry_p
                    ):
                        close_trade(trade, current_bid, f"Kinetic Profit Lock ({time_held:.1f}s)")
                        continue

                    if time_held >= KINETIC_SNIPER_MAX_HOLD:
                        close_trade(trade, current_bid, f"Kinetic Max Hold ({KINETIC_SNIPER_MAX_HOLD:.0f}s)")
                        continue
                        
                    last_bid = trade.get('last_bid_price', 0.0)
                    if current_bid < last_bid and current_bid > entry_p:
                        trade['ticks_down'] = trade.get('ticks_down', 0) + 1
                    elif current_bid > last_bid:
                        trade['ticks_down'] = 0
                        
                    trade['last_bid_price'] = current_bid
                    
                    if trade.get('ticks_down', 0) >= 1 and current_bid > entry_p and time_held >= 3.0:
                        close_trade(trade, current_bid, "Kinetic Reversal (1 Tick Down)")
                        continue

                    if current_bid <= entry_p * KINETIC_SNIPER_SL_DROP and 'sl_countdown' not in trade:
                        trade['sl_countdown'] = time.time()
                        log(f"⚠️ [ID: {trade['short_id']:02d}] Kinetic -7% drop. 5s countdown started.")
                    
                    if 'sl_countdown' in trade and (time.time() - trade['sl_countdown'] >= KINETIC_SNIPER_TIMEOUT):
                        close_trade(trade, current_bid, f"Kinetic Timeout SL ({KINETIC_SNIPER_TIMEOUT}s)")
                        continue
                    if current_bid > entry_p:
                        trade.pop('sl_countdown', None)

            # =====================================================================
            # SIGNAL GENERATION
            # =====================================================================        
            m_cfg = config.get('mid_arb', {}) if is_strategy_enabled(config, 'mid_arb') else {}
            if m_cfg:
                mid_arb_flag = f"mid_arb_{m_id}"
                if not is_base_fetched:
                    note_skip('mid_arb', 'base_not_fetched')
                elif is_paused:
                    note_skip('mid_arb', 'market_paused', timeframe_key)
                elif not is_clean:
                    note_skip('mid_arb', 'market_not_clean')
                elif mid_arb_flag in EXECUTED_STRAT[m_id]:
                    note_skip('mid_arb', 'already_executed')
                elif not (m_cfg.get('win_end', 0) < sec_left < m_cfg.get('win_start', 0)):
                    note_skip('mid_arb', 'window_miss', f"sec_left={sec_left:.1f}")
                elif adj_delta > m_cfg.get('delta', 0):
                    if 0 < b_up <= m_cfg.get('max_p', 0):
                        executed, failure_reason = execute_trade(
                            m_id, timeframe, "Mid-Game Arb", "UP", 1.0, b_up, symbol, m_cfg.get('wr', 50.0), m_cfg.get('id', '')
                        )
                        if executed:
                            EXECUTED_STRAT[m_id].append(mid_arb_flag)
                        else:
                            note_skip('mid_arb', f"execute_trade_{failure_reason}", f"direction=UP,bid={b_up:.4f}")
                    else:
                        note_skip('mid_arb', 'price_filter_blocked', f"direction=UP,bid={b_up:.4f},max_p={m_cfg.get('max_p', 0):.4f}")
                elif adj_delta < -m_cfg.get('delta', 0):
                    if 0 < b_dn <= m_cfg.get('max_p', 0):
                        executed, failure_reason = execute_trade(
                            m_id, timeframe, "Mid-Game Arb", "DOWN", 1.0, b_dn, symbol, m_cfg.get('wr', 50.0), m_cfg.get('id', '')
                        )
                        if executed:
                            EXECUTED_STRAT[m_id].append(mid_arb_flag)
                        else:
                            note_skip('mid_arb', f"execute_trade_{failure_reason}", f"direction=DOWN,bid={b_dn:.4f}")
                    else:
                        note_skip('mid_arb', 'price_filter_blocked', f"direction=DOWN,bid={b_dn:.4f},max_p={m_cfg.get('max_p', 0):.4f}")
                else:
                    note_skip('mid_arb', 'delta_not_met', f"delta={adj_delta:.4f},limit={m_cfg.get('delta', 0):.4f}")

            otm_cfg = config.get('otm', {}) if is_strategy_enabled(config, 'otm') else {}
            if otm_cfg and otm_cfg.get('wr', 0.0) > 0.0:
                otm_flag = f"otm_{m_id}"
                if not is_base_fetched:
                    note_skip('otm', 'base_not_fetched')
                elif is_paused:
                    note_skip('otm', 'market_paused', timeframe_key)
                elif otm_flag in EXECUTED_STRAT[m_id]:
                    note_skip('otm', 'already_executed')
                elif not (otm_cfg.get('win_end', 0) <= sec_left <= otm_cfg.get('win_start', 0)):
                    note_skip('otm', 'window_miss', f"sec_left={sec_left:.1f}")
                elif abs(adj_delta) >= 40.0:
                    note_skip('otm', 'delta_abs_too_large', f"delta={adj_delta:.4f}")
                elif 0 < b_up <= otm_cfg.get('max_p', 0):
                    executed, failure_reason = execute_trade(
                        m_id, timeframe, "OTM Bargain", "UP", 1.0, b_up, symbol, otm_cfg.get('wr', 50.0), otm_cfg.get('id', '')
                    )
                    if executed:
                        EXECUTED_STRAT[m_id].append(otm_flag)
                    else:
                        note_skip('otm', f"execute_trade_{failure_reason}", f"direction=UP,bid={b_up:.4f}")
                elif 0 < b_dn <= otm_cfg.get('max_p', 0):
                    executed, failure_reason = execute_trade(
                        m_id, timeframe, "OTM Bargain", "DOWN", 1.0, b_dn, symbol, otm_cfg.get('wr', 50.0), otm_cfg.get('id', '')
                    )
                    if executed:
                        EXECUTED_STRAT[m_id].append(otm_flag)
                    else:
                        note_skip('otm', f"execute_trade_{failure_reason}", f"direction=DOWN,bid={b_dn:.4f}")
                else:
                    note_skip('otm', 'price_filter_blocked', f"up={b_up:.4f},down={b_dn:.4f},max_p={otm_cfg.get('max_p', 0):.4f}")

            mom_cfg = config.get('momentum', {}) if is_strategy_enabled(config, 'momentum') else {}
            if mom_cfg:
                if not is_base_fetched:
                    note_skip('momentum', 'base_not_fetched')
                elif is_paused:
                    note_skip('momentum', 'market_paused', timeframe_key)
                elif not is_clean:
                    note_skip('momentum', 'market_not_clean')
                elif 'momentum' in EXECUTED_STRAT[m_id]:
                    note_skip('momentum', 'already_executed')
                elif not (mom_cfg.get('win_end', 0) <= sec_left <= mom_cfg.get('win_start', 0)):
                    note_skip('momentum', 'window_miss', f"sec_left={sec_left:.1f}")
                elif adj_delta >= mom_cfg.get('delta', 0):
                    if 0 < b_up <= mom_cfg.get('max_p', 0):
                        executed, failure_reason = execute_trade(
                            m_id, timeframe, "1-Min Mom", "UP", 1.0, b_up, symbol, mom_cfg.get('wr', 50.0), mom_cfg.get('id', '')
                        )
                        if executed:
                            EXECUTED_STRAT[m_id].append('momentum')
                        else:
                            note_skip('momentum', f"execute_trade_{failure_reason}", f"direction=UP,bid={b_up:.4f}")
                    else:
                        note_skip('momentum', 'price_filter_blocked', f"direction=UP,bid={b_up:.4f},max_p={mom_cfg.get('max_p', 0):.4f}")
                elif adj_delta <= -mom_cfg.get('delta', 0):
                    if 0 < b_dn <= mom_cfg.get('max_p', 0):
                        executed, failure_reason = execute_trade(
                            m_id, timeframe, "1-Min Mom", "DOWN", 1.0, b_dn, symbol, mom_cfg.get('wr', 50.0), mom_cfg.get('id', '')
                        )
                        if executed:
                            EXECUTED_STRAT[m_id].append('momentum')
                        else:
                            note_skip('momentum', f"execute_trade_{failure_reason}", f"direction=DOWN,bid={b_dn:.4f}")
                    else:
                        note_skip('momentum', 'price_filter_blocked', f"direction=DOWN,bid={b_dn:.4f},max_p={mom_cfg.get('max_p', 0):.4f}")
                else:
                    note_skip('momentum', 'delta_not_met', f"delta={adj_delta:.4f},limit={mom_cfg.get('delta', 0):.4f}")

            kin_cfg = config.get('kinetic_sniper', {}) if is_strategy_enabled(config, 'kinetic_sniper') else {}
            if kin_cfg:
                if not is_base_fetched:
                    note_skip('kinetic_sniper', 'base_not_fetched')
                elif trigger_source != "BINANCE_TICK":
                    note_skip('kinetic_sniper', 'trigger_source_mismatch', trigger_source)
                elif is_paused:
                    note_skip('kinetic_sniper', 'market_paused', timeframe_key)
                elif not is_clean:
                    note_skip('kinetic_sniper', 'market_not_clean')
                else:
                    up_change = b_up - m_data['prev_up']
                    dn_change = b_dn - m_data['prev_dn']
                    max_target_dist = kin_cfg.get('max_target_dist', float('inf'))
                    if abs(adj_delta) > max_target_dist:
                        note_skip('kinetic_sniper', 'target_distance_too_far', f"delta={adj_delta:.4f},max_target_dist={max_target_dist:.4f}")
                    else:
                        trigger_pct = kin_cfg.get('trigger_pct', 0.0)
                        max_price = kin_cfg.get('max_price', 0.85)
                        max_slippage = kin_cfg.get('max_slippage', 0.025)
                        history = LOCAL_STATE['price_history'].get(pair, deque())
                        if len(history) < 2:
                            note_skip('kinetic_sniper', 'insufficient_price_history', f"history_len={len(history)}")
                        else:
                            oldest_price = history[0][1]
                            if oldest_price <= 0:
                                note_skip('kinetic_sniper', 'invalid_reference_price')
                            else:
                                price_delta_pct = (live_p - oldest_price) / oldest_price
                                if price_delta_pct >= trigger_pct:
                                    if abs(up_change) > max_slippage:
                                        note_skip('kinetic_sniper', 'slippage_blocked', f"direction=UP,change={abs(up_change):.4f},max={max_slippage:.4f}")
                                    elif not (0 < b_up <= max_price):
                                        note_skip('kinetic_sniper', 'price_filter_blocked', f"direction=UP,bid={b_up:.4f},max_price={max_price:.4f}")
                                    else:
                                        executed, failure_reason = execute_trade(
                                            m_id, timeframe, "Kinetic Sniper", "UP", 1.0, b_up, symbol, kin_cfg.get('wr', 50.0), kin_cfg.get('id', '')
                                        )
                                        if not executed:
                                            note_skip('kinetic_sniper', f"execute_trade_{failure_reason}", f"direction=UP,bid={b_up:.4f}")
                                elif price_delta_pct <= -trigger_pct:
                                    if abs(dn_change) > max_slippage:
                                        note_skip('kinetic_sniper', 'slippage_blocked', f"direction=DOWN,change={abs(dn_change):.4f},max={max_slippage:.4f}")
                                    elif not (0 < b_dn <= max_price):
                                        note_skip('kinetic_sniper', 'price_filter_blocked', f"direction=DOWN,bid={b_dn:.4f},max_price={max_price:.4f}")
                                    else:
                                        executed, failure_reason = execute_trade(
                                            m_id, timeframe, "Kinetic Sniper", "DOWN", 1.0, b_dn, symbol, kin_cfg.get('wr', 50.0), kin_cfg.get('id', '')
                                        )
                                        if not executed:
                                            note_skip('kinetic_sniper', f"execute_trade_{failure_reason}", f"direction=DOWN,bid={b_dn:.4f}")
                                else:
                                    note_skip('kinetic_sniper', 'price_jump_not_met', f"jump={price_delta_pct:.6f},trigger={trigger_pct:.6f}")

            if trigger_source == "BINANCE_TICK" and is_base_fetched:
                m_data['prev_up'], m_data['prev_dn'] = b_up, b_dn
    except Exception as e:
        log_error("Strategy Engine", e)

# ==========================================
# MAIN ORCHESTRATION LOOP
# ==========================================
async def main():
    global INITIAL_BALANCE, PORTFOLIO_BALANCE, LAST_FLUSH_TS, TRADING_MODE, ASYNC_CLOB_CLIENT, STATE_STORE, COMMAND_BUS
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--portfolio', type=float, default=100.0)
    parser.add_argument('--mode', type=str, choices=['paper', 'live', 'observe_only'], default='paper')
    args = parser.parse_args()
    
    INITIAL_BALANCE = args.portfolio
    PORTFOLIO_BALANCE = args.portfolio
    LAST_FLUSH_TS = (int(time.time()) // 300) * 300
    TRADING_MODE = args.mode
    MODE_MANAGER.configure_startup(args.mode)

    STATE_STORE = RuntimeStateStore(
        observed_configs=OBSERVED_CONFIGS,
        get_state=lambda: LOCAL_STATE,
        get_active_markets=lambda: ACTIVE_MARKETS,
        get_pre_warming_markets=lambda: PRE_WARMING_MARKETS,
        get_live_market_data=lambda: LIVE_MARKET_DATA,
        get_trades=lambda: PAPER_TRADES,
        get_trade_history=lambda: TRADE_HISTORY,
        get_balance=lambda: PORTFOLIO_BALANCE,
        get_initial_balance=lambda: INITIAL_BALANCE,
        get_market_cache=lambda: MARKET_CACHE,
        extract_orderbook_metrics=extract_orderbook_metrics,
        mode_manager=MODE_MANAGER,
        is_strategy_enabled=is_strategy_enabled,
        loss_recovery_timeout=LOSS_RECOVERY_TIMEOUT,
        kinetic_timeout=KINETIC_SNIPER_TIMEOUT,
    )
    COMMAND_BUS = CommandBus(
        stop_market=stop_market,
        resume_market=restart_market,
        close_market=close_market_trades,
        close_position=close_trade_by_trade_id,
        set_strategy_enabled=set_strategy_enabled,
        set_mode=switch_operation_mode,
        dump_session_log=dump_session_log,
    )
    
    log(f"🚀 LOCAL ORACLE SYSTEM INITIALIZATION. Session ID: {SESSION_ID}")
    write_session_log("session startup")
    
    if TRADING_MODE == 'live':
        log("🔌 LIVE MODE INITIALIZED. Connecting to Polymarket API...")
        try:
            client = ClobClient(
                host=HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID, 
                signature_type=SIGNATURE_TYPE, funder=PROXY_WALLET_ADDRESS
            )
            ASYNC_CLOB_CLIENT = AsyncClobClient(client)
            creds = await ASYNC_CLOB_CLIENT.init_creds()
            client.set_api_creds(creds)
            MODE_MANAGER.connection_ready = True
            log("✅ Polymarket Live API Authorization Successful.")
            try:
                real_balance = await ASYNC_CLOB_CLIENT.get_collateral_balance()
                if real_balance > 0:
                    INITIAL_BALANCE = real_balance
                    PORTFOLIO_BALANCE = real_balance
                    log(f"💼 [LIVE] Wallet initialized. Starting Capital: ${real_balance:.2f} USDC")
            except Exception as e:
                log_error("Failed to fetch initial collateral. Using CLI param.", e)
        except Exception as e:
            MODE_MANAGER.connection_ready = False
            log_error("CRITICAL: Polymarket API Auth Failed", e)
            os._exit(1)
    elif TRADING_MODE == 'observe_only':
        MODE_MANAGER.connection_ready = False
        LOCAL_STATE['market_status'] = {
            market_key(cfg): "[paused] [observe-only]" if cfg else "[paused]"
            for cfg in OBSERVED_CONFIGS
        }
        LOCAL_STATE['paused_markets'] = {market_key(cfg) for cfg in OBSERVED_CONFIGS}
        log("👁️ Observe-only mode initialized. No new trades will be opened.")
    
    await init_db()
    refresh_market_runtime_flags()
    
    loop = asyncio.get_event_loop()
    try:
        loop.add_reader(sys.stdin.fileno(), handle_stdin)
    except NotImplementedError:
        pass
        
    await asyncio.gather(
        DB_WORKER.start_worker(),
        binance_ws_listener(),
        polymarket_ws_listener(),
        user_ws_listener(),
        live_position_reconciliation_worker(),
        fetch_and_track_markets(),
        ui_updater_worker(),
        dashboard_api_worker(),
        async_smart_flush_worker()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        os._exit(0)
