import asyncio
import logging
import uuid
import os
import json
import aiohttp
from typing import Any
from dotenv import load_dotenv

# Polymarket SDK
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType, BalanceAllowanceParams, AssetType

# ==========================================
# 1. KONFIGURACJA & ŚRODOWISKO
# ==========================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s [%(name)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("WatcherTestExecutor")

HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

PRIVATE_KEY = os.getenv("PRIVATE_KEY")
PROXY_WALLET_ADDRESS = os.getenv("PROXY_WALLET_ADDRESS")
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE", "1")) # Gnosis Safe

# NOWY TARGET RYNKOWY
MARKET_SLUG = "btc-updown-15m-1771958700"

# ==========================================
# 2. ASYNC WRAPPERS (Watcher Thread Isolation)
# ==========================================
class AsyncClobClient:
    def __init__(self, client: ClobClient):
        self.client = client

    async def init_creds(self):
        return await asyncio.to_thread(self.client.create_or_derive_api_creds)

    async def get_actual_balance(self, token_id: str) -> float:
        """Pobiera realne saldo NETTO i normalizuje jednostki atomowe (10^6)."""
        params = BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL,
            token_id=token_id,
            signature_type=SIGNATURE_TYPE
        )
        res = await asyncio.to_thread(self.client.get_balance_allowance, params)
        raw_balance = float(res.get("balance", 0.0))
        return raw_balance / 1_000_000.0

    async def execute_market_order(self, args: MarketOrderArgs):
        order = await asyncio.to_thread(self.client.create_market_order, args)
        return await asyncio.to_thread(self.client.post_order, order, OrderType.FOK)

# ==========================================
# 3. LOGIKA EGZEKUCJI HFT (Verified Net Flow)
# ==========================================
async def run_trade_cycle(async_client: AsyncClobClient, token_id: str, session_id: str):
    logger.info(f"--- START CYKLU v10.31 | Rynek: {MARKET_SLUG} | Sesja: {session_id} ---")

    # KROK 1: KUPNO ($1.00 USD)
    buy_args = MarketOrderArgs(token_id=token_id, amount=1.0, side="BUY")
    try:
        logger.info("Wysyłanie zlecenia BUY (Market FOK)...")
        buy_res = await async_client.execute_market_order(buy_args)
        
        if not buy_res.get("success"):
            logger.error(f"Błąd zakupu: {buy_res}")
            return

        # Pobieramy wartość brutto (przed opłatami) jako punkt odniesienia
        gross_shares = float(buy_res.get("takingAmount", 0))
        logger.info(f"✅ Zakup dopasowany. Brutto: {gross_shares:.6f} udziałów.")

        # KROK 2: WERYFIKACJA SALDA NETTO (Polling z filtrem dustu)
        # Polymarket pobiera opłaty rynkowe (ok. 0.2-1%), więc szukamy salda bliskiego brutto.
        net_shares = 0.0
        min_threshold = gross_shares * 0.90 # Oczekujemy co najmniej 90% zakupionej ilości

        logger.info("Czekam na synchronizację salda netto po odliczeniu opłat...")
        for attempt in range(10): # Max 20 sekund pollingu
            await asyncio.sleep(2.0)
            net_shares = await async_client.get_actual_balance(token_id)
            
            if net_shares >= min_threshold:
                logger.info(f"✅ Wykryto poprawne saldo netto: {net_shares:.6f} (Próba {attempt+1})")
                break
            else:
                logger.warning(f"Próba {attempt+1}: Saldo {net_shares:.6f} poniżej progu {min_threshold:.6f}...")

        if net_shares < min_threshold:
            logger.error("BŁĄD: Przekroczono czas oczekiwania na saldo. Przerwanie sprzedaży.")
            return

        # KROK 3: SPRZEDAŻ (Likwidacja rzeczywistej kwoty po opłatach)
        sell_args = MarketOrderArgs(token_id=token_id, amount=net_shares, side="SELL")
        logger.info(f"Likwidacja pozycji netto ({net_shares:.6f} udziałów)...")
        sell_res = await async_client.execute_market_order(sell_args)
        
        if sell_res.get("success"):
            logger.info(f"✅ CYKL DOMKNIĘTY. Zrealizowano sprzedaż netto.")
        else:
            logger.error(f"❌ BŁĄD SPRZEDAŻY: {sell_res}")
            
    except Exception as e:
        logger.error(f"⚠️ KRYTYCZNA AWARIA SILNIKA: {e}")

# ==========================================
# 4. ORKIESTRACJA & MAIN
# ==========================================
async def get_clob_token_id(slug: str) -> str:
    """Pobiera Token ID bezpośrednio z Gamma API."""
    async with aiohttp.ClientSession() as session:
        url = f"{GAMMA_API}/markets/slug/{slug}"
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                ids = json.loads(data['clobTokenIds']) if isinstance(data['clobTokenIds'], str) else data['clobTokenIds']
                return ids[0]
    return None

async def main():
    session_id = str(uuid.uuid4())[:8]
    
    # Pobranie Token ID
    token_id = await get_clob_token_id(MARKET_SLUG)
    if not token_id:
        logger.error("Nie udało się pobrać ID tokena. Sprawdź czy rynek jest aktywny.")
        return

    # Inicjalizacja klienta Enterprise
    client = ClobClient(
        host=HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID, 
        signature_type=SIGNATURE_TYPE, funder=PROXY_WALLET_ADDRESS
    )
    async_client = AsyncClobClient(client)

    try:
        # Autoryzacja i start
        creds = await async_client.init_creds()
        client.set_api_creds(creds)
        await run_trade_cycle(async_client, token_id, session_id)
    except Exception as e:
        logger.error(f"Błąd uwierzytelniania: {e}")

if __name__ == "__main__":
    asyncio.run(main())