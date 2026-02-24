import os
import requests
from pathlib import Path
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.constants import POLYGON

def load_local_env(env_path: str = ".env"):
    path = Path(env_path)
    if not path.exists(): return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip().strip("'").strip('"')

def main():
    print("\n" + "="*50)
    print(" 🧪 FINALNE PRZEŁAMANIE BLOKADY PODPISU")
    print("="*50)
    
    load_local_env()
    
    private_key = os.getenv("PRIVATE_KEY")
    proxy_wallet = os.getenv("PROXY_WALLET_ADDRESS")
    sig_type = int(os.getenv("SIGNATURE_TYPE", "2"))
    
    # 1. STANDARYZACJA ADRESU (Checksum)
    # Ręczna korekta wielkości liter adresu L1
    try:
        from eth_utils import to_checksum_address
        proxy_wallet = to_checksum_address(proxy_wallet)
    except ImportError:
        pass # Jeśli nie masz eth_utils, biblioteka web3 też to zrobi
    
    TARGET_TOKEN = "74236558180084870415445857757305093153888034235425859246010240565993311833130"
    
    print(f"🔗 Łączenie z portfelem: {proxy_wallet}")
    print(f"🔑 SigType: {sig_type} (2=Safe/Metamask, 1=Magic)")

    client = ClobClient(
        host="https://clob.polymarket.com",
        key=private_key,
        chain_id=137,
        funder=proxy_wallet,
        signature_type=sig_type
    )
    
    try:
        # 2. AUTORYZACJA L2
        client.set_api_creds(client.create_or_derive_api_creds())
        print("✅ Autoryzacja L2 OK!")

        # 3. ZLECENIE LIMIT 0.01$ (Bezpieczne 5$)
        # Używamy create_and_post_order, która sama pobiera feeRateBps z serwera!
        order_args = OrderArgs(
            price=0.01,
            size=500.0,
            side="BUY",
            token_id=TARGET_TOKEN
        )
        
        print("\n🚀 Wysyłanie zlecenia metodą hybrydową (Automatyczne pobieranie opłat)...")
        
        # Ta metoda rozwiązuje problem mismatchu hasha przy opłatach giełdowych
        response = client.create_and_post_order(order_args)
        
        if response and isinstance(response, dict) and response.get('success'):
            print("\n" + "*"*40)
            print("🎉🎉🎉 PEŁEN SUKCES! ZLECENIE PRZYJĘTE 🎉🎉🎉")
            print(f"ID Zlecenia: {response.get('orderID')}")
            print("*"*40)
        else:
            print(f"\n❌ Błąd giełdy: {response}")

    except Exception as e:
        print(f"\n❌ Błąd krytyczny: {e}")
        # Wypiszmy traceback, żeby widzieć dokładnie gdzie pęka
        traceback.print_exc()

if __name__ == "__main__":
    import traceback
    main()