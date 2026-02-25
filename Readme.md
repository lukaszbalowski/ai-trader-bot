# 👁️ Watcher HFT v10.31 (Enterprise Edition)

**Watcher** to wysoko wydajny, asynchroniczny bot typu High-Frequency Trading (HFT) zaprojektowany specjalnie dla rynków opcji binarnych **Polymarket CLOB**. System operuje na danych w czasie rzeczywistym z Binance (Oracle) oraz Polymarket (Orderbook L2), realizując strategie arbitrażowe i momentum z milisekundową precyzją.



---

## 🚀 Kluczowe Funkcje (v10.31)

- **Dual-Mode Execution**: Możliwość pracy w trybie `Live` (prawdziwe środki) lub `Paper` (bezpieczna symulacja).
- **FAK (Fill-And-Kill) Orders**: Natywna obsługa zleceń FAK, pozwalająca na częściową realizację przy zachowaniu maksymalnej szybkości wejścia.
- **State Synchronization**: Automatyczna korekta stanu portfela na podstawie informacji zwrotnych z API (`takingAmount`/`makingAmount`).
- **Net Flow Polling**: Inteligentne sprawdzanie salda netto na blockchainie Polygon po potrąceniu dynamicznych prowizji (Taker Fees).
- **Asynchroniczna Architektura**: System oparty w całości na `asyncio`, eliminujący blokady procesora przy obsłudze 12 rynków jednocześnie.

---

## 🛠️ Technologie

| Komponent | Technologia |
| :--- | :--- |
| **Language** | Python 3.11 (Asyncio) |
| **Data Stream** | WebSockets (Binance & Polymarket) |
| **Execution** | Polymarket CLOB SDK (FAK Strategy) |
| **Database** | SQLite (Async) |
| **Containerization** | Docker |

---

## 📦 Instalacja i Konfiguracja

### 1. Klonowanie i Środowisko
Upewnij się, że posiadasz zainstalowanego Docker-a. Stwórz plik `.env` w folderze głównym:

```env
PRIVATE_KEY=twój_klucz_prywatny
PROXY_WALLET_ADDRESS=twój_adres_proxy
CHAIN_ID=137
SIGNATURE_TYPE=1

2. Budowa KonteneraBash./watcher.sh build
🎮 Instrukcja OperacyjnaSystem zarządzany jest przez skrypt orkiestracyjny watcher.sh.KomendaOpis./watcher.sh paper 500Startuje bota w trybie symulacji z saldem $500../watcher.sh liveTRYB LIVE: Bot łączy się z portfelem i handluje realnym kapitałem../watcher.sh backtestAnaliza ostatniej sesji i optymalizacja strategii../watcher.sh test-executorSzybki test łączności i egzekucji (Buy/Sell cycle).🛡️ Zarządzanie Ryzykiem (Iron Rules)Global 2-Sec Rule: Blokada otwierania pozycji na 2 sekundy przed zamknięciem rynku.Drawdown Protection: Automatyczne zatrzymanie systemu (Panic Stop) przy spadku kapitału o 30%.Exposure Limit: Maksymalna ekspozycja na pojedynczy rynek to 15% całkowitego salda.Sanity Check: Automatyczne odrzucanie sygnałów, gdy różnica ceny (spread) między giełdami przekracza progi bezpieczeństwa.📊 Monitoring i Panel SterowaniaPodczas pracy bota dostępny jest interaktywny dashboard (renderowany w terminalu), który wyświetla:Status 12 rynków w czasie rzeczywistym.Bieżący PnL (Zrealizowany i Floating).Logi egzekucji Live oraz ewentualne błędy API.⚖️ DisclaimerHandel kryptowalutami i opcjami binarnymi wiąże się z wysokim ryzykiem utraty kapitału. Autor oprogramowania nie ponosi odpowiedzialności za straty finansowe wynikające z błędów w konfiguracji, luk w płynności rynku lub awarii API.Watcher HFT Project Lead | v10.31 Production Ready
### Jak to wdrożyć?
1. Otwórz plik `README.md` w VS Code.
2. Usuń starą treść.
3. Wklej powyższy kod (od `# 👁️ Watcher...` do samego końca).
4. Zrób `git commit -m "docs: finalized professional README for v10.31"` i wypchnij na GitHub.

Dzięki zastosowaniu znaczników `[Image of...]`, GitHub automatycznie przygotuje miejsce pod grafiki, a tabele i bloki kodu `bash` będą miały eleganckie podświetlanie składni.

Czy chciałbyś, abym dołączył do tego opisy konkretnych flag konfiguracyjnych, które można zmieniać w `tracked_configs.json`?