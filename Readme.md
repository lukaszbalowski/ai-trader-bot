🚀 Watcher v10.15 | HFT Polymarket Bot
Watcher to zaawansowany symulator handlu wysokiej częstotliwości (High-Frequency Trading) dla rynków opcji binarnych (Bitcoin Up/Down) na platformie Polymarket. System wykorzystuje architekturę sterowaną zdarzeniami (Event-Driven) i komunikację przez WebSockety, aby reagować na zmiany rynkowe w milisekundach.

🛠 Kluczowe Funkcje
Ultra-Low Latency: Bezpośrednie strumienie danych z Binance (kurs BTC) i Polymarket (Order Book).

Architektura Asynchroniczna: Oparta na asyncio, eliminująca opóźnienia znane z tradycyjnego odpytywania API (Polling).

7 Strategii Handlowych: Od agresywnego skalpowania opóźnień (Lag Sniper) po inteligentne zarządzanie ryzykiem i wyjściem (Safety Cashout).

Smart Snapshot Mechanism: Autorska funkcja "Twardego Restartu", wymuszająca na giełdzie przesyłanie pełnych zrzutów arkusza zleceń przy każdym nowym rynku.

Weryfikacja Wizualna (Playwright): Asynchroniczny sędzia pobierający "Price to beat" bezpośrednio ze strony HTML w tle.

Dockerized: Pełna konteneryzacja zapewniająca stabilność na systemach macOS (ARM/M1/M2) oraz Linux/Windows.

📉 Strategie (7 Filarów)
Wejścia (Entry):
Lag Sniper (HFT): Kupuje opcje w milisekundach po gwałtownym skoku BTC na Binance, zanim Polymarket zaktualizuje ceny.

Straddle & Cut: Otwiera dwie nogi (UP i DOWN) w rynkach bocznych, automatycznie tnąc nierentowną stronę.

1-Min Momentum: Podpięcie pod silny trend na minutę przed zamknięciem rynku.

Deep Snipe: Agresywne wejście większym kapitałem w ostatnich 30 sekundach "pewnych" rynków.

60-Sec Power Snipe: Dołożenie do pozycji przy ekstremalnym odchyleniu (Delta > $100) na minutę przed końcem.

Wyjścia i Bezpieczeństwo (Exit/Safety):
Straddle Early Exit: Realizacja zysku (Take Profit) na poziomie 80% (przy cenie 90¢), aby uniknąć ryzyka końcowego.

2-Second Safety Cashout: Zamykanie zyskownych pozycji na 2 sekundy przed końcem, by wyeliminować ryzyko nagłej manipulacji kursem na zamknięciu (tzw. "The Flip").

⚙️ Instalacja i Uruchomienie
Wymagania:
Docker & Docker Desktop

Git

Szybki start:
Sklonuj repozytorium:

Bash
git clone https://github.com/lukaszbalowski/ai-trader-bot.git
cd ai-trader-bot
Zbuduj kontener:

Bash
docker build -t polymarket-bot .
Uruchom bota (Paper Trading):

Bash
docker run --rm -it --name watcher -v "$(pwd)/data:/app/data" polymarket-bot python main.py --portfolio 500
⌨️ Obsługa w czasie rzeczywistym
Bot jest interaktywny. Podczas działania możesz wpisać w terminalu:

p + Enter: Wyświetla Status Portfela (Mark-to-Market) z uwzględnieniem aktualnych cen z RAM.

q + Enter: Awaryjna Likwidacja – bot sprzedaje wszystkie otwarte pozycje po cenach rynkowych, zapisuje bazę danych i bezpiecznie zamyka system.

📊 Analiza Danych
Wszystkie dane zapisywane są asynchronicznie w folderze data/polymarket.db (SQLite).

market_logs_v10: Historia każdego tiku (ceny Binance, Polymarket, wolumeny).

trade_logs_v10: Szczegółowa historia transakcji z wyliczonym zyskiem/stratą (PnL) i powodem zamknięcia.

⚠️ Zastrzeżenie (Disclaimer)
To oprogramowanie służy wyłącznie do celów edukacyjnych i symulacji (Paper Trading). Handel kryptowalutami i opcjami wiąże się z wysokim ryzykiem utraty kapitału. Autor nie ponosi odpowiedzialności za jakiekolwiek straty finansowe wynikające z użycia tego bota w handlu realnym.

Autor: Łukasz Balowski

Wersja: 10.15 Event-Driven Edition