🚀 Watcher v10.27 | Enterprise HFT Polymarket Bot
Watcher to zaawansowany system do handlu wysokiej częstotliwości (High-Frequency Trading) dla rynków opcji binarnych na platformie Polymarket. Wersja v10.27 rozszerza możliwości bota o jednoczesną obsługę 12 rynków w różnych interwałach czasowych (5m, 15m, 1h) oraz wprowadza inteligentne zarządzanie priorytetami weryfikacji.

🛠 Kluczowe Funkcje (v10.27)
Multi-Market Engine: Równoległa obsługa 12 rynków dla BTC, ETH, SOL i XRP w interwałach 5m, 15m oraz 1h.

Priority Verification Queue: System kolejkowania Playwrighta oparty na PriorityQueue. Rynki o krótkim czasie trwania (5m) otrzymują priorytet "P1", co eliminuje opóźnienia w ich weryfikacji.

Dynamic Slug Router: Automatyczne generowanie adresów URL dla rynków standardowych oraz SEO (np. rynki godzinowe), co pozwala na pracę ciągłą 24/7 bez interwencji człowieka.

Smart ID Pool (0-99): Każda otwarta pozycja otrzymuje unikalny identyfikator z kontrolowanej puli, co umożliwia precyzyjne śledzenie transakcji i przygotowuje system pod sterowanie ręczne.

Integrated PnL Dashboard: Nowy interfejs ASCII wyświetlający drzewo operacji bezpośrednio pod każdym rynkiem, z kalkulacją zysku pływającego (Float PnL%) oraz zrealizowanego (Session PnL%).

Advanced Circuit Breakers: Systemy ochrony kapitału: Max Drawdown (-30%), Market Exposure (15%) oraz Burst Guard (ochrona przed nadmiarem sygnałów).

📈 Filarowe Strategie (Grid-Optimized)
System wykorzystuje cztery główne strategie, dostrojone za pomocą milionów symulacji w module Backtestera:

Lag Sniper: Wykorzystuje milisekundowe opóźnienia między giełdą Binance a wyrocznią Polymarketu. Posiada dynamiczne progi czułości dla fazy bazowej i końcowej rynku.

1-Min Momentum: Agresywne podpięcie pod ukształtowany trend w ostatniej minucie trwania rynku.

Mid-Game Arb: Arbitraż statystyczny w środkowej fazie rynku, wykorzystujący błędy w wycenie Market Makerów.

OTM Bargain: Polowanie na skrajnie tanie opcje (2-5 centów) przy dużej zmienności (zablokowane dla rynków 1h ze względu na niski WinRate).

⚙️ Instalacja i Uruchomienie
Szybki start (Docker):
Budowa obrazu:

Bash
docker build -t ai-trader .
Uruchomienie bota (Live Paper Trading):

Bash
docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader
📊 Analiza i Backtesting
Watcher v10.27 dostarcza kompletne środowisko diagnostyczne:

Analiza Post-Mortem: Uruchomienie backtester.py generuje szczegółowy raport z bazy trade_logs_v10, wskazując, która strategia na którym interwale generuje najwyższy profit.

Grid Search: Silnik symuluje tysiące kombinacji parametrów na surowych danych Level 2 (market_logs_v11), aby wygenerować optymalne ustawienia dla pliku main.py.

Komenda do analizy:

Bash
docker run --rm -v "$(pwd)/data:/app/data" ai-trader python backtester.py
⌨️ Obsługa Terminala
q + Enter: Awaryjne zamknięcie wszystkich pozycji, zapis buforów RAM do SQLite i bezpieczne wyjście z systemu.

⚠️ Zastrzeżenie (Disclaimer)
To oprogramowanie służy wyłącznie do celów edukacyjnych i symulacji (Paper Trading). Autor nie ponosi odpowiedzialności za jakiekolwiek straty finansowe wynikające z użycia bota.

Autor: Łukasz Balowski
Wersja: 10.27 Enterprise Multi-Timeframe Edition