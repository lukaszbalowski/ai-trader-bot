import sqlite3
import pandas as pd
import os
from dotenv import load_dotenv
import google.generativeai as genai

# 1. Wczytanie zmiennych środowiskowych (klucz API)
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=api_key)

# 2. Konfiguracja modelu AI
# Używamy modelu gemini-2.5-pro, który świetnie radzi sobie z wnioskowaniem i analizą danych
model = genai.GenerativeModel('gemini-2.5-pro')

def load_data(db_path="data/polymarket.db"):
    """Łączy się z bazą bota i pobiera historię transakcji."""
    try:
        conn = sqlite3.connect(db_path)
        # Pobieramy tabelę trade_logs_v10 opisaną w dokumentacji
        query = "SELECT * FROM trade_logs_v10"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Błąd połączenia z bazą: {e}")
        return None

def analyze_strategies(df):
    """Oblicza kluczowe wskaźniki (KPI) dla każdej strategii z 7 filarów."""
    if df is None or df.empty:
        return "Brak danych do analizy."

    # Konwersja PnL na wartości liczbowe
    df['pnl'] = pd.to_numeric(df['pnl'])
    
    # Grupowanie po nazwie strategii i obliczanie statystyk
    stats = df.groupby('strategy').agg(
        total_trades=('trade_id', 'count'),
        total_pnl=('pnl', 'sum'),
        avg_pnl_per_trade=('pnl', 'mean'),
        win_rate=('pnl', lambda x: (x > 0).mean() * 100) # Procent zyskownych
    ).round(2)
    
    # Formatowanie danych do formatu tekstowego (JSON/Dict) dla AI
    return stats.to_dict(orient='index')

def ask_google_ai_for_optimization(stats_dict):
    """Wysyła wyniki do Gemini API z prośbą o wyciągnięcie wniosków."""
    print("🧠 Wysyłam dane do Google AI w celu analizy...\n")
    
    prompt = f"""
    Jesteś analitykiem ilościowym optymalizującym bota HFT (High-Frequency Trading) na platformie Polymarket.
    Bot działa w architekturze asynchronicznej i używa 7 różnych strategii. 
    Oto zagregowane statystyki historycznych transakcji mojego bota:
    
    {stats_dict}
    
    Twoim zadaniem jest:
    1. Przeanalizować powyższe wskaźniki (szczególnie win_rate oraz avg_pnl_per_trade).
    2. Zidentyfikować, która strategia działa najlepiej, a która "przepala" kapitał.
    3. Zaproponować konkretne hipotezy, jak można zoptymalizować najsłabszą strategię (np. zmiana momentu wejścia, modyfikacja progu odchylenia FIXED_OFFSET, który wynosi u mnie -35.0).
    Odpowiedz zwięźle i profesjonalnie.
    """
    
    response = model.generate_content(prompt)
    return response.text

if __name__ == "__main__":
    print("📊 Uruchamianie Optymalizatora Strategii Watcher v10.15...")
    
    # Krok 1: Wczytanie danych
    df_trades = load_data()
    
    if df_trades is not None and not df_trades.empty:
        print(f"✅ Wczytano {len(df_trades)} transakcji z bazy danych.")
        
        # Krok 2: Analiza danych przy użyciu Pandas
        kpi_stats = analyze_strategies(df_trades)
        print("\n📈 Bieżące wskaźniki strategii (KPI):")
        for strategy, metrics in kpi_stats.items():
            print(f"- {strategy}: {metrics}")
            
        # Krok 3: Wnioskowanie AI
        ai_recommendations = ask_google_ai_for_optimization(kpi_stats)
        
        print("\n" + "="*50)
        print("🤖 WNIOSKI I REKOMENDACJE GOOGLE AI:")
        print("="*50)
        print(ai_recommendations)
    else:
        print("❌ Nie znaleziono transakcji. Upewnij się, że bot Watcher wykonał już jakieś Paper Trades i zrzucił bazę.")