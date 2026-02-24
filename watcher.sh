#!/bin/bash

# Utility script for managing the Watcher v10.29 system

COMMAND=$1
ARG=$2

# Create data directory if it doesn't exist to avoid mount errors
mkdir -p data

case "$COMMAND" in
  build)
    echo "🔨 Fast Docker image build..."
    docker build -t ai-trader .
    ;;
    
  trader)
    PORTFOLIO=${ARG:-100}
    echo "📈 Starting live bot. Portfolio: $PORTFOLIO USD"
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python main.py --portfolio $PORTFOLIO
    ;;
    
  backtest)
    echo "🧪 Starting Backtester (Level 2 Analysis + Post Mortem) for the LATEST session..."
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python backtester.py
    ;;
    
  backtest-all)
    echo "🌍 Starting Backtester on FULL historical database..."
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python backtester.py --all-history
    ;;
    
  fast-track)
    echo "⚡ Fast track best configurations from Alpha Vault to tracked_configs.json..."
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python backtester.py --fast-track
    ;;

    test-executor)
    echo "🚀 Uruchamiam test egzekucji (Buy/Sell Cycle)..."
    docker run --rm -it \
        -v "$(pwd)/data:/app/data" \
        --env-file .env \
        ai-trader python executor_test.py
    ;;
    
  *)
    echo "=== 🤖 WATCHER v10.29 CONTROL CONSOLE ==="
    echo "Usage: ./watcher.sh [command] [arguments]"
    echo ""
    echo "Available commands:"
    echo "  build            - Builds the container image (run after every code change)"
    echo "  trader [amount]  - Starts the HFT Bot (e.g., ./watcher.sh trader 500)"
    echo "  backtest         - Runs grid simulation and updates strategies from the latest session"
    echo "  backtest-all     - Runs grid simulation on the ENTIRE historical database"
    echo "  fast-track       - Dumps best historical settings to tracked_configs.json"
    echo "============================================"
    ;;
esac