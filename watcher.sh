#!/bin/bash

# Utility script for managing the Watcher v10.31 Enterprise system

COMMAND=$1
ARG=$2

# Create data directory if it doesn't exist to avoid mount errors
mkdir -p data

case "$COMMAND" in
  build)
    echo "🛠️ Fast Docker image build..."
    docker build -t ai-trader .
    ;;
    
  paper)
    PORTFOLIO=${ARG:-100}
    echo "📄 Starting PAPER TRADING bot. Portfolio: $PORTFOLIO USD"
    docker run --rm -it \
        -v "$(pwd)/data:/app/data" \
        --env-file .env \
        ai-trader python main.py --mode paper --portfolio $PORTFOLIO
    ;;

  live)
    echo "⚠️ 🚀 Starting LIVE TRADING bot. Using real Polymarket wallet balance."
    docker run --rm -it \
        -v "$(pwd)/data:/app/data" \
        --env-file .env \
        ai-trader python main.py --mode live
    ;;
    
  backtest)
    echo "📊 Starting Backtester (Level 2 Analysis + Post Mortem) for the LATEST session..."
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python backtester.py
    ;;
    
  backtest-all)
    echo "📈 Starting Backtester on FULL historical database..."
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python backtester.py --all-history
    ;;
    
  fast-track)
    echo "⚡ Fast track best configurations from Alpha Vault to tracked_configs.json..."
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python backtester.py --fast-track
    ;;

  test-executor)
    echo "⚙️ Running execution test (Buy/Sell Cycle)..."
    docker run --rm -it \
        -v "$(pwd)/data:/app/data" \
        --env-file .env \
        ai-trader python executor_test.py
    ;;
    
  *)
    echo "=== 🤖 WATCHER v10.31 CONTROL CONSOLE ==="
    echo "Usage: ./watcher.sh [command] [arguments]"
    echo ""
    echo "Available commands:"
    echo "  build            - Builds the container image (run after every code change)"
    echo "  paper [amount]   - Starts the PAPER TRADING Bot (e.g., ./watcher.sh paper 500)"
    echo "  live             - Starts the LIVE TRADING Bot (uses real wallet balance)"
    echo "  backtest         - Runs grid simulation and updates strategies from the latest session"
    echo "  backtest-all     - Runs grid simulation on the ENTIRE historical database"
    echo "  fast-track       - Dumps best historical settings to tracked_configs.json"
    echo "  test-executor    - Runs the Clob API execution test"
    echo "============================================"
    ;;
esac