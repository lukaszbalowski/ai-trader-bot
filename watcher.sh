#!/bin/bash

# Utility script for managing the Watcher v10.31 Enterprise system

COMMAND=$1
ARG=$2
QUICK_FLAG=""
DASHBOARD_PORT=${DASHBOARD_PORT:-8000}

if [[ "$2" == "quick" || "$3" == "quick" ]]; then
  QUICK_FLAG="--quick"
fi

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
        -p "${DASHBOARD_PORT}:${DASHBOARD_PORT}" \
        -v "$(pwd)/data:/app/data" \
        --env-file .env \
        ai-trader python main.py --mode paper --portfolio $PORTFOLIO
    ;;

  live)
    echo "⚠️ 🚀 Starting LIVE TRADING bot. Using real Polymarket wallet balance."
    docker run --rm -it \
        -p "${DASHBOARD_PORT}:${DASHBOARD_PORT}" \
        -v "$(pwd)/data:/app/data" \
        --env-file .env \
        ai-trader python main.py --mode live
    ;;

  observe)
    PORTFOLIO=${ARG:-100}
    echo "👁️ Starting OBSERVE ONLY bot. Portfolio reference: $PORTFOLIO USD"
    docker run --rm -it \
        -p "${DASHBOARD_PORT}:${DASHBOARD_PORT}" \
        -v "$(pwd)/data:/app/data" \
        --env-file .env \
        ai-trader python main.py --mode observe_only --portfolio $PORTFOLIO
    ;;
    
  backtest)
    echo "📊 Starting Backtester (Level 2 Analysis + Post Mortem) for the LATEST session..."
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python backtester.py $QUICK_FLAG
    ;;
    
  backtest-all)
    echo "📈 Starting Backtester on FULL historical database..."
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python backtester.py --all-history $QUICK_FLAG
    ;;
    
  fast-track)
    echo "⚡ Fast track best configurations from Alpha Vault to tracked_configs.json..."
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python backtester.py --fast-track
    ;;

  trades)
    echo "🤖 Exporting 24h trades with orderbook data for AI analysis..."
    docker run --rm -it -v "$(pwd)/data:/app/data" ai-trader python backtester.py --trades
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
    echo "  observe [amount] - Starts the bot in observe-only mode with all markets paused"
    echo "  backtest         - Runs grid simulation and updates strategies from the latest session"
    echo "  backtest quick   - Runs accelerated Monte Carlo sampling on the latest session"
    echo "  backtest-all     - Runs grid simulation on the ENTIRE historical database"
    echo "  backtest-all quick - Runs accelerated Monte Carlo sampling on the entire historical database"
    echo "  fast-track       - Dumps best historical settings to tracked_configs.json"
    echo "  trades           - Exports all trades from the last 24h with orderbook snapshots to CSV"
    echo "  test-executor    - Runs the Clob API execution test"
    echo "============================================"
    ;;
esac
