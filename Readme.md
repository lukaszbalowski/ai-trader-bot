# 🚀 Watcher v10.29 | Enterprise HFT Polymarket Bot
Watcher is an advanced High-Frequency Trading (HFT) simulator for binary options on the Polymarket platform. Version 10.29 combines proven high profitability with a new database architecture, API-Only tracking, and task prioritization.

## 🛠 Key Features (v10.29)

* **Multi-Market Performance:** Parallel handling of 12 markets (BTC, ETH, SOL, XRP) across 5m, 15m, and 1h intervals while maintaining ultra-low latencies.
* **Local Oracle Snapshot:** Innovative pre-warming mechanism using an internal data pipeline to instantly freeze Binance strike prices at exactly the 0-second mark, bypassing Polymarket API delays.
* **Alpha Vault (Historical Optimization):** A module (`backtest_history.db`) registering every successful optimization in JSON format, building a knowledge base for future machine learning models.
* **UUID Trade Security:** Implementation of cryptographic UUID hashes for each transaction, eliminating database writing errors (`IntegrityError`) during simultaneous HFT operations.
* **Smart ID Pool (00-99):** Active position window management system with unique identifiers visible in the terminal.

## 📈 Strategies (Optimized v10.29)
The system uses dynamic thresholds adjusted to the specifics of each cryptocurrency:

* **Lag Sniper:** Scalping millisecond price differences between Binance and Polymarket.
* **1-Min Momentum:** Exploiting market momentum in the final phase of the contract.
* **Mid-Game Arb:** Statistical arbitrage during the middle phase of the market.
* **OTM Bargain:** Selective hunting for extremely cheap options (2-6 cents) in high-volatility markets.

## ⚙️ Installation and Launch
**Quick start (Docker & CLI Script):**

Build the image:
```bash
./watcher.sh build

Run the bot (Live Paper Trading with $500 portfolio):

```bash
./watcher.sh trader 500

