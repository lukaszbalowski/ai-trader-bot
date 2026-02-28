from __future__ import annotations

import datetime
import time
from typing import Any, Callable

from dashboard_models import (
    DashboardSnapshot,
    MarketSnapshot,
    PositionSnapshot,
    ServiceStatusSnapshot,
    SessionSnapshot,
    StrategySnapshot,
)

STRATEGY_LABELS = {
    "kinetic_sniper": "Kinetic Sniper",
    "momentum": "1-Min Mom",
    "mid_arb": "Mid-Game Arb",
    "otm": "OTM Bargain",
}

TIMEFRAME_ORDER = {"5m": 0, "15m": 1, "1h": 2, "4h": 3}
SYMBOL_ORDER = {"BTC": 0, "ETH": 1, "SOL": 2, "XRP": 3}


class RuntimeModeManager:
    def __init__(self) -> None:
        self.startup_mode = "paper"
        self.operation_mode = "paper"
        self.execution_enabled = True
        self.connection_ready = False

    def configure_startup(self, startup_mode: str) -> None:
        self.startup_mode = startup_mode
        self.operation_mode = startup_mode
        self.execution_enabled = startup_mode != "observe_only"
        self.connection_ready = startup_mode == "live"

    def allowed_modes(self) -> list[str]:
        if self.startup_mode == "observe_only":
            return ["observe_only"]
        return [self.startup_mode, "observe_only"]

    def set_operation_mode(self, target_mode: str) -> None:
        if target_mode not in self.allowed_modes():
            allowed = ", ".join(self.allowed_modes())
            raise ValueError(f"Unsupported mode '{target_mode}'. Allowed: {allowed}")
        self.operation_mode = target_mode
        self.execution_enabled = target_mode != "observe_only"


class RuntimeStateStore:
    def __init__(
        self,
        *,
        observed_configs: list[dict[str, Any]],
        get_state: Callable[[], dict[str, Any]],
        get_active_markets: Callable[[], dict[str, Any]],
        get_pre_warming_markets: Callable[[], dict[str, Any]],
        get_live_market_data: Callable[[], dict[str, Any]],
        get_trades: Callable[[], list[dict[str, Any]]],
        get_trade_history: Callable[[], list[dict[str, Any]]],
        get_balance: Callable[[], float],
        get_initial_balance: Callable[[], float],
        get_market_cache: Callable[[], dict[str, Any]],
        extract_orderbook_metrics: Callable[[str], tuple[float, float, float, float, float]],
        mode_manager: RuntimeModeManager,
        is_strategy_enabled: Callable[[dict[str, Any], str], bool],
        loss_recovery_timeout: float,
        kinetic_timeout: float,
    ) -> None:
        self._observed_configs = observed_configs
        self._get_state = get_state
        self._get_active_markets = get_active_markets
        self._get_pre_warming_markets = get_pre_warming_markets
        self._get_live_market_data = get_live_market_data
        self._get_trades = get_trades
        self._get_trade_history = get_trade_history
        self._get_balance = get_balance
        self._get_initial_balance = get_initial_balance
        self._get_market_cache = get_market_cache
        self._extract_orderbook_metrics = extract_orderbook_metrics
        self._mode_manager = mode_manager
        self._is_strategy_enabled = is_strategy_enabled
        self._loss_recovery_timeout = loss_recovery_timeout
        self._kinetic_timeout = kinetic_timeout

    @staticmethod
    def market_key(config: dict[str, Any]) -> str:
        return f"{config['symbol']}_{config['timeframe']}"

    @staticmethod
    def _sort_configs(config: dict[str, Any]) -> tuple[int, int]:
        return (
            TIMEFRAME_ORDER.get(config["timeframe"], 99),
            SYMBOL_ORDER.get(config["symbol"], 99),
        )

    def _build_session_snapshot(
        self,
        state: dict[str, Any],
        trades: list[dict[str, Any]],
        live_market_data: dict[str, Any],
        balance: float,
        initial_balance: float,
        now_ts: float,
    ) -> SessionSnapshot:
        session_start = state.get("session_start", now_ts)
        invested_value = sum(trade.get("invested", 0.0) for trade in trades)
        floating_value = 0.0
        for trade in trades:
            market_id = trade.get("market_id")
            direction = trade.get("direction")
            current_bid = live_market_data.get(market_id, {}).get(f"{direction}_SELL", 0.0)
            floating_value += current_bid * trade.get("shares", 0.0)

        total_value = balance + floating_value
        pnl_value = total_value - initial_balance
        pnl_percent = (pnl_value / initial_balance) * 100 if initial_balance else 0.0
        duration_sec = max(int(now_ts - session_start), 0)

        return SessionSnapshot(
            session_id=state.get("session_id", "n/a"),
            started_at=datetime.datetime.fromtimestamp(session_start).isoformat(),
            duration_sec=duration_sec,
            duration_label=str(datetime.timedelta(seconds=duration_sec)),
            startup_mode=self._mode_manager.startup_mode,
            operation_mode=self._mode_manager.operation_mode,
            execution_enabled=self._mode_manager.execution_enabled,
            initial_balance=initial_balance,
            cash_balance=balance,
            invested_value=invested_value,
            floating_value=floating_value,
            total_portfolio_value=total_value,
            pnl_value=pnl_value,
            pnl_percent=pnl_percent,
        )

    def _build_service_snapshots(self, state: dict[str, Any]) -> list[ServiceStatusSnapshot]:
        services = []
        for name, payload in state.get("service_status", {}).items():
            if isinstance(payload, dict):
                status = payload.get("status", "unknown")
                details = payload.get("details", "")
            else:
                status = str(payload)
                details = ""
            services.append(ServiceStatusSnapshot(name=name, status=status, details=details))
        services.sort(key=lambda item: item.name)
        return services

    def _resolve_orderbook_prices(
        self, market_id: str, market_cache: dict[str, Any]
    ) -> tuple[float, float, float, float]:
        if market_id not in market_cache:
            return 0.0, 0.0, 0.0, 0.0
        up_id = market_cache[market_id].get("up_id")
        down_id = market_cache[market_id].get("dn_id")
        if not up_id or not down_id:
            return 0.0, 0.0, 0.0, 0.0
        up_buy, _, up_sell, _, _ = self._extract_orderbook_metrics(up_id)
        down_buy, _, down_sell, _, _ = self._extract_orderbook_metrics(down_id)
        return up_buy, up_sell, down_buy, down_sell

    def _build_market_snapshots(
        self,
        state: dict[str, Any],
        active_markets: dict[str, Any],
        pre_warming_markets: dict[str, Any],
        live_market_data: dict[str, Any],
        trades: list[dict[str, Any]],
        trade_history: list[dict[str, Any]],
        initial_balance: float,
    ) -> list[MarketSnapshot]:
        market_cache = self._get_market_cache()
        market_views = []
        for config in sorted(self._observed_configs, key=self._sort_configs):
            market_key = self.market_key(config)
            pair = config["pair"]
            live_price = state["binance_live_price"].get(pair, 0.0) + config.get("offset", 0.0)
            current_state = active_markets.get(market_key) or pre_warming_markets.get(market_key) or {}
            market_id = current_state.get("m_id", "")
            timing = state.get(f"timing_{market_id}", {}) if market_id else {}
            market_data = timing.get("m_data", {})
            is_pre_warming = bool(timing.get("is_pre_warming", False))
            strike_price = current_state.get("target", 0.0)
            delta = live_price - strike_price if strike_price else 0.0

            raw_status = state.get("market_status", {}).get(market_key, "[loading...]")
            if self._mode_manager.operation_mode == "observe_only" and any(
                self._is_strategy_enabled(config, strategy_key) for strategy_key in STRATEGY_LABELS
            ):
                status = "[paused] [observe-only]"
            else:
                status = raw_status

            if is_pre_warming:
                base_status = "PRE-WARM"
            elif market_data.get("base_fetched"):
                base_status = "BASE OK"
            elif market_id:
                base_status = "FETCHING"
            else:
                base_status = "WAITING"

            market_trades = [
                trade
                for trade in trades
                if f"{trade.get('symbol')}_{trade.get('timeframe')}" == market_key
            ]
            floating_pnl_value = 0.0
            floating_invested = 0.0
            for trade in market_trades:
                current_bid = live_market_data.get(trade["market_id"], {}).get(
                    f"{trade['direction']}_SELL", 0.0
                )
                floating_pnl_value += (current_bid * trade.get("shares", 0.0)) - trade.get("invested", 0.0)
                floating_invested += trade.get("invested", 0.0)
            floating_pnl_percent = (
                (floating_pnl_value / floating_invested) * 100 if floating_invested else 0.0
            )

            realized_pnl_value = sum(
                item.get("pnl", 0.0)
                for item in trade_history
                if f"{item.get('symbol')}_{item.get('timeframe')}" == market_key
            )
            realized_pnl_percent = (
                (realized_pnl_value / initial_balance) * 100 if initial_balance else 0.0
            )
            up_buy_price, up_sell_price, down_buy_price, down_sell_price = self._resolve_orderbook_prices(
                market_id, market_cache
            )

            market_views.append(
                MarketSnapshot(
                    market_key=market_key,
                    ui_key=config.get("ui_key", ""),
                    symbol=config["symbol"],
                    timeframe=config["timeframe"],
                    title=f"{config['symbol']} {config['timeframe']}",
                    status=status,
                    base_status=base_status,
                    market_id=market_id,
                    is_active=market_key in active_markets,
                    is_pre_warming=is_pre_warming,
                    live_price=live_price,
                    strike_price=strike_price,
                    delta=delta,
                    sec_left=max(float(timing.get("sec_left", 0.0)), 0.0),
                    up_buy_price=up_buy_price,
                    up_sell_price=up_sell_price,
                    down_buy_price=down_buy_price,
                    down_sell_price=down_sell_price,
                    open_positions=len(market_trades),
                    floating_pnl_value=floating_pnl_value,
                    floating_pnl_percent=floating_pnl_percent,
                    realized_pnl_value=realized_pnl_value,
                    realized_pnl_percent=realized_pnl_percent,
                    can_stop=True,
                    can_resume=self._mode_manager.operation_mode != "observe_only",
                    can_close_all=True,
                )
            )
        return market_views

    def _build_position_snapshots(
        self,
        trades: list[dict[str, Any]],
        live_market_data: dict[str, Any],
        now_ts: float,
    ) -> list[PositionSnapshot]:
        positions = []
        for trade in trades:
            market_key = f"{trade.get('symbol')}_{trade.get('timeframe')}"
            current_price = live_market_data.get(trade.get("market_id"), {}).get(
                f"{trade.get('direction')}_SELL", 0.0
            )
            current_value = current_price * trade.get("shares", 0.0)
            invested = trade.get("invested", 0.0)
            pnl_value = current_value - invested
            pnl_percent = (pnl_value / invested) * 100 if invested else 0.0

            countdown_type = None
            countdown_left = None
            countdown_label = ""
            if "loss_countdown" in trade:
                countdown_type = "loss_recovery"
                countdown_left = max(
                    self._loss_recovery_timeout - (now_ts - trade["loss_countdown"]),
                    0.0,
                )
                countdown_label = "Loss recovery countdown"
            elif "sl_countdown" in trade:
                countdown_type = "kinetic_stop_loss"
                countdown_left = max(
                    self._kinetic_timeout - (now_ts - trade["sl_countdown"]),
                    0.0,
                )
                countdown_label = "Kinetic SL countdown"

            positions.append(
                PositionSnapshot(
                    trade_id=trade.get("id", ""),
                    short_id=int(trade.get("short_id", 0)),
                    market_key=market_key,
                    market_title=f"{trade.get('symbol')} {trade.get('timeframe')}",
                    market_id=trade.get("market_id", ""),
                    symbol=trade.get("symbol", ""),
                    timeframe=trade.get("timeframe", ""),
                    strategy=trade.get("strategy", ""),
                    direction=trade.get("direction", ""),
                    entry_price=trade.get("entry_price", 0.0),
                    current_price=current_price,
                    invested=invested,
                    current_value=current_value,
                    pnl_value=pnl_value,
                    pnl_percent=pnl_percent,
                    countdown_type=countdown_type,
                    countdown_seconds_left=countdown_left,
                    countdown_label=countdown_label,
                    close_blocked=bool(trade.get("close_blocked", False)),
                )
            )
        positions.sort(key=lambda item: item.short_id)
        return positions

    def _build_strategy_snapshots(
        self,
        trades: list[dict[str, Any]],
        trade_history: list[dict[str, Any]],
        live_market_data: dict[str, Any],
    ) -> list[StrategySnapshot]:
        snapshots = []
        for config in sorted(self._observed_configs, key=self._sort_configs):
            market_key = self.market_key(config)
            market_title = f"{config['symbol']} {config['timeframe']}"
            for strategy_key, strategy_label in STRATEGY_LABELS.items():
                strategy_config = config.get(strategy_key)
                if not strategy_config:
                    continue
                strategy_id = strategy_config.get("id", "")
                enabled = self._is_strategy_enabled(config, strategy_key)
                matching_open_trades = [
                    trade
                    for trade in trades
                    if (
                        trade.get("strat_id") == strategy_id
                        or (
                            not strategy_id
                            and trade.get("strategy") == strategy_label
                            and f"{trade.get('symbol')}_{trade.get('timeframe')}" == market_key
                        )
                    )
                ]
                matching_history = [
                    item
                    for item in trade_history
                    if (
                        item.get("strat_id") == strategy_id
                        or (
                            not strategy_id
                            and item.get("strategy") == strategy_label
                            and f"{item.get('symbol')}_{item.get('timeframe')}" == market_key
                        )
                    )
                ]
                floating_pnl = 0.0
                invested = 0.0
                for trade in matching_open_trades:
                    current_price = live_market_data.get(trade.get("market_id"), {}).get(
                        f"{trade.get('direction')}_SELL", 0.0
                    )
                    floating_pnl += (current_price * trade.get("shares", 0.0)) - trade.get("invested", 0.0)
                    invested += trade.get("invested", 0.0)
                realized_pnl = sum(item.get("pnl", 0.0) for item in matching_history)
                total_pnl = realized_pnl + floating_pnl
                pnl_percent = (total_pnl / invested) * 100 if invested else 0.0
                snapshots.append(
                    StrategySnapshot(
                        market_key=market_key,
                        market_title=market_title,
                        strategy_key=strategy_key,
                        strategy_label=strategy_label,
                        strategy_id=strategy_id,
                        enabled=enabled,
                        execution_active=enabled and self._mode_manager.execution_enabled,
                        open_positions=len(matching_open_trades),
                        pnl_value=total_pnl,
                        pnl_percent=pnl_percent,
                    )
                )
        return snapshots

    def snapshot(self) -> DashboardSnapshot:
        now_ts = time.time()
        state = self._get_state()
        active_markets = self._get_active_markets()
        pre_warming_markets = self._get_pre_warming_markets()
        live_market_data = self._get_live_market_data()
        trades = self._get_trades()
        trade_history = self._get_trade_history()
        balance = self._get_balance()
        initial_balance = self._get_initial_balance()

        return DashboardSnapshot(
            generated_at=datetime.datetime.utcnow().isoformat() + "Z",
            session=self._build_session_snapshot(
                state, trades, live_market_data, balance, initial_balance, now_ts
            ),
            services=self._build_service_snapshots(state),
            markets=self._build_market_snapshots(
                state,
                active_markets,
                pre_warming_markets,
                live_market_data,
                trades,
                trade_history,
                initial_balance,
            ),
            positions=self._build_position_snapshots(trades, live_market_data, now_ts),
            strategies=self._build_strategy_snapshots(trades, trade_history, live_market_data),
        )


class CommandBus:
    def __init__(
        self,
        *,
        stop_market: Callable[[str], str],
        resume_market: Callable[[str], str],
        close_market: Callable[[str], str],
        close_position: Callable[[str], str],
        set_strategy_enabled: Callable[[str, str, bool], str],
        set_mode: Callable[[str], str],
    ) -> None:
        self._stop_market = stop_market
        self._resume_market = resume_market
        self._close_market = close_market
        self._close_position = close_position
        self._set_strategy_enabled = set_strategy_enabled
        self._set_mode = set_mode

    async def stop_market(self, market_key: str) -> dict[str, Any]:
        return {"ok": True, "message": self._stop_market(market_key)}

    async def resume_market(self, market_key: str) -> dict[str, Any]:
        return {"ok": True, "message": self._resume_market(market_key)}

    async def close_market(self, market_key: str) -> dict[str, Any]:
        return {"ok": True, "message": self._close_market(market_key)}

    async def close_position(self, trade_id: str) -> dict[str, Any]:
        return {"ok": True, "message": self._close_position(trade_id)}

    async def enable_strategy(self, market_key: str, strategy_key: str) -> dict[str, Any]:
        return {
            "ok": True,
            "message": self._set_strategy_enabled(market_key, strategy_key, True),
        }

    async def disable_strategy(self, market_key: str, strategy_key: str) -> dict[str, Any]:
        return {
            "ok": True,
            "message": self._set_strategy_enabled(market_key, strategy_key, False),
        }

    async def set_mode(self, target_mode: str) -> dict[str, Any]:
        return {"ok": True, "message": self._set_mode(target_mode)}
