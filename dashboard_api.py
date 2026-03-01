from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from backtester_service import BacktesterService
from dashboard_models import CommandResult


def create_dashboard_app(state_store, command_bus, mode_manager) -> FastAPI:
    app = FastAPI(title="Watcher Dashboard API", version="0.1.0")
    static_path = Path(__file__).parent / "static" / "dashboard.html"
    backtester_static_path = Path(__file__).parent / "static" / "backtester.html"
    backtester_service = BacktesterService()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "null",
            "http://localhost:8000",
            "http://127.0.0.1:8000",
            "http://localhost:3000",
            "http://127.0.0.1:3000",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/", response_class=HTMLResponse)
    async def dashboard_page() -> str:
        return static_path.read_text(encoding="utf-8")

    @app.get("/backtester", response_class=HTMLResponse)
    async def backtester_page() -> str:
        return backtester_static_path.read_text(encoding="utf-8")

    @app.get("/api/dashboard")
    async def get_dashboard() -> dict:
        return state_store.snapshot().dict()

    @app.get("/api/session")
    async def get_session() -> dict:
        return state_store.snapshot().session.dict()

    @app.get("/api/markets")
    async def get_markets() -> list[dict]:
        return [market.dict() for market in state_store.snapshot().markets]

    @app.get("/api/positions")
    async def get_positions() -> list[dict]:
        return [position.dict() for position in state_store.snapshot().positions]

    @app.get("/api/strategies")
    async def get_strategies() -> list[dict]:
        return [strategy.dict() for strategy in state_store.snapshot().strategies]

    @app.get("/api/system/health")
    async def get_health() -> dict:
        snapshot = state_store.snapshot()
        return {
            "services": [service.dict() for service in snapshot.services],
            "startup_mode": mode_manager.startup_mode,
            "operation_mode": mode_manager.operation_mode,
            "allowed_modes": mode_manager.allowed_modes(),
            "execution_enabled": mode_manager.execution_enabled,
            "connection_ready": mode_manager.connection_ready,
        }

    async def _dispatch(coro) -> CommandResult:
        try:
            result = await coro
            return CommandResult(**result)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/markets/{market_key}/stop")
    async def stop_market(market_key: str) -> CommandResult:
        return await _dispatch(command_bus.stop_market(market_key))

    @app.post("/api/markets/{market_key}/resume")
    async def resume_market(market_key: str) -> CommandResult:
        return await _dispatch(command_bus.resume_market(market_key))

    @app.post("/api/markets/{market_key}/close-all")
    async def close_market(market_key: str) -> CommandResult:
        return await _dispatch(command_bus.close_market(market_key))

    @app.post("/api/positions/{trade_id}/close")
    async def close_position(trade_id: str) -> CommandResult:
        return await _dispatch(command_bus.close_position(trade_id))

    @app.post("/api/strategies/{market_key}/{strategy_key}/enable")
    async def enable_strategy(market_key: str, strategy_key: str) -> CommandResult:
        return await _dispatch(command_bus.enable_strategy(market_key, strategy_key))

    @app.post("/api/strategies/{market_key}/{strategy_key}/disable")
    async def disable_strategy(market_key: str, strategy_key: str) -> CommandResult:
        return await _dispatch(command_bus.disable_strategy(market_key, strategy_key))

    @app.post("/api/system/mode/{target_mode}")
    async def set_mode(target_mode: str) -> CommandResult:
        return await _dispatch(command_bus.set_mode(target_mode))

    @app.post("/api/system/dump-session-log")
    async def dump_session_log() -> CommandResult:
        return await _dispatch(command_bus.dump_session_log())

    @app.get("/api/backtester/catalog")
    async def get_backtester_catalog(
        scope: str = "latest_session",
        mode: str = "full",
        adaptive: str = "false",
        markets: str = "",
        strategies: str = "",
        manual_samples: str = "",
    ) -> dict:
        selected_markets = [item for item in markets.split(",") if item]
        selected_strategies = [item for item in strategies.split(",") if item]
        manual_sample_map = {}
        if manual_samples:
            try:
                manual_sample_map = {key: int(value) for key, value in json.loads(manual_samples).items()}
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=f"Niepoprawny manual_samples: {exc}") from exc
        try:
            return backtester_service.get_catalog(
                scope=scope,
                mode=mode,
                adaptive=adaptive,
                selected_markets=selected_markets,
                selected_strategies=selected_strategies,
                manual_samples=manual_sample_map,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/backtester/status")
    async def get_backtester_status() -> dict:
        return backtester_service.get_status()

    @app.post("/api/backtester/start")
    async def start_backtester(payload: dict) -> dict:
        try:
            return backtester_service.start(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/backtester/stop")
    async def stop_backtester() -> dict:
        try:
            return backtester_service.stop()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.websocket("/ws/dashboard")
    async def websocket_dashboard(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            while True:
                await websocket.send_json(state_store.snapshot().dict())
                await asyncio.sleep(1.0)
        except WebSocketDisconnect:
            return

    return app
