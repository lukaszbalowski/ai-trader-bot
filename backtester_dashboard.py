from __future__ import annotations

from types import SimpleNamespace

import uvicorn

from dashboard_api import create_dashboard_app
from dashboard_models import DashboardSnapshot, SessionSnapshot


class DummyStateStore:
    def snapshot(self) -> DashboardSnapshot:
        return DashboardSnapshot(
            generated_at="",
            session=SessionSnapshot(
                session_id="backtester-only",
                started_at="",
                duration_sec=0,
                duration_label="00:00",
                startup_mode="standalone",
                operation_mode="standalone",
                execution_enabled=False,
                initial_balance=0.0,
                cash_balance=0.0,
                invested_value=0.0,
                floating_value=0.0,
                total_portfolio_value=0.0,
                pnl_value=0.0,
                pnl_percent=0.0,
            ),
            services=[],
            markets=[],
            positions=[],
            strategies=[],
            system_logs=[],
        )


class DummyCommandBus:
    async def _unsupported(self, *args, **kwargs) -> dict:
        raise ValueError("Watcher commands są niedostępne w standalone backtester dashboard.")

    stop_market = _unsupported
    resume_market = _unsupported
    close_market = _unsupported
    close_position = _unsupported
    enable_strategy = _unsupported
    disable_strategy = _unsupported
    set_mode = _unsupported
    dump_session_log = _unsupported


def main() -> None:
    app = create_dashboard_app(
        DummyStateStore(),
        DummyCommandBus(),
        SimpleNamespace(
            startup_mode="standalone",
            operation_mode="standalone",
            execution_enabled=False,
            connection_ready=False,
            allowed_modes=lambda: ["standalone"],
        ),
    )
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="warning",
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    server.run()


if __name__ == "__main__":
    main()
