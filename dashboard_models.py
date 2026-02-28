from pydantic import BaseModel, Field


class SessionSnapshot(BaseModel):
    session_id: str
    started_at: str
    duration_sec: int
    duration_label: str
    startup_mode: str
    operation_mode: str
    execution_enabled: bool
    initial_balance: float
    cash_balance: float
    invested_value: float
    floating_value: float
    total_portfolio_value: float
    pnl_value: float
    pnl_percent: float


class ServiceStatusSnapshot(BaseModel):
    name: str
    status: str
    details: str = ""


class MarketSnapshot(BaseModel):
    market_key: str
    ui_key: str
    symbol: str
    timeframe: str
    title: str
    status: str
    base_status: str
    market_id: str = ""
    is_active: bool = False
    is_pre_warming: bool = False
    live_price: float = 0.0
    strike_price: float = 0.0
    delta: float = 0.0
    sec_left: float = 0.0
    up_buy_price: float = 0.0
    up_sell_price: float = 0.0
    down_buy_price: float = 0.0
    down_sell_price: float = 0.0
    open_positions: int = 0
    floating_pnl_value: float = 0.0
    floating_pnl_percent: float = 0.0
    realized_pnl_value: float = 0.0
    realized_pnl_percent: float = 0.0
    can_stop: bool = True
    can_resume: bool = True
    can_close_all: bool = True


class PositionSnapshot(BaseModel):
    trade_id: str
    short_id: int
    market_key: str
    market_title: str
    market_id: str
    symbol: str
    timeframe: str
    strategy: str
    direction: str
    entry_price: float
    current_price: float
    invested: float
    current_value: float
    pnl_value: float
    pnl_percent: float
    countdown_type: str | None = None
    countdown_seconds_left: float | None = None
    countdown_label: str = ""
    close_blocked: bool = False


class StrategySnapshot(BaseModel):
    market_key: str
    market_title: str
    strategy_key: str
    strategy_label: str
    strategy_id: str = ""
    enabled: bool
    execution_active: bool
    open_positions: int = 0
    pnl_value: float = 0.0
    pnl_percent: float = 0.0


class SystemLogSnapshot(BaseModel):
    timestamp: str
    message: str


class DashboardSnapshot(BaseModel):
    generated_at: str
    session: SessionSnapshot
    services: list[ServiceStatusSnapshot] = Field(default_factory=list)
    markets: list[MarketSnapshot] = Field(default_factory=list)
    positions: list[PositionSnapshot] = Field(default_factory=list)
    strategies: list[StrategySnapshot] = Field(default_factory=list)
    system_logs: list[SystemLogSnapshot] = Field(default_factory=list)


class CommandResult(BaseModel):
    ok: bool
    message: str
