import argparse
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


DB_PATH = Path("data/polymarket.db")
TIMED_EXIT_SECONDS = [5, 15, 30, 60, 120]
TAKE_PROFIT_LEVELS = [0.05, 0.10, 0.20, 0.30, 0.50, 1.00]


@dataclass
class ScenarioResult:
    label: str
    exit_price: float
    exit_time: datetime | None
    return_pct: float
    detail: str


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def get_latest_session_id(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT session_id FROM trade_logs_v10 WHERE session_id IS NOT NULL "
        "GROUP BY session_id ORDER BY MAX(exit_time) DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def load_trades(conn: sqlite3.Connection, session_id: str | None) -> list[sqlite3.Row]:
    if session_id:
        rows = conn.execute(
            "SELECT * FROM trade_logs_v10 WHERE session_id = ? ORDER BY entry_time ASC",
            (session_id,),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM trade_logs_v10 ORDER BY entry_time ASC").fetchall()
    return rows


def load_market_ticks(
    conn: sqlite3.Connection,
    market_id: str,
    entry_time: datetime,
    exit_time: datetime | None,
) -> list[sqlite3.Row]:
    end_time = exit_time.isoformat() if exit_time else "9999-12-31T23:59:59"
    return conn.execute(
        "SELECT fetched_at, live_price, buy_up, sell_up, buy_down, sell_down, up_obi, dn_obi "
        "FROM market_logs_v11 WHERE market_id = ? AND fetched_at >= ? AND fetched_at <= ? "
        "ORDER BY fetched_at ASC",
        (market_id, entry_time.isoformat(), end_time),
    ).fetchall()


def get_exit_quote(row: sqlite3.Row, direction: str) -> float:
    return float(row["buy_up"] if direction == "UP" else row["buy_down"] or 0.0)


def compute_return_pct(entry_price: float, exit_price: float) -> float:
    if entry_price <= 0:
        return 0.0
    return ((exit_price - entry_price) / entry_price) * 100.0


def first_tick_at_or_after(
    ticks: list[sqlite3.Row],
    entry_time: datetime,
    seconds: int,
) -> sqlite3.Row | None:
    target_ts = entry_time.timestamp() + seconds
    for tick in ticks:
        tick_time = parse_ts(tick["fetched_at"])
        if tick_time and tick_time.timestamp() >= target_ts:
            return tick
    return None


def first_tick_hitting_profit(
    ticks: list[sqlite3.Row],
    direction: str,
    target_exit_price: float,
) -> sqlite3.Row | None:
    for tick in ticks:
        exit_quote = get_exit_quote(tick, direction)
        if exit_quote >= target_exit_price:
            return tick
    return None


def build_scenarios(
    trade: sqlite3.Row,
    ticks: list[sqlite3.Row],
) -> tuple[list[ScenarioResult], str | None]:
    entry_price = float(trade["entry_price"] or 0.0)
    actual_exit_price = float(trade["exit_price"] or 0.0)
    actual_exit_time = parse_ts(trade["exit_time"])
    scenarios: list[ScenarioResult] = []

    if entry_price <= 0:
        return scenarios, "entry_price<=0"

    scenarios.append(
        ScenarioResult(
            label="actual",
            exit_price=actual_exit_price,
            exit_time=actual_exit_time,
            return_pct=compute_return_pct(entry_price, actual_exit_price),
            detail=f"Recorded reason: {trade['reason']}",
        )
    )

    if float(trade["settlement_value"] or 0.0) > 0 or str(trade["reason"]).startswith("RESOLVED"):
        settlement_value = float(trade["settlement_value"] if trade["settlement_value"] is not None else actual_exit_price)
        scenarios.append(
            ScenarioResult(
                label="hold_to_resolution",
                exit_price=settlement_value,
                exit_time=actual_exit_time,
                return_pct=compute_return_pct(entry_price, settlement_value),
                detail="Hold to final settlement/oracle resolution.",
            )
        )

    for seconds in TIMED_EXIT_SECONDS:
        tick = first_tick_at_or_after(ticks, parse_ts(trade["entry_time"]), seconds)
        if not tick:
            continue
        exit_quote = get_exit_quote(tick, trade["direction"])
        if exit_quote <= 0:
            continue
        scenarios.append(
            ScenarioResult(
                label=f"exit_after_{seconds}s",
                exit_price=exit_quote,
                exit_time=parse_ts(tick["fetched_at"]),
                return_pct=compute_return_pct(entry_price, exit_quote),
                detail=f"Timed exit using first sellable quote after {seconds}s.",
            )
        )

    for pct in TAKE_PROFIT_LEVELS:
        target_exit_price = entry_price * (1.0 + pct)
        tick = first_tick_hitting_profit(ticks, trade["direction"], target_exit_price)
        if not tick:
            continue
        exit_quote = get_exit_quote(tick, trade["direction"])
        scenarios.append(
            ScenarioResult(
                label=f"take_profit_{int(pct * 100)}pct",
                exit_price=exit_quote,
                exit_time=parse_ts(tick["fetched_at"]),
                return_pct=compute_return_pct(entry_price, exit_quote),
                detail=f"First quote reaching at least +{int(pct * 100)}%.",
            )
        )

    best_tick = None
    best_exit_price = -math.inf
    worst_exit_price = math.inf
    worst_tick = None
    for tick in ticks:
        exit_quote = get_exit_quote(tick, trade["direction"])
        if exit_quote <= 0:
            continue
        if exit_quote > best_exit_price:
            best_exit_price = exit_quote
            best_tick = tick
        if exit_quote < worst_exit_price:
            worst_exit_price = exit_quote
            worst_tick = tick

    if best_tick is not None:
        scenarios.append(
            ScenarioResult(
                label="perfect_exit_upper_bound",
                exit_price=best_exit_price,
                exit_time=parse_ts(best_tick["fetched_at"]),
                return_pct=compute_return_pct(entry_price, best_exit_price),
                detail="Best sellable quote seen between entry and exit. Hindsight upper bound.",
            )
        )

    if worst_tick is not None:
        scenarios.append(
            ScenarioResult(
                label="worst_seen_exit",
                exit_price=worst_exit_price,
                exit_time=parse_ts(worst_tick["fetched_at"]),
                return_pct=compute_return_pct(entry_price, worst_exit_price),
                detail="Worst sellable quote seen while the trade was open.",
            )
        )

    return scenarios, None


def select_best_simple_rule(scenarios: list[ScenarioResult]) -> ScenarioResult | None:
    eligible = [
        scenario
        for scenario in scenarios
        if scenario.label not in {"actual", "perfect_exit_upper_bound", "worst_seen_exit"}
    ]
    if not eligible:
        return None
    return max(eligible, key=lambda item: item.return_pct)


def format_dt(value: datetime | None) -> str:
    return value.isoformat(timespec="seconds") if value else "n/a"


def generate_report(db_path: Path, session_id: str | None, limit: int | None) -> str:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        effective_session = session_id or get_latest_session_id(conn)
        trades = load_trades(conn, effective_session)
        if limit is not None:
            trades = trades[:limit]

        lines: list[str] = []
        lines.append("# Trade Decision Review")
        lines.append("")
        lines.append(f"- Database: `{db_path}`")
        lines.append(f"- Session: `{effective_session or 'ALL'}`")
        lines.append(f"- Trades reviewed: `{len(trades)}`")
        lines.append("")

        strategy_rule_counter: dict[str, Counter] = defaultdict(Counter)
        strategy_rule_improvement: dict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))

        for trade in trades:
            entry_time = parse_ts(trade["entry_time"])
            exit_time = parse_ts(trade["exit_time"])
            if not entry_time:
                continue

            ticks = load_market_ticks(conn, trade["market_id"], entry_time, exit_time)
            scenarios, error = build_scenarios(trade, ticks)

            lines.append(f"## {trade['trade_id']}")
            lines.append("")
            lines.append(
                f"- Market: `{trade['timeframe']}` / `{trade['market_id']}` / `{trade['direction']}` / `{trade['strategy']}`"
            )
            lines.append(
                f"- Entry: `{entry_time.isoformat(timespec='seconds')}` at `{float(trade['entry_price'] or 0.0):.4f}`"
            )
            lines.append(
                f"- Recorded exit: `{format_dt(exit_time)}` at `{float(trade['exit_price'] or 0.0):.4f}`"
            )
            lines.append(f"- Reason: `{trade['reason']}`")
            lines.append(f"- Market ticks in window: `{len(ticks)}`")

            if error:
                lines.append(f"- Review status: skipped (`{error}`)")
                lines.append("")
                continue

            scenario_map = {scenario.label: scenario for scenario in scenarios}
            actual = scenario_map["actual"]
            best_simple = select_best_simple_rule(scenarios)
            perfect = scenario_map.get("perfect_exit_upper_bound")
            worst = scenario_map.get("worst_seen_exit")

            if best_simple:
                improvement = best_simple.return_pct - actual.return_pct
                strategy_rule_counter[trade["strategy"]][best_simple.label] += 1
                strategy_rule_improvement[trade["strategy"]][best_simple.label] += improvement
                lines.append(
                    f"- Best simple rule: `{best_simple.label}` at `{best_simple.exit_price:.4f}` "
                    f"on `{format_dt(best_simple.exit_time)}` => `{best_simple.return_pct:+.2f}%` "
                    f"(improvement vs actual `{improvement:+.2f} pp`)"
                )
                lines.append(f"- Recommendation: {best_simple.detail}")
            else:
                lines.append("- Best simple rule: `n/a`")

            if perfect:
                lines.append(
                    f"- Upper bound: `{perfect.exit_price:.4f}` on `{format_dt(perfect.exit_time)}` "
                    f"=> `{perfect.return_pct:+.2f}%`"
                )
            if worst:
                lines.append(
                    f"- Worst seen quote: `{worst.exit_price:.4f}` on `{format_dt(worst.exit_time)}` "
                    f"=> `{worst.return_pct:+.2f}%`"
                )

            lines.append("")
            lines.append("| Scenario | Exit price | Exit time | Return % | Note |")
            lines.append("| --- | ---: | --- | ---: | --- |")
            ordered = sorted(
                [scenario for scenario in scenarios if scenario.label not in {"worst_seen_exit"}],
                key=lambda item: (item.label != "actual", -item.return_pct),
            )
            for scenario in ordered:
                lines.append(
                    f"| {scenario.label} | {scenario.exit_price:.4f} | {format_dt(scenario.exit_time)} | "
                    f"{scenario.return_pct:+.2f}% | {scenario.detail} |"
                )
            lines.append("")

        lines.append("## Strategy Summary")
        lines.append("")
        if not strategy_rule_counter:
            lines.append("No strategy-level recommendations could be derived from the selected trades.")
        else:
            lines.append("| Strategy | Winning rule | Wins | Avg improvement (pp) |")
            lines.append("| --- | --- | ---: | ---: |")
            for strategy, counter in sorted(strategy_rule_counter.items()):
                best_rule, wins = counter.most_common(1)[0]
                avg_improvement = strategy_rule_improvement[strategy][best_rule] / wins
                lines.append(
                    f"| {strategy} | {best_rule} | {wins} | {avg_improvement:+.2f} |"
                )
        lines.append("")
        lines.append("## Notes")
        lines.append("")
        lines.append("- Returns are normalized per share from entry price to candidate exit price, not dollar-accurate realized PnL.")
        lines.append("- `perfect_exit_upper_bound` is hindsight only; use it as a ceiling, not a deployable rule.")
        lines.append("- Because `trade_logs_v10` stores the final trade state after partial closes, this review is best for decision quality, not full execution accounting.")
        return "\n".join(lines)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Review each recorded trade and compare simple exit rules.")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to polymarket SQLite database.")
    parser.add_argument("--session", default=None, help="Optional session_id to isolate the review.")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit on trades reviewed.")
    parser.add_argument("--output", default=None, help="Optional output path for the markdown report.")
    args = parser.parse_args()

    db_path = Path(args.db)
    report = generate_report(db_path, args.session, args.limit)
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(report, encoding="utf-8")
        print(f"Report written to {output_path}")
    else:
        print(report)


if __name__ == "__main__":
    main()
