from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path


EXIT_LOG_DIR = Path("data/backtester/exit_logs")


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def append_backtester_exit_log(
    *,
    source: str,
    status: str,
    run_id: str | None = None,
    reason: str | None = None,
    scope: str | None = None,
    mode: str | None = None,
    adaptive: bool | None = None,
    session_id: str | None = None,
    pid: int | None = None,
    finished_at: str | None = None,
    summary: dict | None = None,
    error: str | None = None,
    extra: dict | None = None,
) -> Path:
    EXIT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = EXIT_LOG_DIR / f"backtester_exit_{datetime.now().strftime('%Y%m%d')}.jsonl"
    payload = {
        "logged_at": utc_now_iso(),
        "finished_at": finished_at or utc_now_iso(),
        "source": source,
        "status": status,
        "run_id": run_id,
        "reason": reason,
        "scope": scope,
        "mode": mode,
        "adaptive": adaptive,
        "session_id": session_id,
        "pid": pid if pid is not None else os.getpid(),
        "summary": summary or {},
        "error": error,
    }
    if extra:
        payload["extra"] = extra
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return log_path
