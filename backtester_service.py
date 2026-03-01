from __future__ import annotations

import os
import signal
import subprocess
import sys
from datetime import datetime
import json
import re
from pathlib import Path

from backtester_exit_log import append_backtester_exit_log
from backtester_runtime import (
    RUNTIME_DIR,
    WORKSPACE_PATH,
    apply_pending_tracked_updates,
    get_pending_tracked_updates,
    get_strategy_history_details,
    build_catalog,
    build_idle_state,
    build_job_payload,
    queue_tracked_strategy_update,
    queue_tracked_strategy_update_variant,
    read_state,
    utc_now_iso,
    write_state,
)


class BacktesterService:
    def __init__(self) -> None:
        self.process: subprocess.Popen | None = None

    @staticmethod
    def _decorate_state(state: dict) -> dict:
        payload = dict(state)
        payload["tracked_updates"] = get_pending_tracked_updates()
        return payload

    def _state(self) -> dict:
        self._sync_process_status()
        return read_state()

    def _sync_process_status(self) -> None:
        if self.process is not None and self.process.poll() is None:
            return

        state = read_state()
        if state.get("status") not in {"starting", "running"}:
            return

        pid = state.get("pid") or (self.process.pid if self.process else None)
        if pid and self._pid_exists(pid):
            return

        exit_code = self.process.returncode if self.process else None
        if exit_code == 0:
            state["status"] = "completed"
        elif exit_code is None:
            state["status"] = "stopped"
        else:
            state["status"] = "failed"
        state["finished_at"] = utc_now_iso()
        state["generated_at"] = utc_now_iso()
        write_state(state)
        append_backtester_exit_log(
            source="service",
            status=state["status"],
            run_id=state.get("run_id"),
            reason="process_exit_detected",
            scope=state.get("scope"),
            mode=state.get("mode"),
            adaptive=state.get("adaptive"),
            session_id=state.get("session_id"),
            pid=pid,
            finished_at=state.get("finished_at"),
            summary=state.get("summary"),
            error=state.get("error"),
            extra={"exit_code": exit_code},
        )

    @staticmethod
    def _pid_exists(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def get_catalog(
        self,
        scope: str,
        mode: str,
        adaptive: bool = False,
        session_id: str | None = None,
        selected_markets: list[str] | None = None,
        selected_strategies: list[str] | None = None,
        manual_samples: dict[str, int] | None = None,
    ) -> dict:
        catalog = build_catalog(
            scope=scope,
            mode=mode,
            adaptive=adaptive,
            session_id=session_id,
            selected_markets=selected_markets,
            selected_strategies=selected_strategies,
            manual_samples=manual_samples,
        )
        catalog["status"] = self._decorate_state(self._state())
        return catalog

    def get_status(self) -> dict:
        state = self._decorate_state(self._state())
        state["cpu"] = self._cpu_snapshot()
        return state

    def get_strategy_details(self, market_key: str, strategy_key: str) -> dict:
        return get_strategy_history_details(market_key, strategy_key)

    def queue_tracked_update(self, market_key: str, strategy_key: str, selected_variant: dict | None = None) -> dict:
        if selected_variant is not None:
            return queue_tracked_strategy_update_variant(market_key, strategy_key, selected_variant)
        return queue_tracked_strategy_update(market_key, strategy_key)

    def apply_tracked_updates(self) -> dict:
        return apply_pending_tracked_updates()

    def _cpu_snapshot(self) -> dict:
        per_core = self._sample_linux_proc_stat()
        source = "proc_stat"
        available = True
        note = ""
        if per_core is None:
            per_core = self._sample_macos_top()
            source = "top"
        if per_core is None:
            per_core = [None] * (os.cpu_count() or 1)
            available = False
            source = "unavailable"
            note = "Brak dostępu do metryk per rdzeń w aktualnym środowisku."

        numeric_values = [value for value in per_core if isinstance(value, (int, float))]
        overall = round(sum(numeric_values) / len(numeric_values), 1) if numeric_values else None
        return {
            "available": available,
            "source": source,
            "overall_percent": overall,
            "logical_cores": len(per_core),
            "per_core_percent": per_core,
            "note": note,
        }

    def _sample_linux_proc_stat(self) -> list[float] | None:
        stat_path = Path("/proc/stat")
        if not stat_path.exists():
            return None
        try:
            first = self._read_proc_stat()
            if not first:
                return None
            import time
            time.sleep(0.15)
            second = self._read_proc_stat()
            if not second or len(first) != len(second):
                return None
            loads = []
            for previous, current in zip(first, second):
                prev_total = sum(previous)
                curr_total = sum(current)
                total_delta = curr_total - prev_total
                idle_delta = current[3] - previous[3]
                if total_delta <= 0:
                    loads.append(0.0)
                else:
                    loads.append(round(max(0.0, min(100.0, (1.0 - (idle_delta / total_delta)) * 100.0)), 1))
            return loads
        except Exception:
            return None

    def _read_proc_stat(self) -> list[tuple[int, ...]] | None:
        stat_path = Path("/proc/stat")
        rows = []
        for line in stat_path.read_text(encoding="utf-8").splitlines():
            if not re.match(r"^cpu\d+\s", line):
                continue
            parts = line.split()
            rows.append(tuple(int(part) for part in parts[1:8]))
        return rows or None

    def _sample_macos_top(self) -> list[float | None] | None:
        try:
            result = subprocess.run(
                ["/usr/bin/top", "-l", "1", "-stats", "cpu"],
                capture_output=True,
                text=True,
                timeout=2,
            )
        except Exception:
            return None
        if result.returncode != 0 or not result.stdout:
            return None

        pattern = re.compile(r"CPU\s*(\d+):\s*([\d.]+)%\s*user,\s*([\d.]+)%\s*sys,\s*([\d.]+)%\s*idle", re.IGNORECASE)
        matches = pattern.findall(result.stdout)
        if not matches:
            return None

        indexed = {}
        for index, user_pct, sys_pct, idle_pct in matches:
            active = max(0.0, min(100.0, float(user_pct) + float(sys_pct)))
            indexed[int(index)] = round(active, 1)
        if not indexed:
            return None
        return [indexed.get(i) for i in range(max(indexed.keys()) + 1)]

    def start(self, payload: dict | None = None) -> dict:
        state = self._state()
        if state.get("status") in {"starting", "running"}:
            raise ValueError("Backtester jest już uruchomiony.")

        job = build_job_payload(payload or {})
        RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        job_path = RUNTIME_DIR / f"job_{job['run_id']}.json"
        job_path.write_text(json.dumps(job, indent=2), encoding="utf-8")

        write_state(
            {
                **build_idle_state(),
                "run_id": job["run_id"],
                "status": "starting",
                "generated_at": utc_now_iso(),
                "started_at": utc_now_iso(),
                "scope": job["scope"],
                "mode": job["mode"],
                "adaptive": job["adaptive"],
                "selected_markets": job["selected_markets"],
                "selected_strategies": job["selected_strategies"],
                "log_tail": [f"{datetime.now().strftime('%H:%M:%S')}  Uruchamianie backtestera..."],
            }
        )

        self.process = subprocess.Popen(
            [sys.executable, str(WORKSPACE_PATH / "backtester_runtime.py"), "--job-file", str(job_path)],
            cwd=str(WORKSPACE_PATH),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return self.get_status()

    def stop(self) -> dict:
        state = self._state()
        pid = state.get("pid") or (self.process.pid if self.process else None)
        if state.get("status") not in {"starting", "running"} or not pid:
            raise ValueError("Backtester nie jest aktualnie uruchomiony.")

        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

        if self.process is not None:
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=2)

        state = read_state()
        state["status"] = "stopped"
        state["finished_at"] = utc_now_iso()
        state["generated_at"] = utc_now_iso()
        state["log_tail"] = (state.get("log_tail", []) + [f"{datetime.now().strftime('%H:%M:%S')}  Backtester zatrzymany ręcznie."])[-24:]
        write_state(state)
        append_backtester_exit_log(
            source="service",
            status="stopped",
            run_id=state.get("run_id"),
            reason="manual_stop",
            scope=state.get("scope"),
            mode=state.get("mode"),
            adaptive=state.get("adaptive"),
            session_id=state.get("session_id"),
            pid=pid,
            finished_at=state.get("finished_at"),
            summary=state.get("summary"),
            error=state.get("error"),
        )
        return state
