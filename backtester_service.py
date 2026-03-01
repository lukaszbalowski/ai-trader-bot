from __future__ import annotations

import os
import signal
import subprocess
import sys
from datetime import datetime
import json

from backtester_runtime import (
    RUNTIME_DIR,
    WORKSPACE_PATH,
    build_catalog,
    build_idle_state,
    build_job_payload,
    read_state,
    utc_now_iso,
    write_state,
)


class BacktesterService:
    def __init__(self) -> None:
        self.process: subprocess.Popen | None = None

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
        selected_markets: list[str] | None,
        selected_strategies: list[str] | None,
        manual_samples: dict[str, int] | None,
    ) -> dict:
        catalog = build_catalog(
            scope=scope,
            mode=mode,
            selected_markets=selected_markets,
            selected_strategies=selected_strategies,
            manual_samples=manual_samples,
        )
        catalog["status"] = self._state()
        return catalog

    def get_status(self) -> dict:
        return self._state()

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
        return state
