from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _is_pid_alive(pid: int) -> bool:
    try:
        proc = subprocess.run(
            ["tasklist", "/FI", f"PID eq {int(pid)}", "/FO", "CSV", "/NH"],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return False
    text = (proc.stdout or "").strip()
    if not text or text.startswith("INFO:"):
        return False
    return f'"{int(pid)}"' in text


def _taskkill_tree(pid: int) -> None:
    try:
        subprocess.run(
            ["taskkill", "/PID", str(int(pid)), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def _append_log(path: Path, message: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(f"{_now_iso()} {message}\n")
    except Exception:
        pass


def _validate_v2_launch(workdir: Path, config_path: Path) -> Dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    config_data: Dict[str, Any] = {}

    if not workdir.exists():
        errors.append(f"workdir does not exist: {workdir}")
    if not (workdir / "copytrade_run.py").exists():
        errors.append(f"copytrade_run.py not found under workdir: {workdir}")
    if not config_path.exists():
        errors.append(f"config file not found: {config_path}")
    else:
        try:
            loaded = json.loads(config_path.read_text(encoding="utf-8-sig"))
            if isinstance(loaded, dict):
                config_data = loaded
            else:
                errors.append(f"config file is not a JSON object: {config_path}")
        except Exception as exc:
            errors.append(f"failed to parse config file {config_path}: {exc}")

    try:
        __import__("py_clob_client_v2")
    except Exception as exc:
        errors.append(f"py_clob_client_v2 import failed: {exc}")

    poly_host = str(config_data.get("poly_host") or "https://clob.polymarket.com").strip()
    if poly_host not in {"https://clob.polymarket.com", "https://clob-v2.polymarket.com"}:
        warnings.append(f"unexpected poly_host for V2 migration: {poly_host}")
    if "require_pusd_ready" not in config_data:
        warnings.append("require_pusd_ready is not set; runtime defaults will be used")
    if "market_info_cache_ttl_sec" not in config_data:
        warnings.append("market_info_cache_ttl_sec is not set; runtime defaults will be used")
    if "cutover_force_remote_refresh" not in config_data:
        warnings.append("cutover_force_remote_refresh is not set; runtime defaults will be used")

    return {"errors": errors, "warnings": warnings, "config": config_data}


def _session_template(
    *,
    run_id: str,
    workdir: Path,
    config: Path,
    poll: int,
    mode: str,
    prefix: str,
    session_path: Path,
    stop_flag: Path,
    stdout_path: Path,
    stderr_path: Path,
    supervisor_log: Path,
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "workdir": str(workdir),
        "config": str(config),
        "poll": int(poll),
        "mode": str(mode),
        "prefix": str(prefix),
        "session": str(session_path),
        "stop_flag": str(stop_flag),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "supervisor_log": str(supervisor_log),
        "status": "starting",
        "desired_state": "running",
        "started_at": _now_iso(),
        "updated_at": _now_iso(),
        "supervisor_pid": 0,
        "child_pid": 0,
        "restart_count": 0,
        "last_exit_code": None,
        "last_exit_at": "",
        "last_restart_at": "",
    }


def _child_cmd(session: Dict[str, Any]) -> list[str]:
    cmd = [
        sys.executable,
        "copytrade_run.py",
        "--config",
        str(session.get("config") or "copytrade_config.json"),
        "--poll",
        str(int(session.get("poll") or 20)),
    ]
    if str(session.get("mode") or "live").strip().lower() == "dry":
        cmd.append("--dry-run")
    return cmd


def _spawn_child(session: Dict[str, Any]) -> subprocess.Popen[Any]:
    stdout_path = Path(str(session.get("stdout")))
    stderr_path = Path(str(session.get("stderr")))
    stdout_handle = stdout_path.open("ab")
    stderr_handle = stderr_path.open("ab")
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    child = subprocess.Popen(
        _child_cmd(session),
        cwd=str(Path(str(session.get("workdir")))),
        stdout=stdout_handle,
        stderr=stderr_handle,
        creationflags=creationflags,
    )
    stdout_handle.close()
    stderr_handle.close()
    return child


def cmd_launch(args: argparse.Namespace) -> int:
    workdir = Path(args.workdir).resolve()
    config_path = (
        Path(args.config).resolve() if Path(args.config).is_absolute() else (workdir / str(args.config)).resolve()
    )
    preflight = _validate_v2_launch(workdir, config_path)
    if preflight["warnings"]:
        for warning in preflight["warnings"]:
            print(f"[PRELAUNCH_WARN] {warning}", file=sys.stderr)
    if preflight["errors"]:
        raise ValueError("V2 launch preflight failed:\n- " + "\n- ".join(preflight["errors"]))

    logs_dir = workdir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    prefix = str(args.prefix or "persistent_live").strip()
    session_path = logs_dir / str(args.session_name or f"{prefix}_session.json")
    existing = _load_json(session_path)
    existing_supervisor = int(existing.get("supervisor_pid") or 0) if existing else 0
    if existing and str(existing.get("status") or "").lower() == "running" and existing_supervisor > 0 and _is_pid_alive(existing_supervisor):
        print(
            json.dumps(
                {
                    "already_running": True,
                    "session": str(session_path),
                    "supervisor_pid": existing_supervisor,
                    "child_pid": int(existing.get("child_pid") or 0),
                },
                ensure_ascii=False,
            )
        )
        return 0

    now = datetime.now()
    run_id = args.run_id or now.strftime("%Y%m%d_%H%M%S")
    stdout_path = logs_dir / f"{prefix}_{run_id}_stdout.log"
    stderr_path = logs_dir / f"{prefix}_{run_id}_stderr.log"
    supervisor_log = logs_dir / f"{prefix}_{run_id}_supervisor.log"
    stop_flag = logs_dir / f"{prefix}_{run_id}.stop"
    if stop_flag.exists():
        stop_flag.unlink()

    session = _session_template(
        run_id=run_id,
        workdir=workdir,
        config=config_path,
        poll=int(args.poll),
        mode=str(args.mode),
        prefix=prefix,
        session_path=session_path,
        stop_flag=stop_flag,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        supervisor_log=supervisor_log,
    )
    _write_json(session_path, session)

    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "supervise",
        "--session",
        str(session_path),
        "--restart-delay-sec",
        str(int(args.restart_delay_sec)),
        "--max-restart-delay-sec",
        str(int(args.max_restart_delay_sec)),
        "--quick-fail-window-sec",
        str(int(args.quick_fail_window_sec)),
    ]
    creationflags = 0
    if os.name == "nt":
        creationflags = (
            subprocess.CREATE_NEW_PROCESS_GROUP
            | getattr(subprocess, "DETACHED_PROCESS", 0)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
    supervisor = subprocess.Popen(
        cmd,
        cwd=str(workdir),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creationflags,
    )
    session["supervisor_pid"] = int(supervisor.pid)
    session["status"] = "running"
    session["updated_at"] = _now_iso()
    _write_json(session_path, session)
    print(
        json.dumps(
            {
                "session": str(session_path),
                "supervisor_pid": supervisor.pid,
                "stdout": str(stdout_path),
                "stderr": str(stderr_path),
                "supervisor_log": str(supervisor_log),
                "stop_flag": str(stop_flag),
            },
            ensure_ascii=False,
        )
    )
    return 0


def cmd_supervise(args: argparse.Namespace) -> int:
    session_path = Path(args.session).resolve()
    session = _load_json(session_path)
    if not session:
        return 1
    log_path = Path(str(session.get("supervisor_log") or session_path.with_suffix(".log")))
    restart_delay_sec = max(1, int(args.restart_delay_sec))
    max_restart_delay_sec = max(restart_delay_sec, int(args.max_restart_delay_sec))
    quick_fail_window_sec = max(1, int(args.quick_fail_window_sec))

    session["supervisor_pid"] = int(os.getpid())
    session["status"] = "running"
    session["updated_at"] = _now_iso()
    _write_json(session_path, session)
    _append_log(log_path, f"[SUPERVISOR] started session={session_path}")

    child: subprocess.Popen[Any] | None = None
    consecutive_quick_failures = 0

    while True:
        session = _load_json(session_path)
        if not session:
            _append_log(log_path, "[SUPERVISOR] session missing, exiting")
            return 0
        stop_flag = Path(str(session.get("stop_flag") or ""))
        desired_state = str(session.get("desired_state") or "running").strip().lower()
        stop_requested = desired_state == "stopped" or (stop_flag.exists() if stop_flag else False)

        if child is None or child.poll() is not None:
            if child is not None:
                exit_code = int(child.poll() or 0)
                last_start_text = str(session.get("child_started_at") or "").strip()
                runtime_sec = 0
                if last_start_text:
                    try:
                        runtime_sec = max(
                            0,
                            int((datetime.now() - datetime.fromisoformat(last_start_text)).total_seconds()),
                        )
                    except Exception:
                        runtime_sec = 0
                session["child_pid"] = 0
                session["last_exit_code"] = exit_code
                session["last_exit_at"] = _now_iso()
                if stop_requested:
                    session["status"] = "stopped"
                    session["stop_reason"] = "requested_stop"
                    session["updated_at"] = _now_iso()
                    _write_json(session_path, session)
                    _append_log(log_path, f"[SUPERVISOR] child exited after requested stop exit_code={exit_code}")
                    return 0
                consecutive_quick_failures = (
                    consecutive_quick_failures + 1 if runtime_sec < quick_fail_window_sec else 0
                )
                sleep_sec = min(
                    max_restart_delay_sec,
                    restart_delay_sec * max(1, consecutive_quick_failures + 1),
                )
                session["status"] = "restarting"
                session["restart_count"] = int(session.get("restart_count") or 0) + 1
                session["last_restart_at"] = _now_iso()
                session["updated_at"] = _now_iso()
                _write_json(session_path, session)
                _append_log(
                    log_path,
                    f"[SUPERVISOR] child exited exit_code={exit_code} runtime_sec={runtime_sec} restart_in={sleep_sec}s",
                )
                time.sleep(float(sleep_sec))
                continue

            if stop_requested:
                session["status"] = "stopped"
                session["stop_reason"] = "requested_stop_before_spawn"
                session["updated_at"] = _now_iso()
                _write_json(session_path, session)
                _append_log(log_path, "[SUPERVISOR] stop requested before child spawn")
                return 0

            try:
                child = _spawn_child(session)
            except Exception as exc:
                session["status"] = "restarting"
                session["last_exit_code"] = None
                session["last_exit_at"] = _now_iso()
                session["restart_count"] = int(session.get("restart_count") or 0) + 1
                session["updated_at"] = _now_iso()
                _write_json(session_path, session)
                _append_log(log_path, f"[SUPERVISOR] child spawn failed err={exc!r}")
                time.sleep(float(restart_delay_sec))
                continue

            session["child_pid"] = int(child.pid)
            session["child_started_at"] = _now_iso()
            session["status"] = "running"
            session["updated_at"] = _now_iso()
            _write_json(session_path, session)
            _append_log(log_path, f"[SUPERVISOR] child started pid={child.pid}")

        if stop_requested:
            if child is not None and child.poll() is None:
                _append_log(log_path, f"[SUPERVISOR] stopping child pid={child.pid}")
                _taskkill_tree(int(child.pid))
                deadline = time.time() + 30.0
                while time.time() < deadline and child.poll() is None:
                    time.sleep(0.5)
            session = _load_json(session_path)
            session["status"] = "stopped"
            session["stop_reason"] = "requested_stop"
            session["child_pid"] = 0
            session["updated_at"] = _now_iso()
            _write_json(session_path, session)
            _append_log(log_path, "[SUPERVISOR] stopped cleanly")
            return 0

        session["status"] = "running"
        session["updated_at"] = _now_iso()
        _write_json(session_path, session)
        time.sleep(5.0)


def cmd_stop(args: argparse.Namespace) -> int:
    session_path = Path(args.session).resolve() if args.session else (Path(args.workdir).resolve() / "logs" / str(args.session_name)).resolve()
    session = _load_json(session_path)
    if not session:
        print(json.dumps({"stopped": False, "reason": "session_not_found", "session": str(session_path)}, ensure_ascii=False))
        return 0

    stop_flag = Path(str(session.get("stop_flag") or ""))
    if stop_flag:
        stop_flag.parent.mkdir(parents=True, exist_ok=True)
        stop_flag.write_text(f"STOP={_now_iso()}\n", encoding="utf-8")
    session["desired_state"] = "stopped"
    session["updated_at"] = _now_iso()
    _write_json(session_path, session)

    supervisor_pid = int(session.get("supervisor_pid") or 0)
    child_pid = int(session.get("child_pid") or 0)
    wait_until = time.time() + max(1, int(args.wait_sec))
    while time.time() < wait_until:
        supervisor_alive = supervisor_pid > 0 and _is_pid_alive(supervisor_pid)
        child_alive = child_pid > 0 and _is_pid_alive(child_pid)
        if not supervisor_alive and not child_alive:
            break
        time.sleep(1.0)

    supervisor_alive = supervisor_pid > 0 and _is_pid_alive(supervisor_pid)
    child_alive = child_pid > 0 and _is_pid_alive(child_pid)
    if child_alive:
        _taskkill_tree(child_pid)
        child_alive = child_pid > 0 and _is_pid_alive(child_pid)
    if supervisor_alive:
        _taskkill_tree(supervisor_pid)
        supervisor_alive = supervisor_pid > 0 and _is_pid_alive(supervisor_pid)

    session = _load_json(session_path) or session
    session["status"] = "stopped"
    session["stop_reason"] = "requested_stop"
    session["child_pid"] = 0
    session["updated_at"] = _now_iso()
    _write_json(session_path, session)
    print(
        json.dumps(
            {
                "stopped": True,
                "session": str(session_path),
                "supervisor_alive": supervisor_alive,
                "child_alive": child_alive,
            },
            ensure_ascii=False,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Persistent Windows-friendly supervisor for copytrade_run."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    launch = sub.add_parser("launch")
    launch.add_argument("--workdir", required=True)
    launch.add_argument("--config", default="copytrade_config.json")
    launch.add_argument("--mode", choices=("live", "dry"), default="live")
    launch.add_argument("--poll", type=int, default=20)
    launch.add_argument("--prefix", default="persistent_live")
    launch.add_argument("--run-id", default="")
    launch.add_argument("--session-name", default="persistent_live_session.json")
    launch.add_argument("--restart-delay-sec", type=int, default=10)
    launch.add_argument("--max-restart-delay-sec", type=int, default=60)
    launch.add_argument("--quick-fail-window-sec", type=int, default=30)
    launch.set_defaults(func=cmd_launch)

    supervise = sub.add_parser("supervise")
    supervise.add_argument("--session", required=True)
    supervise.add_argument("--restart-delay-sec", type=int, default=10)
    supervise.add_argument("--max-restart-delay-sec", type=int, default=60)
    supervise.add_argument("--quick-fail-window-sec", type=int, default=30)
    supervise.set_defaults(func=cmd_supervise)

    stop = sub.add_parser("stop")
    stop.add_argument("--session", default="")
    stop.add_argument("--workdir", default=".")
    stop.add_argument("--session-name", default="persistent_live_session.json")
    stop.add_argument("--wait-sec", type=int, default=20)
    stop.set_defaults(func=cmd_stop)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
