from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


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


def _session_template(
    *,
    run_id: str,
    pid: int,
    started_at: str,
    planned_end: str,
    stdout_path: Path,
    stderr_path: Path,
    ctrl_path: Path,
    workdir: Path,
    mode: str,
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "pid": int(pid),
        "started_at": started_at,
        "planned_end": planned_end,
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "ctrl": str(ctrl_path),
        "workdir": str(workdir),
        "mode": mode,
        "status": "running",
    }


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
    now = datetime.now()
    run_id = args.run_id or now.strftime("%Y%m%d_%H%M%S")
    started_at = now.isoformat(timespec="seconds")
    planned_end = (now + timedelta(seconds=int(args.duration_sec))).isoformat(timespec="seconds")

    prefix = str(args.prefix or ("dry_run" if args.mode == "dry" else "live_run")).strip()
    stdout_path = logs_dir / f"{prefix}_{run_id}_stdout.log"
    stderr_path = logs_dir / f"{prefix}_{run_id}_stderr.log"
    ctrl_path = logs_dir / f"{prefix}_{run_id}_ctrl.log"
    session_path = logs_dir / str(args.session_name or f"{prefix}_session.json")

    cmd = [
        sys.executable,
        "copytrade_run.py",
        "--config",
        str(config_path),
        "--poll",
        str(int(args.poll)),
    ]
    if args.mode == "dry":
        cmd.append("--dry-run")

    stdout_handle = stdout_path.open("wb")
    stderr_handle = stderr_path.open("wb")
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    child = subprocess.Popen(
        cmd,
        cwd=str(workdir),
        stdout=stdout_handle,
        stderr=stderr_handle,
        creationflags=creationflags,
    )
    stdout_handle.close()
    stderr_handle.close()

    session = _session_template(
        run_id=run_id,
        pid=child.pid,
        started_at=started_at,
        planned_end=planned_end,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        ctrl_path=ctrl_path,
        workdir=workdir,
        mode=args.mode,
    )
    _write_json(session_path, session)
    ctrl_path.write_text(
        "\n".join(
            [
                f"START={started_at}",
                f"END={planned_end}",
                f"PID={child.pid}",
                f"STDOUT={stdout_path}",
                f"STDERR={stderr_path}",
                f"CTRL={ctrl_path}",
                f"SESSION={session_path}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    watch_cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "watch",
        "--session",
        str(session_path),
    ]
    watch_flags = 0
    if os.name == "nt":
        watch_flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    subprocess.Popen(
        watch_cmd,
        cwd=str(workdir),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=watch_flags,
    )

    print(
        json.dumps(
            {
                "session": str(session_path),
                "pid": child.pid,
                "stdout": str(stdout_path),
                "stderr": str(stderr_path),
                "ctrl": str(ctrl_path),
                "planned_end": planned_end,
            },
            ensure_ascii=False,
        )
    )
    return 0


def cmd_watch(args: argparse.Namespace) -> int:
    session_path = Path(args.session).resolve()
    while True:
        session = _load_json(session_path)
        if not session:
            return 0
        pid = int(session.get("pid") or 0)
        planned_end_text = str(session.get("planned_end") or "").strip()
        if pid <= 0 or not planned_end_text:
            return 0
        try:
            planned_end = datetime.fromisoformat(planned_end_text)
        except ValueError:
            return 0
        if not _is_pid_alive(pid):
            session["status"] = "stopped"
            session.setdefault("stopped_at", datetime.now().isoformat(timespec="seconds"))
            session.setdefault("stop_reason", "process_exited_before_planned_end")
            _write_json(session_path, session)
            return 0
        now = datetime.now()
        if now >= planned_end:
            _taskkill_tree(pid)
            session = _load_json(session_path)
            session["status"] = "stopped"
            session["stopped_at"] = datetime.now().isoformat(timespec="seconds")
            session["stop_reason"] = "planned_end_auto_stop"
            _write_json(session_path, session)
            return 0
        time.sleep(min(30.0, max(1.0, (planned_end - now).total_seconds())))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch copytrade_run with an auto-stop watchdog.")
    sub = parser.add_subparsers(dest="command", required=True)

    launch = sub.add_parser("launch")
    launch.add_argument("--workdir", required=True)
    launch.add_argument("--config", default="copytrade_config.json")
    launch.add_argument("--mode", choices=("dry", "live"), required=True)
    launch.add_argument("--duration-sec", type=int, required=True)
    launch.add_argument("--poll", type=int, default=20)
    launch.add_argument("--prefix", default="")
    launch.add_argument("--run-id", default="")
    launch.add_argument("--session-name", default="")
    launch.set_defaults(func=cmd_launch)

    watch = sub.add_parser("watch")
    watch.add_argument("--session", required=True)
    watch.set_defaults(func=cmd_watch)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
