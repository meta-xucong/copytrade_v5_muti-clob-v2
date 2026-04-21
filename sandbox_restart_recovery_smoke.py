from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import IO, Any, Callable, Dict, Tuple

import requests

from ct_clob_gateway import cancel_order_v2, init_v2_client, place_limit_order_v2


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "copytrade_config.json"
ACCOUNTS_PATH = BASE_DIR / "accounts.json"
COPYTRADE_RUN_PATH = BASE_DIR / "copytrade_run.py"

STANDARD_TEST_MARKET = {
    "label": "restart_recovery_standard_buy",
    "question": "US-Iran nuclear deal before 2027?",
    "token_id": "45763018441764333771124945243746174684578244015331389396782339063349542289693",
}


def _load_runtime() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))
    accounts = json.loads(ACCOUNTS_PATH.read_text(encoding="utf-8-sig")).get("accounts", [])
    account = next(acct for acct in accounts if acct.get("enabled", True))
    return cfg, account


def _book(token_id: str, host: str) -> Dict[str, Any]:
    response = requests.get(
        f"{str(host).rstrip('/')}/book",
        params={"token_id": token_id},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def _best_bid(book: Dict[str, Any]) -> float | None:
    bids = book.get("bids") or []
    return max((float(level["price"]) for level in bids), default=None)


def _first_target_address(cfg: Dict[str, Any]) -> str:
    target_list = cfg.get("target_addresses")
    if isinstance(target_list, list) and target_list:
        first = target_list[0]
        if isinstance(first, dict):
            address = str(first.get("address") or "").strip()
            if address:
                return address
        elif isinstance(first, str):
            address = str(first).strip()
            if address:
                return address
    address = str(cfg.get("target_address") or "").strip()
    if not address:
        raise RuntimeError("could not resolve target_address from config")
    return address


def _short_part(address: str) -> str:
    short = address.strip().lower()
    if short.startswith("0x") and len(short) >= 10:
        return f"{short[2:6]}_{short[-4:]}"
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", short)[:16] or "unknown"


def _state_path_for_account(base_dir: Path, target_address: str, my_address: str) -> Path:
    return base_dir / "logs" / "state" / f"state_{_short_part(target_address)}_{_short_part(my_address)}.json"


def _prepare_runtime_dir(cfg: Dict[str, Any], account: Dict[str, Any]) -> Tuple[Path, Path, Path]:
    stamp = time.strftime("%Y%m%d_%H%M%S")
    runtime_dir = BASE_DIR / "logs" / f"restart_recovery_{stamp}"
    if runtime_dir.exists():
        shutil.rmtree(runtime_dir)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    runtime_cfg = dict(cfg)
    runtime_cfg["accounts_file"] = "accounts.json"
    runtime_cfg["log_dir"] = "logs"
    runtime_cfg["account_workers"] = 1
    runtime_cfg["poll_interval_sec"] = 8
    runtime_cfg["poll_interval_sec_exiting"] = 3
    runtime_cfg["order_visibility_grace_sec"] = 3
    runtime_cfg["adopt_existing_orders_on_boot"] = True

    config_path = runtime_dir / "copytrade_config.json"
    accounts_path = runtime_dir / "accounts.json"
    config_path.write_text(json.dumps(runtime_cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    accounts_path.write_text(
        json.dumps({"accounts": [account]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    target_address = _first_target_address(runtime_cfg)
    state_path = _state_path_for_account(runtime_dir, target_address, str(account["my_address"]))
    return runtime_dir, config_path, state_path


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _wait_for_state(
    state_path: Path,
    predicate: Callable[[Dict[str, Any]], bool],
    *,
    timeout_sec: float,
    poll_sec: float = 1.0,
) -> Dict[str, Any]:
    deadline = time.time() + timeout_sec
    last_state: Dict[str, Any] = {}
    while time.time() < deadline:
        last_state = _load_json(state_path)
        if last_state and predicate(last_state):
            return last_state
        time.sleep(poll_sec)
    raise TimeoutError(f"state predicate not satisfied within {timeout_sec}s: {state_path}")


def _state_has_order(state: Dict[str, Any], order_id: str) -> bool:
    target_id = str(order_id)
    managed_ids = {str(item) for item in (state.get("managed_order_ids") or [])}
    if target_id in managed_ids:
        return True
    open_orders = state.get("open_orders") or {}
    if isinstance(open_orders, dict):
        for orders in open_orders.values():
            for order in orders or []:
                oid = str(order.get("order_id") or order.get("id") or "")
                if oid == target_id:
                    return True
    return False


def _spawn_copytrade(
    config_path: Path,
    runtime_dir: Path,
    name: str,
) -> Tuple[subprocess.Popen[str], Path, IO[str]]:
    logs_dir = runtime_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    output_path = logs_dir / f"{name}.stdout.log"
    handle = output_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [
            sys.executable,
            str(COPYTRADE_RUN_PATH),
            "--config",
            str(config_path),
            "--dry-run",
            "--poll",
            "8",
        ],
        cwd=str(BASE_DIR),
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc, output_path, handle


def _stop_process(proc: subprocess.Popen[str], handle: IO[str]) -> int | None:
    try:
        if proc.poll() is not None:
            return proc.returncode
        proc.terminate()
        try:
            return proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            return proc.wait(timeout=15)
    finally:
        try:
            handle.close()
        except Exception:
            pass


def _tail(path: Path, lines: int = 30) -> list[str]:
    if not path.exists():
        return []
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return content[-lines:]


def _place_remote_order(client: Any, host: str) -> Dict[str, Any]:
    token_id = STANDARD_TEST_MARKET["token_id"]
    book = _book(token_id, host)
    best_bid = _best_bid(book)
    price = best_bid or 0.25
    size = float(book.get("min_order_size") or 5.0)
    result = place_limit_order_v2(
        client,
        token_id=token_id,
        side="BUY",
        price=price,
        size=size,
        timeout=30.0,
    )
    order_id = str(result.get("order_id") or "")
    if not order_id:
        raise RuntimeError(f"maker order missing order_id: {result}")
    return {
        "question": STANDARD_TEST_MARKET["question"],
        "token_id": token_id,
        "price": price,
        "size": size,
        "book": {
            "best_bid": best_bid,
            "best_ask": min((float(level["price"]) for level in (book.get("asks") or [])), default=None),
            "min_order_size": book.get("min_order_size"),
            "tick_size": book.get("tick_size"),
        },
        "order_id": order_id,
        "place_result": result,
    }


def main() -> int:
    cfg, account = _load_runtime()
    host = str(cfg.get("poly_host") or "https://clob-v2.polymarket.com")
    client = init_v2_client(account["private_key"], account["my_address"], cfg)
    runtime_dir, config_path, state_path = _prepare_runtime_dir(cfg, account)

    payload: Dict[str, Any] = {
        "account": {
            "name": account.get("name"),
            "my_address": account.get("my_address"),
        },
        "runtime_dir": str(runtime_dir),
        "state_path": str(state_path),
    }

    remote_order = _place_remote_order(client, host)
    order_id = str(remote_order["order_id"])
    payload["remote_order"] = remote_order

    proc1, proc1_log, proc1_handle = _spawn_copytrade(config_path, runtime_dir, "boot_adopt")
    try:
        adopted_state = _wait_for_state(
            state_path,
            lambda state: bool(state.get("adopted_existing_orders")) and _state_has_order(state, order_id),
            timeout_sec=90.0,
        )
    finally:
        _stop_process(proc1, proc1_handle)
    payload["first_boot"] = {
        "stdout_log": str(proc1_log),
        "adopted_existing_orders": bool(adopted_state.get("adopted_existing_orders")),
        "managed_order_ids": list(adopted_state.get("managed_order_ids") or []),
        "order_present": _state_has_order(adopted_state, order_id),
        "open_orders_keys": sorted((adopted_state.get("open_orders") or {}).keys()),
        "tail": _tail(proc1_log, 20),
    }

    proc2, proc2_log, proc2_handle = _spawn_copytrade(config_path, runtime_dir, "restart_verify")
    try:
        restart_state = _wait_for_state(
            state_path,
            lambda state: bool(state.get("adopted_existing_orders")) and _state_has_order(state, order_id),
            timeout_sec=60.0,
        )
    finally:
        _stop_process(proc2, proc2_handle)
    payload["second_boot"] = {
        "stdout_log": str(proc2_log),
        "adopted_existing_orders": bool(restart_state.get("adopted_existing_orders")),
        "managed_order_ids": list(restart_state.get("managed_order_ids") or []),
        "order_present": _state_has_order(restart_state, order_id),
        "tail": _tail(proc2_log, 20),
    }

    cancel_result = cancel_order_v2(client, order_id, timeout=30.0)
    payload["remote_cancel"] = cancel_result
    time.sleep(4.0)

    proc3, proc3_log, proc3_handle = _spawn_copytrade(config_path, runtime_dir, "restart_prune")
    try:
        pruned_state = _wait_for_state(
            state_path,
            lambda state: not _state_has_order(state, order_id),
            timeout_sec=90.0,
        )
    finally:
        _stop_process(proc3, proc3_handle)
    payload["third_boot_after_cancel"] = {
        "stdout_log": str(proc3_log),
        "managed_order_ids": list(pruned_state.get("managed_order_ids") or []),
        "order_present": _state_has_order(pruned_state, order_id),
        "open_orders": pruned_state.get("open_orders"),
        "tail": _tail(proc3_log, 20),
    }

    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
