from __future__ import annotations

import argparse
import hashlib
import json
import glob
import logging
import logging.handlers
import os
import re
import random
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from zoneinfo import ZoneInfo


# ============================================================
# MULTI-ACCOUNT & MULTI-TARGET SUPPORT (v3_muti)
# - Follower accounts loaded from accounts.json
# - Sequential round-robin processing for multiple follower accounts
# - Support multiple target addresses (target_addresses array)
# - When multiple targets hold same token, take MAXIMUM position change
# ============================================================

@dataclass
class AccountContext:
    """Context for a single follower account in multi-account mode."""
    name: str  # Account name/label
    my_address: str
    private_key: str  # Private key loaded from config
    follow_ratio: float
    clob_client: Any  # ClobClient instance
    state: Dict[str, Any]
    state_path: Path
    clob_read_client: Any | None = None  # V2 read-first client during staged migration
    enabled: bool = True
    max_notional_per_token: Optional[float] = None
    max_notional_total: Optional[float] = None

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from smartmoney_query.poly_martmoney_query.api_client import DataApiClient

from ct_clob_gateway import init_v2_client, preflight_pusd_ready_v2, refresh_market_info_cache_v2
from ct_data import (
    configure_data_http_rate_limit,
    fetch_positions_norm,
    fetch_target_actions_since,
    fetch_target_trades_since,
)
from ct_exec import (
    apply_actions,
    configure_clob_rate_limit,
    fetch_open_orders_norm,
    get_orderbook,
    reconcile_one,
)
from ct_resolver import (
    gamma_fetch_markets_by_clob_token_ids,
    market_tradeable_state,
    resolve_token_id,
)
from ct_risk import accumulator_check, risk_check
from ct_state import load_state, save_state


DEFAULT_CONFIG_PATH = Path(__file__).with_name("copytrade_config.json")
DEFAULT_LOG_DIR = Path(__file__).with_name("logs")
DEFAULT_STATE_DIR = DEFAULT_LOG_DIR / "state"
DEFAULT_STATE_PATH = DEFAULT_STATE_DIR / "state.json"


class LogDeduplicator:
    """
    Deduplicate repetitive log messages within a time window.

    For logs like [NOOP] that fire every loop iteration with the same reason,
    this class suppresses duplicates and shows a count when finally logging.
    """

    def __init__(self, window_sec: float = 60.0):
        self._window_sec = window_sec
        self._last_log_time: Dict[str, float] = {}  # key -> timestamp of last log
        self._suppressed_count: Dict[str, int] = {}  # key -> count of suppressed logs

    def set_window(self, window_sec: float) -> None:
        """Update the deduplication window."""
        self._window_sec = max(0.0, window_sec)

    def should_log(self, key: str) -> tuple[bool, int]:
        """
        Check if this log should be output.

        Returns:
            (should_log, suppressed_count): Whether to log, and how many were suppressed
        """
        if self._window_sec <= 0:
            # Deduplication disabled
            return True, 0

        now = time.time()
        last_time = self._last_log_time.get(key, 0.0)

        if now - last_time >= self._window_sec:
            # Time to log - get suppressed count and reset
            suppressed = self._suppressed_count.get(key, 0)
            self._last_log_time[key] = now
            self._suppressed_count[key] = 0
            return True, suppressed
        else:
            # Suppress this log
            self._suppressed_count[key] = self._suppressed_count.get(key, 0) + 1
            return False, 0

    def clear(self) -> None:
        """Clear all deduplication state."""
        self._last_log_time.clear()
        self._suppressed_count.clear()


# Global log deduplicator instance
_log_dedup = LogDeduplicator()

_REPLAY_BOOT_MODES = {
    "baseline_replay",
    "replay_24h",
    "replay_actions",
    "replay",
}
_DEFAULT_BOOT_REPLAY_WINDOW_SEC = 86400
WORKER_RETIRED_EXIT_CODE = 88


def _state_path_for_target(state_path: Path, target_address: str) -> Path:
    """Derive per-target state file path when user didn't explicitly provide one."""
    addr = str(target_address or "").strip().lower()
    if addr.startswith("0x") and len(addr) >= 10:
        fname = f"state_{addr[2:6]}_{addr[-4:]}.json"
    else:
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", addr)[:32] or "unknown"
        fname = f"state_{safe}.json"
    return state_path.with_name(fname)


def _state_dir_for_base(base_dir: Path) -> Path:
    state_dir = base_dir / "logs" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir


def _resolve_effective_worker_shard_count(
    valid_account_count: int,
    worker_count: int,
    worker_index: int,
) -> tuple[int, bool]:
    """
    Resolve the usable worker-count after account initialization.

    Auto-scaling is done from raw accounts.json before runtime validation, so the
    actual number of initialized accounts may be lower. When that happens:
    - shrink the active worker_count to the number of valid accounts
    - retire worker processes whose shard becomes empty after the shrink
    """
    valid_accounts = max(0, int(valid_account_count))
    configured_workers = max(1, int(worker_count))
    worker_slot = max(0, int(worker_index))
    if valid_accounts <= 0:
        return 0, False
    active_workers = min(configured_workers, valid_accounts)
    retire_worker = worker_slot >= active_workers
    return active_workers, retire_worker


def _load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    # Use utf-8-sig to tolerate BOM-prefixed JSON from Windows editors.
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("Config file must contain a JSON object")
    return payload


def _normalize_privkey(key: str) -> str:
    return key[2:] if key.startswith(("0x", "0X")) else key


_EVM_ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def _is_placeholder_addr(value: Optional[str]) -> bool:
    if not value:
        return True
    text = value.strip()
    if text.lower() in ("0x...", "0x"):
        return True
    if "..." in text:
        return True
    return False


def _is_pure_reprice(actions: Optional[list[dict]]) -> bool:
    if not actions:
        return False
    places = [action for action in actions if action.get("type") == "place"]
    if len(places) != 1:
        return False
    if not bool(places[0].get("_reprice")):
        return False
    for action in actions:
        action_type = action.get("type")
        if action_type in ("cancel", "place"):
            continue
        return False
    return True


def _update_sell_health_monitor(
    state: Dict[str, Any],
    cfg: Dict[str, Any],
    now_ts: int,
    sell_signals_inc: int,
    sell_actions_inc: int,
    logger: logging.Logger,
    account_label: str = "",
) -> None:
    """
    Track sell signal->execution health in a rolling window and emit warning on imbalance.

    signals: target SELL actions observed this loop.
    actions: SELL place actions that passed all local gates and were sent to apply_actions.
    """
    window_sec = int(cfg.get("sell_health_window_sec") or 600)
    if window_sec <= 0:
        return
    min_signals = max(1, int(cfg.get("sell_health_min_signals") or 10))
    min_exec_ratio = float(cfg.get("sell_health_min_exec_ratio") or 0.4)
    max_gap = max(1, int(cfg.get("sell_health_max_signal_action_gap") or 20))
    warn_cooldown_sec = int(cfg.get("sell_health_warn_cooldown_sec") or window_sec)
    monitor = state.setdefault("sell_health_monitor", {})
    if not isinstance(monitor, dict):
        state["sell_health_monitor"] = {}
        monitor = state["sell_health_monitor"]
    start_ts = int(monitor.get("start_ts") or now_ts)
    signals = int(monitor.get("signals") or 0) + max(0, int(sell_signals_inc))
    actions = int(monitor.get("actions") or 0) + max(0, int(sell_actions_inc))
    monitor["start_ts"] = start_ts
    monitor["signals"] = signals
    monitor["actions"] = actions
    if now_ts - start_ts < window_sec:
        return

    ratio = (actions / signals) if signals > 0 else 1.0
    gap = max(0, signals - actions)
    is_bad = signals >= min_signals and (ratio + 1e-9 < min_exec_ratio or gap >= max_gap)
    acct_text = f" account={account_label}" if account_label else ""
    if is_bad:
        last_warn_ts = int(monitor.get("last_warn_ts") or 0)
        if warn_cooldown_sec <= 0 or now_ts - last_warn_ts >= warn_cooldown_sec:
            logger.warning(
                "[SELL_HEALTH]%s window_sec=%s signals=%s actions=%s ratio=%.3f gap=%s thr_ratio=%.3f thr_gap=%s",
                acct_text,
                window_sec,
                signals,
                actions,
                ratio,
                gap,
                min_exec_ratio,
                max_gap,
            )
            monitor["last_warn_ts"] = now_ts
    elif bool(cfg.get("sell_health_log_ok", False)) and signals >= min_signals:
        logger.info(
            "[SELL_HEALTH_OK]%s window_sec=%s signals=%s actions=%s ratio=%.3f",
            acct_text,
            window_sec,
            signals,
            actions,
            ratio,
        )
    monitor["start_ts"] = now_ts
    monitor["signals"] = 0
    monitor["actions"] = 0


def _is_evm_address(value: Optional[str]) -> bool:
    if not value:
        return False
    return bool(_EVM_ADDR_RE.match(value.strip()))


def _get_env_first(keys: list[str]) -> Optional[str]:
    for key in keys:
        env_value = os.getenv(key)
        if env_value and env_value.strip():
            return env_value.strip()
    return None


_TARGET_LEVELS = {"A", "B", "C"}
_DEFAULT_TARGET_LEVEL_SKIP_RATIOS = {"A": 0.0, "B": 0.4, "C": 0.8}


def _normalize_target_level(level: Any, default: str = "A") -> str:
    level_text = str(level or "").strip().upper()
    if level_text in _TARGET_LEVELS:
        return level_text
    default_text = str(default or "").strip().upper()
    if default_text in _TARGET_LEVELS:
        return default_text
    return ""


def _resolve_target_level_skip_ratios(cfg: Dict[str, Any]) -> Dict[str, float]:
    ratios = dict(_DEFAULT_TARGET_LEVEL_SKIP_RATIOS)
    raw = cfg.get("target_level_skip_ratios")
    if isinstance(raw, dict):
        for level_raw, ratio_raw in raw.items():
            level = _normalize_target_level(level_raw, "")
            if level not in _TARGET_LEVELS:
                continue
            try:
                ratio = float(ratio_raw)
            except (TypeError, ValueError):
                continue
            ratios[level] = min(1.0, max(0.0, ratio))
    return ratios


def _resolve_target_level_map(
    cfg: Dict[str, Any],
    target_addresses: List[str],
) -> Dict[str, str]:
    default_level = _normalize_target_level(cfg.get("default_target_level"), "A")
    target_levels: Dict[str, str] = {
        str(addr).strip().lower(): default_level
        for addr in (target_addresses or [])
    }

    target_list = cfg.get("target_addresses")
    if isinstance(target_list, list):
        for item in target_list:
            if not isinstance(item, dict):
                continue
            addr = str(item.get("address") or "").strip().lower()
            if not _is_evm_address(addr):
                continue
            target_levels[addr] = _normalize_target_level(item.get("level"), default_level)

    explicit_map = cfg.get("target_levels")
    if isinstance(explicit_map, dict):
        for addr_raw, level_raw in explicit_map.items():
            addr = str(addr_raw or "").strip().lower()
            if addr in target_levels:
                target_levels[addr] = _normalize_target_level(level_raw, default_level)

    return target_levels


def _round_half_up(value: float) -> int:
    if value <= 0:
        return 0
    return int(value + 0.5)


def _calc_skip_count(total_accounts: int, skip_ratio: float) -> int:
    if total_accounts <= 0:
        return 0
    ratio = min(1.0, max(0.0, float(skip_ratio)))
    skip_count = _round_half_up(total_accounts * ratio)
    return max(0, min(total_accounts, skip_count))


def _build_target_level_signal_marker(
    buy_action_ms: int,
    t_now: Optional[float],
    now_ts: int,
) -> str:
    if buy_action_ms > 0:
        return f"act:{buy_action_ms}"
    if t_now is not None:
        return f"pos:{int(round(float(t_now) * 1_000_000))}"
    return f"time:{int(now_ts)}"


def _build_target_level_signal_id(
    token_id: str,
    source_target: str,
    signal_marker: str,
) -> str:
    source = str(source_target or "unknown").strip().lower() or "unknown"
    return f"{source}|{token_id}|{signal_marker}"


def _resolve_target_level_skip_log_path(cfg: Dict[str, Any], base_dir: Path) -> Path:
    log_dir_value = cfg.get("log_dir") or "logs"
    log_dir = Path(log_dir_value)
    if not log_dir.is_absolute():
        log_dir = base_dir / log_dir
    file_value = str(cfg.get("target_level_skip_log_file") or "target_level_skip_log.jsonl")
    file_path = Path(file_value)
    if file_path.is_absolute():
        return file_path
    return log_dir / file_path


def _append_target_level_skip_log(
    log_path: Path,
    payload: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning("[TARGET_LEVEL] skip log write failed path=%s err=%s", log_path, exc)


def _prune_target_level_decisions(
    decisions: Dict[str, Dict[str, Any]],
    now_ts: int,
    ttl_sec: int,
    max_entries: int = 5000,
) -> None:
    if not isinstance(decisions, dict) or not decisions:
        return
    if ttl_sec > 0:
        expired = [
            key
            for key, item in decisions.items()
            if now_ts - int((item or {}).get("ts") or 0) > ttl_sec
        ]
        for key in expired:
            decisions.pop(key, None)
    if len(decisions) <= max_entries:
        return
    to_drop = len(decisions) - max_entries
    for key, _item in sorted(decisions.items(), key=lambda kv: int((kv[1] or {}).get("ts") or 0))[:to_drop]:
        decisions.pop(key, None)


def _resolve_signal_source_target(
    token_id: str,
    buy_signal_source_by_token: Dict[str, str],
    position_source: Dict[str, str],
    target_addresses: List[str],
) -> str:
    token = str(token_id or "")
    src = str(buy_signal_source_by_token.get(token) or "").strip().lower()
    if src:
        return src
    src = str(position_source.get(token) or "").strip().lower()
    if src:
        return src
    if target_addresses:
        return str(target_addresses[0]).strip().lower()
    return ""


def _pick_target_level_skipped_accounts(
    signal_id: str,
    all_account_ids: List[str],
    skip_ratio: float,
    now_ts: int,
    decision_cache: Dict[str, Dict[str, Any]],
) -> List[str]:
    total_accounts = len(all_account_ids)
    skip_count = _calc_skip_count(total_accounts, skip_ratio)
    if skip_count <= 0:
        return []
    entry = decision_cache.get(signal_id) or {}
    cached_accounts = entry.get("skipped_accounts")
    if (
        entry
        and int(entry.get("total_accounts") or 0) == total_accounts
        and int(entry.get("skip_count") or 0) == skip_count
        and abs(float(entry.get("skip_ratio") or 0.0) - float(skip_ratio)) < 1e-9
        and isinstance(cached_accounts, list)
    ):
        entry["ts"] = now_ts
        decision_cache[signal_id] = entry
        return [str(item).strip().lower() for item in cached_accounts if str(item).strip()]

    picked = random.sample(all_account_ids, skip_count)
    decision_cache[signal_id] = {
        "ts": int(now_ts),
        "skip_count": int(skip_count),
        "total_accounts": int(total_accounts),
        "skip_ratio": float(skip_ratio),
        "skipped_accounts": list(picked),
    }
    return [str(item).strip().lower() for item in picked if str(item).strip()]


def _normalize_account_ids(account_ids: List[str]) -> List[str]:
    normalized: List[str] = []
    seen: Set[str] = set()
    for item in account_ids:
        acct_id = str(item or "").strip().lower()
        if not acct_id or acct_id in seen:
            continue
        seen.add(acct_id)
        normalized.append(acct_id)
    normalized.sort()
    return normalized


def _normalize_account_ids_keep_order(account_ids: List[str]) -> List[str]:
    normalized: List[str] = []
    seen: Set[str] = set()
    for item in account_ids:
        acct_id = str(item or "").strip().lower()
        if not acct_id or acct_id in seen:
            continue
        seen.add(acct_id)
        normalized.append(acct_id)
    return normalized


def _resolve_target_level_seed(cfg: Dict[str, Any]) -> str:
    seed = str(cfg.get("target_level_seed") or "").strip()
    return seed or "token_sticky_v1"


def _pick_token_sticky_follow_accounts(
    token_id: str,
    all_account_ids: List[str],
    skip_ratio: float,
    now_ts: int,
    decision_cache: Dict[str, Dict[str, Any]],
    seed: str,
    assigned_level: str = "",
) -> List[str]:
    token_key = str(token_id or "").strip()
    normalized_accounts = _normalize_account_ids(all_account_ids)
    total_accounts = len(normalized_accounts)
    skip_count = _calc_skip_count(total_accounts, skip_ratio)
    follow_count = max(0, total_accounts - skip_count)
    if total_accounts <= 0:
        return []

    entry = decision_cache.get(token_key) or {}
    cached_follow = entry.get("follow_accounts")
    if (
        entry
        and int(entry.get("total_accounts") or 0) == total_accounts
        and isinstance(cached_follow, list)
        and str(entry.get("seed") or "") == str(seed or "")
    ):
        entry["ts"] = int(now_ts)
        decision_cache[token_key] = entry
        return [acct for acct in _normalize_account_ids_keep_order(cached_follow) if acct]

    ranked_accounts = sorted(
        normalized_accounts,
        key=lambda acct_id: hashlib.sha256(
            f"{seed}|{token_key}|{acct_id}".encode("utf-8")
        ).hexdigest(),
    )
    follow_accounts = ranked_accounts[:follow_count]
    follow_set = set(follow_accounts)
    skipped_accounts = [acct_id for acct_id in normalized_accounts if acct_id not in follow_set]
    decision_cache[token_key] = {
        "ts": int(now_ts),
        "seed": str(seed or ""),
        "skip_count": int(skip_count),
        "follow_count": int(follow_count),
        "total_accounts": int(total_accounts),
        "skip_ratio": float(skip_ratio),
        "assigned_level": str(assigned_level or ""),
        "follow_accounts": list(follow_accounts),
        "skipped_accounts": list(skipped_accounts),
    }
    return list(follow_accounts)


def _shorten_address(address: str) -> str:
    text = address.strip()
    if len(text) <= 12:
        return text
    return f"{text[:6]}..{text[-4:]}"


def _is_replay_mode(cfg: Dict[str, Any]) -> bool:
    mode = str(cfg.get("boot_sync_mode") or "baseline_only").lower()
    return mode in _REPLAY_BOOT_MODES


def _get_actions_replay_window_sec(cfg: Dict[str, Any], is_replay_mode: Optional[bool] = None) -> int:
    if is_replay_mode is None:
        is_replay_mode = _is_replay_mode(cfg)
    raw = cfg.get("actions_replay_window_sec")
    if raw is None or raw == "":
        return _DEFAULT_BOOT_REPLAY_WINDOW_SEC if is_replay_mode else 600
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return _DEFAULT_BOOT_REPLAY_WINDOW_SEC if is_replay_mode else 600


def _get_replay_floor_ms(cfg: Dict[str, Any], state: Dict[str, Any]) -> int:
    run_start_ms = int(state.get("run_start_ms") or 0)
    if not _is_replay_mode(cfg):
        return run_start_ms
    window_sec = _get_actions_replay_window_sec(cfg, True)
    return max(0, run_start_ms - window_sec * 1000)


def _setup_logging(
    cfg: Dict[str, Any],
    target_address: str,
    base_dir: Path,
) -> logging.Logger:
    log_dir_value = cfg.get("log_dir") or "logs"
    log_dir = Path(log_dir_value)
    if not log_dir.is_absolute():
        log_dir = base_dir / log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    short = _shorten_address(target_address)
    today = datetime.now().strftime("%Y-%m-%d")
    log_path = log_dir / f"copytrade_{short}_{today}.log"

    log_retention_days = int(cfg.get("log_retention_days") or 7)

    level_name = str(cfg.get("log_level") or "INFO").upper()
    level = logging.INFO
    if level_name in logging._nameToLevel:
        level = logging._nameToLevel[level_name]

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)

    # Daily log file 鈥?filename already contains the date; no rotation handler needed
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    root_logger.addHandler(stream_handler)
    root_logger.addHandler(file_handler)

    # Suppress verbose third-party HTTP request logs unless in DEBUG mode
    # These libraries log every HTTP request at INFO level, which clutters output
    _suppress_verbose_third_party_loggers(level)

    logger = logging.getLogger(__name__)
    logger.debug("logging initialized: %s (daily log, retention=%dd)", log_path, log_retention_days)

    # Run one-time cleanup of legacy log files (old naming with pid/timestamp)
    _cleanup_old_logs(log_dir, log_retention_days, logger)

    return logger


def _cleanup_old_logs(
    log_dir: Path,
    retention_days: int,
    logger: logging.Logger,
) -> int:
    """Delete log files older than *retention_days* by mtime.

    Covers both legacy files (copytrade_*_*_pid*.log) and rotated files
    (copytrade_*.log.YYYY-MM-DD).  Returns number of files removed.
    """
    cutoff = time.time() - retention_days * 86400
    removed = 0
    for pattern in ("copytrade_*.log", "copytrade_*.log.*"):
        for path_str in glob.glob(str(log_dir / pattern)):
            try:
                if os.path.getmtime(path_str) < cutoff:
                    os.remove(path_str)
                    removed += 1
            except OSError:
                pass
    if removed:
        logger.info("[LOG] cleaned up %d old log file(s) (retention=%dd)", removed, retention_days)
    return removed


def _suppress_verbose_third_party_loggers(app_level: int) -> None:
    """
    Suppress verbose INFO-level logs from third-party HTTP libraries.
    Only show their logs in DEBUG mode; otherwise set them to WARNING.
    """
    verbose_loggers = [
        "httpx",           # HTTP Request: GET/POST ... logs
        "httpcore",        # httpx's underlying library
        "urllib3",         # requests library's HTTP logs
        "requests",        # requests library
        "hpack",           # HTTP/2 header compression
        "h2",              # HTTP/2 protocol
        "h11",             # HTTP/1.1 protocol
    ]
    # Only show third-party HTTP logs in DEBUG mode
    third_party_level = logging.DEBUG if app_level <= logging.DEBUG else logging.WARNING
    for name in verbose_loggers:
        logging.getLogger(name).setLevel(third_party_level)


def _resolve_addr(name: str, current: Optional[str], env_keys: list[str]) -> str:
    if _is_placeholder_addr(current):
        current = _get_env_first(env_keys)

    if not _is_evm_address(current):
        raise ValueError(
            f"{name} is missing or invalid: {current!r}. "
            f"Expected EVM address format 0x + 40 hex chars. "
            f"Set {name} in copytrade_config.json or env vars: {env_keys}"
        )
    return current.strip()


def _resolve_target_addresses(cfg: Dict[str, Any], logger: logging.Logger) -> List[str]:
    """
    Resolve multiple target addresses from config.

    Supports both:
    - target_addresses: ["0x...", "0x..."] (new multi-target format)
    - target_address: "0x..." (backward compatible single target)

    Returns list of valid target addresses.
    """
    targets: List[str] = []

    # Try target_addresses array first
    target_list = cfg.get("target_addresses")
    if isinstance(target_list, list) and target_list:
        for addr in target_list:
            addr_str = str(addr).strip()
            if _is_evm_address(addr_str) and not _is_placeholder_addr(addr_str):
                targets.append(addr_str)
            else:
                logger.warning("[MULTI-TARGET] Invalid target address skipped: %s", addr_str)

    # Fall back to single target_address if no valid targets from array
    if not targets:
        single_target = cfg.get("target_address")
        if single_target and _is_evm_address(single_target) and not _is_placeholder_addr(single_target):
            targets.append(str(single_target).strip())

    if not targets:
        raise ValueError("No valid target addresses configured. Set target_addresses or target_address.")

    return targets


def _fetch_all_target_positions(
    data_client: Any,
    target_addresses: List[str],
    target_ratios: Dict[str, float],
    target_blacklists: Dict[str, List[str]],
    size_threshold: float,
    positions_limit: int,
    positions_max_pages: int,
    refresh_sec: int,
    cache_bust_mode: str,
    header_keys: List[str],
    logger: logging.Logger,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, str]]:
    """
    Fetch positions from all target addresses and merge them.

    Merge strategy: For each token, take the MAXIMUM position size across all targets.

    Returns:
        - merged_positions: List of merged position dicts
        - merged_info: Combined info dict
        - position_source: Dict mapping token_id to source target address
    """
    from ct_data import fetch_positions_norm

    all_positions_by_token: Dict[str, Dict[str, Any]] = {}  # token_key -> best position
    position_source: Dict[str, str] = {}  # token_id -> source target address
    any_ok = False
    any_incomplete = False

    for target_addr in target_addresses:
        try:
            positions, info = fetch_positions_norm(
                data_client,
                target_addr,
                size_threshold,
                positions_limit=positions_limit,
                positions_max_pages=positions_max_pages,
                refresh_sec=refresh_sec,
                force_http=True,
                cache_bust_mode=cache_bust_mode,
                header_keys=header_keys,
            )

            if info.get("ok"):
                any_ok = True
            if info.get("incomplete"):
                any_incomplete = True

            # Merge positions - take maximum for each token (apply per-target ratio)
            ratio = target_ratios.get(target_addr.lower(), 1.0)
            blacklist = target_blacklists.get(target_addr.lower(), [])
            for pos in positions:
                token_key = str(pos.get("token_key") or "")
                if not token_key:
                    continue

                # Per-target blacklist filter (matches token title)
                if blacklist:
                    title_l = str(pos.get("title") or "").lower()
                    if any(str(bl_item).lower() in title_l for bl_item in blacklist if bl_item is not None):
                        continue

                size = float(pos.get("size") or 0.0) * ratio
                token_id = pos.get("token_id") or pos.get("raw", {}).get("asset")

                existing = all_positions_by_token.get(token_key)
                if existing is None or size > float(existing.get("size") or 0.0):
                    # This target has larger position, use it
                    pos_copy = dict(pos)
                    pos_copy["size"] = size
                    pos_copy["_source_target"] = target_addr  # Track source
                    pos_copy["_target_ratio"] = ratio
                    all_positions_by_token[token_key] = pos_copy
                    if token_id:
                        position_source[str(token_id)] = target_addr

            logger.debug(
                "[MULTI-TARGET] Fetched %d positions from target=%s",
                len(positions),
                _shorten_address(target_addr),
            )

        except Exception as exc:
            logger.warning(
                "[MULTI-TARGET] Failed to fetch positions from target=%s: %s",
                _shorten_address(target_addr),
                exc,
            )

    merged_positions = list(all_positions_by_token.values())
    merged_info = {
        "ok": any_ok,
        "incomplete": any_incomplete,
        "target_count": len(target_addresses),
        "merged_token_count": len(merged_positions),
    }

    if len(target_addresses) > 1:
        logger.info(
            "[MULTI-TARGET] Merged positions from %d targets: %d unique tokens",
            len(target_addresses),
            len(merged_positions),
        )

    return merged_positions, merged_info, position_source


def _fetch_all_target_actions(
    data_client: Any,
    target_addresses: List[str],
    cursor_ms: int,
    use_trades_api: bool,
    page_size: int,
    max_offset: int,
    taker_only: bool,
    logger: logging.Logger,
    target_blacklists: Dict[str, List[str]] | None = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Fetch actions/trades from all target addresses and merge them.

    Actions are aggregated - all buy/sell actions from all targets are combined.

    Returns:
        - merged_actions: Combined list of actions from all targets
        - merged_info: Combined info dict (includes latest_ms for cursor updates)
    """
    from ct_data import fetch_target_actions_since, fetch_target_trades_since

    all_actions: List[Dict[str, Any]] = []
    any_ok = False
    any_incomplete = False
    max_latest_ms = 0  # Track latest timestamp across all targets

    for target_addr in target_addresses:
        try:
            if use_trades_api:
                actions, info = fetch_target_trades_since(
                    data_client,
                    target_addr,
                    cursor_ms,
                    page_size=page_size,
                    max_offset=max_offset,
                    taker_only=taker_only,
                )
            else:
                actions, info = fetch_target_actions_since(
                    data_client,
                    target_addr,
                    cursor_ms,
                    page_size=page_size,
                    max_offset=max_offset,
                )

            if info.get("ok"):
                any_ok = True
            if info.get("incomplete"):
                any_incomplete = True

            # Track latest timestamp from this target
            target_latest_ms = int(info.get("latest_ms") or 0)
            if target_latest_ms > max_latest_ms:
                max_latest_ms = target_latest_ms

            # Add source target to each action (with per-target blacklist filter on BUY only)
            blacklist = (target_blacklists or {}).get(target_addr.lower(), [])
            skipped_blacklist = 0
            for action in actions:
                action_copy = dict(action)
                action_copy["_source_target"] = target_addr
                side = str(action_copy.get("side") or "").upper()
                if blacklist and side == "BUY":
                    title_l = str(
                        action_copy.get("title")
                        or (action_copy.get("raw") or {}).get("title")
                        or ""
                    ).lower()
                    if any(str(bl_item).lower() in title_l for bl_item in blacklist if bl_item is not None):
                        skipped_blacklist += 1
                        continue
                all_actions.append(action_copy)
            if skipped_blacklist:
                logger.debug(
                    "[MULTI-TARGET] Skipped %d blacklisted BUY actions from target=%s",
                    skipped_blacklist,
                    _shorten_address(target_addr),
                )

            if actions:
                logger.debug(
                    "[MULTI-TARGET] Fetched %d actions from target=%s",
                    len(actions),
                    _shorten_address(target_addr),
                )

        except Exception as exc:
            logger.warning(
                "[MULTI-TARGET] Failed to fetch actions from target=%s: %s",
                _shorten_address(target_addr),
                exc,
            )

    # Sort by timestamp
    all_actions.sort(key=lambda a: int(a.get("timestamp_ms") or a.get("ts") or 0))

    # Also check latest from merged actions (in case info.latest_ms wasn't set)
    if all_actions:
        last_action = all_actions[-1]
        action_ts = int(last_action.get("timestamp_ms") or last_action.get("ts") or 0)
        if action_ts > max_latest_ms:
            max_latest_ms = action_ts

    merged_info = {
        "ok": any_ok,
        "incomplete": any_incomplete,
        "target_count": len(target_addresses),
        "total_actions": len(all_actions),
        "latest_ms": max_latest_ms,  # CRITICAL: needed for cursor updates
    }

    return all_actions, merged_info


def _cfg_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on", "enable", "enabled"}:
            return True
        if text in {"0", "false", "no", "n", "off", "disable", "disabled", ""}:
            return False
    return bool(value)


def _should_accept_buy_action_source(
    mode: str,
    source_target: str,
    preferred_source: str,
) -> bool:
    """Filter BUY signal source in multi-target mode."""
    mode_norm = str(mode or "all").strip().lower()
    if mode_norm in ("position_source", "position_source_consistent"):
        if preferred_source and source_target and source_target != preferred_source:
            return False
    return True


def _should_hold_reentry_buy(
    *,
    now_ts: int,
    my_shares: float,
    last_exit_ts: int,
    reentry_cooldown_sec: int,
    signal_buy_shares: float,
    order_buy_shares: float,
    order_buy_usd: float,
    force_buy_shares: float,
    force_buy_usd: float,
    eps: float,
) -> tuple[bool, str]:
    """
    Hold BUY for a short window after a completed exit to avoid whipsaw churn.
    SELL path is intentionally never blocked here.
    """
    if reentry_cooldown_sec <= 0 or last_exit_ts <= 0:
        return False, "disabled_or_no_exit"
    if my_shares > eps:
        return False, "has_inventory"
    elapsed = int(now_ts - last_exit_ts)
    if elapsed < 0 or elapsed >= reentry_cooldown_sec:
        return False, "outside_window"
    strong_by_shares = (
        force_buy_shares > 0
        and max(signal_buy_shares, order_buy_shares) >= force_buy_shares
    )
    strong_by_usd = force_buy_usd > 0 and order_buy_usd >= force_buy_usd
    if strong_by_shares or strong_by_usd:
        return False, "force_override"
    return True, "cooldown_hold"


def _should_execute_sell_source_signal(
    *,
    state: Dict[str, Any],
    token_id: str,
    now_ts: int,
    current_sell_sources: Dict[str, int],
    primary_entry_source: str,
    enabled: bool,
    secondary_consensus_count: int,
    secondary_window_sec: int,
    primary_immediate: bool = True,
) -> tuple[bool, str, List[str]]:
    """
    Source-aware SELL gating.

    - Primary entry source SELL can exit immediately.
    - Secondary sources must form a small recent consensus.
    - If the primary source is unknown, fall back to the legacy immediate path
      so older positions do not get stranded.
    """
    token = str(token_id or "").strip()
    if not token:
        return True, "missing_token", []
    if not enabled:
        return True, "disabled", sorted(
            str(src or "").strip().lower()
            for src in (current_sell_sources or {}).keys()
            if str(src or "").strip()
        )

    primary = str(primary_entry_source or "").strip().lower()
    vote_bucket = state.setdefault("sell_source_votes", {})
    if not isinstance(vote_bucket, dict):
        vote_bucket = {}
        state["sell_source_votes"] = vote_bucket
    raw_votes = vote_bucket.get(token)
    token_votes: Dict[str, int] = {}
    if isinstance(raw_votes, dict):
        cutoff = int(now_ts) - max(1, int(secondary_window_sec or 0))
        for src, ts in raw_votes.items():
            src_norm = str(src or "").strip().lower()
            if not src_norm:
                continue
            try:
                ts_i = int(ts or 0)
            except Exception:
                continue
            if secondary_window_sec <= 0 or ts_i >= cutoff:
                token_votes[src_norm] = ts_i

    current_norm: Dict[str, int] = {}
    for src, ts in (current_sell_sources or {}).items():
        src_norm = str(src or "").strip().lower()
        if not src_norm:
            continue
        try:
            ts_i = int(ts or 0)
        except Exception:
            ts_i = 0
        if ts_i <= 0:
            ts_i = int(now_ts)
        current_norm[src_norm] = max(current_norm.get(src_norm, 0), ts_i)
        token_votes[src_norm] = max(token_votes.get(src_norm, 0), ts_i)

    if token_votes:
        vote_bucket[token] = token_votes
    else:
        vote_bucket.pop(token, None)

    sellers = sorted(token_votes)
    if not current_norm:
        return True, "no_source_fallback", sellers
    if not primary:
        vote_bucket.pop(token, None)
        return True, "missing_primary_source_fallback", sorted(current_norm)
    if primary_immediate and primary in current_norm:
        vote_bucket.pop(token, None)
        return True, "primary_source_sell", sorted(current_norm)

    secondary_sources = sorted(src for src in token_votes if src != primary)
    needed = max(1, int(secondary_consensus_count or 0))
    if len(secondary_sources) >= needed:
        vote_bucket.pop(token, None)
        return True, "secondary_source_consensus", secondary_sources
    return False, "secondary_source_consensus_wait", secondary_sources


def _topic_risk_decay_score(score: float, elapsed_sec: int, window_sec: int) -> float:
    """Exponentially decay topic risk score over time."""
    score_f = float(score or 0.0)
    if score_f <= 0:
        return 0.0
    if elapsed_sec <= 0 or window_sec <= 0:
        return max(0.0, score_f)
    try:
        return max(0.0, score_f * math.exp(-float(elapsed_sec) / max(1.0, float(window_sec))))
    except Exception:
        return max(0.0, score_f)


def _orderbook_min_order_shares(orderbook: Dict[str, Any] | None) -> float:
    """Extract market min order size from orderbook payload when available."""
    if not isinstance(orderbook, dict):
        return 0.0
    for key in ("min_order_size", "minimum_order_size", "minOrderSize"):
        raw = orderbook.get(key)
        try:
            val = float(raw or 0.0)
        except Exception:
            continue
        if val > 0:
            return val
    return 0.0


def _book_min_order_shares(
    client: Any,
    token_id: str,
    timeout_sec: Optional[float] = None,
) -> float:
    """
    Read token-level minimum order size from CLOB orderbook payload.
    This is the authoritative per-market minimum tradable shares threshold.
    """
    book_fetcher = getattr(client, "get_order_book", None)
    if not callable(book_fetcher):
        return 0.0
    payload: Any = None
    try:
        try:
            payload = book_fetcher(str(token_id), timeout=timeout_sec)
        except TypeError:
            payload = book_fetcher(str(token_id))
    except Exception:
        return 0.0
    if hasattr(payload, "dict"):
        try:
            payload = payload.dict()
        except Exception:
            pass
    elif hasattr(payload, "__dict__") and not isinstance(payload, dict):
        try:
            payload = dict(payload.__dict__)
        except Exception:
            pass
    if not isinstance(payload, dict):
        return 0.0
    try:
        return max(
            0.0,
            float(
                payload.get("min_order_size")
                or payload.get("minimum_order_size")
                or payload.get("minOrderSize")
                or 0.0
            ),
        )
    except Exception:
        return 0.0


def _collect_target_sell_token_ids(
    cfg: Dict[str, Any],
    data_client: Any,
    target_addresses: List[str],
    logger: logging.Logger,
) -> set[str]:
    now_ms = int(time.time() * 1000)
    window_sec = max(60, int(cfg.get("hemostasis_recovery_window_sec") or 86400))
    cursor_ms = now_ms - window_sec * 1000
    actions_source = str(cfg.get("actions_source") or "trades").lower()
    use_trades_api = actions_source in ("trade", "trades")
    page_size = max(50, int(cfg.get("actions_page_size") or 300))
    max_offset = max(300, min(int(cfg.get("actions_max_offset") or 3000), 3000))
    actions_taker_only = _cfg_bool(cfg.get("actions_taker_only"), False)
    token_map: Dict[str, str] = {}
    sell_token_ids: set[str] = set()

    actions_list, actions_info = _fetch_all_target_actions(
        data_client=data_client,
        target_addresses=target_addresses,
        cursor_ms=cursor_ms,
        use_trades_api=use_trades_api,
        page_size=page_size,
        max_offset=max_offset,
        taker_only=actions_taker_only,
        logger=logger,
    )
    for action in actions_list:
        side = str(action.get("side") or "").upper()
        if side != "SELL":
            continue
        try:
            size = float(action.get("size") or 0.0)
        except Exception:
            size = 0.0
        if size <= 0:
            continue
        token_id = str(action.get("token_id") or "").strip()
        if not token_id:
            token_id = str(_extract_token_id_from_raw(action.get("raw")) or "").strip()
        token_key = str(action.get("token_key") or "").strip()
        if not token_id and token_key:
            token_id = str(token_map.get(token_key) or "").strip()
        if not token_id and token_key:
            try:
                resolved = resolve_token_id(
                    token_key,
                    {
                        "token_key": token_key,
                        "condition_id": action.get("condition_id"),
                        "outcome_index": action.get("outcome_index"),
                        "raw": action.get("raw"),
                    },
                    token_map,
                )
                token_id = str(resolved or "").strip()
            except Exception:
                token_id = ""
        if token_id:
            if token_key:
                token_map[token_key] = token_id
            sell_token_ids.add(token_id)

    logger.info(
        "[HEMOSTASIS] target_sell_scan window_sec=%s actions=%s ok=%s incomplete=%s sell_tokens=%s",
        window_sec,
        len(actions_list),
        actions_info.get("ok"),
        actions_info.get("incomplete"),
        len(sell_token_ids),
    )
    return sell_token_ids


def _reconcile_accumulator_for_account(
    cfg: Dict[str, Any],
    acct_ctx: AccountContext,
    logger: logging.Logger,
) -> None:
    max_position_usd_per_token = float(cfg.get("max_position_usd_per_token") or 0.0)
    if max_position_usd_per_token <= 0:
        return
    state = acct_ctx.state
    accumulator = state.get("buy_notional_accumulator")
    if not isinstance(accumulator, dict) or not accumulator:
        return
    my_positions = state.get("my_positions", [])
    my_by_token_id: Dict[str, float] = {}
    for pos in my_positions:
        tid = str(pos.get("token_id") or pos.get("asset_id") or "").strip()
        if tid:
            try:
                my_by_token_id[tid] = float(pos.get("size") or 0.0)
            except Exception:
                my_by_token_id[tid] = 0.0
    mid_cache = state.get("last_mid_price_by_token_id", {})
    status_cache = state.get("market_status_cache", {})
    for token_id, acc_data in list(accumulator.items()):
        if not isinstance(acc_data, dict):
            continue
        acc_usd = float(acc_data.get("usd", 0.0))
        if acc_usd <= max_position_usd_per_token:
            continue
        ref_price = float(mid_cache.get(token_id) or 0.0)
        if ref_price <= 0:
            cached = status_cache.get(token_id) or {}
            meta = cached.get("meta") or {}
            ref_price = float(meta.get("lastTradePrice") or 0.0)
        my_shares = my_by_token_id.get(token_id, 0.0)
        planned_usd = my_shares * ref_price if ref_price > 0 else 0.0
        if planned_usd <= max_position_usd_per_token * 0.95:
            if planned_usd <= 0.01:
                accumulator.pop(token_id, None)
                logger.warning(
                    "[ACCUMULATOR_RECONCILE] token_id=%s old=%s new=0 reason=position_below_threshold",
                    token_id,
                    acc_usd,
                )
            else:
                acc_data["usd"] = planned_usd
                logger.warning(
                    "[ACCUMULATOR_RECONCILE] token_id=%s old=%s new=%s reason=align_to_position",
                    token_id,
                    acc_usd,
                    planned_usd,
                )


def _run_hemostasis_recovery_for_account(
    cfg: Dict[str, Any],
    data_client: Any,
    acct_ctx: AccountContext,
    sell_token_ids: set[str],
    logger: logging.Logger,
    dry_run: bool = False,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "account": str(acct_ctx.my_address),
        "enabled": True,
        "scan_sell_tokens": len(sell_token_ids),
        "rounds": 0,
        "candidate_tokens": [],
        "placed_tokens": [],
        "skip_no_bid_tokens": [],
        "blocked_tokens": {},
        "remaining_tokens": [],
        "remaining_count": 0,
        "status": "ok",
    }
    if not sell_token_ids:
        summary["status"] = "skip_no_sell_tokens"
        return summary

    max_rounds = max(1, int(cfg.get("hemostasis_recovery_max_rounds") or 5))
    poll_sec = max(0.2, float(cfg.get("hemostasis_recovery_poll_sec") or 2.0))
    min_shares = max(0.0, float(cfg.get("hemostasis_recovery_min_shares") or 0.0))
    sell_buffer = max(0.0, float(cfg.get("hemostasis_recovery_sell_buffer_shares") or 0.0))
    min_trade_usd_gate = max(0.0, float(cfg.get("hemostasis_recovery_min_trade_usd") or 0.0))
    # If an attempted token still has almost unchanged shares on the next round,
    # treat it as "no progress" and stop retrying during this startup recovery.
    no_progress_eps_shares = max(0.01, float(cfg.get("hemostasis_no_progress_eps_shares") or 0.01))
    positions_limit = max(50, int(cfg.get("positions_limit") or 500))
    positions_max_pages = max(1, int(cfg.get("positions_max_pages") or 20))
    refresh_sec = int(cfg.get("target_positions_refresh_sec") or 25)
    my_positions_force_http = _cfg_bool(cfg.get("my_positions_force_http"), False)
    cache_bust_mode = str(cfg.get("target_cache_bust_mode") or "bucket")
    header_keys = cfg.get("positions_cache_header_keys") or [
        "Age",
        "CF-Cache-Status",
        "X-Cache",
        "Via",
        "Cache-Control",
    ]
    try:
        api_timeout_sec = float(cfg.get("api_timeout_sec") or 15.0)
    except Exception:
        api_timeout_sec = 15.0
    if api_timeout_sec <= 0:
        api_timeout_sec = None

    my_address = str(acct_ctx.my_address)
    acct_state = acct_ctx.state
    must_exit_tokens = acct_state.setdefault("must_exit_tokens", {})
    if not isinstance(must_exit_tokens, dict):
        must_exit_tokens = {}
        acct_state["must_exit_tokens"] = must_exit_tokens
    last_nonzero_my_shares = acct_state.setdefault("last_nonzero_my_shares", {})
    if not isinstance(last_nonzero_my_shares, dict):
        last_nonzero_my_shares = {}
        acct_state["last_nonzero_my_shares"] = last_nonzero_my_shares
    known_for_seed: set[str] = set()
    open_orders_ledger = acct_state.get("open_orders", {})
    if isinstance(open_orders_ledger, dict):
        known_for_seed.update(str(tid) for tid in open_orders_ledger.keys())
    if isinstance(last_nonzero_my_shares, dict):
        known_for_seed.update(str(tid) for tid in last_nonzero_my_shares.keys())
    topic_state_seed = acct_state.get("topic_state", {})
    if isinstance(topic_state_seed, dict):
        known_for_seed.update(str(tid) for tid in topic_state_seed.keys())
    target_last_seed = acct_state.get("target_last_shares", {})
    if isinstance(target_last_seed, dict):
        known_for_seed.update(str(tid) for tid in target_last_seed.keys())
    for token_id in sell_token_ids:
        if token_id in known_for_seed:
            _mark_must_exit_token(
                acct_state,
                str(token_id),
                int(time.time()),
                source="hemostasis_seed",
            )
    logger.info(
        "[HEMOSTASIS] account=%s begin max_rounds=%s sell_token_count=%s",
        _shorten_address(my_address),
        max_rounds,
        len(sell_token_ids),
    )
    blocked_tokens: Dict[str, str] = {}
    attempted_shares: Dict[str, float] = {}
    book_min_shares_cache: Dict[str, float] = {}

    max_position_usd_per_token = float(cfg.get("max_position_usd_per_token") or 0.0)
    for round_idx in range(1, max_rounds + 1):
        my_positions, my_info = fetch_positions_norm(
            data_client,
            my_address,
            size_threshold=0.0,
            positions_limit=positions_limit,
            positions_max_pages=positions_max_pages,
            refresh_sec=refresh_sec,
            force_http=my_positions_force_http,
            cache_bust_mode=cache_bust_mode,
            header_keys=header_keys,
        )
        if not my_info.get("ok", True):
            logger.warning(
                "[HEMOSTASIS] account=%s fetch_positions failed: %s",
                _shorten_address(my_address),
                my_info.get("error_msg"),
            )
            summary["status"] = "fetch_positions_failed"
            break

        read_client = getattr(acct_ctx, "clob_read_client", None) or acct_ctx.clob_client
        remote_orders, ok, err = fetch_open_orders_norm(read_client, api_timeout_sec)
        if not ok:
            logger.warning(
                "[HEMOSTASIS] account=%s fetch_open_orders failed: %s",
                _shorten_address(my_address),
                err,
            )
            remote_orders = []

        sell_or_must_exit_tokens = set(str(tid) for tid in sell_token_ids)
        sell_or_must_exit_tokens.update(str(tid) for tid in must_exit_tokens.keys())
        candidate_shares_by_token: Dict[str, float] = {}

        for pos in my_positions:
            try:
                shares = float(pos.get("size") or 0.0)
            except Exception:
                shares = 0.0
            if shares <= min_shares:
                continue
            token_id = str(pos.get("token_id") or "").strip()
            if not token_id:
                token_id = str(_extract_token_id_from_raw(pos.get("raw")) or "").strip()
            if not token_id or token_id not in sell_or_must_exit_tokens:
                continue
            candidate_shares_by_token[token_id] = max(
                float(candidate_shares_by_token.get(token_id) or 0.0),
                float(shares),
            )

        for order in remote_orders:
            token_id = str(order.get("token_id") or "").strip()
            if not token_id or token_id not in sell_or_must_exit_tokens:
                continue
            side = str(order.get("side") or "").upper()
            if side != "SELL":
                continue
            try:
                size = float(order.get("size") or 0.0)
            except Exception:
                size = 0.0
            if size <= min_shares:
                continue
            candidate_shares_by_token[token_id] = max(
                float(candidate_shares_by_token.get(token_id) or 0.0),
                float(size),
            )

        open_orders_ledger = acct_state.get("open_orders", {})
        if isinstance(open_orders_ledger, dict):
            for token_id, orders in open_orders_ledger.items():
                token_id = str(token_id or "").strip()
                if not token_id or token_id not in sell_or_must_exit_tokens:
                    continue
                for order in orders or []:
                    if str(order.get("side") or "").upper() != "SELL":
                        continue
                    try:
                        size = float(order.get("size") or 0.0)
                    except Exception:
                        size = 0.0
                    if size <= min_shares:
                        continue
                    candidate_shares_by_token[token_id] = max(
                        float(candidate_shares_by_token.get(token_id) or 0.0),
                        float(size),
                    )

        for token_id in sell_or_must_exit_tokens:
            if token_id in candidate_shares_by_token:
                continue
            shares_est = _estimate_recovery_shares_from_state(acct_state, token_id)
            if shares_est > min_shares:
                candidate_shares_by_token[token_id] = float(shares_est)

        candidates: list[dict[str, Any]] = []
        for token_id, shares in candidate_shares_by_token.items():
            if token_id in blocked_tokens:
                continue
            if max_position_usd_per_token > 0:
                acc_usd = float(
                    acct_state.get("buy_notional_accumulator", {}).get(token_id, {}).get("usd", 0.0)
                )
                if acc_usd > max_position_usd_per_token:
                    logger.info(
                        "[HEMOSTASIS] account=%s skip token=%s reason=grandfather_over_limit acc=%s max=%s",
                        _shorten_address(my_address),
                        token_id,
                        acc_usd,
                        max_position_usd_per_token,
                    )
                    continue
            prev_attempt_shares = attempted_shares.get(token_id)
            if prev_attempt_shares is not None and abs(shares - prev_attempt_shares) <= no_progress_eps_shares:
                blocked_tokens[token_id] = "no_progress_after_attempt"
                continue
            candidates.append({"token_id": token_id, "shares": shares})
            _mark_must_exit_token(
                acct_state,
                token_id,
                int(time.time()),
                source="hemostasis_candidate",
            )
        summary["rounds"] = round_idx

        if not candidates:
            pending_tokens = []
            for token_id in sorted(set(str(tid) for tid in must_exit_tokens.keys())):
                shares_est = _estimate_recovery_shares_from_state(acct_state, token_id)
                has_remote_sell_order = any(
                    str(order.get("token_id") or "") == token_id
                    and str(order.get("side") or "").upper() == "SELL"
                    for order in remote_orders
                )
                if shares_est > min_shares or has_remote_sell_order:
                    pending_tokens.append(token_id)
            if pending_tokens:
                summary["status"] = "pending_no_visible_inventory"
                summary["remaining_count"] = len(pending_tokens)
                summary["remaining_tokens"] = pending_tokens
                logger.warning(
                    "[HEMOSTASIS] account=%s pending_no_visible_inventory round=%s tokens=%s",
                    _shorten_address(my_address),
                    round_idx,
                    len(pending_tokens),
                )
                return summary
            logger.info(
                "[HEMOSTASIS] account=%s complete at round=%s",
                _shorten_address(my_address),
                round_idx,
            )
            summary["status"] = "cleared"
            summary["remaining_count"] = 0
            return summary

        candidate_token_ids = {str(item["token_id"]) for item in candidates}
        summary["candidate_tokens"] = sorted(candidate_token_ids)
        actions: list[dict[str, Any]] = []
        for order in remote_orders:
            if str(order.get("token_id") or "") not in candidate_token_ids:
                continue
            order_id = str(order.get("order_id") or "").strip()
            if not order_id:
                continue
            actions.append({"type": "cancel", "order_id": order_id})

        for item in candidates:
            token_id = str(item["token_id"])
            shares = float(item["shares"])
            sell_shares = max(0.0, shares - sell_buffer)
            if sell_shares <= min_shares:
                sell_shares = shares
            if sell_shares <= 0:
                blocked_tokens[token_id] = "non_positive_sell_shares"
                continue
            read_client = getattr(acct_ctx, "clob_read_client", None) or acct_ctx.clob_client
            orderbook = get_orderbook(read_client, token_id, api_timeout_sec)
            best_bid = orderbook.get("best_bid")
            if best_bid is None or float(best_bid) <= 0:
                logger.warning(
                    "[HEMOSTASIS] account=%s skip token=%s reason=no_best_bid shares=%s",
                    _shorten_address(my_address),
                    token_id,
                    shares,
                )
                blocked_tokens[token_id] = "no_best_bid"
                skipped = summary.get("skip_no_bid_tokens")
                if isinstance(skipped, list) and token_id not in skipped:
                    skipped.append(token_id)
                continue
            if token_id not in book_min_shares_cache:
                book_min_shares_cache[token_id] = _book_min_order_shares(
                    read_client,
                    token_id,
                    api_timeout_sec,
                )
            min_shares_token = float(book_min_shares_cache.get(token_id) or 0.0)
            if min_shares_token > 0 and sell_shares + 1e-12 < min_shares_token:
                blocked_tokens[token_id] = "below_book_min_order_shares"
                continue
            order_usd = abs(sell_shares) * float(best_bid)
            if min_trade_usd_gate > 0 and order_usd + 1e-9 < min_trade_usd_gate:
                blocked_tokens[token_id] = "below_min_trade_usd"
                continue
            actions.append(
                {
                    "type": "place",
                    "token_id": token_id,
                    "side": "SELL",
                    "price": float(best_bid),
                    "size": float(sell_shares),
                    "_taker": True,
                    "_available_shares": float(shares),
                    "_exit_flow": True,
                    "_exit_stage": 3,
                }
            )
            placed = summary.get("placed_tokens")
            if isinstance(placed, list) and token_id not in placed:
                placed.append(token_id)
            attempted_shares[token_id] = float(shares)

        place_count = sum(1 for action in actions if action.get("type") == "place")
        if place_count <= 0:
            logger.warning(
                "[HEMOSTASIS] account=%s no executable sell actions at round=%s",
                _shorten_address(my_address),
                round_idx,
            )
            summary["status"] = "no_executable_actions"
            break

        now_ts = int(time.time())
        apply_actions(
            client=acct_ctx.clob_client,
            actions=actions,
            open_orders=remote_orders,
            now_ts=now_ts,
            dry_run=dry_run,
            cfg=cfg,
            state=acct_ctx.state,
        )
        logger.info(
            "[HEMOSTASIS] account=%s round=%s candidates=%s places=%s dry_run=%s",
            _shorten_address(my_address),
            round_idx,
            len(candidates),
            place_count,
            dry_run,
        )
        time.sleep(poll_sec)

    my_positions, _ = fetch_positions_norm(
        data_client,
        my_address,
        size_threshold=0.0,
        positions_limit=positions_limit,
        positions_max_pages=positions_max_pages,
        refresh_sec=refresh_sec,
        force_http=my_positions_force_http,
        cache_bust_mode=cache_bust_mode,
        header_keys=header_keys,
    )
    remain = 0
    remaining_tokens: List[str] = []
    remain_scope_tokens: set[str] = set(str(tid) for tid in sell_token_ids)
    remain_scope_tokens.update(str(tid) for tid in must_exit_tokens.keys())
    for pos in my_positions:
        token_id = str(pos.get("token_id") or "").strip()
        if not token_id:
            token_id = str(_extract_token_id_from_raw(pos.get("raw")) or "").strip()
        if not token_id or token_id not in remain_scope_tokens:
            continue
        try:
            shares = float(pos.get("size") or 0.0)
        except Exception:
            shares = 0.0
        if shares > min_shares:
            remain += 1
            if token_id not in remaining_tokens:
                remaining_tokens.append(token_id)
    summary["remaining_count"] = remain
    summary["remaining_tokens"] = sorted(remaining_tokens)
    summary["blocked_tokens"] = dict(sorted(blocked_tokens.items()))
    if remain > 0:
        logger.warning(
            "[HEMOSTASIS] account=%s exit_with_remaining=%s (max_rounds=%s)",
            _shorten_address(my_address),
            remain,
            max_rounds,
        )
        if summary.get("status") == "ok":
            summary["status"] = "exit_with_remaining"
    else:
        if summary.get("status") == "ok":
            summary["status"] = "cleared"
    return summary


def _run_hemostasis_recovery_startup(
    cfg: Dict[str, Any],
    data_client: Any,
    account_contexts: List[AccountContext],
    target_addresses: List[str],
    logger: logging.Logger,
    dry_run: bool = False,
) -> None:
    if not _cfg_bool(cfg.get("hemostasis_recovery_enabled"), False):
        return
    if not account_contexts:
        return
    try:
        sell_token_ids = _collect_target_sell_token_ids(cfg, data_client, target_addresses, logger)
    except Exception as exc:
        logger.warning("[HEMOSTASIS] scan failed, skip recovery: %s", exc)
        return
    if not sell_token_ids:
        logger.info("[HEMOSTASIS] no target sell token found in lookback window; skip recovery")
        return
    logger.info(
        "[HEMOSTASIS] startup begin accounts=%s sell_tokens=%s",
        len(account_contexts),
        len(sell_token_ids),
    )
    summaries: List[Dict[str, Any]] = []
    for acct_ctx in account_contexts:
        try:
            _reconcile_accumulator_for_account(cfg, acct_ctx, logger)
            summary = _run_hemostasis_recovery_for_account(
                cfg=cfg,
                data_client=data_client,
                acct_ctx=acct_ctx,
                sell_token_ids=sell_token_ids,
                logger=logger,
                dry_run=dry_run,
            )
            if isinstance(summary, dict):
                summaries.append(summary)
                logger.info(
                    "[HEMOSTASIS_SUMMARY] account=%s status=%s rounds=%s candidates=%s placed=%s remain=%s",
                    _shorten_address(str(summary.get("account") or acct_ctx.my_address)),
                    summary.get("status"),
                    summary.get("rounds"),
                    len(summary.get("candidate_tokens") or []),
                    len(summary.get("placed_tokens") or []),
                    summary.get("remaining_count"),
                )
                if summary.get("remaining_tokens"):
                    logger.warning(
                        "[HEMOSTASIS_SUMMARY] account=%s remaining_tokens=%s",
                        _shorten_address(str(summary.get("account") or acct_ctx.my_address)),
                        ",".join(str(x) for x in (summary.get("remaining_tokens") or [])),
                    )
                blocked = summary.get("blocked_tokens") or {}
                if isinstance(blocked, dict) and blocked:
                    logger.warning(
                        "[HEMOSTASIS_SUMMARY] account=%s blocked_tokens=%s",
                        _shorten_address(str(summary.get("account") or acct_ctx.my_address)),
                        ",".join(f"{k}:{v}" for k, v in sorted(blocked.items())),
                    )
            try:
                save_state(str(acct_ctx.state_path), acct_ctx.state)
            except Exception as save_exc:
                logger.warning(
                    "[HEMOSTASIS] account=%s save_state failed: %s",
                    _shorten_address(acct_ctx.my_address),
                    save_exc,
                )
        except Exception as exc:
            logger.warning(
                "[HEMOSTASIS] account=%s failed: %s",
                _shorten_address(acct_ctx.my_address),
                exc,
            )
            summaries.append(
                {
                    "account": str(acct_ctx.my_address),
                    "status": "exception",
                    "error": str(exc),
                    "remaining_count": None,
                }
            )
    if summaries:
        cleared = sum(1 for item in summaries if str(item.get("status")) == "cleared")
        remained = sum(
            1
            for item in summaries
            if str(item.get("status")) in {
                "exit_with_remaining",
                "no_executable_actions",
                "pending_no_visible_inventory",
            }
        )
        failed = sum(1 for item in summaries if str(item.get("status")) in {"exception", "fetch_positions_failed"})
        logger.info(
            "[HEMOSTASIS] startup_summary accounts=%s cleared=%s remained=%s failed=%s",
            len(summaries),
            cleared,
            remained,
            failed,
        )
    logger.info("[HEMOSTASIS] startup complete")


def _derive_api_creds(client):
    for name in ("derive_api_creds", "derive_api_key"):
        method = getattr(client, name, None)
        if callable(method):
            return method()
    return None


def init_clob_client(
    private_key: str,
    funder_address: str,
    cfg: Optional[Dict[str, Any]] = None,
):
    """Initialize the primary V2 CLOB client for a specific account."""
    return init_v2_client(
        private_key=private_key,
        funder_address=funder_address,
        cfg=cfg,
    )


def init_clob_read_client(
    private_key: str,
    funder_address: str,
    cfg: Optional[Dict[str, Any]] = None,
):
    """Initialize a V2 read client when a dedicated read instance is still desired."""
    return init_v2_client(private_key=private_key, funder_address=funder_address, cfg=cfg)


def _mid_price(orderbook: Dict[str, Optional[float]]) -> Optional[float]:
    bid = orderbook.get("best_bid")
    ask = orderbook.get("best_ask")
    if bid is not None and bid <= 0:
        bid = None
    if ask is not None and ask <= 0:
        ask = None
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    if bid is not None:
        return bid
    if ask is not None:
        return ask
    return None


def _parse_market_end_ts(meta: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(meta, dict):
        return None
    value = (
        meta.get("end_time")
        or meta.get("endTime")
        or meta.get("end_date")
        or meta.get("endDate")
        or meta.get("endDateIso")
    )
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            num = float(value)
            if num > 1e12:
                num /= 1000.0
            return int(num)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
                parsed = datetime.strptime(text, "%Y-%m-%d").replace(
                    hour=23,
                    minute=59,
                    second=59,
                    tzinfo=ZoneInfo("America/New_York"),
                )
                return int(parsed.timestamp())
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("America/New_York"))
            return int(parsed.timestamp())
    except Exception:
        return None
    return None


def _is_closed_by_end_date(pos: Dict[str, Any], now_ts: int) -> tuple[bool, Optional[int]]:
    end_ts = _parse_market_end_ts(pos)
    if end_ts is None:
        return False, None
    return end_ts <= now_ts, end_ts


def _filter_closed_positions(
    positions: list[Dict[str, Any]],
    closed_keys: Dict[str, int],
) -> tuple[list[Dict[str, Any]], int]:
    if not positions or not closed_keys:
        return positions, 0
    kept: list[Dict[str, Any]] = []
    removed = 0
    for pos in positions:
        token_key = pos.get("token_key")
        if token_key and token_key in closed_keys:
            removed += 1
            continue
        kept.append(pos)
    return kept, removed


def _extract_mid_cache_meta(state: Dict[str, Any]) -> tuple[Optional[float], Dict[str, Any]]:
    meta_keys = (
        "mid_cache_ttl_sec",
        "mid_cache_ttl",
        "mid_cache_update_ts",
        "mid_cache_update_ms",
        "mid_cache_updated_at",
        "last_mid_price_update_ts",
        "last_mid_price_update_ms",
    )
    meta: Dict[str, Any] = {}
    for key in meta_keys:
        if key in state:
            meta[key] = state.get(key)

    ts_by_token = state.get("last_mid_price_ts_by_token_id")
    if isinstance(ts_by_token, dict):
        ts_values = [
            ts for ts in ts_by_token.values() if isinstance(ts, (int, float)) and ts > 0
        ]
        if ts_values:
            meta["last_mid_price_ts_max"] = max(ts_values)

    last_mid_update = None
    if "last_mid_price_ts_max" in meta:
        last_mid_update = meta["last_mid_price_ts_max"]
    else:
        for key in (
            "last_mid_price_update_ts",
            "last_mid_price_update_ms",
            "mid_cache_update_ts",
            "mid_cache_update_ms",
            "mid_cache_updated_at",
        ):
            value = meta.get(key)
            if isinstance(value, (int, float)) and value > 0:
                last_mid_update = value
                break
    return last_mid_update, meta


def _is_lowp_token(cfg: Dict[str, Any], ref_price: float) -> bool:
    if not bool(cfg.get("lowp_guard_enabled", False)):
        return False
    thr = float(cfg.get("lowp_price_threshold") or 0.0)
    return ref_price > 0 and thr > 0 and ref_price <= thr


def _lowp_cfg(cfg: Dict[str, Any], is_lowp: bool) -> Dict[str, Any]:
    if not is_lowp:
        return cfg
    out = dict(cfg)
    mapping = {
        "min_order_usd": "lowp_min_order_usd",
        "max_order_usd": "lowp_max_order_usd",
        "probe_order_usd": "lowp_probe_order_usd",
        "max_notional_per_token": "lowp_max_notional_per_token",
    }
    for base_key, lowp_key in mapping.items():
        if lowp_key in cfg and cfg.get(lowp_key) is not None:
            out[base_key] = cfg.get(lowp_key)
    return out


def _lowp_buy_ratio(cfg: Dict[str, Any], is_lowp: bool) -> float:
    base = float(cfg.get("follow_ratio") or 0.0)
    if not is_lowp:
        return base
    mult = float(cfg.get("lowp_follow_ratio_mult") or 1.0)
    return base * mult


def _calc_used_notional_totals(
    my_by_token_id: Dict[str, float],
    open_orders_by_token_id: Dict[str, list[dict]],
    mid_cache: Dict[str, float],
    max_position_usd_per_token: float,
    fallback_mid_price: float,
) -> tuple[float, Dict[str, float], Dict[str, Dict[str, object]]]:
    total = 0.0
    by_token: Dict[str, float] = {}
    order_info_by_id: Dict[str, Dict[str, object]] = {}

    for token_id, shares in my_by_token_id.items():
        mid = float(mid_cache.get(token_id, 0.0))
        if mid <= 0:
            # 鎷夸笉鍒颁环鏍?鏃犵洏鍙ｏ細浣跨敤 fallback_mid_price 鍏滃簳锛岄伩鍏嶆寔浠撲及鍊艰娓呴浂
            mid = 0.0
            if fallback_mid_price > 0 and abs(shares) > 0:
                mid = fallback_mid_price
        if mid < 0:
            mid = 0.0
        elif mid > 1.0:
            mid = 1.0
        usd = abs(shares) * mid
        by_token[token_id] = by_token.get(token_id, 0.0) + usd
        total += usd

    for token_id, orders in open_orders_by_token_id.items():
        for order in orders or []:
            side = str(order.get("side") or "").upper()
            if side != "BUY":
                continue
            size = float(order.get("size") or 0.0)
            price = float(order.get("price") or 0.0)
            if price <= 0 or size <= 0:
                continue
            usd = abs(size) * price
            by_token[token_id] = by_token.get(token_id, 0.0) + usd
            total += usd
            order_id = str(order.get("order_id") or "")
            if order_id:
                order_info_by_id[order_id] = {
                    "token_id": token_id,
                    "side": "BUY",
                    "usd": usd,
                }

    return total, by_token, order_info_by_id


def _calc_shadow_buy_notional(
    state: Dict[str, Any],
    now_ts: int,
    ttl_sec: int,
) -> tuple[float, Dict[str, float]]:
    if ttl_sec <= 0:
        state["shadow_buy_orders"] = []
        return 0.0, {}
    taker_orders = state.get("taker_buy_orders")
    shadow_orders = state.get("shadow_buy_orders")
    if isinstance(taker_orders, list) and taker_orders:
        orders_key = "taker_buy_orders"
        shadow_orders = taker_orders
    elif isinstance(shadow_orders, list) and shadow_orders:
        orders_key = "shadow_buy_orders"
    else:
        orders_key = (
            "taker_buy_orders" if isinstance(taker_orders, list) else "shadow_buy_orders"
        )
        shadow_orders = taker_orders if isinstance(taker_orders, list) else shadow_orders
    if not isinstance(shadow_orders, list):
        state[orders_key] = []
        return 0.0, {}
    kept: list[dict] = []
    total = 0.0
    by_token: Dict[str, float] = {}
    for order in shadow_orders:
        if not isinstance(order, dict):
            continue
        token_id = str(order.get("token_id") or "")
        if not token_id:
            continue
        ts = int(order.get("ts") or 0)
        if ts <= 0 or (now_ts - ts) > ttl_sec:
            continue
        usd = float(order.get("usd") or 0.0)
        if usd <= 0:
            continue
        kept.append(order)
        total += usd
        by_token[token_id] = by_token.get(token_id, 0.0) + usd
    state[orders_key] = kept
    if orders_key == "shadow_buy_orders":
        state["taker_buy_orders"] = list(kept)
    return total, by_token


def _calc_recent_buy_notional(
    state: Dict[str, Any],
    now_ts: int,
    window_sec: int,
) -> tuple[float, Dict[str, float]]:
    if window_sec <= 0:
        state["recent_buy_orders"] = []
        return 0.0, {}
    recent_orders = state.get("recent_buy_orders")
    if not isinstance(recent_orders, list):
        state["recent_buy_orders"] = []
        return 0.0, {}
    kept: list[dict] = []
    total = 0.0
    by_token: Dict[str, float] = {}
    for order in recent_orders:
        if not isinstance(order, dict):
            continue
        token_id = str(order.get("token_id") or "")
        if not token_id:
            continue
        ts = int(order.get("ts") or 0)
        if ts <= 0 or (now_ts - ts) > window_sec:
            continue
        usd = float(order.get("usd") or 0.0)
        if usd <= 0:
            continue
        kept.append(order)
        total += usd
        by_token[token_id] = by_token.get(token_id, 0.0) + usd
    state["recent_buy_orders"] = kept
    return total, by_token


def _calc_planned_notional_totals(
    my_by_token_id: Dict[str, float],
    open_orders_by_token_id: Dict[str, list[dict]],
    mid_cache: Dict[str, float],
    max_position_usd_per_token: float,
    state: Dict[str, Any],
    now_ts: int,
    shadow_ttl_sec: int,
    fallback_mid_price: float,
    include_shadow: bool = True,
) -> tuple[float, Dict[str, float], Dict[str, Dict[str, object]], float]:
    total, by_token, order_info_by_id = _calc_used_notional_totals(
        my_by_token_id,
        open_orders_by_token_id,
        mid_cache,
        max_position_usd_per_token,
        fallback_mid_price,
    )
    shadow_total, shadow_by_token = _calc_shadow_buy_notional(
        state, now_ts, shadow_ttl_sec
    )
    if include_shadow and shadow_total > 0:
        total += shadow_total
        for token_id, usd in shadow_by_token.items():
            by_token[token_id] = by_token.get(token_id, 0.0) + usd
    return total, by_token, order_info_by_id, shadow_total


def _calc_used_notional_total(
    my_by_token_id: Dict[str, float],
    open_orders_by_token_id: Dict[str, list[dict]],
    mid_cache: Dict[str, float],
    max_position_usd_per_token: float,
    fallback_mid_price: float,
) -> float:
    total, _, _ = _calc_used_notional_totals(
        my_by_token_id,
        open_orders_by_token_id,
        mid_cache,
        max_position_usd_per_token,
        fallback_mid_price,
    )
    return total


def _calc_planned_notional_with_fallback(
    my_by_token_id: Dict[str, float],
    open_orders_by_token_id: Dict[str, list[dict]],
    mid_cache: Dict[str, float],
    max_position_usd_per_token: float,
    state: Dict[str, Any],
    now_ts: int,
    shadow_ttl_sec: int,
    fallback_mid_price: float,
    logger: logging.Logger,
    include_shadow: bool = True,
) -> tuple[float, Dict[str, float], Dict[str, Dict[str, object]], float]:
    total, by_token, order_info_by_id, shadow_total = _calc_planned_notional_totals(
        my_by_token_id,
        open_orders_by_token_id,
        mid_cache,
        max_position_usd_per_token,
        state,
        now_ts,
        shadow_ttl_sec,
        fallback_mid_price,
        include_shadow=include_shadow,
    )
    if total > 0 or not my_by_token_id or fallback_mid_price <= 0:
        state["planned_zero_streak"] = 0
        return total, by_token, order_info_by_id, shadow_total

    total, by_token, order_info_by_id, shadow_total = _calc_planned_notional_totals(
        my_by_token_id,
        open_orders_by_token_id,
        mid_cache,
        max_position_usd_per_token,
        state,
        now_ts,
        shadow_ttl_sec,
        fallback_mid_price,
        include_shadow=include_shadow,
    )
    planned_zero_streak = int(state.get("planned_zero_streak") or 0) + 1
    state["planned_zero_streak"] = planned_zero_streak
    if planned_zero_streak <= 3 or planned_zero_streak % 20 == 0:
        missing_mid_tokens = [
            token_id
            for token_id in my_by_token_id
            if float(mid_cache.get(token_id, 0.0) or 0.0) <= 0
        ]
        missing_mid_sample = missing_mid_tokens[:5]
        last_mid_update, mid_cache_meta = _extract_mid_cache_meta(state)
        logger.warning(
            "[ALERT] planned_notional_zero fallback_mid=%s positions=%s streak=%s "
            "token_count=%s last_mid_update=%s missing_mid_sample=%s mid_cache_meta=%s",
            fallback_mid_price,
            len(my_by_token_id),
            planned_zero_streak,
            len(mid_cache),
            last_mid_update,
            missing_mid_sample,
            mid_cache_meta,
        )
    return total, by_token, order_info_by_id, shadow_total


def _get_condition_id(state: Dict[str, Any], token_id: str) -> Optional[str]:
    cache = state.get("market_status_cache", {})
    data = cache.get(token_id)
    if isinstance(data, dict):
        meta = data.get("meta") or {}
        return meta.get("conditionId")
    return None


def _get_event_id(state: Dict[str, Any], token_id: str) -> Optional[str]:
    cache = state.get("market_status_cache", {})
    data = cache.get(token_id)
    if isinstance(data, dict):
        meta = data.get("meta") or {}
        event_id = meta.get("eventId")
        if not event_id and isinstance(meta.get("events"), list) and meta["events"]:
            event_id = meta["events"][0].get("id")
        return event_id
    return None


def _shrink_on_risk_limit(
    act: Dict[str, Any],
    max_total: float,
    planned_total: float,
    max_per_token: float,
    planned_token: float,
    min_usd: float,
    min_shares: float,
    token_key: str,
    token_id: str,
    logger: logging.Logger,
) -> Optional[tuple[Dict[str, Any], float]]:
    side = str(act.get("side") or "").upper()
    if side != "BUY":
        return None
    price = float(act.get("price") or 0.0)
    size = float(act.get("size") or 0.0)
    if price <= 0 or size <= 0:
        return None

    order_usd = abs(size) * price
    cap_total_remaining = (max_total - planned_total) if max_total > 0 else None
    cap_token_remaining = (max_per_token - planned_token) if max_per_token > 0 else None

    candidates = [order_usd]
    if cap_total_remaining is not None:
        candidates.append(cap_total_remaining)
    if cap_token_remaining is not None:
        candidates.append(cap_token_remaining)

    allowed_usd = min(candidates)
    effective_min_usd = float(min_usd or 0.0)
    if float(min_shares or 0.0) > 0:
        effective_min_usd = max(effective_min_usd, float(min_shares) * price)

    if allowed_usd <= 0 or allowed_usd + 1e-9 < effective_min_usd:
        return None
    if allowed_usd >= order_usd * (1 - 1e-9):
        return None

    new_act = dict(act)
    new_act["size"] = allowed_usd / price
    logger.warning(
        "[RISK_RESIZE] %s token=%s side=%s old_usd=%s new_usd=%s planned_total=%s",
        token_key,
        token_id,
        side,
        order_usd,
        allowed_usd,
        planned_total,
    )
    return new_act, allowed_usd


def _collect_order_ids(open_orders_by_token_id: Dict[str, list[dict]]) -> set[str]:
    order_ids: set[str] = set()
    for orders in open_orders_by_token_id.values():
        for order in orders or []:
            order_id = order.get("order_id")
            if order_id:
                order_ids.add(str(order_id))
    return order_ids


def _refresh_managed_order_ids(state: Dict[str, Any]) -> None:
    managed_ids = _collect_order_ids(state.get("open_orders", {}))
    state["managed_order_ids"] = sorted(managed_ids)


_MUST_EXIT_SOURCE_PRIORITY: Dict[str, int] = {
    "target_sell_action": 100,
    "topic_exit_signal": 80,
    "topic_exit_recover": 70,
    "sell_confirm_drop": 60,
    "hemostasis_candidate": 50,
    "hemostasis_seed": 40,
    "reconcile_loop": 10,
}


def _must_exit_source_priority(source: str) -> int:
    return int(_MUST_EXIT_SOURCE_PRIORITY.get(str(source or "").strip().lower(), 20))


def _get_must_exit_signal_ms(meta: Dict[str, Any], last_target_sell_ms: int = 0) -> int:
    signal_ms = 0
    if isinstance(meta, dict):
        try:
            signal_ms = max(signal_ms, int(meta.get("target_sell_ms") or 0))
        except Exception:
            pass
        try:
            signal_ms = max(signal_ms, int(meta.get("signal_ms") or 0))
        except Exception:
            pass
        try:
            last_ts = int(meta.get("last_ts") or meta.get("first_ts") or 0)
        except Exception:
            last_ts = 0
        if last_ts > 0:
            signal_ms = max(signal_ms, last_ts * 1000)
    try:
        signal_ms = max(signal_ms, int(last_target_sell_ms or 0))
    except Exception:
        pass
    return int(max(0, signal_ms))


def _is_must_exit_fresh(
    *,
    meta: Dict[str, Any],
    last_target_sell_ms: int,
    now_ms: int,
    fresh_window_sec: int,
) -> bool:
    if not isinstance(meta, dict):
        return False
    if fresh_window_sec <= 0:
        return True
    signal_ms = _get_must_exit_signal_ms(meta, last_target_sell_ms=last_target_sell_ms)
    if signal_ms <= 0:
        return False
    return (now_ms - signal_ms) <= max(1, int(fresh_window_sec)) * 1000


def _should_clear_stale_must_exit_on_buy(
    *,
    must_exit_active: bool,
    must_exit_fresh: bool,
    t_now_present: bool,
    t_now: Optional[float],
    has_buy: bool,
    buy_sum: float,
    min_target_buy_shares: float,
) -> bool:
    if not must_exit_active or must_exit_fresh:
        return False
    if not t_now_present or float(t_now or 0.0) <= 0:
        return False
    if not has_buy:
        return False
    return float(buy_sum or 0.0) >= max(0.0, float(min_target_buy_shares or 0.0))


def _mark_must_exit_token(
    state: Dict[str, Any],
    token_id: str,
    now_ts: int,
    source: str,
    target_sell_ms: int = 0,
) -> None:
    token_id = str(token_id or "").strip()
    if not token_id:
        return
    if source in {"target_sell_action", "reconcile_loop"} and _is_exit_finalization_active(
        state, token_id, now_ts
    ):
        return
    must_exit = state.setdefault("must_exit_tokens", {})
    if not isinstance(must_exit, dict):
        must_exit = {}
        state["must_exit_tokens"] = must_exit
    meta = must_exit.get(token_id)
    if not isinstance(meta, dict):
        meta = {
            "first_ts": int(now_ts),
        }
    if int(meta.get("first_ts") or 0) <= 0:
        meta["first_ts"] = int(now_ts)
    source = str(source or "unknown")
    old_source = str(meta.get("source") or "")
    old_priority = _must_exit_source_priority(old_source)
    new_priority = _must_exit_source_priority(source)
    if not old_source or new_priority >= old_priority:
        meta["source"] = source
    # Do not keep stale must_exit alive forever: weak reconcile-loop touches
    # without fresh target SELL evidence should not refresh last_ts.
    if not (source == "reconcile_loop" and int(target_sell_ms or 0) <= 0):
        meta["last_ts"] = int(now_ts)
    if int(target_sell_ms or 0) > int(meta.get("target_sell_ms") or 0):
        meta["target_sell_ms"] = int(target_sell_ms)
    # Track signal freshness in ms for stale-must-exit unlock.
    if int(target_sell_ms or 0) > 0:
        meta["signal_ms"] = max(int(meta.get("signal_ms") or 0), int(target_sell_ms))
    elif new_priority >= 60:
        meta["signal_ms"] = max(int(meta.get("signal_ms") or 0), int(now_ts) * 1000)
    must_exit[token_id] = meta


def _get_last_nonzero_shares(
    state: Dict[str, Any],
    token_id: str,
) -> tuple[float, int]:
    cache = state.get("last_nonzero_my_shares", {})
    if not isinstance(cache, dict):
        return 0.0, 0
    meta = cache.get(str(token_id))
    if not isinstance(meta, dict):
        return 0.0, 0
    try:
        shares = float(meta.get("shares") or 0.0)
    except Exception:
        shares = 0.0
    try:
        ts = int(meta.get("ts") or 0)
    except Exception:
        ts = 0
    return max(0.0, shares), max(0, ts)


def _estimate_recovery_shares_from_state(
    state: Dict[str, Any],
    token_id: str,
) -> float:
    token_id = str(token_id or "").strip()
    if not token_id:
        return 0.0
    est = 0.0
    shares_last, _ = _get_last_nonzero_shares(state, token_id)
    est = max(est, shares_last)
    open_orders = state.get("open_orders", {})
    if isinstance(open_orders, dict):
        for order in (open_orders.get(token_id) or []):
            if str(order.get("side") or "").upper() != "SELL":
                continue
            try:
                size = float(order.get("size") or 0.0)
            except Exception:
                size = 0.0
            if size > est:
                est = size
    return max(0.0, est)


def _should_clear_must_exit_without_inventory(
    state: Dict[str, Any],
    token_id: str,
    now_ts: int,
    eps: float,
    cfg: Dict[str, Any],
) -> bool:
    token_id = str(token_id or "").strip()
    if not token_id:
        return False
    acc_usd = 0.0
    acc = state.get("buy_notional_accumulator", {})
    if isinstance(acc, dict):
        acc_meta = acc.get(token_id)
        if isinstance(acc_meta, dict):
            try:
                acc_usd = float(acc_meta.get("usd") or 0.0)
            except Exception:
                acc_usd = 0.0
    last_shares, last_ts = _get_last_nonzero_shares(state, token_id)
    cache_hold_sec = int(cfg.get("must_exit_cache_hold_sec") or 1800)
    cache_hold_sec = max(0, cache_hold_sec)
    cache_active = (
        last_shares > max(0.0, eps)
        and last_ts > 0
        and now_ts - int(last_ts) <= cache_hold_sec
    )
    return acc_usd <= 0.01 and (not cache_active)


def _prune_exit_finalization(state: Dict[str, Any], now_ts: int) -> None:
    bucket = state.get("exit_finalization")
    if not isinstance(bucket, dict):
        state["exit_finalization"] = {}
        return
    for token_id, meta in list(bucket.items()):
        if not isinstance(meta, dict):
            bucket.pop(token_id, None)
            continue
        until_ts = int(meta.get("until") or 0)
        if until_ts <= 0 or now_ts >= until_ts:
            bucket.pop(token_id, None)


def _clear_exit_finalization_on_exposure(
    state: Dict[str, Any],
    token_id: str,
    my_shares: float,
    open_orders: List[Dict[str, Any]],
    eps: float,
) -> None:
    if float(my_shares or 0.0) <= max(0.0, float(eps or 0.0)) and not open_orders:
        return
    bucket = state.get("exit_finalization")
    if isinstance(bucket, dict):
        bucket.pop(str(token_id or "").strip(), None)


def _is_exit_finalization_active(state: Dict[str, Any], token_id: str, now_ts: int) -> bool:
    bucket = state.get("exit_finalization")
    if not isinstance(bucket, dict):
        return False
    meta = bucket.get(str(token_id or "").strip())
    if not isinstance(meta, dict):
        return False
    return int(meta.get("until") or 0) > int(now_ts or 0)


def _finalize_exited_token_state(
    state: Dict[str, Any],
    token_id: str,
    now_ts: int,
    cfg: Dict[str, Any],
    logger: logging.Logger,
    reason: str,
) -> None:
    token_id = str(token_id or "").strip()
    if not token_id:
        return
    hold_sec = max(0, int(cfg.get("exit_finalization_hold_sec") or 180))
    bucket = state.setdefault("exit_finalization", {})
    if not isinstance(bucket, dict):
        state["exit_finalization"] = {}
        bucket = state["exit_finalization"]
    until_ts = int(now_ts) + hold_sec
    bucket[token_id] = {"ts": int(now_ts), "until": until_ts, "reason": str(reason or "")}

    for key in (
        "must_exit_tokens",
        "last_nonzero_my_shares",
        "last_allowed_target_sell_action_ts_by_token",
        "sell_shares_accumulator",
        "sell_source_votes",
        "exit_sell_state",
        "intent_keys",
        "topic_unfilled_attempts",
    ):
        container = state.get(key)
        if isinstance(container, dict):
            container.pop(token_id, None)

    last_exit = state.setdefault("last_exit_ts_by_token", {})
    if isinstance(last_exit, dict):
        last_exit[token_id] = int(now_ts)

    logger.info(
        "[EXIT_FINALIZED] token_id=%s reason=%s hold_until=%s",
        token_id,
        reason or "exit_complete",
        until_ts,
    )


def _intent_key(phase: str, desired_side: str, desired_shares: float) -> Dict[str, Any]:
    return {
        "phase": phase,
        "desired_side": desired_side,
        "desired_shares": float(desired_shares),
    }


def _update_intent_state(
    state: Dict[str, Any],
    token_id: str,
    new_key: Dict[str, Any],
    eps: float,
    logger: logging.Logger,
) -> tuple[bool, bool]:
    intents = state.setdefault("intent_keys", {})
    prev = intents.get(token_id)
    reasons: list[str] = []
    desired_down = False
    if isinstance(prev, dict):
        if prev.get("phase") != new_key.get("phase"):
            reasons.append("phase_changed")
        if prev.get("desired_side") != new_key.get("desired_side"):
            reasons.append("side_changed")
        prev_shares = float(prev.get("desired_shares") or 0.0)
        if float(new_key.get("desired_shares") or 0.0) < prev_shares - eps:
            reasons.append("desired_shares_down")
            desired_down = True
    intents[token_id] = new_key
    if reasons:
        logger.info(
            "[INTENT] token_id=%s old=%s new=%s reasons=%s",
            token_id,
            prev,
            new_key,
            ",".join(reasons),
        )
    return bool(reasons), desired_down


def _action_identity(action: Dict[str, object]) -> str:
    raw = action.get("raw") or {}
    token_id = str(action.get("token_id") or "").strip()
    side = str(action.get("side") or "").strip().upper()
    price = action.get("price")
    size = action.get("size")
    if isinstance(raw, dict):
        tx_hash = raw.get("txHash") or raw.get("tx_hash") or raw.get("transactionHash")
        log_index = raw.get("logIndex") or raw.get("log_index")
        fill_id = raw.get("fillId") or raw.get("fill_id")
        if tx_hash and log_index is not None:
            return f"tx:{tx_hash}:{log_index}"
        if fill_id is not None:
            return f"fill:{fill_id}"
        if tx_hash:
            return f"tx:{tx_hash}:{token_id}:{side}:{price}:{size}"
    token_id = action.get("token_id") or ""
    side = action.get("side") or ""
    size = action.get("size") or ""
    ts = action.get("timestamp")
    action_ms = int(ts.timestamp() * 1000) if ts else 0
    price = ""
    if isinstance(raw, dict):
        price = raw.get("price") or raw.get("fillPrice") or raw.get("avgPrice") or ""
    return f"fallback:{token_id}:{side}:{size}:{price}:{action_ms}"


def _action_ms(action: Dict[str, object]) -> int:
    ts = action.get("timestamp")
    if isinstance(ts, datetime):
        return int(ts.timestamp() * 1000)
    return int(action.get("timestamp_ms") or action.get("ts") or 0)


def _extract_token_id_from_raw(raw: object) -> Optional[str]:
    """Extract token_id from position/raw/action payload without network calls."""
    if raw is None:
        return None

    direct_keys = (
        "tokenId",
        "token_id",
        "clobTokenId",
        "clob_token_id",
        "asset",
        "assetId",
        "asset_id",
        "outcomeTokenId",
        "outcome_token_id",
    )

    if isinstance(raw, dict):
        for key in direct_keys:
            value = raw.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        value = raw.get("id")
        if value is not None:
            text = str(value).strip()
            if text:
                return text

    keyset = set(direct_keys)
    id_parent_ok = {"asset", "token", "outcomeToken", "outcome_token", "clobToken", "clob_token"}
    stack: list[tuple[object, int, Optional[str]]] = [(raw, 0, None)]
    seen: set[int] = set()
    while stack:
        cur, depth, parent = stack.pop()
        if depth > 6:
            continue
        oid = id(cur)
        if oid in seen:
            continue
        seen.add(oid)

        if isinstance(cur, dict):
            for key, value in cur.items():
                if key in keyset and value is not None:
                    text = str(value).strip()
                    if text:
                        return text
                if key == "id" and parent in id_parent_ok and value is not None:
                    text = str(value).strip()
                    if text:
                        return text
                if isinstance(value, (dict, list)):
                    stack.append((value, depth + 1, key))
        elif isinstance(cur, list):
            for value in cur:
                if isinstance(value, (dict, list)):
                    stack.append((value, depth + 1, parent))
    return None


def _prune_order_ts_by_id(state: Dict[str, Any]) -> None:
    order_ts_by_id = state.get("order_ts_by_id")
    if not isinstance(order_ts_by_id, dict):
        state["order_ts_by_id"] = {}
        return
    active_ids = _collect_order_ids(state.get("open_orders", {}))
    for order_id in list(order_ts_by_id.keys()):
        if str(order_id) not in active_ids:
            order_ts_by_id.pop(order_id, None)


def _record_orderbook_empty(
    state: Dict[str, Any],
    token_id: str,
    logger: logging.Logger,
    cfg: Dict[str, Any],
    now_ts: int,
) -> bool:
    streaks = state.setdefault("orderbook_empty_streak", {})
    if not isinstance(streaks, dict):
        streaks = {}
        state["orderbook_empty_streak"] = streaks
    prev = int(streaks.get(token_id) or 0)
    current = prev + 1
    streaks[token_id] = current
    if current <= 3 or current % 10 == 0:
        logger.warning(
            "[ALERT] orderbook_empty token_id=%s streak=%s",
            token_id,
            current,
        )
    close_streak = int(cfg.get("orderbook_empty_close_streak") or 3)
    if close_streak > 0 and current >= close_streak:
        closed_token_keys = state.setdefault("closed_token_keys", {})
        if isinstance(closed_token_keys, dict) and str(token_id) not in closed_token_keys:
            closed_token_keys[str(token_id)] = int(now_ts)
            logger.warning(
                "[CLOSE] orderbook_empty token_id=%s streak=%s",
                token_id,
                current,
            )
            return True
    return False


def _clear_orderbook_empty(state: Dict[str, Any], token_id: str) -> None:
    streaks = state.get("orderbook_empty_streak")
    if isinstance(streaks, dict):
        streaks.pop(token_id, None)


def _maybe_update_target_last(
    state: Dict[str, Any],
    token_id: str,
    t_now: Optional[float],
    should_update: bool,
) -> None:
    if should_update and t_now is not None:
        state.setdefault("target_last_shares", {})[token_id] = float(t_now)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket Copytrade v1")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--target", dest="target_address")
    parser.add_argument("--my", dest="my_address")
    parser.add_argument("--ratio", type=float, dest="follow_ratio")
    parser.add_argument("--poll", type=int, dest="poll_interval_sec")
    parser.add_argument("--poll-exit", type=int, dest="poll_interval_sec_exiting")
    parser.add_argument(
        "--my-positions-force-http",
        action="store_true",
        dest="my_positions_force_http",
        default=None,
        help="Force HTTP direct fetch for my positions (override config).",
    )
    parser.add_argument("--worker-index", type=int, dest="worker_index", default=0)
    parser.add_argument("--worker-count", type=int, dest="worker_count", default=1)
    parser.add_argument(
        "--worker-supervised",
        action="store_true",
        dest="worker_supervised",
        help="Internal flag used by worker supervisor.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _configure_global_api_rate_limits(
    cfg: Dict[str, Any],
    *,
    worker_count: int,
    logger: logging.Logger,
) -> None:
    total_data_rps = max(0.2, _to_float(cfg.get("global_data_api_rps"), 2.0))
    total_data_http_rps = max(0.2, _to_float(cfg.get("global_data_http_rps"), 4.0))
    total_clob_rps = max(0.2, _to_float(cfg.get("global_clob_api_rps"), 8.0))
    wc = max(1, int(worker_count))

    per_worker_data_rps = max(0.2, total_data_rps / wc)
    per_worker_data_http_rps = max(0.2, total_data_http_rps / wc)
    per_worker_clob_rps = max(0.2, total_clob_rps / wc)

    try:
        from smartmoney_query.poly_martmoney_query import api_client as data_api_mod

        if hasattr(data_api_mod, "RateLimiter"):
            data_api_mod._GLOBAL_LIMITER = data_api_mod.RateLimiter(per_worker_data_rps)
            data_api_mod.MAX_REQUESTS_PER_SECOND = per_worker_data_rps
    except Exception as exc:
        logger.warning("[RATE_LIMIT] failed to set data-api limiter: %s", exc)

    try:
        configure_data_http_rate_limit(per_worker_data_http_rps)
    except Exception as exc:
        logger.warning("[RATE_LIMIT] failed to set data-http limiter: %s", exc)

    try:
        configure_clob_rate_limit(per_worker_clob_rps)
    except Exception as exc:
        logger.warning("[RATE_LIMIT] failed to set clob limiter: %s", exc)

    logger.info(
        "[RATE_LIMIT] workers=%s data_rps(total=%.3f per_worker=%.3f) "
        "data_http_rps(total=%.3f per_worker=%.3f) "
        "clob_rps(total=%.3f per_worker=%.3f)",
        wc,
        total_data_rps,
        per_worker_data_rps,
        total_data_http_rps,
        per_worker_data_http_rps,
        total_clob_rps,
        per_worker_clob_rps,
    )


def _build_worker_cmd(
    args: argparse.Namespace,
    *,
    worker_index: int,
    worker_count: int,
) -> List[str]:
    cmd: List[str] = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--config",
        str(args.config),
        "--state",
        str(args.state),
        "--worker-index",
        str(worker_index),
        "--worker-count",
        str(worker_count),
        "--worker-supervised",
    ]
    if args.target_address:
        cmd.extend(["--target", str(args.target_address)])
    if args.my_address:
        cmd.extend(["--my", str(args.my_address)])
    if args.follow_ratio is not None:
        cmd.extend(["--ratio", str(args.follow_ratio)])
    if args.poll_interval_sec is not None:
        cmd.extend(["--poll", str(args.poll_interval_sec)])
    if args.poll_interval_sec_exiting is not None:
        cmd.extend(["--poll-exit", str(args.poll_interval_sec_exiting)])
    if args.my_positions_force_http:
        cmd.append("--my-positions-force-http")
    if args.dry_run:
        cmd.append("--dry-run")
    return cmd


def _run_worker_supervisor_if_needed(
    *,
    args: argparse.Namespace,
    cfg: Dict[str, Any],
    base_dir: Path,
    logger: logging.Logger,
) -> bool:
    configured_workers = _resolve_auto_worker_count(
        cfg,
        base_dir,
        logger,
    )
    if args.worker_supervised or configured_workers <= 1:
        return False

    logger.info("[WORKER] supervisor starting %s worker processes", configured_workers)

    total_data_rps = max(0.2, _to_float(cfg.get("global_data_api_rps"), 2.0))
    total_data_http_rps = max(0.2, _to_float(cfg.get("global_data_http_rps"), 4.0))
    total_clob_rps = max(0.2, _to_float(cfg.get("global_clob_api_rps"), 8.0))

    per_worker_data_rps = max(0.2, total_data_rps / configured_workers)
    per_worker_data_http_rps = max(0.2, total_data_http_rps / configured_workers)
    per_worker_clob_rps = max(0.2, total_clob_rps / configured_workers)
    worker_restart_limit = max(0, _to_int(cfg.get("worker_restart_limit"), 2))
    worker_restart_window_sec = max(
        60, _to_int(cfg.get("worker_restart_window_sec"), 900)
    )

    children: List[tuple[int, subprocess.Popen[Any]]] = []
    restart_history: Dict[int, List[float]] = {}

    def _spawn_worker(
        worker_index: int,
        *,
        restart_count: int = 0,
    ) -> subprocess.Popen[Any]:
        cmd = _build_worker_cmd(
            args,
            worker_index=worker_index,
            worker_count=configured_workers,
        )
        env = os.environ.copy()
        env["SMART_QUERY_MAX_RPS"] = str(per_worker_data_rps)
        env["CT_DATA_HTTP_MAX_RPS"] = str(per_worker_data_http_rps)
        env["CT_CLOB_MAX_RPS"] = str(per_worker_clob_rps)
        proc = subprocess.Popen(cmd, cwd=str(base_dir), env=env)
        if restart_count > 0:
            logger.warning(
                "[WORKER] restarted worker=%s/%s pid=%s restart=%s/%s window=%ss",
                worker_index + 1,
                configured_workers,
                proc.pid,
                restart_count,
                worker_restart_limit,
                worker_restart_window_sec,
            )
        else:
            logger.info(
                "[WORKER] started worker=%s/%s pid=%s",
                worker_index + 1,
                configured_workers,
                proc.pid,
            )
        return proc

    try:
        for worker_index in range(configured_workers):
            proc = _spawn_worker(worker_index)
            children.append((worker_index, proc))

        while True:
            for idx in range(len(children) - 1, -1, -1):
                worker_index, proc = children[idx]
                rc = proc.poll()
                if rc is not None:
                    if rc == WORKER_RETIRED_EXIT_CODE:
                        logger.info(
                            "[WORKER] worker=%s/%s pid=%s retired after init; no valid shard remained",
                            worker_index + 1,
                            configured_workers,
                            proc.pid,
                        )
                        children.pop(idx)
                        continue
                    now_monotonic = time.time()
                    history = restart_history.setdefault(worker_index, [])
                    history[:] = [
                        ts
                        for ts in history
                        if now_monotonic - ts <= worker_restart_window_sec
                    ]
                    if len(history) >= worker_restart_limit:
                        raise RuntimeError(
                            f"worker {worker_index + 1}/{configured_workers} exited "
                            f"with code {rc} after {len(history)} restarts "
                            f"within {worker_restart_window_sec}s"
                        )
                    history.append(now_monotonic)
                    logger.warning(
                        "[WORKER] worker=%s/%s pid=%s exited rc=%s; "
                        "attempting restart %s/%s",
                        worker_index + 1,
                        configured_workers,
                        proc.pid,
                        rc,
                        len(history),
                        worker_restart_limit,
                    )
                    children[idx] = (
                        worker_index,
                        _spawn_worker(worker_index, restart_count=len(history)),
                    )
            if not children:
                raise RuntimeError("all worker shards retired; no valid accounts remain after initialization")
            time.sleep(1.0)
    finally:
        for _, proc in children:
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
        deadline = time.time() + 5.0
        for _, proc in children:
            if proc.poll() is None:
                timeout = max(0.0, deadline - time.time())
                try:
                    proc.wait(timeout=timeout)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
    return True


def _apply_worker_shard(
    account_contexts: List[AccountContext],
    *,
    worker_index: int,
    worker_count: int,
    logger: logging.Logger,
) -> List[AccountContext]:
    wc = max(1, int(worker_count))
    wi = int(worker_index)
    if wc <= 1:
        return account_contexts
    if wi < 0 or wi >= wc:
        raise ValueError(f"Invalid worker_index={wi} for worker_count={wc}")
    sharded = [acct for idx, acct in enumerate(account_contexts) if (idx % wc) == wi]
    if not sharded:
        raise ValueError(f"Worker shard empty: worker_index={wi}, worker_count={wc}")
    logger.info(
        "[WORKER] shard worker=%s/%s accounts=%s/%s",
        wi + 1,
        wc,
        len(sharded),
        len(account_contexts),
    )
    return sharded


def _state_path_for_account(base_dir: Path, target_address: str, my_address: str) -> Path:
    """Generate state file path for a specific account."""
    target_short = target_address.strip().lower()
    if target_short.startswith("0x") and len(target_short) >= 10:
        target_part = f"{target_short[2:6]}_{target_short[-4:]}"
    else:
        target_part = re.sub(r"[^a-zA-Z0-9_-]+", "_", target_short)[:16] or "unknown"

    my_short = my_address.strip().lower()
    if my_short.startswith("0x") and len(my_short) >= 10:
        my_part = f"{my_short[2:6]}_{my_short[-4:]}"
    else:
        my_part = re.sub(r"[^a-zA-Z0-9_-]+", "_", my_short)[:16] or "unknown"

    return _state_dir_for_base(base_dir) / f"state_{target_part}_{my_part}.json"


def _read_accounts_from_file(accounts_file: Path) -> List[Dict[str, Any]]:
    """Read raw account entries from external accounts.json file."""
    if not accounts_file.exists():
        raise FileNotFoundError(f"Accounts file not found: {accounts_file}")

    try:
        # Use utf-8-sig to tolerate BOM-prefixed JSON from Windows editors.
        content = accounts_file.read_text(encoding="utf-8-sig")
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in accounts file: {e}")

    accounts = data.get("accounts", [])
    if not accounts:
        raise ValueError(f"No accounts found in {accounts_file}")

    return accounts


def _load_accounts_from_file(accounts_file: Path, logger: logging.Logger) -> List[Dict[str, Any]]:
    """Load accounts from external accounts.json file."""
    accounts = _read_accounts_from_file(accounts_file)
    logger.info("[MULTI] Loaded %d account(s) from %s", len(accounts), accounts_file.name)
    return accounts


def _count_runnable_accounts(accounts_cfg: List[Dict[str, Any]]) -> int:
    """Count accounts that are enabled and syntactically valid for startup."""
    runnable = 0
    for acct_cfg in accounts_cfg:
        if not acct_cfg.get("enabled", True):
            continue
        my_address = str(acct_cfg.get("my_address") or "").strip()
        private_key = str(acct_cfg.get("private_key") or "").strip()
        if not my_address or _is_placeholder_addr(my_address):
            continue
        if not _is_evm_address(my_address):
            continue
        if not private_key or private_key.startswith("YOUR_PRIVATE_KEY"):
            continue
        runnable += 1
    return runnable


def _resolve_auto_worker_count(
    cfg: Dict[str, Any],
    base_dir: Path,
    logger: logging.Logger,
) -> int:
    """
    Auto-scale worker count for the current accounts file.

    Current policy is intentionally simple:
    - 1 runnable account -> 1 worker
    - 2+ runnable accounts -> 2 workers
    """
    fallback_workers = max(1, _to_int(cfg.get("account_workers"), 2))
    accounts_file_name = str(cfg.get("accounts_file") or "accounts.json")
    accounts_file = base_dir / accounts_file_name

    try:
        accounts_cfg = _read_accounts_from_file(accounts_file)
    except Exception as exc:
        logger.warning(
            "[WORKER] auto-scale fallback workers=%s reason=%s",
            fallback_workers,
            exc,
        )
        return fallback_workers

    runnable_accounts = _count_runnable_accounts(accounts_cfg)
    effective_workers = 2 if runnable_accounts > 1 else 1
    if effective_workers != fallback_workers:
        logger.info(
            "[WORKER] auto-scale adjusted workers configured=%s effective=%s runnable_accounts=%s",
            fallback_workers,
            effective_workers,
            runnable_accounts,
        )
    else:
        logger.info(
            "[WORKER] auto-scale workers=%s runnable_accounts=%s",
            effective_workers,
            runnable_accounts,
        )
    return effective_workers


def _init_account_contexts(
    cfg: Dict[str, Any],
    base_dir: Path,
    logger: logging.Logger,
) -> List[AccountContext]:
    """Initialize AccountContext for each enabled account in accounts.json."""

    # Load accounts from external file
    accounts_file_name = cfg.get("accounts_file", "accounts.json")
    accounts_file = base_dir / accounts_file_name
    accounts_cfg = _load_accounts_from_file(accounts_file, logger)

    target_address = cfg["target_address"]
    contexts: List[AccountContext] = []

    for idx, acct_cfg in enumerate(accounts_cfg):
        if not acct_cfg.get("enabled", True):
            logger.info("[MULTI] Account #%d disabled, skipping", idx)
            continue

        acct_name = acct_cfg.get("name", f"Account_{idx}")
        my_address = acct_cfg.get("my_address")
        private_key = acct_cfg.get("private_key")

        if not my_address or _is_placeholder_addr(my_address):
            logger.warning("[MULTI] Account '%s' has no valid my_address, skipping", acct_name)
            continue

        if not _is_evm_address(my_address):
            logger.warning("[MULTI] Account '%s' address invalid: %s, skipping", acct_name, my_address)
            continue

        if not private_key or private_key.startswith("YOUR_PRIVATE_KEY"):
            logger.warning("[MULTI] Account '%s' has no valid private_key, skipping", acct_name)
            continue

        # follow_ratio is per-account, defined in accounts.json (default: 0.05)
        follow_ratio = float(acct_cfg.get("follow_ratio") or 0.05)

        # Initialize CLOB client for this account using private_key from config
        try:
            clob_client = init_clob_client(
                private_key=private_key,
                funder_address=my_address,
                cfg=cfg,
            )
        except Exception as exc:
            logger.error("[MULTI] Account '%s' (%s) CLOB init failed: %s", acct_name, my_address, exc)
            continue
        clob_read_client = clob_client

        # Create per-account state file path
        state_path = _state_path_for_account(base_dir, target_address, my_address)

        # Load state for this account
        state = load_state(str(state_path))

        # Safety: if state was for a different target, reset bootstrap fields
        prev_target = str(state.get("target") or "").lower().strip()
        cur_target = str(target_address or "").lower().strip()
        if prev_target and cur_target and prev_target != cur_target:
            logger.warning(
                "[MULTI] Account '%s' state target mismatch, resetting bootstrap",
                acct_name,
            )
            state["bootstrapped"] = False
            state["boot_token_ids"] = []
            state["boot_token_keys"] = []
            state["target_last_shares_by_token_key"] = {}
            state["target_last_shares"] = {}
            state["target_last_seen_ts"] = {}
            state["target_missing_streak"] = {}
            state["last_target_sell_action_ts_by_token"] = {}
            state["topic_state"] = {}
            state["open_orders"] = {}
            state["open_orders_all"] = []
            state["must_exit_tokens"] = {}
            state["last_nonzero_my_shares"] = {}
            if boot_sync_mode == "baseline_only" or fresh_boot:
                state["seen_action_ids"] = []
            state["target_actions_cursor_ms"] = 0
            state["target_trades_cursor_ms"] = 0

        # Initialize all required state fields for this account
        state.setdefault("open_orders", {})
        state.setdefault("open_orders_all", {})
        state.setdefault("seen_my_trade_ids", [])
        state.setdefault("my_trades_cursor_ms", 0)
        state.setdefault("managed_order_ids", [])
        state.setdefault("intent_keys", {})
        state.setdefault("token_map", {})
        state.setdefault("bootstrapped", False)
        state.setdefault("boot_token_ids", [])
        state.setdefault("boot_token_keys", [])
        state.setdefault("target_last_shares_by_token_key", {})
        state.setdefault("target_last_shares", {})
        state.setdefault("target_last_seen_ts", {})
        state.setdefault("target_missing_streak", {})
        state.setdefault("last_target_sell_action_ts_by_token", {})
        state.setdefault("cooldown_until", {})
        state.setdefault("topic_state", {})
        state.setdefault("target_actions_cursor_ms", 0)
        state.setdefault("ignored_tokens", {})
        state.setdefault("topic_unfilled_attempts", {})
        state.setdefault("probed_token_ids", [])
        state.setdefault("market_status_cache", {})
        state.setdefault("last_mid_price_by_token_id", {})
        state.setdefault("orderbook_empty_streak", {})
        state.setdefault("order_ts_by_id", {})
        state.setdefault("seen_action_ids", [])
        state.setdefault("last_reprice_ts_by_token", {})
        state.setdefault("place_fail_until", {})
        state.setdefault("exit_sell_state", {})
        state.setdefault("exit_finalization", {})
        state.setdefault("sell_reconcile_lock_until", {})
        state.setdefault("missing_data_freeze", {})
        state.setdefault("resolver_fail_cache", {})
        state.setdefault("closed_token_keys", {})
        state.setdefault("must_exit_tokens", {})
        state.setdefault("last_nonzero_my_shares", {})
        state.setdefault("market_info_cache", {})
        state.setdefault("cutover_rebuild_pending", False)
        state.setdefault("remote_order_snapshot_ts", 0)
        state.setdefault("collateral_preflight", {})
        if isinstance(state.get("exit_sell_state"), dict) and state["exit_sell_state"]:
            logger.info(
                "[STATE] account=%s clearing stale exit_sell_state entries=%s",
                acct_name,
                len(state["exit_sell_state"]),
            )
            state["exit_sell_state"] = {}
        if isinstance(state.get("exit_finalization"), dict) and state["exit_finalization"]:
            logger.info(
                "[STATE] account=%s clearing stale exit_finalization entries=%s",
                acct_name,
                len(state["exit_finalization"]),
            )
            state["exit_finalization"] = {}

        try:
            api_timeout_sec = float(cfg.get("api_timeout_sec") or 15.0)
        except Exception:
            api_timeout_sec = 15.0
        if api_timeout_sec <= 0:
            api_timeout_sec = None
        collateral_preflight = preflight_pusd_ready_v2(clob_client, api_timeout_sec)
        collateral_preflight["ts"] = int(time.time())
        state["collateral_preflight"] = collateral_preflight
        if collateral_preflight.get("ready") is False:
            logger.warning(
                "[PUSD_PREFLIGHT] account=%s ready=%s balance=%s allowance=%s msg=%s",
                acct_name,
                collateral_preflight.get("ready"),
                collateral_preflight.get("balance"),
                collateral_preflight.get("allowance"),
                collateral_preflight.get("message"),
            )
            if _cfg_bool(cfg.get("require_pusd_ready"), True):
                logger.error(
                    "[MULTI] Account '%s' (%s) skipped: pUSD collateral not ready",
                    acct_name,
                    my_address,
                )
                continue
        elif collateral_preflight.get("ok") is False:
            logger.warning(
                "[PUSD_PREFLIGHT_WARN] account=%s msg=%s",
                acct_name,
                collateral_preflight.get("message"),
            )

        ctx = AccountContext(
            name=acct_name,
            my_address=my_address.strip(),
            private_key=private_key,
            follow_ratio=follow_ratio,
            clob_client=clob_client,
            clob_read_client=clob_read_client,
            state=state,
            state_path=state_path,
            enabled=True,
            max_notional_per_token=acct_cfg.get("max_notional_per_token"),
            max_notional_total=acct_cfg.get("max_notional_total"),
        )
        contexts.append(ctx)

        logger.info(
            "[MULTI] Account '%s' initialized: addr=%s ratio=%.4f state=%s",
            acct_name,
            _shorten_address(my_address),
            follow_ratio,
            state_path.name,
        )

    if not contexts:
        raise ValueError("No valid accounts configured or all accounts failed initialization")

    logger.info("[MULTI] Total %d account(s) initialized successfully", len(contexts))
    return contexts


def main() -> None:
    args = _parse_args()
    cfg = _load_config(Path(args.config))
    arg_overrides: Dict[str, Any] = {}
    for key in (
        "target_address",
        "follow_ratio",
        "poll_interval_sec",
        "poll_interval_sec_exiting",
        "my_positions_force_http",
    ):
        arg_val = getattr(args, key, None)
        if arg_val is not None:
            cfg[key] = arg_val
            arg_overrides[key] = arg_val

    # ============================================================
    # RESOLVE TARGET ADDRESSES (before logging setup)
    # Priority: target_addresses array > target_address > env vars
    # ============================================================
    base_dir = Path(args.config).parent

    # First try to get targets from target_addresses array
    target_addresses: List[str] = []
    target_ratios: Dict[str, float] = {}
    target_blacklists: Dict[str, List[str]] = {}
    target_list = cfg.get("target_addresses")
    if isinstance(target_list, list) and target_list:
        for item in target_list:
            if isinstance(item, str):
                addr_str = str(item).strip()
                if _is_evm_address(addr_str) and not _is_placeholder_addr(addr_str):
                    target_addresses.append(addr_str)
                    target_ratios[addr_str.lower()] = 1.0
                    target_blacklists[addr_str.lower()] = cfg.get("blacklist_token_keys") or []
                else:
                    pass  # silently skip invalid address before logger init
            elif isinstance(item, dict):
                addr_str = str(item.get("address") or "").strip()
                if _is_evm_address(addr_str) and not _is_placeholder_addr(addr_str):
                    ratio = float(item.get("ratio", 1.0))
                    ratio = max(0.0, ratio)
                    target_addresses.append(addr_str)
                    target_ratios[addr_str.lower()] = ratio
                    per_target_bl = item.get("blacklist_token_keys")
                    if isinstance(per_target_bl, list):
                        target_blacklists[addr_str.lower()] = per_target_bl
                    else:
                        target_blacklists[addr_str.lower()] = cfg.get("blacklist_token_keys") or []
                else:
                    pass  # silently skip invalid address before logger init
            else:
                pass  # silently skip invalid entry before logger init

    # Fall back to target_address or env vars if no valid targets from array
    if not target_addresses:
        single_target = cfg.get("target_address")
        if not single_target or _is_placeholder_addr(single_target):
            # Try environment variables
            single_target = _get_env_first([
                "COPYTRADE_TARGET",
                "CT_TARGET",
                "POLY_TARGET_ADDRESS",
                "TARGET_ADDRESS",
            ])
        if single_target and _is_evm_address(single_target) and not _is_placeholder_addr(single_target):
            target_addresses.append(str(single_target).strip())
            target_ratios[str(single_target).strip().lower()] = 1.0
            target_blacklists[str(single_target).strip().lower()] = cfg.get("blacklist_token_keys") or []

    if not target_addresses:
        raise ValueError(
            "No valid target addresses configured. "
            "Set target_addresses array in config, or target_address, or env vars."
        )

    # Set cfg["target_address"] to first target for backward compatibility
    cfg["target_address"] = target_addresses[0]
    target_levels = _resolve_target_level_map(cfg, target_addresses)
    target_level_skip_ratios = _resolve_target_level_skip_ratios(cfg)

    # Setup logging using first target address
    logger = _setup_logging(cfg, cfg["target_address"], base_dir)
    target_level_skip_log_path = _resolve_target_level_skip_log_path(cfg, base_dir)

    # Optional process-level worker supervisor: spawn N worker subprocesses,
    # each handling a disjoint account shard.
    _run_worker_supervisor_if_needed(
        args=args,
        cfg=cfg,
        base_dir=base_dir,
        logger=logger,
    )

    effective_worker_count = max(1, _to_int(args.worker_count, 1))
    effective_worker_index = max(0, _to_int(args.worker_index, 0))
    _configure_global_api_rate_limits(
        cfg,
        worker_count=effective_worker_count,
        logger=logger,
    )

    # Log resolved targets
    logger.info(
        "[MULTI-TARGET] Resolved %d target address(es): %s",
        len(target_addresses),
        ", ".join(_shorten_address(a) for a in target_addresses),
    )
    logger.info(
        "[TARGET_LEVEL] policy skip_ratios=%s levels=%s",
        target_level_skip_ratios,
        {
            _shorten_address(addr): target_levels.get(str(addr).lower(), "A")
            for addr in target_addresses
        },
    )

    # ============================================================
    # MULTI-ACCOUNT INITIALIZATION
    # ============================================================
    all_account_contexts = _init_account_contexts(cfg, base_dir, logger)
    all_enabled_account_ids_global: List[str] = [
        str(acct.my_address or "").strip().lower()
        for acct in all_account_contexts
        if str(acct.my_address or "").strip()
    ]
    all_account_name_by_id: Dict[str, str] = {
        str(acct.my_address or "").strip().lower(): str(acct.name or _shorten_address(acct.my_address))
        for acct in all_account_contexts
    }
    effective_worker_count, retire_worker = _resolve_effective_worker_shard_count(
        len(all_account_contexts),
        effective_worker_count,
        effective_worker_index,
    )
    if retire_worker:
        if args.worker_supervised:
            logger.info(
                "[WORKER] retiring worker=%s/%s after init valid_accounts=%s active_workers=%s",
                effective_worker_index + 1,
                max(1, _to_int(args.worker_count, 1)),
                len(all_account_contexts),
                effective_worker_count,
            )
            raise SystemExit(WORKER_RETIRED_EXIT_CODE)
        raise ValueError(
            f"Worker shard empty after account init: worker_index={effective_worker_index}, "
            f"worker_count={effective_worker_count}, valid_accounts={len(all_account_contexts)}"
        )
    if effective_worker_count <= 0:
        raise ValueError("No valid accounts configured or all accounts failed initialization")
    if effective_worker_count != max(1, _to_int(args.worker_count, 1)):
        logger.info(
            "[WORKER] shrink after init configured=%s active=%s valid_accounts=%s",
            max(1, _to_int(args.worker_count, 1)),
            effective_worker_count,
            len(all_account_contexts),
        )
    account_contexts = _apply_worker_shard(
        all_account_contexts,
        worker_index=effective_worker_index,
        worker_count=effective_worker_count,
        logger=logger,
    )
    current_account_idx = 0  # Used for round-robin account selection
    all_account_ids: List[str] = [
        str(acct.my_address or "").strip().lower()
        for acct in account_contexts
        if str(acct.my_address or "").strip()
    ]
    target_level_token_decisions: Dict[str, Dict[str, Any]] = {}
    target_level_logged_tokens: set[str] = set()
    target_level_seed = _resolve_target_level_seed(cfg)

    logger.info(
        "[MULTI] Initialized %d follower account(s) for %d target(s)",
        len(account_contexts),
        len(target_addresses),
    )

    # For backward compatibility, use first account's settings as default
    cfg["my_address"] = account_contexts[0].my_address
    cfg["follow_ratio"] = account_contexts[0].follow_ratio
    args.state = str(account_contexts[0].state_path)
    state = account_contexts[0].state
    # Safety: if user accidentally reuses a state file across targets, reset bootstrap-related fields.
    prev_target = str(state.get("target") or "").lower().strip()
    cur_target = str(cfg.get("target_address") or "").lower().strip()
    if prev_target and cur_target and prev_target != cur_target:
        logger.warning(
            "[STATE] state target mismatch (state=%s cfg=%s); resetting bootstrap fields",
            prev_target,
            cur_target,
        )
        state["bootstrapped"] = False
        state["boot_token_ids"] = []
        state["boot_token_keys"] = []
        state["target_last_shares_by_token_key"] = {}
        state["target_last_shares"] = {}
        state["target_last_seen_ts"] = {}
        state["target_missing_streak"] = {}
        state["last_target_sell_action_ts_by_token"] = {}
        state["topic_state"] = {}
        state["open_orders"] = {}
        state["open_orders_all"] = []
        state["must_exit_tokens"] = {}
        state["last_nonzero_my_shares"] = {}
        state["seen_action_ids"] = []
        state["target_actions_cursor_ms"] = 0
        state["target_trades_cursor_ms"] = 0
    state.pop("cumulative_buy_usd_total", None)
    state.pop("cumulative_buy_usd_by_token", None)
    run_start_ms = int(time.time() * 1000)
    state["run_start_ms"] = run_start_ms
    logger.info("[STATE] path=%s run_start_ms=%s", args.state, run_start_ms)
    state.setdefault("sizing", {})
    state["sizing"].setdefault("ema_delta_usd", None)
    logger.info(
        "[CFG] targets=%s my=%s ratio=%s",
        [_shorten_address(a) for a in target_addresses],
        cfg["my_address"],
        cfg.get("follow_ratio"),
    )
    state["target"] = cfg.get("target_address")  # Keep primary target for backward compatibility
    state["target_addresses"] = target_addresses  # Store all targets
    state["my_address"] = cfg.get("my_address")
    state["follow_ratio"] = cfg.get("follow_ratio")
    state.setdefault("open_orders", {})
    state.setdefault("open_orders_all", {})
    state.setdefault("seen_my_trade_ids", [])
    state.setdefault("my_trades_cursor_ms", 0)
    state.setdefault("my_trades_unreliable_until", 0)
    state.setdefault("managed_order_ids", [])
    state.setdefault("intent_keys", {})
    state.setdefault("token_map", {})
    state.setdefault("bootstrapped", False)
    state.setdefault("boot_token_ids", [])
    state.setdefault("boot_token_keys", [])
    state.setdefault("target_last_shares_by_token_key", {})
    state.setdefault("boot_run_start_ms", 0)
    state.setdefault("probed_token_ids", [])
    state.setdefault("ignored_tokens", {})
    state.setdefault("topic_unfilled_attempts", {})
    state.setdefault("market_status_cache", {})
    state.setdefault("target_last_shares", {})
    state.setdefault("target_last_seen_ts", {})
    state.setdefault("target_missing_streak", {})
    state.setdefault("last_target_sell_action_ts_by_token", {})
    state.setdefault("cooldown_until", {})
    state.setdefault("target_last_event_ts", {})
    state.setdefault("topic_state", {})
    state.setdefault("target_actions_cursor_ms", 0)
    state.setdefault("last_mid_price_by_token_id", {})
    state.setdefault("last_mid_price_update_ts", 0)
    state.setdefault("orderbook_empty_streak", {})
    state.setdefault("order_ts_by_id", {})
    state.setdefault("seen_action_ids", [])
    state.setdefault("last_reprice_ts_by_token", {})
    state.setdefault("adopted_existing_orders", False)
    state.setdefault("place_fail_until", {})
    state.setdefault("exit_sell_state", {})
    state.setdefault("exit_finalization", {})
    state.setdefault("sell_reconcile_lock_until", {})
    state.setdefault("missing_data_freeze", {})
    state.setdefault("resolver_fail_cache", {})
    state.setdefault("target_positions_nonce_last_ts", 0)
    state.setdefault("target_positions_nonce_actions", 0)
    state.setdefault("must_exit_tokens", {})
    state.setdefault("last_nonzero_my_shares", {})
    state.setdefault("market_info_cache", {})
    state.setdefault("cutover_rebuild_pending", False)
    state.setdefault("remote_order_snapshot_ts", 0)
    state.setdefault("collateral_preflight", {})
    if not isinstance(state.get("open_orders"), dict):
        state["open_orders"] = {}
    if not isinstance(state.get("open_orders_all"), dict):
        state["open_orders_all"] = {}
    if not isinstance(state.get("managed_order_ids"), list):
        state["managed_order_ids"] = []
    if not isinstance(state.get("intent_keys"), dict):
        state["intent_keys"] = {}
    if not isinstance(state.get("token_map"), dict):
        state["token_map"] = {}
    if not isinstance(state.get("bootstrapped"), bool):
        state["bootstrapped"] = False
    if not isinstance(state.get("boot_token_ids"), list):
        state["boot_token_ids"] = []
    if not isinstance(state.get("boot_token_keys"), list):
        state["boot_token_keys"] = []
    if not isinstance(state.get("target_last_shares_by_token_key"), dict):
        state["target_last_shares_by_token_key"] = {}
    if not isinstance(state.get("boot_run_start_ms"), (int, float)):
        state["boot_run_start_ms"] = 0
    if not isinstance(state.get("probed_token_ids"), list):
        state["probed_token_ids"] = []
    if not isinstance(state.get("exit_finalization"), dict):
        state["exit_finalization"] = {}
    if not isinstance(state.get("ignored_tokens"), dict):
        state["ignored_tokens"] = {}
    if not isinstance(state.get("market_status_cache"), dict):
        state["market_status_cache"] = {}
    if not isinstance(state.get("target_last_shares"), dict):
        state["target_last_shares"] = {}
    if not isinstance(state.get("target_last_seen_ts"), dict):
        state["target_last_seen_ts"] = {}
    if not isinstance(state.get("target_missing_streak"), dict):
        state["target_missing_streak"] = {}
    if not isinstance(state.get("last_target_sell_action_ts_by_token"), dict):
        state["last_target_sell_action_ts_by_token"] = {}
    if not isinstance(state.get("cooldown_until"), dict):
        state["cooldown_until"] = {}
    if not isinstance(state.get("target_last_event_ts"), dict):
        state["target_last_event_ts"] = {}
    if not isinstance(state.get("topic_state"), dict):
        state["topic_state"] = {}
    if not isinstance(state.get("target_actions_cursor_ms"), (int, float)):
        state["target_actions_cursor_ms"] = 0
    if not isinstance(state.get("last_mid_price_by_token_id"), dict):
        state["last_mid_price_by_token_id"] = {}
    if not isinstance(state.get("last_mid_price_update_ts"), (int, float)):
        state["last_mid_price_update_ts"] = 0
    if not isinstance(state.get("orderbook_empty_streak"), dict):
        state["orderbook_empty_streak"] = {}
    if not isinstance(state.get("order_ts_by_id"), dict):
        state["order_ts_by_id"] = {}
    if not isinstance(state.get("seen_action_ids"), list):
        state["seen_action_ids"] = []
    if not isinstance(state.get("last_reprice_ts_by_token"), dict):
        state["last_reprice_ts_by_token"] = {}
    if not isinstance(state.get("adopted_existing_orders"), bool):
        state["adopted_existing_orders"] = False
    if not isinstance(state.get("place_fail_until"), dict):
        state["place_fail_until"] = {}
    if not isinstance(state.get("exit_sell_state"), dict):
        state["exit_sell_state"] = {}
    if not isinstance(state.get("sell_reconcile_lock_until"), dict):
        state["sell_reconcile_lock_until"] = {}
    if not isinstance(state.get("target_positions_nonce_last_ts"), (int, float)):
        state["target_positions_nonce_last_ts"] = 0
    if not isinstance(state.get("target_positions_nonce_actions"), (int, float)):
        state["target_positions_nonce_actions"] = 0
    if not isinstance(state.get("missing_data_freeze"), dict):
        state["missing_data_freeze"] = {}
    if not isinstance(state.get("resolver_fail_cache"), dict):
        state["resolver_fail_cache"] = {}
    if not isinstance(state.get("closed_token_keys"), dict):
        state["closed_token_keys"] = {}
    if not isinstance(state.get("must_exit_tokens"), dict):
        state["must_exit_tokens"] = {}
    if not isinstance(state.get("last_nonzero_my_shares"), dict):
        state["last_nonzero_my_shares"] = {}

    data_client = DataApiClient()
    # Note: clob_client is now per-account in account_contexts[i].clob_client
    clob_client = account_contexts[0].clob_client

    poll_interval = 20
    poll_interval_exiting = 20
    size_threshold = 0.0
    skip_closed = True
    refresh_sec = 300
    positions_limit = 500
    positions_max_pages = 20
    target_positions_refresh_sec = 25
    log_cache_headers = False
    header_keys: list[str] = [
        "Age",
        "CF-Cache-Status",
        "X-Cache",
        "Via",
        "Cache-Control",
    ]
    target_cache_bust_mode = "bucket"
    my_positions_force_http = False
    actions_page_size = 300
    actions_max_offset = 10000
    heartbeat_interval_sec = 600
    config_reload_sec = 600
    max_resolve_target_positions_per_loop = 20
    last_config_reload_ts = time.time()
    last_config_mtime: Optional[float] = None
    resolved_target_address = cfg["target_address"]
    resolved_my_address = cfg["my_address"]
    risk_summary_interval_sec = 60

    def _apply_overrides(payload: Dict[str, Any]) -> None:
        for key, value in arg_overrides.items():
            payload[key] = value

    def _apply_cfg_settings() -> None:
        nonlocal poll_interval
        nonlocal poll_interval_exiting
        nonlocal size_threshold
        nonlocal skip_closed
        nonlocal refresh_sec
        nonlocal positions_limit
        nonlocal positions_max_pages
        nonlocal target_positions_refresh_sec
        nonlocal log_cache_headers
        nonlocal header_keys
        nonlocal target_cache_bust_mode
        nonlocal my_positions_force_http
        nonlocal actions_page_size
        nonlocal actions_max_offset
        nonlocal heartbeat_interval_sec
        nonlocal config_reload_sec
        nonlocal max_resolve_target_positions_per_loop
        nonlocal risk_summary_interval_sec
        poll_interval = int(cfg.get("poll_interval_sec") or 20)
        poll_interval_exiting = int(cfg.get("poll_interval_sec_exiting") or poll_interval)
        size_threshold = float(cfg.get("size_threshold") or 0)
        skip_closed = bool(cfg.get("skip_closed_markets", True))
        refresh_sec = int(cfg.get("market_status_refresh_sec") or 300)
        positions_limit = int(cfg.get("positions_limit") or 500)
        positions_max_pages = int(cfg.get("positions_max_pages") or 20)
        target_positions_refresh_sec = int(cfg.get("target_positions_refresh_sec") or 25)
        log_cache_headers = bool(cfg.get("log_positions_cache_headers"))
        header_keys = cfg.get("positions_cache_header_keys") or [
            "Age",
            "CF-Cache-Status",
            "X-Cache",
            "Via",
            "Cache-Control",
        ]
        target_cache_bust_mode = str(cfg.get("target_cache_bust_mode") or "bucket")
        my_positions_force_http = bool(cfg.get("my_positions_force_http", False))
        actions_page_size = int(cfg.get("actions_page_size") or 300)
        actions_max_offset = int(cfg.get("actions_max_offset") or 10000)
        if actions_max_offset > 3000:
            actions_max_offset = 3000
        heartbeat_interval_sec = int(cfg.get("heartbeat_interval_sec") or 600)
        config_reload_sec = int(cfg.get("config_reload_sec") or 600)
        max_resolve_target_positions_per_loop = int(
            cfg.get("max_resolve_target_positions_per_loop") or 20
        )
        risk_summary_interval_sec = int(cfg.get("risk_summary_interval_sec") or 120)
        # Log deduplication: suppress repetitive logs within this window (0 = disabled)
        log_dedup_window = float(cfg.get("log_dedup_window_sec") or 300.0)
        _log_dedup.set_window(log_dedup_window)

    def _refresh_log_level() -> None:
        level_name = str(cfg.get("log_level") or "INFO").upper()
        level = logging._nameToLevel.get(level_name, logging.INFO)
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        for handler in root_logger.handlers:
            handler.setLevel(level)
        # Keep third-party loggers suppressed after level refresh
        _suppress_verbose_third_party_loggers(level)

    def _reload_config(reason: str) -> None:
        nonlocal cfg, last_config_reload_ts, last_config_mtime
        nonlocal target_levels, target_level_skip_ratios
        nonlocal target_level_skip_log_path
        nonlocal target_level_seed
        try:
            new_cfg = _load_config(Path(args.config))
        except Exception as exc:
            logger.warning("[CFG] reload failed (%s): %s", reason, exc)
            last_config_reload_ts = time.time()
            return
        _apply_overrides(new_cfg)
        new_target = new_cfg.get("target_address")
        new_my = new_cfg.get("my_address")
        if not str(new_target or "").strip():
            new_cfg["target_address"] = resolved_target_address
        elif str(new_target).strip() != str(resolved_target_address).strip():
            logger.warning(
                "[CFG] target_address 鍙樻洿灏嗚蹇界暐锛岄渶瑕侀噸鍚? %s -> %s",
                resolved_target_address,
                new_target,
            )
            new_cfg["target_address"] = resolved_target_address
        if not str(new_my or "").strip():
            new_cfg["my_address"] = resolved_my_address
        elif str(new_my).strip() != str(resolved_my_address).strip():
            logger.warning(
                "[CFG] my_address 鍙樻洿灏嗚蹇界暐锛岄渶瑕侀噸鍚? %s -> %s",
                resolved_my_address,
                new_my,
            )
            new_cfg["my_address"] = resolved_my_address
        cfg = new_cfg
        target_levels = _resolve_target_level_map(cfg, target_addresses)
        target_level_skip_ratios = _resolve_target_level_skip_ratios(cfg)
        target_level_skip_log_path = _resolve_target_level_skip_log_path(cfg, base_dir)
        target_level_seed = _resolve_target_level_seed(cfg)
        state["follow_ratio"] = cfg.get("follow_ratio")
        _apply_cfg_settings()
        _refresh_log_level()
        last_config_reload_ts = time.time()
        try:
            last_config_mtime = Path(args.config).stat().st_mtime
        except Exception:
            last_config_mtime = None
        logger.info(
            "[CFG] reloaded (%s) target_level_skip_ratios=%s",
            reason,
            target_level_skip_ratios,
        )

    _apply_cfg_settings()
    _refresh_log_level()
    try:
        last_config_mtime = Path(args.config).stat().st_mtime
    except Exception:
        last_config_mtime = None
    last_heartbeat_ts = 0
    last_risk_summary_ts = 0

    replay_floor_ms = _get_replay_floor_ms(cfg, state)
    if int(state.get("target_actions_cursor_ms") or 0) <= 0:
        state["target_actions_cursor_ms"] = replay_floor_ms
    if int(state.get("target_actions_cursor_ms") or 0) < replay_floor_ms:
        state["target_actions_cursor_ms"] = replay_floor_ms
    # Also enforce floor on target_trades_cursor_ms (used when actions_source="trades")
    if int(state.get("target_trades_cursor_ms") or 0) <= 0:
        state["target_trades_cursor_ms"] = replay_floor_ms
    if int(state.get("target_trades_cursor_ms") or 0) < replay_floor_ms:
        state["target_trades_cursor_ms"] = replay_floor_ms
    if int(state.get("my_trades_cursor_ms") or 0) <= 0:
        state["my_trades_cursor_ms"] = int(state.get("run_start_ms") or time.time() * 1000)
    if int(state.get("my_trades_cursor_ms") or 0) < int(state.get("run_start_ms") or 0):
        state["my_trades_cursor_ms"] = int(state.get("run_start_ms") or 0)
    if int(state.get("my_trades_unreliable_until") or 0) < 0:
        state["my_trades_unreliable_until"] = 0

    missing_notice_tokens: set[str] = set()

    # --- Daily log cleanup state ---
    _log_dir_value = cfg.get("log_dir") or "logs"
    _log_dir_path = Path(_log_dir_value)
    if not _log_dir_path.is_absolute():
        _log_dir_path = Path(args.config).parent / _log_dir_path
    _log_retention_days = int(cfg.get("log_retention_days") or 7)
    _log_cleanup_hour = int(cfg.get("log_cleanup_hour") or 12)
    _last_log_cleanup_date: str = ""
    last_http_timeout: Optional[float] = None

    def _get_poll_interval() -> int:
        topic_state = state.get("topic_state", {})
        if isinstance(topic_state, dict):
            for st in topic_state.values():
                if (st or {}).get("phase") == "EXITING":
                    return poll_interval_exiting
        return poll_interval

    def _get_api_timeout_sec() -> Optional[float]:
        try:
            timeout = float(cfg.get("api_timeout_sec") or 15.0)
        except Exception:
            timeout = 15.0
        if timeout <= 0:
            return None
        return timeout

    def _configure_clob_http_timeout(timeout_sec: Optional[float]) -> None:
        nonlocal last_http_timeout
        if timeout_sec == last_http_timeout:
            return
        try:
            import httpx
            from py_clob_client_v2.http_helpers import helpers as clob_http_helpers

            clob_http_helpers._http_client = httpx.Client(http2=True, timeout=timeout_sec)
            last_http_timeout = timeout_sec
            logger.info("[HTTP_TIMEOUT] clob_client httpx timeout=%s", timeout_sec)
        except Exception as exc:
            logger.warning("[HTTP_TIMEOUT] failed to set timeout=%s: %s", timeout_sec, exc)

    # Optional startup recovery:
    # replay target SELL actions within a lookback window and force-sell matching holdings
    # before entering the normal copy-trading loop.
    _run_hemostasis_recovery_startup(
        cfg=cfg,
        data_client=data_client,
        account_contexts=account_contexts,
        target_addresses=target_addresses,
        logger=logger,
        dry_run=bool(args.dry_run),
    )

    if effective_worker_count > 1:
        logger.info(
            "[MULTI] Starting main loop worker=%s/%s with %d account(s) in round-robin mode",
            effective_worker_index + 1,
            effective_worker_count,
            len(account_contexts),
        )
    else:
        logger.info(
            "[MULTI] Starting main loop with %d account(s) in round-robin mode",
            len(account_contexts),
        )

    # Shared cross-account cache for target data
    shared_target_cache: Dict[str, Any] = {}
    _current_log_date = datetime.now().date()

    while True:
        # Daily log file rotation without restarting process
        if datetime.now().date() != _current_log_date:
            _current_log_date = datetime.now().date()
            logger = _setup_logging(cfg, cfg["target_address"], base_dir)
            logger.info("[LOG] Date changed -> reopened daily log file")

        now_ts = int(time.time())
        now_wall = time.time()
        for acct_ctx_tmp in account_contexts:
            _prune_exit_finalization(acct_ctx_tmp.state, now_ts)

        # ============================================================
        # SHARED TARGET DATA CACHE (cross-account)
        # ============================================================
        cache_ttl_sec = max(5, int(cfg.get("shared_target_cache_ttl_sec") or target_positions_refresh_sec))
        cache_key = (
            tuple(target_addresses),
            tuple(sorted((k, round(v, 6)) for k, v in target_ratios.items())),
            tuple(
                sorted(
                    (k, tuple(sorted(v)))
                    for k, v in target_blacklists.items()
                )
            ),
            positions_limit,
            positions_max_pages,
            round(size_threshold, 9),
            target_positions_refresh_sec,
            target_cache_bust_mode,
            tuple(header_keys),
            actions_page_size,
            actions_max_offset,
            bool(cfg.get("actions_taker_only", False)),
        )
        if (
            shared_target_cache.get("cache_key") != cache_key
            or (now_ts - shared_target_cache.get("cached_at", 0)) >= cache_ttl_sec
        ):
            try:
                fresh_target_pos, fresh_target_info, fresh_position_source = _fetch_all_target_positions(
                    data_client,
                    target_addresses,
                    target_ratios,
                    target_blacklists,
                    size_threshold,
                    positions_limit=positions_limit,
                    positions_max_pages=positions_max_pages,
                    refresh_sec=target_positions_refresh_sec,
                    cache_bust_mode=target_cache_bust_mode,
                    header_keys=header_keys,
                    logger=logger,
                )
            except Exception as exc:
                logger.warning("[SHARED_CACHE] fetch target positions failed: %s", exc)
                fresh_target_pos, fresh_target_info, fresh_position_source = [], {"ok": False, "incomplete": True}, {}

            actions_source_cfg = str(cfg.get("actions_source") or "trades").lower()
            use_trades_api_cache = actions_source_cfg in ("trade", "trades")
            actions_cursor_key_cache = (
                "target_trades_cursor_ms" if use_trades_api_cache else "target_actions_cursor_ms"
            )
            min_cursor_ms = min(
                (int(acct.state.get(actions_cursor_key_cache) or 0) for acct in account_contexts),
                default=0,
            )
            replay_floor_ms_cache = _get_replay_floor_ms(cfg, account_contexts[0].state)
            min_cursor_ms = max(min_cursor_ms, replay_floor_ms_cache)
            try:
                fresh_actions_list, fresh_actions_info = _fetch_all_target_actions(
                    data_client,
                    target_addresses,
                    min_cursor_ms,
                    use_trades_api=use_trades_api_cache,
                    page_size=actions_page_size,
                    max_offset=actions_max_offset,
                    taker_only=bool(cfg.get("actions_taker_only", False)),
                    logger=logger,
                    target_blacklists=target_blacklists,
                )
            except Exception as exc:
                logger.warning("[SHARED_CACHE] fetch target actions failed: %s", exc)
                fresh_actions_list, fresh_actions_info = [], {"ok": False, "incomplete": True}

            shared_target_cache = {
                "cache_key": cache_key,
                "cached_at": now_ts,
                "positions": (fresh_target_pos, fresh_target_info, fresh_position_source),
                "actions": (fresh_actions_list, fresh_actions_info),
                "actions_cursor_key": actions_cursor_key_cache,
            }
            logger.debug(
                "[SHARED_CACHE] refreshed positions=%s actions=%s ttl=%s",
                len(fresh_target_pos),
                len(fresh_actions_list),
                cache_ttl_sec,
            )
        else:
            logger.debug(
                "[SHARED_CACHE] hit positions=%s actions=%s age=%s",
                len(shared_target_cache["positions"][0]),
                len(shared_target_cache["actions"][0]),
                now_ts - shared_target_cache["cached_at"],
            )

        cached_positions, cached_target_info, cached_position_source = shared_target_cache["positions"]
        cached_actions, cached_actions_info = shared_target_cache["actions"]
        cached_actions_cursor_key = shared_target_cache["actions_cursor_key"]

        # ============================================================
        # MULTI-ACCOUNT: Select current account (round-robin)
        # ============================================================
        acct_ctx = account_contexts[current_account_idx]
        state = acct_ctx.state
        clob_client = acct_ctx.clob_client
        clob_read_client = getattr(acct_ctx, "clob_read_client", None) or clob_client
        current_my_address = acct_ctx.my_address
        current_account_id = str(current_my_address or "").strip().lower()
        cfg["my_address"] = current_my_address
        cfg["follow_ratio"] = acct_ctx.follow_ratio
        args.state = str(acct_ctx.state_path)

        # Ensure per-account state has run_start_ms and cursors initialized.
        if int(state.get("run_start_ms") or 0) <= 0:
            state["run_start_ms"] = run_start_ms
        replay_floor_ms = _get_replay_floor_ms(cfg, state)
        if int(state.get("target_actions_cursor_ms") or 0) <= 0:
            state["target_actions_cursor_ms"] = replay_floor_ms
        if int(state.get("target_actions_cursor_ms") or 0) < replay_floor_ms:
            state["target_actions_cursor_ms"] = replay_floor_ms
        if int(state.get("target_trades_cursor_ms") or 0) <= 0:
            state["target_trades_cursor_ms"] = replay_floor_ms
        if int(state.get("target_trades_cursor_ms") or 0) < replay_floor_ms:
            state["target_trades_cursor_ms"] = replay_floor_ms

        # Apply per-account config overrides
        if acct_ctx.max_notional_per_token is not None:
            cfg["max_notional_per_token"] = acct_ctx.max_notional_per_token
        if acct_ctx.max_notional_total is not None:
            cfg["max_notional_total"] = acct_ctx.max_notional_total

        if len(account_contexts) > 1:
            logger.info(
                "[MULTI] === Processing account %d/%d: %s ratio=%.4f ===",
                current_account_idx + 1,
                len(account_contexts),
                _shorten_address(acct_ctx.my_address),
                acct_ctx.follow_ratio,
            )

        # --- Daily log cleanup: run once per day at the configured hour ---
        _now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        _today_str = _now_dt.strftime("%Y-%m-%d")
        if _today_str != _last_log_cleanup_date and _now_dt.hour >= _log_cleanup_hour:
            _last_log_cleanup_date = _today_str
            try:
                _cleanup_old_logs(_log_dir_path, _log_retention_days, logger)
            except Exception:
                pass  # best-effort; don't disrupt trading

        actions_missing_ratio = 0.0
        unresolved_trade_candidates: list[Dict[str, Any]] = []
        resolver_fail_cooldown_sec = int(cfg.get("resolver_fail_cooldown_sec") or 300)
        resolver_fail_cache = state.setdefault("resolver_fail_cache", {})
        if not isinstance(resolver_fail_cache, dict):
            resolver_fail_cache = {}
            state["resolver_fail_cache"] = resolver_fail_cache
        must_exit_tokens = state.setdefault("must_exit_tokens", {})
        if not isinstance(must_exit_tokens, dict):
            must_exit_tokens = {}
            state["must_exit_tokens"] = must_exit_tokens
        last_nonzero_my_shares = state.setdefault("last_nonzero_my_shares", {})
        if not isinstance(last_nonzero_my_shares, dict):
            last_nonzero_my_shares = {}
            state["last_nonzero_my_shares"] = last_nonzero_my_shares
        if resolver_fail_cooldown_sec > 0:
            expired_keys = [
                token_key
                for token_key, ts in resolver_fail_cache.items()
                if now_ts - int(ts or 0) >= resolver_fail_cooldown_sec
            ]
            for token_key in expired_keys:
                resolver_fail_cache.pop(token_key, None)
        else:
            resolver_fail_cache.clear()
        if now_wall - last_config_reload_ts >= max(config_reload_sec, 1):
            reason = "interval"
            try:
                mtime = Path(args.config).stat().st_mtime
                if last_config_mtime is None or mtime != last_config_mtime:
                    reason = "mtime"
            except Exception:
                reason = "interval"
            _reload_config(reason)
            # MULTI-ACCOUNT: ensure per-account identity survives config reload.
            cfg["my_address"] = acct_ctx.my_address
            cfg["follow_ratio"] = acct_ctx.follow_ratio
        api_timeout_sec = _get_api_timeout_sec()
        _configure_clob_http_timeout(api_timeout_sec)
        managed_ids = {str(order_id) for order_id in (state.get("managed_order_ids") or [])}
        try:
            remote_orders, ok, err = fetch_open_orders_norm(clob_read_client, api_timeout_sec)
            if ok:
                remote_by_token: Dict[str, list[dict]] = {}
                order_ts_by_id = state.setdefault("order_ts_by_id", {})
                remote_order_ids: set[str] = set()
                for order in remote_orders:
                    order_id = str(order["order_id"])
                    ts = order.get("ts") or order_ts_by_id.get(order_id) or now_ts
                    remote_order_ids.add(order_id)
                    order_payload = {
                        "order_id": order_id,
                        "side": order["side"],
                        "price": order["price"],
                        "size": order["size"],
                        "ts": int(ts),
                    }
                    remote_by_token.setdefault(order["token_id"], []).append(order_payload)
                adopt_existing = bool(cfg.get("adopt_existing_orders_on_boot", False))
                if adopt_existing and not state.get("adopted_existing_orders", False):
                    if len(managed_ids) < 3:
                        adoptable_ids: set[str] = set()
                        for orders in remote_by_token.values():
                            for order in orders:
                                price = float(order.get("price") or 0.0)
                                size = float(order.get("size") or 0.0)
                                if price <= 0 or price > 1.0:
                                    continue
                                if size <= 0:
                                    continue
                                order_id = order.get("order_id")
                                if order_id:
                                    adoptable_ids.add(str(order_id))
                        if adoptable_ids:
                            logger.info(
                                "[BOOT] adopt_existing_orders_on_boot: adopted=%s",
                                len(adoptable_ids),
                            )
                            managed_ids |= adoptable_ids
                    state["adopted_existing_orders"] = True
                # --- begin: ORDSYNC ledger-first (fix eventual consistency) ---
                prev_managed = state.get("open_orders")
                if not isinstance(prev_managed, dict):
                    prev_managed = {}
                managed_by_token: Dict[str, list[dict]] = {
                    str(token_id): [dict(order) for order in (orders or [])]
                    for token_id, orders in prev_managed.items()
                }

                # Merge remote visibility into ledger WITHOUT dropping unseen managed orders.
                # This makes order_visibility_grace_sec effective even when remote is partially consistent.
                managed_index: Dict[str, tuple[str, int]] = {}
                for t_id, orders in managed_by_token.items():
                    for i, o in enumerate(orders or []):
                        oid = str(o.get("order_id") or "")
                        if oid:
                            managed_index[oid] = (str(t_id), i)

                for t_id, orders in remote_by_token.items():
                    for order in orders or []:
                        oid = str(order.get("order_id") or "")
                        if not oid or oid not in managed_ids:
                            continue

                        if oid not in order_ts_by_id:
                            order_ts_by_id[oid] = int(order.get("ts") or now_ts)
                        order["ts"] = int(order.get("ts") or order_ts_by_id.get(oid) or now_ts)

                        hit = managed_index.get(oid)
                        if hit:
                            t0, i0 = hit
                            # Update the existing ledger order in-place (do NOT overwrite the whole token list)
                            try:
                                managed_by_token[t0][i0].update(order)
                            except Exception:
                                # Fallback if index drifted for any reason
                                t_id_s = str(t_id)
                                managed_by_token.setdefault(t_id_s, []).append(dict(order))
                                managed_index[oid] = (t_id_s, len(managed_by_token[t_id_s]) - 1)
                        else:
                            t_id_s = str(t_id)
                            managed_by_token.setdefault(t_id_s, []).append(dict(order))
                            managed_index[oid] = (t_id_s, len(managed_by_token[t_id_s]) - 1)

                grace_sec = int(cfg.get("order_visibility_grace_sec") or 180)
                pruned = 0

                for token_id, orders in list(managed_by_token.items()):
                    kept: list[dict] = []
                    for order in orders or []:
                        order_id = str(order.get("order_id") or "")
                        if not order_id or order_id not in managed_ids:
                            continue

                        ts = int(order.get("ts") or order_ts_by_id.get(order_id) or now_ts)
                        order["ts"] = ts

                        if order_id not in remote_order_ids and (now_ts - ts) > grace_sec:
                            pruned += 1
                            continue

                        kept.append(order)

                    if kept:
                        managed_by_token[token_id] = kept
                    else:
                        managed_by_token.pop(token_id, None)

                managed_ids = _collect_order_ids(managed_by_token)

                for order_id in list(order_ts_by_id.keys()):
                    if str(order_id) not in managed_ids:
                        order_ts_by_id.pop(order_id, None)

                state["open_orders_all"] = remote_by_token
                state["open_orders"] = managed_by_token
                state["managed_order_ids"] = sorted(managed_ids)

                if pruned:
                    logger.info(
                        "[ORDSYNC] pruned_missing_after_grace=%s grace_sec=%s",
                        pruned,
                        grace_sec,
                    )
                # --- end: ORDSYNC ledger-first ---
            else:
                logger.warning("[WARN] sync open orders failed: %s", err)
        except Exception as exc:
            logger.exception("[ERR] sync open orders failed: %s", exc)
        _prune_order_ts_by_id(state)

        has_buy_by_token: Dict[str, bool] = {}
        has_sell_by_token: Dict[str, bool] = {}
        buy_sum_by_token: Dict[str, float] = {}
        sell_sum_by_token: Dict[str, float] = {}
        buy_signal_source_by_token: Dict[str, str] = {}
        sell_signal_sources_by_token: Dict[str, Dict[str, int]] = {}
        buy_signal_ms_by_token: Dict[str, int] = {}
        sell_signal_ms_by_token: Dict[str, int] = {}
        actions_info: Dict[str, object] = {"ok": True, "incomplete": False}
        actions_list: list[Dict[str, object]] = []
        actions_source = str(cfg.get("actions_source") or "trades").lower()
        actions_cursor_key = (
            "target_trades_cursor_ms" if actions_source in ("trade", "trades") else "target_actions_cursor_ms"
        )
        actions_cursor_ms = int(state.get(actions_cursor_key) or 0)
        replay_floor_ms = _get_replay_floor_ms(cfg, state)
        actions_cursor_ms = max(actions_cursor_ms, replay_floor_ms)
        is_replay_mode = _is_replay_mode(cfg)
        actions_replay_window_sec = _get_actions_replay_window_sec(cfg, is_replay_mode)
        _cfg_lag_threshold = int(cfg.get("actions_lag_threshold_sec") or 180)
        # Scale lag threshold by account count: with N accounts in round-robin,
        # each account polls every ~N*poll_interval, so the effective lag floor
        # is much higher than in single-account V2.  Ensure the threshold is
        # at least N * poll_interval to avoid perpetual false-positive lag_high.
        _n_accounts = len(account_contexts)
        _poll_sec = int(cfg.get("poll_interval_sec") or 20)
        actions_lag_threshold_sec = max(_cfg_lag_threshold, _n_accounts * _poll_sec + _poll_sec)
        actions_unreliable_hold_sec = int(cfg.get("actions_unreliable_hold_sec") or 120)
        sell_confirm_max = int(cfg.get("sell_confirm_max") or 5)
        sell_confirm_window_sec = int(cfg.get("sell_confirm_window_sec") or 300)
        force_ratio_raw = cfg.get("sell_confirm_force_ratio")
        sell_confirm_force_ratio = 0.5 if force_ratio_raw is None else float(force_ratio_raw)
        force_shares_raw = cfg.get("sell_confirm_force_shares")
        sell_confirm_force_shares = 0.0 if force_shares_raw is None else float(force_shares_raw)
        reentry_cooldown_sec = max(0, int(cfg.get("reentry_cooldown_sec") or 0))
        reentry_force_buy_shares = max(0.0, float(cfg.get("reentry_force_buy_shares") or 0.0))
        reentry_force_buy_usd = max(0.0, float(cfg.get("reentry_force_buy_usd") or 0.0))
        topic_risk_overlay_enabled = bool(cfg.get("topic_risk_overlay_enabled", True))
        topic_risk_flip_window_sec = max(30, int(cfg.get("topic_risk_flip_window_sec") or 900))
        topic_risk_l1_threshold = float(cfg.get("topic_risk_l1_threshold") or 2.0)
        topic_risk_l2_threshold = float(cfg.get("topic_risk_l2_threshold") or 4.0)
        if topic_risk_l2_threshold <= topic_risk_l1_threshold:
            topic_risk_l2_threshold = topic_risk_l1_threshold + 0.5
        topic_risk_l2_freeze_sec = max(60, int(cfg.get("topic_risk_l2_freeze_sec") or 1800))
        topic_risk_l1_follow_mult = max(
            0.0, min(1.0, float(cfg.get("topic_risk_l1_follow_mult") or 0.5))
        )
        topic_risk_l1_deadband_mult = max(1.0, float(cfg.get("topic_risk_l1_deadband_mult") or 1.5))
        topic_risk_l1_reentry_mult = max(1.0, float(cfg.get("topic_risk_l1_reentry_mult") or 2.0))
        topic_risk_book_min_shares_cache: Dict[str, float] = {}
        must_exit_fresh_sell_window_sec = max(
            60, int(cfg.get("must_exit_fresh_sell_window_sec") or 1800)
        )
        must_exit_clear_on_buy_min_target_shares = max(
            0.0, float(cfg.get("must_exit_clear_on_buy_min_target_shares") or 1.0)
        )
        buy_actions_source_mode = str(cfg.get("buy_actions_source_mode") or "all").strip().lower()
        lag_ms = 0
        now_ms = int(now_ts * 1000)
        replay_from_ms = int(state.get("actions_replay_from_ms") or 0)
        if replay_from_ms > 0 and replay_from_ms != actions_cursor_ms:
            logger.info(
                "[ACTIONS] replay_from_ms=%s cursor_ms=%s",
                replay_from_ms,
                actions_cursor_ms,
            )
            actions_cursor_ms = replay_from_ms
        seen_actions_key = (
            "seen_trade_ids" if actions_source in ("trade", "trades") else "seen_action_ids"
        )
        sell_health_round = {"signals": 0, "actions": 0}
        sell_signal_count_by_token: Dict[str, int] = {}
        last_target_sell_action_ts_by_token = state.setdefault(
            "last_target_sell_action_ts_by_token", {}
        )
        last_exit_ts_by_token = state.setdefault("last_exit_ts_by_token", {})
        position_source = dict(cached_position_source)
        buy_source_filtered = 0

        def _record_action(
            token_id: str,
            side: str,
            size: float,
            action_ms: int = 0,
            source_target: str = "",
        ) -> None:
            if not token_id or size <= 0:
                return
            if side == "BUY":
                has_buy_by_token[token_id] = True
                buy_sum_by_token[token_id] = buy_sum_by_token.get(token_id, 0.0) + size
            elif side == "SELL":
                has_sell_by_token[token_id] = True
                sell_sum_by_token[token_id] = sell_sum_by_token.get(token_id, 0.0) + size
                sell_signal_count_by_token[token_id] = int(
                    sell_signal_count_by_token.get(token_id) or 0
                ) + 1
                if action_ms > 0:
                    prev_ms = int(last_target_sell_action_ts_by_token.get(token_id) or 0)
                    if action_ms > prev_ms:
                        last_target_sell_action_ts_by_token[token_id] = int(action_ms)
                    prev_signal_ms = int(sell_signal_ms_by_token.get(token_id) or 0)
                    if action_ms > prev_signal_ms:
                        sell_signal_ms_by_token[token_id] = int(action_ms)
                src = str(source_target or "").strip().lower()
                if src:
                    token_sources = sell_signal_sources_by_token.setdefault(token_id, {})
                    token_sources[src] = max(int(token_sources.get(src) or 0), int(now_ts))

        # Use shared cached target actions (filtered by per-account cursor)
        actions_list = [dict(a) for a in cached_actions]
        actions_info = dict(cached_actions_info)
        if cached_actions_cursor_key == actions_cursor_key and actions_cursor_ms > 0:
            pre_filter_len = len(actions_list)
            actions_list = [a for a in actions_list if _action_ms(a) > actions_cursor_ms]
            if pre_filter_len != len(actions_list):
                logger.debug(
                    "[SHARED_CACHE] filtered actions for account %s: %s -> %s",
                    _shorten_address(acct_ctx.my_address),
                    pre_filter_len,
                    len(actions_list),
                )
        use_trades_api = actions_source in ("trade", "trades")
        seen_action_ids = state.setdefault(seen_actions_key, [])
        # Replay mode: keep seen ids stable across moving replay windows.
        # Clearing seen ids here will re-consume historical actions and can amplify positions.
        if replay_from_ms > 0:
            replay_reset_key = f"{seen_actions_key}_replay_reset_ms"
            if int(state.get(replay_reset_key) or 0) != replay_from_ms:
                logger.info(
                    "[ACTIONS] replay window moved key=%s keep_seen_ids=%s replay_from_ms=%s",
                    seen_actions_key,
                    len(seen_action_ids),
                    replay_from_ms,
                )
                state[replay_reset_key] = replay_from_ms
        seen_action_set = {str(item) for item in seen_action_ids}
        filtered_actions: list[Dict[str, object]] = []
        for action in actions_list:
            action_id = _action_identity(action)
            if action_id in seen_action_set:
                continue
            filtered_actions.append(action)
            seen_action_ids.append(action_id)
            seen_action_set.add(action_id)
        max_seen = int(cfg.get("seen_action_ids_cap") or 5000)
        if replay_from_ms > 0:
            # Replay mode needs a larger dedupe window to avoid evicting still-replayable ids.
            max_seen = max(max_seen, int(cfg.get("seen_action_ids_cap_replay") or 50000))
        if len(seen_action_ids) > max_seen:
            del seen_action_ids[:-max_seen]
        actions_list = filtered_actions

        miss_token = 0
        miss_samples: list[list[str]] = []
        for action in actions_list:
            side = str(action.get("side") or "").upper()
            size = float(action.get("size") or 0.0)
            action_ms = _action_ms(action)
            source_target = str(action.get("_source_target") or "").strip().lower()

            token_id = action.get("token_id") or _extract_token_id_from_raw(
                action.get("raw") or {}
            )
            if token_id:
                tid = str(token_id)
                if side == "BUY":
                    preferred_source = str(position_source.get(tid) or "").strip().lower()
                    if not _should_accept_buy_action_source(
                        buy_actions_source_mode,
                        source_target,
                        preferred_source,
                    ):
                        buy_source_filtered += 1
                        continue
                action["token_id"] = tid
                _record_action(tid, side, size, action_ms, source_target)
                if side == "BUY":
                    prev_action_ms = int(buy_signal_ms_by_token.get(tid) or 0)
                    if action_ms > 0 and action_ms >= prev_action_ms:
                        buy_signal_ms_by_token[tid] = int(action_ms)
                        if source_target:
                            buy_signal_source_by_token[tid] = source_target
                    elif source_target and tid not in buy_signal_source_by_token:
                        buy_signal_source_by_token[tid] = source_target
            else:
                miss_token += 1
                if len(miss_samples) < 3:
                    raw = action.get("raw") or {}
                    if isinstance(raw, dict):
                        miss_samples.append(sorted(list(raw.keys()))[:25])

        if actions_list:
            actions_missing_ratio = miss_token / len(actions_list)
        if miss_token:
            logger.warning(
                "[ACT] actions_total=%s token_mapped=%s missing=%s sample_raw_keys=%s",
                len(actions_list),
                len(actions_list) - miss_token,
                miss_token,
                miss_samples,
            )
            logger.warning(
                "[ACT] token_missing_ratio=%.3f",
                actions_missing_ratio,
            )
        if buy_source_filtered > 0:
            logger.info(
                "[ACT] buy_source_filtered=%s mode=%s",
                buy_source_filtered,
                buy_actions_source_mode,
            )
        latest_action_ms = int(actions_info.get("latest_ms") or 0)
        actions_ok = bool(actions_info.get("ok"))
        actions_incomplete = bool(actions_info.get("incomplete"))
        actions_unreliable = (not actions_ok) or actions_incomplete
        if actions_unreliable:
            state["actions_unreliable_until"] = now_ts + actions_unreliable_hold_sec
            state["actions_replay_from_ms"] = max(
                0, now_ms - actions_replay_window_sec * 1000
            )
            logger.warning(
                "[ACTIONS] unreliable ok=%s incomplete=%s keep_cursor_ms=%s replay_from_ms=%s",
                actions_ok,
                actions_incomplete,
                actions_cursor_ms,
                state["actions_replay_from_ms"],
            )
        else:
            state.pop("actions_unreliable_until", None)
            if latest_action_ms > actions_cursor_ms:
                state[actions_cursor_key] = latest_action_ms
            if replay_from_ms > 0 and latest_action_ms >= actions_cursor_ms:
                state.pop("actions_replay_from_ms", None)
            # When target is idle (no actions returned, latest=0),
            # clear stale replay_from_ms to stop the perpetual
            # replay loop that wastes API calls every cycle.
            if replay_from_ms > 0 and latest_action_ms == 0 and not actions_list:
                state.pop("actions_replay_from_ms", None)
            lag_ms = now_ms - latest_action_ms if latest_action_ms > 0 else 0
            if lag_ms > actions_lag_threshold_sec * 1000:
                lag_replay_window_sec = int(
                    cfg.get("lag_replay_window_sec")
                    or (actions_replay_window_sec if is_replay_mode else min(actions_replay_window_sec, 120))
                )
                lag_replay_cooldown_sec = int(cfg.get("lag_replay_cooldown_sec") or 120)
                last_lag_replay_ts = int(state.get("last_lag_replay_ts") or 0)
                if now_ts - last_lag_replay_ts >= max(0, lag_replay_cooldown_sec):
                    state["actions_replay_from_ms"] = max(
                        0, now_ms - max(1, lag_replay_window_sec) * 1000
                    )
                    state["last_lag_replay_ts"] = now_ts
                    logger.warning(
                        "[ACTIONS] lag_ms=%s replay_from_ms=%s latest_ms=%s win_sec=%s cooldown_sec=%s",
                        lag_ms,
                        state["actions_replay_from_ms"],
                        latest_action_ms,
                        lag_replay_window_sec,
                        lag_replay_cooldown_sec,
                    )
                else:
                    logger.info(
                        "[ACTIONS] lag_ms=%s replay_suppressed latest_ms=%s cooldown_remain=%s",
                        lag_ms,
                        latest_action_ms,
                        max(0, lag_replay_cooldown_sec - (now_ts - last_lag_replay_ts)),
                    )

        my_trades_unreliable_hold_sec = int(cfg.get("my_trades_unreliable_hold_sec") or 0)
        if my_trades_unreliable_hold_sec <= 0:
            my_trades_unreliable_hold_sec = actions_unreliable_hold_sec
        try:
            my_trades_cursor_ms = int(state.get("my_trades_cursor_ms") or 0)
            my_trades, my_trades_info = fetch_target_trades_since(
                data_client,
                current_my_address,
                my_trades_cursor_ms,
                page_size=actions_page_size,
                max_offset=actions_max_offset,
            )
            seen_my_trade_ids = state.setdefault("seen_my_trade_ids", [])
            seen_my_trade_set = {str(item) for item in seen_my_trade_ids}
            filtered_my_trades: list[Dict[str, object]] = []
            for trade in my_trades:
                trade_id = _action_identity(trade)
                if trade_id in seen_my_trade_set:
                    continue
                filtered_my_trades.append(trade)
                seen_my_trade_ids.append(trade_id)
                seen_my_trade_set.add(trade_id)
            max_seen = int(cfg.get("seen_action_ids_cap") or 5000)
            if len(seen_my_trade_ids) > max_seen:
                del seen_my_trade_ids[:-max_seen]
            my_trades = filtered_my_trades

            miss_trade_token = 0
            miss_trade_samples: list[list[str]] = []
            for trade in my_trades:
                side = str(trade.get("side") or "").upper()
                if side != "BUY":
                    continue
                token_key = trade.get("token_key")
                token_id = trade.get("token_id") or _extract_token_id_from_raw(
                    trade.get("raw") or {}
                )
                if not token_id:
                    miss_trade_token += 1
                    if token_key:
                        unresolved_trade_candidates.append(
                            {
                                "token_key": token_key,
                                "condition_id": trade.get("condition_id"),
                                "outcome_index": trade.get("outcome_index"),
                                "slug": None,
                                "raw": trade.get("raw") or {},
                            }
                        )
                    if len(miss_trade_samples) < 3:
                        raw = trade.get("raw") or {}
                        if isinstance(raw, dict):
                            miss_trade_samples.append(sorted(list(raw.keys()))[:25])
            if miss_trade_token:
                miss_trade_ratio = (
                    miss_trade_token / len(my_trades) if my_trades else 0.0
                )
                logger.warning(
                    "[MY_TRADES] token_missing=%s total=%s ratio=%.3f sample_raw_keys=%s",
                    miss_trade_token,
                    len(my_trades),
                    miss_trade_ratio,
                    miss_trade_samples,
                )
            trades_ok = bool(my_trades_info.get("ok", True))
            trades_incomplete = bool(my_trades_info.get("incomplete", False))
            if not trades_ok or trades_incomplete:
                state["my_trades_unreliable_until"] = now_ts + my_trades_unreliable_hold_sec
                logger.warning(
                    "[MY_TRADES] unreliable ok=%s incomplete=%s hold_sec=%s",
                    trades_ok,
                    trades_incomplete,
                    my_trades_unreliable_hold_sec,
                )
            else:
                state["my_trades_unreliable_until"] = 0
            latest_trade_ms = int(my_trades_info.get("latest_ms") or 0)
            if latest_trade_ms > my_trades_cursor_ms:
                state["my_trades_cursor_ms"] = latest_trade_ms
        except Exception as exc:
            state["my_trades_unreliable_until"] = now_ts + my_trades_unreliable_hold_sec
            logger.exception("[ERR] fetch my trades failed: %s", exc)

        has_new_actions = bool(actions_list)
        if has_new_actions:
            state["target_positions_nonce_actions"] = int(
                state.get("target_positions_nonce_actions") or 0
            ) + len(actions_list)
        nonce_min_interval_sec = max(1, min(target_positions_refresh_sec, poll_interval))
        nonce_action_window = max(
            1, int(max(target_positions_refresh_sec, poll_interval) / max(poll_interval, 1))
        )
        last_nonce_ts = float(state.get("target_positions_nonce_last_ts") or 0)
        allow_nonce = (
            has_new_actions
            and (now_ts - last_nonce_ts) >= nonce_min_interval_sec
            and int(state.get("target_positions_nonce_actions") or 0) >= nonce_action_window
        )
        if allow_nonce:
            target_cache_mode = "nonce"
            state["target_positions_nonce_last_ts"] = now_ts
            state["target_positions_nonce_actions"] = 0
        else:
            target_cache_mode = target_cache_bust_mode

        # Use shared cached target positions
        target_pos = [dict(p) for p in cached_positions]
        target_info = dict(cached_target_info)
        position_source = dict(cached_position_source)
        hard_cap = positions_limit * positions_max_pages
        if len(target_pos) >= hard_cap:
            target_info["incomplete"] = True
            logger.info("[SAFE] target positions 鍙兘鎴柇(len>=hard_cap=%s), 璺宠繃鏈疆", hard_cap)

        my_pos, my_info = fetch_positions_norm(
            data_client,
            current_my_address,
            0.0,
            positions_limit=positions_limit,
            positions_max_pages=positions_max_pages,
            refresh_sec=target_positions_refresh_sec if my_positions_force_http else None,
            force_http=my_positions_force_http,
            cache_bust_mode=target_cache_bust_mode,
            header_keys=header_keys,
        )
        # Verify positions belong to the expected profile/proxy wallet.
        proxy_wallets = set()
        for pos in my_pos:
            raw = pos.get("raw") or {}
            if isinstance(raw, dict):
                proxy_wallet = raw.get("proxyWallet") or raw.get("proxy_wallet")
                if proxy_wallet:
                    proxy_wallets.add(str(proxy_wallet).lower())
        if proxy_wallets:
            my_addr_l = str(current_my_address or "").lower()
            if my_addr_l and my_addr_l not in proxy_wallets:
                my_info["proxy_mismatch"] = True
                my_info["proxy_wallets"] = sorted(proxy_wallets)
                logger.warning(
                    "[WARN] my_positions proxy_wallet mismatch my=%s proxy_wallets=%s -> ignore positions",
                    current_my_address,
                    my_info["proxy_wallets"],
                )
                my_pos = []
        if len(my_pos) >= hard_cap:
            my_info["incomplete"] = True
            logger.info("[SAFE] my positions 鍙兘鎴柇(len>=hard_cap=%s), 璺宠繃鏈疆", hard_cap)

        closed_token_keys = state.get("closed_token_keys")
        if not isinstance(closed_token_keys, dict):
            closed_token_keys = {}
            state["closed_token_keys"] = closed_token_keys
        new_closed = 0
        for pos in target_pos + my_pos:
            token_key = pos.get("token_key")
            if not token_key or token_key in closed_token_keys:
                continue
            closed, end_ts = _is_closed_by_end_date(pos, now_ts)
            if closed:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[SKIP_DEBUG] closed_by_end_date token_key=%s token_id=%s slug=%s end_date=%s end_ts=%s now_ts=%s",
                        token_key,
                        pos.get("token_id"),
                        pos.get("slug"),
                        pos.get("end_date") or pos.get("endDate"),
                        end_ts,
                        now_ts,
                    )
                closed_token_keys[str(token_key)] = int(end_ts or now_ts)
                new_closed += 1
        if new_closed:
            logger.info("[SKIP] closed_token_keys added count=%s", new_closed)

        # Always keep an unfiltered snapshot of my positions for position/risk recognition.
        # Trading-side filtering must not rewrite the account's actual holdings.
        my_pos_for_risk_snapshot = list(my_pos)

        if closed_token_keys:
            target_pos, removed_target = _filter_closed_positions(target_pos, closed_token_keys)
            if removed_target:
                logger.info(
                    "[SKIP] closed_positions filtered target=%s my=%s (my positions kept unfiltered for risk/holdings)",
                    removed_target,
                    0,
                )

        should_log_heartbeat = has_new_actions or (
            now_ts - last_heartbeat_ts >= heartbeat_interval_sec
        )
        if should_log_heartbeat:
            logger.info(
                "[POS] target_count=%s my_count=%s target_incomplete=%s my_incomplete=%s | "
                "t_src=%s t_cb=%s t_rsec=%s t_mode=%s t_http=%s t_hit=%s",
                len(target_pos),
                len(my_pos),
                bool(target_info.get("incomplete")),
                bool(my_info.get("incomplete")),
                target_info.get("source"),
                target_info.get("cache_bucket"),
                target_info.get("refresh_sec"),
                target_info.get("cache_bust_mode"),
                target_info.get("http_status"),
                target_info.get("cache_hit_hint"),
            )
            if target_info.get("incomplete"):
                logger.info(
                    "[POS] target positions info limit=%s total=%s max_pages=%s",
                    target_info.get("limit"),
                    target_info.get("total"),
                    target_info.get("max_pages"),
                )
            if log_cache_headers:
                logger.info(
                    "[POS] target cache_headers_first=%s",
                    target_info.get("cache_headers_first"),
                )
                logger.info(
                    "[POS] target cache_headers_last=%s",
                    target_info.get("cache_headers_last"),
                )
            last_heartbeat_ts = now_ts

        if not target_info.get("ok") or target_info.get("incomplete"):
            logger.warning("[SAFE] target positions incomplete; skipping this loop")
            save_state(args.state, state)
            current_account_idx = (current_account_idx + 1) % len(account_contexts)
            time.sleep(_get_poll_interval())
            continue

        if not my_info.get("ok") or my_info.get("incomplete"):
            logger.warning("[SAFE] my positions incomplete; skipping this loop")
            save_state(args.state, state)
            current_account_idx = (current_account_idx + 1) % len(account_contexts)
            time.sleep(_get_poll_interval())
            continue

        boot_sync_mode = str(cfg.get("boot_sync_mode") or "baseline_only").lower()
        fresh_boot = bool(cfg.get("fresh_boot_on_start", False))
        boot_run_start_ms = int(state.get("boot_run_start_ms") or 0)
        run_start_ms = int(state.get("run_start_ms") or 0)
        boot_needed = boot_sync_mode in (
            "baseline_only",
            "baseline_replay",
            "replay_24h",
            "replay_actions",
            "replay",
        ) and (
            (not state.get("bootstrapped"))
            or (
                fresh_boot
                and boot_run_start_ms != run_start_ms
            )
            or (
                boot_sync_mode in _REPLAY_BOOT_MODES
                and boot_run_start_ms != run_start_ms
            )
        )
        if boot_needed:
            # In multi-account mode, seed late-joining account with baseline tracking
            # state from an already-bootstrapped peer so it doesn't miss active tokens.
            for peer_ctx in account_contexts:
                peer_state = peer_ctx.state
                if peer_state is state:
                    continue
                if peer_state.get("bootstrapped"):
                    peer_token_map = peer_state.get("token_map")
                    if isinstance(peer_token_map, dict):
                        token_map = state.get("token_map", {})
                        if not isinstance(token_map, dict):
                            token_map = {}
                        for tk, tid in peer_token_map.items():
                            if tk and tid and tk not in token_map:
                                token_map[tk] = str(tid)
                        state["token_map"] = token_map
                    for key in (
                        "target_last_shares",
                        "target_last_seen_ts",
                        "target_missing_streak",
                        "last_target_sell_action_ts_by_token",
                    ):
                        peer_val = peer_state.get(key)
                        if isinstance(peer_val, dict):
                            local_val = state.setdefault(key, {})
                            for tid, v in peer_val.items():
                                if tid not in local_val:
                                    local_val[tid] = v
                    peer_boot_by_key = peer_state.get("target_last_shares_by_token_key")
                    if isinstance(peer_boot_by_key, dict):
                        local_boot_by_key = state.setdefault("target_last_shares_by_token_key", {})
                        for tk, v in peer_boot_by_key.items():
                            if tk not in local_boot_by_key:
                                local_boot_by_key[tk] = v
                    peer_closed = peer_state.get("closed_token_keys")
                    if isinstance(peer_closed, dict):
                        local_closed = state.setdefault("closed_token_keys", {})
                        for tk, v in peer_closed.items():
                            if tk not in local_closed:
                                local_closed[tk] = v
                    logger.info(
                        "[BOOT] account seeded from peer=%s token_map=%s last_shares=%s",
                        peer_ctx.name,
                        len(state.get("token_map", {})),
                        len(state.get("target_last_shares", {})),
                    )
                    break

            boot_by_key: Dict[str, float] = {}
            boot_keys: list[str] = []
            token_map = (
                state.get("token_map", {}) if isinstance(state.get("token_map"), dict) else {}
            )
            state["token_map"] = token_map
            for pos in target_pos:
                token_key = str(pos.get("token_key") or "").strip()
                if not token_key:
                    continue
                raw_id = (
                    str(pos.get("token_id") or "").strip()
                    or _extract_token_id_from_raw(pos.get("raw") or {})
                )
                if raw_id:
                    token_map.setdefault(token_key, str(raw_id))
                size = float(pos.get("size") or 0.0)
                boot_by_key[token_key] = size
                boot_keys.append(token_key)
            boot_keys = sorted(set(boot_keys))
            state["boot_token_keys"] = boot_keys
            state["target_last_shares_by_token_key"] = boot_by_key

            boot_token_ids: list[str] = []
            for token_key in boot_keys:
                token_id = token_map.get(token_key)
                if token_id:
                    boot_token_ids.append(token_id)
                    state.setdefault("target_last_shares", {})[token_id] = float(
                        boot_by_key.get(token_key) or 0.0
                    )
                    state.setdefault("target_last_seen_ts", {})[token_id] = now_ts
                    state.setdefault("target_missing_streak", {})[token_id] = 0
            state["boot_token_ids"] = sorted(set(boot_token_ids))

            run_start_ms = int(state.get("run_start_ms") or 0)
            if boot_sync_mode == "baseline_only":
                state["target_actions_cursor_ms"] = run_start_ms
                state["target_trades_cursor_ms"] = run_start_ms
            else:
                replay_window_sec = _get_actions_replay_window_sec(cfg, True)
                replay_from_ms = max(0, run_start_ms - replay_window_sec * 1000)
                state["actions_replay_from_ms"] = replay_from_ms
                state["target_actions_cursor_ms"] = replay_from_ms
                state["target_trades_cursor_ms"] = replay_from_ms
            state["seen_action_ids"] = []
            state["topic_state"] = {}
            state["probed_token_ids"] = []
            state["boot_run_start_ms"] = int(state.get("run_start_ms") or 0)
            # 娓呯悊鏃犲疄闄呮寔浠撶殑 accumulator 姝讳粨
            acc = state.get("buy_notional_accumulator")
            if acc:
                my_ids = {
                    str(p.get("token_id") or p.get("asset_id") or "").strip()
                    for p in my_pos
                }
                for tid in list(acc.keys()):
                    if tid not in my_ids:
                        acc.pop(tid, None)
            state["bootstrapped"] = True
            logger.info(
                "[BOOT] %s: baseline_keys=%s baseline_ids=%s cursor_ms=%s replay_from_ms=%s",
                boot_sync_mode,
                len(boot_keys),
                len(state["boot_token_ids"]),
                state["target_actions_cursor_ms"],
                state.get("actions_replay_from_ms", 0),
            )
            save_state(args.state, state)
            current_account_idx = (current_account_idx + 1) % len(account_contexts)
            time.sleep(_get_poll_interval())
            continue

        token_map = state.get("token_map", {})
        if not isinstance(token_map, dict):
            token_map = {}
            state["token_map"] = token_map

        token_key_by_token_id: Dict[str, str] = {
            str(token_id): str(token_key) for token_key, token_id in token_map.items()
        }

        # Build target shares maps without doing full gamma resolver (avoid freezing on huge accounts).
        # Fast-path: use cached token_map or token_id embedded in pos["raw"].
        target_shares_now_by_token_id: Dict[str, float] = {}
        target_shares_now_by_token_key: Dict[str, float] = {}
        unresolved_target = 0
        resolved_by_cache = 0
        resolved_by_raw = 0
        resolved_by_resolver = 0
        resolver_fail = 0
        resolve_target_budget = max(0, int(max_resolve_target_positions_per_loop or 0))
        for pos in target_pos:
            token_key = str(pos.get("token_key") or "")
            if not token_key:
                continue
            size = float(pos.get("size") or 0.0)
            target_shares_now_by_token_key[token_key] = size

            token_id = token_map.get(token_key)
            if token_id:
                resolved_by_cache += 1
            else:
                token_id = _extract_token_id_from_raw(pos.get("raw") or {})
                if token_id:
                    token_map[token_key] = str(token_id)
                    resolved_by_raw += 1
                elif resolve_target_budget > 0:
                    resolve_target_budget -= 1
                    fail_ts = resolver_fail_cache.get(token_key)
                    if (
                        fail_ts
                        and resolver_fail_cooldown_sec > 0
                        and now_ts - int(fail_ts or 0) < resolver_fail_cooldown_sec
                    ):
                        unresolved_target += 1
                        continue
                    try:
                        token_id = resolve_token_id(token_key, pos, token_map)
                    except Exception as exc:
                        resolver_fail += 1
                        logger.warning("[WARN] resolver failed (target): %s -> %s", token_key, exc)
                        resolver_fail_cache[token_key] = now_ts
                        unresolved_target += 1
                        continue
                    token_map[token_key] = str(token_id)
                    resolved_by_resolver += 1
                else:
                    unresolved_target += 1
                    continue

            tid = str(token_id)
            target_shares_now_by_token_id[tid] = size
            token_key_by_token_id[tid] = token_key
            cur_price = float(pos.get("cur_price") or 0.0)
            if cur_price > 0:
                state.setdefault("last_mid_price_by_token_id", {})[tid] = cur_price
                state["last_mid_price_update_ts"] = now_ts

        if unresolved_target:
            logger.info(
                "[POSMAP] target idmap cache=%d raw=%d resolver=%d pending=%d total=%d",
                resolved_by_cache,
                resolved_by_raw,
                resolved_by_resolver,
                unresolved_target,
                len(target_pos),
            )

        # Build holdings map from unfiltered snapshot so my_shares reflects real positions.
        my_by_token_id: Dict[str, float] = {}
        for pos in my_pos_for_risk_snapshot:
            token_key = str(pos.get("token_key") or "")
            size = float(pos.get("size") or 0.0)

            token_id = pos.get("token_id") or None
            if token_key:
                token_id = token_id or token_map.get(token_key)
            token_id = token_id or _extract_token_id_from_raw(pos.get("raw") or {})
            if not token_id and token_key:
                fail_ts = resolver_fail_cache.get(token_key)
                if (
                    fail_ts
                    and resolver_fail_cooldown_sec > 0
                    and now_ts - int(fail_ts or 0) < resolver_fail_cooldown_sec
                ):
                    continue
                try:
                    token_id = resolve_token_id(token_key, pos, token_map)
                except Exception as exc:
                    logger.warning("[WARN] resolver failed (self): %s -> %s", token_key, exc)
                    resolver_fail_cache[token_key] = now_ts
                    continue
            if not token_id:
                continue

            tid = str(token_id)
            if token_key:
                token_map[token_key] = tid
                token_key_by_token_id.setdefault(tid, token_key)
            cur_price = float(pos.get("cur_price") or 0.0)
            if cur_price > 0:
                state.setdefault("last_mid_price_by_token_id", {})[tid] = cur_price
                state["last_mid_price_update_ts"] = now_ts
            my_by_token_id[tid] = size
            if size > 0:
                last_nonzero_my_shares[tid] = {"shares": float(size), "ts": int(now_ts)}

        resolve_budget = int(cfg.get("max_resolve_actions_per_loop") or 20)
        missing_ratio_threshold = float(cfg.get("resolve_actions_missing_ratio") or 0.3)
        if actions_list and actions_missing_ratio >= missing_ratio_threshold:
            boosted = int(cfg.get("max_resolve_actions_on_missing") or 60)
            if boosted > resolve_budget:
                logger.warning(
                    "[ACT] missing_ratio=%.3f boosting resolver budget %s->%s",
                    actions_missing_ratio,
                    resolve_budget,
                    boosted,
                )
                resolve_budget = boosted
        for action in actions_list:
            token_id = action.get("token_id")
            token_key = action.get("token_key")
            source_target = str(action.get("_source_target") or "").strip().lower()
            if token_id:
                token_key_by_token_id.setdefault(str(token_id), str(token_key or ""))
                side = str(action.get("side") or "").upper()
                if side == "BUY":
                    preferred_source = str(position_source.get(str(token_id)) or "").strip().lower()
                    if not _should_accept_buy_action_source(
                        buy_actions_source_mode,
                        source_target,
                        preferred_source,
                    ):
                        buy_source_filtered += 1
                        continue
                continue
            if not token_key:
                continue
            token_id = token_map.get(str(token_key)) or _extract_token_id_from_raw(action.get("raw") or {})
            if token_id:
                tid = str(token_id)
                token_map[str(token_key)] = tid
                token_key_by_token_id.setdefault(tid, str(token_key))
                side = str(action.get("side") or "").upper()
                if side == "BUY":
                    preferred_source = str(position_source.get(tid) or "").strip().lower()
                    if not _should_accept_buy_action_source(
                        buy_actions_source_mode,
                        source_target,
                        preferred_source,
                    ):
                        buy_source_filtered += 1
                        continue
                size = float(action.get("size") or 0.0)
                _record_action(tid, side, size)
                continue
            if resolve_budget <= 0:
                continue
            resolve_budget -= 1
            fail_ts = resolver_fail_cache.get(str(token_key))
            if (
                fail_ts
                and resolver_fail_cooldown_sec > 0
                and now_ts - int(fail_ts or 0) < resolver_fail_cooldown_sec
            ):
                continue
            try:
                token_id = resolve_token_id(
                    token_key,
                    {
                        "token_key": token_key,
                        "condition_id": action.get("condition_id"),
                        "outcome_index": action.get("outcome_index"),
                        "slug": None,
                        "raw": action.get("raw") or {},
                    },
                    token_map,
                )
            except Exception as exc:
                logger.warning("[WARN] resolver failed (actions): %s -> %s", token_key, exc)
                resolver_fail_cache[str(token_key)] = now_ts
                continue
            side = str(action.get("side") or "").upper()
            size = float(action.get("size") or 0.0)
            tid = str(token_id)
            if side == "BUY":
                preferred_source = str(position_source.get(tid) or "").strip().lower()
                if not _should_accept_buy_action_source(
                    buy_actions_source_mode,
                    source_target,
                    preferred_source,
                ):
                    buy_source_filtered += 1
                    continue
            token_map[str(token_key)] = tid
            _record_action(tid, side, size)
            token_key_by_token_id.setdefault(tid, str(token_key))

        resolve_trade_budget = int(cfg.get("max_resolve_trades_per_loop") or 10)
        if unresolved_trade_candidates and resolve_trade_budget > 0:
            logger.warning(
                "[MY_TRADES] unresolved_trades=%s resolve_budget=%s",
                len(unresolved_trade_candidates),
                resolve_trade_budget,
            )
            for trade in unresolved_trade_candidates:
                if resolve_trade_budget <= 0:
                    break
                token_key = str(trade.get("token_key") or "")
                if not token_key:
                    continue
                if token_map.get(token_key):
                    continue
                resolve_trade_budget -= 1
                fail_ts = resolver_fail_cache.get(token_key)
                if (
                    fail_ts
                    and resolver_fail_cooldown_sec > 0
                    and now_ts - int(fail_ts or 0) < resolver_fail_cooldown_sec
                ):
                    continue
                try:
                    token_id = resolve_token_id(token_key, trade, token_map)
                except Exception as exc:
                    logger.warning("[WARN] resolver failed (trades): %s -> %s", token_key, exc)
                    resolver_fail_cache[token_key] = now_ts
                    continue
                tid = str(token_id)
                token_map[token_key] = tid
                token_key_by_token_id.setdefault(tid, token_key)

        reconcile_set: Set[str] = set(target_shares_now_by_token_id)
        reconcile_set.update(state.get("target_last_shares", {}).keys())
        reconcile_set.update(my_by_token_id)
        reconcile_set.update(state.get("open_orders", {}).keys())
        reconcile_set.update(set(has_buy_by_token.keys()) | set(has_sell_by_token.keys()))
        reconcile_set.update(state.get("topic_state", {}).keys())
        reconcile_set.update(str(tid) for tid in must_exit_tokens.keys())
        lag_high = lag_ms > actions_lag_threshold_sec * 1000
        actions_unreliable_until = int(state.get("actions_unreliable_until") or 0)
        actions_unreliable = actions_unreliable_until > now_ts
        reduce_reconcile = ((not actions_list) and (not actions_unreliable)) or lag_high
        if reduce_reconcile:
            reason = "lag_high" if lag_high else "actions_empty"
            # When no new actions, only reconcile tokens with meaningful state differences
            reduced_set: Set[str] = set()
            for tid in reconcile_set:
                my_shares_t = my_by_token_id.get(tid, 0.0)
                target_shares_t = target_shares_now_by_token_id.get(tid, 0.0)
                has_orders = bool(state.get("open_orders", {}).get(tid))
                in_topic = tid in state.get("topic_state", {})
                has_buy = tid in has_buy_by_token
                has_sell = tid in has_sell_by_token
                must_exit = tid in must_exit_tokens
                if (
                    has_orders
                    or in_topic
                    or my_shares_t > 0
                    or abs(my_shares_t - target_shares_t) > 0.01
                    or has_buy
                    or has_sell
                    or must_exit
                ):
                    reduced_set.add(tid)
            if len(reduced_set) < len(reconcile_set):
                logger.info(
                    "[SAFE] %s reduce_reconcile: %s -> %s tokens",
                    reason,
                    len(reconcile_set),
                    len(reduced_set),
                )
                reconcile_set = reduced_set
            else:
                logger.info(
                    "[SAFE] %s reduce_reconcile disabled: keep full token set (actions=%s actions_unreliable=%s lag_ms=%s)",
                    reason,
                    len(actions_list),
                    actions_unreliable,
                    lag_ms,
                )

        ignored = state["ignored_tokens"]
        expired_ignored = [
            token_id
            for token_id, meta in ignored.items()
            if isinstance(meta, dict)
            and meta.get("expires_at")
            and now_ts >= int(meta.get("expires_at") or 0)
        ]
        for token_id in expired_ignored:
            ignored.pop(token_id, None)

        def _collect_active_ignored() -> Set[str]:
            return {
                token_id
                for token_id, meta in ignored.items()
                if isinstance(meta, dict)
                and meta.get("expires_at")
                and now_ts < int(meta.get("expires_at") or 0)
            }

        active_ignored = _collect_active_ignored()
        if active_ignored:
            for token_id in sorted(active_ignored):
                meta = ignored.get(token_id)
                if not isinstance(meta, dict):
                    continue
                if meta.get("active_logged"):
                    continue
                logger.info(
                    "[SKIP] active_ignore token_id=%s expires_at=%s",
                    token_id,
                    int(meta.get("expires_at") or 0),
                )
                meta["active_logged"] = True
            reconcile_set = {
                token_id
                for token_id in reconcile_set
                if token_id not in active_ignored or token_id in must_exit_tokens
            }
        status_cache = state["market_status_cache"]
        if skip_closed:
            def _ensure_long_ignore(token_id: str, meta: Optional[Dict[str, Any]]) -> None:
                end_date = None
                if isinstance(meta, dict):
                    end_date = meta.get("end_date") or meta.get("endDate")
                end_ts = _parse_market_end_ts(meta)
                min_ttl_sec = int(cfg.get("closed_ignore_min_ttl_sec") or 24 * 3600)
                if min_ttl_sec < 300:
                    min_ttl_sec = 300
                expires_at = now_ts + min_ttl_sec
                if end_ts is not None:
                    expires_at = max(expires_at, int(end_ts))
                existing = ignored.get(token_id) if isinstance(ignored.get(token_id), dict) else {}
                existing_expires = int(existing.get("expires_at") or 0)
                if existing_expires > expires_at:
                    expires_at = existing_expires
                should_log = not existing or not existing.get("logged")
                ignored[token_id] = {
                    "ts": now_ts,
                    "reason": "closed_or_not_tradeable",
                    "expires_at": int(expires_at),
                    "end_date": end_date,
                    "logged": bool(existing.get("logged")),
                }
                if should_log:
                    logger.info(
                        "[SKIP] long_ignore token_id=%s expires_at=%s end_date=%s",
                        token_id,
                        int(expires_at),
                        end_date,
                    )
                    ignored[token_id]["logged"] = True

            need_query = []
            for token_id in reconcile_set:
                if token_id in ignored:
                    continue
                cached = status_cache.get(token_id)
                if not cached or now_ts - int(cached.get("ts") or 0) >= refresh_sec:
                    need_query.append(token_id)

            if need_query:
                meta_map = gamma_fetch_markets_by_clob_token_ids(need_query)
                for token_id in need_query:
                    meta = meta_map.get(token_id)
                    tradeable = market_tradeable_state(meta)
                    status_cache[token_id] = {"ts": now_ts, "tradeable": tradeable, "meta": meta}
                    if tradeable is False:
                        _ensure_long_ignore(token_id, meta)

            for token_id in reconcile_set:
                cached = status_cache.get(token_id) or {}
                if cached.get("tradeable") is False:
                    _ensure_long_ignore(token_id, cached.get("meta"))

            # Recompute active ignored after new long_ignore entries were added in this loop.
            # This prevents closed tokens from leaking into the current risk baseline.
            active_ignored = _collect_active_ignored()
            if active_ignored:
                reconcile_set = {token_id for token_id in reconcile_set if token_id not in active_ignored}

        # Build risk baseline from unfiltered holdings snapshot.
        # Exclude tokens that are known inactive/closed to avoid inflated risk baseline.
        my_pos_for_risk = my_pos_for_risk_snapshot
        inactive_for_risk = set(active_ignored) if skip_closed else set()
        my_by_token_id_for_risk: Dict[str, float] = {}
        for pos in my_pos_for_risk:
            token_key = str(pos.get("token_key") or "")
            size = float(pos.get("size") or 0.0)
            token_id = pos.get("token_id") or None
            if token_key:
                token_id = token_id or token_map.get(token_key)
            token_id = token_id or _extract_token_id_from_raw(pos.get("raw") or {})

            # CRITICAL FIX: Add resolver fallback for risk baseline (same as my_by_token_id)
            # This prevents position limit breaches when API returns positions without token_id
            if not token_id and token_key:
                fail_ts = resolver_fail_cache.get(token_key)
                if (
                    fail_ts
                    and resolver_fail_cooldown_sec > 0
                    and now_ts - int(fail_ts or 0) < resolver_fail_cooldown_sec
                ):
                    logger.warning(
                        "[RISK] skip position due to recent resolver fail: %s (cooldown)",
                        token_key,
                    )
                    continue
                try:
                    token_id = resolve_token_id(token_key, pos, token_map)
                    logger.debug("[RISK] resolved token_id via resolver: %s -> %s", token_key, token_id)
                except Exception as exc:
                    logger.warning("[RISK] resolver fail for position: %s -> %s", token_key, exc)
                    resolver_fail_cache[token_key] = now_ts
                    continue

            if not token_id:
                logger.warning(
                    "[RISK] skip position: missing token_id token_key=%s size=%.2f",
                    token_key,
                    size,
                )
                continue
            tid = str(token_id)
            if tid in inactive_for_risk:
                continue
            my_by_token_id_for_risk[tid] = size

        orderbooks: Dict[str, Dict[str, Optional[float]]] = {}

        mode = str(cfg.get("order_size_mode") or "fixed_shares").lower()
        min_usd = float(cfg.get("min_order_usd") or 5.0)
        max_usd = float(cfg.get("max_order_usd") or 25.0)
        target_mid_usd = (min_usd + max_usd) / 2.0
        max_position_usd_per_token = float(cfg.get("max_position_usd_per_token") or 0.0)
        max_notional_per_token = float(cfg.get("max_notional_per_token") or 0.0)
        max_notional_total = float(cfg.get("max_notional_total") or 0.0)
        buy_window_sec = int(cfg.get("buy_window_sec") or 0)
        buy_window_max_usd_per_token = float(cfg.get("buy_window_max_usd_per_token") or 0.0)
        buy_window_max_usd_total = float(cfg.get("buy_window_max_usd_total") or 0.0)
        fallback_mid_price = float(cfg.get("missing_mid_fallback_price") or 1.0)
        missing_meaningful_min_usd = float(
            cfg.get("missing_meaningful_min_usd")
            or max(1.0, float(cfg.get("min_order_usd") or 1.0))
        )
        if missing_meaningful_min_usd < 0:
            missing_meaningful_min_usd = 0.0
        cooldown_sec = int(cfg.get("cooldown_sec_per_token") or 0)
        shadow_ttl_sec = int(cfg.get("shadow_buy_ttl_sec") or 120)
        missing_timeout_sec = int(cfg.get("missing_timeout_sec") or 0)
        missing_freeze_streak = int(cfg.get("missing_freeze_streak") or 5)
        missing_freeze_min_sec = int(cfg.get("missing_freeze_min_sec") or 600)
        missing_freeze_max_sec = int(cfg.get("missing_freeze_max_sec") or 1800)
        if missing_freeze_min_sec > missing_freeze_max_sec:
            missing_freeze_min_sec, missing_freeze_max_sec = (
                missing_freeze_max_sec,
                missing_freeze_min_sec,
            )
        # Keep renewal deterministic to reduce oscillation noise while preserving freeze semantics.
        missing_freeze_renew_sec = int(
            cfg.get("missing_freeze_renew_sec") or max(missing_freeze_min_sec, missing_freeze_max_sec)
        )
        if missing_freeze_renew_sec < missing_freeze_min_sec:
            missing_freeze_renew_sec = missing_freeze_min_sec
        missing_to_zero_rounds = int(cfg.get("missing_to_zero_rounds") or 0)
        orphan_cancel_rounds = int(cfg.get("orphan_cancel_rounds") or 3)
        orphan_ignore_sec = int(cfg.get("orphan_ignore_sec") or 120)
        debug_token_ids = {str(token_id) for token_id in (cfg.get("debug_token_ids") or [])}
        eps = float(cfg.get("delta_eps") or 1e-9)
        topic_mode = bool(cfg.get("topic_cycle_mode", True))
        entry_settle_sec = int(cfg.get("topic_entry_settle_sec", 60))
        online_sell_recover_window_sec = max(
            60, int(cfg.get("online_sell_recover_window_sec") or 21600)
        )
        online_sell_recover_grace_sec = max(
            0, int(cfg.get("online_sell_recover_grace_sec") or max(30, _poll_sec))
        )
        if isinstance(last_target_sell_action_ts_by_token, dict):
            _sell_cutoff_ms = now_ms - online_sell_recover_window_sec * 1000
            for _tid, _ts in list(last_target_sell_action_ts_by_token.items()):
                try:
                    if int(_ts or 0) < _sell_cutoff_ms:
                        last_target_sell_action_ts_by_token.pop(_tid, None)
                except Exception:
                    last_target_sell_action_ts_by_token.pop(_tid, None)

        ema = state.get("sizing", {}).get("ema_delta_usd")
        if ema is None or ema <= 0:
            ema = target_mid_usd * 3.0

        k = target_mid_usd / max(ema, 1e-9)
        k = max(0.002, min(1.2, k))

        cfg["_auto_order_k"] = k

        delta_usd_samples = []

        # CRITICAL FIX: Use unfiltered positions for risk calculation
        # This prevents position limit breaches from weakened risk baseline
        (
            planned_total_notional,
            planned_by_token_usd,
            order_info_by_id,
            shadow_buy_usd,
        ) = _calc_planned_notional_with_fallback(
            my_by_token_id_for_risk,  # Use unfiltered positions
            state.get("open_orders", {}),
            state.get("last_mid_price_by_token_id", {}),
            max_position_usd_per_token,
            state,
            now_ts,
            shadow_ttl_sec,
            fallback_mid_price,
            logger,
            include_shadow=False,
        )
        (
            planned_total_notional_shadow,
            planned_by_token_usd_shadow,
            _shadow_order_info_by_id,
            _shadow_buy_usd,
        ) = _calc_planned_notional_with_fallback(
            my_by_token_id_for_risk,  # Use unfiltered positions
            state.get("open_orders", {}),
            state.get("last_mid_price_by_token_id", {}),
            max_position_usd_per_token,
            state,
            now_ts,
            shadow_ttl_sec,
            fallback_mid_price,
            logger,
            include_shadow=True,
        )
        open_buy_orders_usd = sum(float(info.get("usd") or 0.0) for info in order_info_by_id.values())
        recent_buy_total, recent_buy_by_token = _calc_recent_buy_notional(
            state,
            now_ts,
            buy_window_sec,
        )
        top_tokens = sorted(planned_by_token_usd.items(), key=lambda item: item[1], reverse=True)[:5]
        top_tokens_fmt = [
            f"{token_key_by_token_id.get(token_id, token_id)}={usd:.4f}" for token_id, usd in top_tokens
        ]
        if risk_summary_interval_sec <= 0 or now_ts - last_risk_summary_ts >= risk_summary_interval_sec:
            logger.info(
                "[RISK_SUMMARY] used_total=%s used_total_shadow=%s open_buy_orders_usd=%s shadow_buy_usd=%s "
                "recent_buy_usd=%s top_tokens=%s",
                planned_total_notional,
                planned_total_notional_shadow,
                open_buy_orders_usd,
                shadow_buy_usd,
                recent_buy_total,
                top_tokens_fmt,
            )
            last_risk_summary_ts = now_ts

        # Self-heal stale accumulator entries:
        # If a token has no observable position/open BUY orders for a long time,
        # old accumulator residue can permanently block follow buys for that token.
        # We only reset entries that are BOTH stale and currently flat by planned notional.
        accumulator_stale_reset_sec = int(cfg.get("accumulator_stale_reset_sec") or 7200)
        if accumulator_stale_reset_sec > 0:
            accumulator = state.get("buy_notional_accumulator")
            if isinstance(accumulator, dict):
                to_reset = []
                for token_id, acc_data in accumulator.items():
                    if not isinstance(acc_data, dict):
                        continue
                    last_ts = int(acc_data.get("last_ts") or 0)
                    if last_ts <= 0 or now_ts - last_ts < accumulator_stale_reset_sec:
                        continue
                    planned_token_usd = float(planned_by_token_usd.get(token_id, 0.0))
                    open_orders_token = state.get("open_orders", {}).get(token_id, [])
                    has_open_buy = any(
                        str(order.get("side") or "").upper() == "BUY"
                        for order in (open_orders_token if isinstance(open_orders_token, list) else [])
                    )
                    if planned_token_usd <= eps and not has_open_buy:
                        to_reset.append(token_id)
                for token_id in to_reset:
                    old_usd = float((accumulator.get(token_id) or {}).get("usd") or 0.0)
                    accumulator.pop(token_id, None)
                    logger.warning(
                        "[ACCUMULATOR_RESET_STALE] token_id=%s old_usd=%s stale_sec=%s reason=flat_and_stale",
                        token_id,
                        old_usd,
                        accumulator_stale_reset_sec,
                    )

        # CRITICAL: Accumulator is maintained ONLY by local BUY/SELL operations
        # DO NOT reconcile with API data to preserve independence from position API sync issues
        # Accumulator updates happen in ct_exec.py:
        # - Incremented on successful BUY (both taker and maker)
        # - Decremented on successful SELL
        # - Cleared when SELL reduces it below threshold (0.01)

        my_trades_unreliable_until = int(state.get("my_trades_unreliable_until") or 0)
        my_trades_unreliable = my_trades_unreliable_until > now_ts
        if my_trades_unreliable:
            logger.warning(
                "[MY_TRADES] unreliable freeze buys until=%s",
                my_trades_unreliable_until,
            )

        max_per_condition = float(cfg.get("max_position_usd_per_condition") or 0.0)
        condition_planned_map: Dict[str, float] = {}
        if max_per_condition > 0:
            for tid, usd in planned_by_token_usd_shadow.items():
                cond = _get_condition_id(state, tid)
                if cond:
                    condition_planned_map[cond] = condition_planned_map.get(cond, 0.0) + usd

        max_per_event = float(cfg.get("max_position_usd_per_event") or 0.0)
        event_planned_map: Dict[str, float] = {}
        if max_per_event > 0:
            for tid, usd in planned_by_token_usd_shadow.items():
                ev = _get_event_id(state, tid)
                if ev:
                    event_planned_map[ev] = event_planned_map.get(ev, 0.0) + usd

        # Clean up stale topic_state for tokens with zero position and no open orders
        topic_state = state.get("topic_state", {})
        topic_unfilled = state.setdefault("topic_unfilled_attempts", {})
        quarantine_sec = int(cfg.get("cleanup_quarantine_sec") or 300)
        if isinstance(topic_state, dict):
            for tid in list(topic_state.keys()):
                my_shares_t = my_by_token_id.get(tid, 0.0)
                orders_t = state.get("open_orders", {}).get(tid, [])
                st_t = topic_state.get(tid) or {}

                # Cross-check with accumulator before declaring zero
                acc_usd = float(
                    (state.get("buy_notional_accumulator") or {}).get(tid, {}).get("usd", 0.0)
                    or 0.0
                )
                if my_shares_t <= eps and acc_usd > 0.5 and not orders_t:
                    logger.warning(
                        "[CLEANUP_DELAY] token_id=%s reason=accumulator_mismatch acc_usd=%s my_shares=%s",
                        tid,
                        acc_usd,
                        my_shares_t,
                    )
                    continue

                if my_shares_t <= eps and not orders_t:
                    in_quarantine = st_t.get("phase") == "SUSPECT_ZERO"
                    quarantine_expired = now_ts - st_t.get("cleanup_ts", now_ts) >= quarantine_sec
                    if in_quarantine and quarantine_expired:
                        # Hard confirmed after quarantine
                        topic_state.pop(tid, None)
                        state.setdefault("sell_shares_accumulator", {}).pop(tid, None)
                        _finalize_exited_token_state(
                            state=state,
                            token_id=tid,
                            now_ts=now_ts,
                            cfg=cfg,
                            logger=logger,
                            reason="zero_position_no_orders",
                        )
                        logger.info(
                            "[TOPIC] CLEANUP_CONFIRMED token_id=%s reason=zero_position_no_orders quarantine=%s",
                            tid,
                            quarantine_sec,
                        )
                        topic_unfilled[tid] = topic_unfilled.get(tid, 0) + 1
                        max_unfilled = int(cfg.get("topic_unfilled_max_rounds") or 3)
                        if max_unfilled > 0 and topic_unfilled[tid] >= max_unfilled:
                            ignore_sec = int(cfg.get("topic_unfilled_ignore_sec") or 1800)
                            state.setdefault("ignored_tokens", {})[tid] = {
                                "ts": now_ts,
                                "reason": "unfilled_timeout",
                                "expires_at": now_ts + ignore_sec,
                            }
                            logger.warning(
                                "[IGNORE] token_id=%s reason=unfilled_timeout rounds=%s until=%s",
                                tid,
                                topic_unfilled[tid],
                                now_ts + ignore_sec,
                            )
                    else:
                        # First zero sighting: enter quarantine instead of hard delete
                        st_t["phase"] = "SUSPECT_ZERO"
                        st_t["cleanup_ts"] = st_t.get("cleanup_ts") or now_ts
                        st_t["desired_shares"] = 0.0
                        st_t["desired_side"] = "SELL"
                        topic_state[tid] = st_t
                        logger.info(
                            "[TOPIC] CLEANUP_SUSPICIOUS token_id=%s quarantine_until=%s",
                            tid,
                            st_t["cleanup_ts"] + quarantine_sec,
                        )

        _shadow_total_for_loop, _shadow_by_token_for_loop = _calc_shadow_buy_notional(
            state, now_ts, shadow_ttl_sec
        )

        for token_id in reconcile_set:
            must_exit_meta = must_exit_tokens.get(token_id)
            must_exit_active = isinstance(must_exit_meta, dict)
            last_target_sell_ms = int(last_target_sell_action_ts_by_token.get(token_id) or 0)
            last_allowed_target_sell_action_ts_by_token = state.setdefault(
                "last_allowed_target_sell_action_ts_by_token", {}
            )
            if not isinstance(last_allowed_target_sell_action_ts_by_token, dict):
                last_allowed_target_sell_action_ts_by_token = {}
                state["last_allowed_target_sell_action_ts_by_token"] = (
                    last_allowed_target_sell_action_ts_by_token
                )
            last_allowed_target_sell_ms = int(
                last_allowed_target_sell_action_ts_by_token.get(token_id) or 0
            )
            must_exit_fresh = (
                _is_must_exit_fresh(
                    meta=must_exit_meta,
                    last_target_sell_ms=last_allowed_target_sell_ms,
                    now_ms=now_ms,
                    fresh_window_sec=must_exit_fresh_sell_window_sec,
                )
                if must_exit_active
                else False
            )
            if token_id in active_ignored and (not must_exit_active):
                continue
            if token_id in active_ignored and must_exit_active:
                logger.info(
                    "[MUST_EXIT] token_id=%s bypass=active_ignore",
                    token_id,
                )
            open_orders = state.get("open_orders", {}).get(token_id, [])
            cached = status_cache.get(token_id) or {}
            token_meta = cached.get("meta") if isinstance(cached, dict) else None
            if isinstance(token_meta, dict):
                token_title = (
                    token_meta.get("title")
                    or token_meta.get("question")
                    or token_meta.get("marketTitle")
                    or token_meta.get("market_title")
                )
            else:
                token_title = None
            cooldown_until = int(state.get("cooldown_until", {}).get(token_id) or 0)
            cooldown_active = cooldown_sec > 0 and now_ts < cooldown_until
            place_fail_until = int(state.get("place_fail_until", {}).get(token_id) or 0)
            place_backoff_active = place_fail_until > 0 and now_ts < place_fail_until
            sell_reconcile_lock_until = int(
                state.get("sell_reconcile_lock_until", {}).get(token_id) or 0
            )
            sell_reconcile_lock_active = (
                sell_reconcile_lock_until > 0 and now_ts < sell_reconcile_lock_until
            )
            if cooldown_active:
                logger.info(
                    "[COOLDOWN] token_id=%s until=%s",
                    token_id,
                    cooldown_until,
                )
            if place_backoff_active:
                logger.info(
                    "[PLACE_BACKOFF] token_id=%s until=%s",
                    token_id,
                    place_fail_until,
                )
            if sell_reconcile_lock_active:
                logger.info(
                    "[SELL_LOCK] token_id=%s until=%s",
                    token_id,
                    sell_reconcile_lock_until,
                )

            missing_freeze = state.setdefault("missing_data_freeze", {})
            freeze_meta = missing_freeze.get(token_id)
            if isinstance(freeze_meta, dict) and freeze_meta.get("expires_at"):
                expires_at = int(freeze_meta.get("expires_at") or 0)
                if expires_at > 0 and now_ts >= expires_at:
                    # If data is still missing, renew the same missing_streak freeze
                    # instead of UNFREEZE -> FREEZE oscillation every few rounds.
                    _reason = str(freeze_meta.get("reason") or "")
                    _my_shares_here = my_by_token_id.get(token_id, 0.0)
                    _still_missing = token_id not in target_shares_now_by_token_id
                    _ref_price_here = state.get("last_mid_price_by_token_id", {}).get(
                        token_id, fallback_mid_price
                    )
                    if not _ref_price_here or _ref_price_here <= 0:
                        _ref_price_here = fallback_mid_price
                    _my_notional_here = max(0.0, _my_shares_here) * float(_ref_price_here)
                    _still_meaningful = (
                        _my_notional_here >= max(0.0, missing_meaningful_min_usd - eps)
                        or bool(open_orders)
                    )
                    if _reason == "missing_streak" and _still_missing and _still_meaningful:
                        freeze_sec = max(0, int(missing_freeze_renew_sec))
                        until_ts = now_ts + freeze_sec
                        freeze_meta["ts"] = now_ts
                        freeze_meta["expires_at"] = until_ts
                        freeze_meta["streak"] = int(
                            max(
                                int(freeze_meta.get("streak") or 0),
                                int(state.get("target_missing_streak", {}).get(token_id) or 0),
                            )
                        )
                        missing_freeze[token_id] = freeze_meta
                        dedup_key = "FREEZE:missing_streak_renew"
                        should_log, suppressed = _log_dedup.should_log(dedup_key)
                        if should_log:
                            if suppressed > 0:
                                logger.warning(
                                    "[FREEZE] token_id=%s reason=missing_streak_renew streak=%s until=%s (suppressed %d)",
                                    token_id,
                                    freeze_meta.get("streak"),
                                    until_ts,
                                    suppressed,
                                )
                            else:
                                logger.warning(
                                    "[FREEZE] token_id=%s reason=missing_streak_renew streak=%s until=%s",
                                    token_id,
                                    freeze_meta.get("streak"),
                                    until_ts,
                                )
                    else:
                        missing_freeze.pop(token_id, None)
                        state.setdefault("target_missing_streak", {})[token_id] = 0
                        logger.info(
                            "[UNFREEZE] token_id=%s reason=%s expired_at=%s",
                            token_id,
                            freeze_meta.get("reason") or "missing_streak",
                            expires_at,
                        )
                        freeze_meta = None
            # missing_streak freeze should only block NEW BUY entry for tokens
            # with no position and no open orders. If we have an existing position
            # or active orders, we must allow SELL / reprice logic to run.
            my_shares_here = my_by_token_id.get(token_id, 0.0)
            if (
                isinstance(freeze_meta, dict)
                and freeze_meta.get("expires_at")
                and freeze_meta.get("reason") == "missing_streak"
                and my_shares_here <= eps
                and not open_orders
            ):
                # If positions data recovered during freeze, unfreeze immediately
                if token_id in target_shares_now_by_token_id:
                    state.setdefault("target_missing_streak", {})[token_id] = 0
                    state.setdefault("target_last_seen_ts", {})[token_id] = now_ts
                    missing_freeze.pop(token_id, None)
                    logger.info("[UNFREEZE] token_id=%s reason=data_recovered", token_id)
                    freeze_meta = None
                else:
                    logger.info(
                        "[SKIP] token_id=%s reason=missing_streak_freeze until=%s",
                        token_id,
                        freeze_meta.get("expires_at"),
                    )
                    continue

            if skip_closed:
                if token_id in ignored:
                    if must_exit_active:
                        logger.info(
                            "[MUST_EXIT] token_id=%s bypass=ignored_token",
                            token_id,
                        )
                    else:
                        if open_orders:
                            logger.info(
                                "[SKIP] ignored token_id=%s open_orders=%s",
                                token_id,
                                len(open_orders),
                            )
                        continue
                    if open_orders:
                        logger.info(
                            "[SKIP] ignored token_id=%s open_orders=%s",
                            token_id,
                            len(open_orders),
                        )
                tradeable = cached.get("tradeable")

                if tradeable is False:
                    if open_orders:
                        actions = [
                            {"type": "cancel", "order_id": order.get("order_id")}
                            for order in open_orders
                            if order.get("order_id")
                        ]
                        if actions:
                            logger.info(
                                "[CLOSE] token_id=%s cancel_managed_orders=%s",
                                token_id,
                                len(actions),
                            )
                            updated_orders = apply_actions(
                                clob_client,
                                actions,
                                open_orders,
                                now_ts,
                                args.dry_run,
                                cfg=cfg,
                                state=state,
                            )
                            if updated_orders:
                                state.setdefault("open_orders", {})[token_id] = updated_orders
                            else:
                                state.get("open_orders", {}).pop(token_id, None)
                            _prune_order_ts_by_id(state)
                            _refresh_managed_order_ids(state)
                            (
                                planned_total_notional,
                                planned_by_token_usd,
                                order_info_by_id,
                                _shadow_buy_usd,
                            ) = _calc_planned_notional_with_fallback(
                                my_by_token_id,
                                state.get("open_orders", {}),
                                state.get("last_mid_price_by_token_id", {}),
                                max_position_usd_per_token,
                                state,
                                now_ts,
                                shadow_ttl_sec,
                                fallback_mid_price,
                                logger,
                                include_shadow=False,
                            )
                            (
                                planned_total_notional_shadow,
                                planned_by_token_usd_shadow,
                                _shadow_order_info_by_id,
                                _shadow_buy_usd,
                            ) = _calc_planned_notional_with_fallback(
                                my_by_token_id,
                                state.get("open_orders", {}),
                                state.get("last_mid_price_by_token_id", {}),
                                max_position_usd_per_token,
                                state,
                                now_ts,
                                shadow_ttl_sec,
                                fallback_mid_price,
                                logger,
                                include_shadow=True,
                            )
                    ignored[token_id] = {"ts": now_ts, "reason": "closed_or_not_tradeable"}
                    meta = cached.get("meta") or {}
                    slug = meta.get("slug") or ""
                    dedup_key = f"SKIP:{token_id}:closed_inactive"
                    should_log, suppressed = _log_dedup.should_log(dedup_key)
                    if should_log:
                        if suppressed > 0:
                            logger.info(
                                "[SKIP] closed/inactive token_id=%s slug=%s (suppressed %d)",
                                token_id, slug, suppressed,
                            )
                        else:
                            logger.info("[SKIP] closed/inactive token_id=%s slug=%s", token_id, slug)
                    continue

                if tradeable is None:
                    block_on_unknown = bool(cfg.get("block_on_unknown_market_state", False))
                    dedup_key = f"WARN:market_unknown:{'block' if block_on_unknown else 'nonblock'}"
                    should_log, suppressed = _log_dedup.should_log(dedup_key)
                    if should_log and block_on_unknown:
                        if suppressed > 0:
                            logger.warning(
                                "[WARN] market 状态未知(阻塞模式): token_id=%s (suppressed %d)",
                                token_id,
                                suppressed,
                            )
                        else:
                            logger.warning("[WARN] market 状态未知(阻塞模式): token_id=%s", token_id)
                    elif should_log:
                        if suppressed > 0:
                            logger.warning(
                                "[WARN] market 状态未知(不阻塞交易): token_id=%s (suppressed %d)",
                                token_id,
                                suppressed,
                            )
                        else:
                            logger.warning("[WARN] market 状态未知(不阻塞交易): token_id=%s", token_id)
                    if block_on_unknown:
                        continue
            t_now_present = token_id in target_shares_now_by_token_id
            t_now = target_shares_now_by_token_id.get(token_id) if t_now_present else None
            token_key = token_key_by_token_id.get(token_id, f"token:{token_id}")
            if (not t_now_present) and isinstance(target_shares_now_by_token_key, dict):
                alt = target_shares_now_by_token_key.get(token_key)
                if alt is not None:
                    t_now_present = True
                    t_now = float(alt)
            signal_source_target = _resolve_signal_source_target(
                token_id=token_id,
                buy_signal_source_by_token=buy_signal_source_by_token,
                position_source=position_source,
                target_addresses=target_addresses,
            )
            signal_target_level = target_levels.get(signal_source_target, "A")
            signal_skip_ratio = float(target_level_skip_ratios.get(signal_target_level, 0.0))
            buy_signal_ms = int(buy_signal_ms_by_token.get(token_id) or 0)
            signal_marker = _build_target_level_signal_marker(
                buy_action_ms=buy_signal_ms,
                t_now=t_now,
                now_ts=now_ts,
            )
            signal_id = _build_target_level_signal_id(
                token_id=token_id,
                source_target=signal_source_target,
                signal_marker=signal_marker,
            )
            missing_data = t_now is None
            boot_key_set = set(state.get("boot_token_keys", []))
            boot_id_set = {str(tid) for tid in state.get("boot_token_ids", [])}
            is_boot_token = (token_key in boot_key_set) or (str(token_id) in boot_id_set)

            ignore_boot_tokens = bool(cfg.get("ignore_boot_tokens", True))
            follow_new_topics_only = bool(cfg.get("follow_new_topics_only", False))
            boot_scope = str(cfg.get("ignore_boot_tokens_scope") or "probe_only").lower()
            # scope 璇存槑锛?            # - "probe_only"锛堥粯璁わ級锛氫粎闃绘 boot token 鐨?probe锛堥槻寮€鏈鸿涔帮級锛屽厑璁稿悗缁閲?BUY 璺熷崟
            # - "all"锛氭棫琛屼负锛宐oot token 鐨?BUY 涔熼樆姝紙涓嶆帹鑽愶級
            probe_blocked_by_boot = (
                ignore_boot_tokens
                and is_boot_token
                and boot_scope in ("probe_only", "probe", "all", "full")
            )
            buy_blocked_by_boot = (
                ignore_boot_tokens and is_boot_token and boot_scope in ("all", "full")
            )
            # Strong mode: ignore all pre-boot tokens for BUY/probe (SELL still allowed).
            if follow_new_topics_only and is_boot_token:
                probe_blocked_by_boot = True
                buy_blocked_by_boot = True
            t_last = state.get("target_last_shares", {}).get(token_id)
            if t_last is None:
                boot_by_key = state.get("target_last_shares_by_token_key", {})
                if isinstance(boot_by_key, dict):
                    base = boot_by_key.get(token_key)
                    if base is not None:
                        state.setdefault("target_last_shares", {})[token_id] = float(base)
                        t_last = float(base)
                        boot_ids = set(state.get("boot_token_ids", []))
                        boot_ids.add(token_id)
                        state["boot_token_ids"] = sorted(boot_ids)
            my_shares = my_by_token_id.get(token_id, 0.0)
            ref_price_for_meaning = state.get("last_mid_price_by_token_id", {}).get(
                token_id, fallback_mid_price
            )
            if not ref_price_for_meaning or ref_price_for_meaning <= 0:
                ref_price_for_meaning = fallback_mid_price
            my_notional_est = max(0.0, my_shares) * float(ref_price_for_meaning)
            if my_shares > eps:
                state.setdefault("topic_unfilled_attempts", {}).pop(token_id, None)
                last_nonzero_my_shares[token_id] = {"shares": float(my_shares), "ts": int(now_ts)}
            open_orders_count = len(open_orders)
            open_sell_orders_count = sum(
                1 for order in open_orders if str(order.get("side") or "").upper() == "SELL"
            )
            _clear_exit_finalization_on_exposure(state, token_id, my_shares, open_orders, eps)
            sell_signal_count = int(sell_signal_count_by_token.get(token_id) or 0)
            if sell_signal_count > 0 and (my_shares > eps or open_sell_orders_count > 0):
                sell_health_round["signals"] = int(sell_health_round.get("signals") or 0) + sell_signal_count
            missing_streak = int(state.get("target_missing_streak", {}).get(token_id) or 0)
            last_seen_ts = int(state.get("target_last_seen_ts", {}).get(token_id) or 0)
            has_buy = bool(has_buy_by_token.get(token_id))
            raw_has_sell = bool(has_sell_by_token.get(token_id))
            buy_sum = float(buy_sum_by_token.get(token_id, 0.0))
            sell_sum = float(sell_sum_by_token.get(token_id, 0.0))
            action_seen = has_buy or raw_has_sell
            topic_state = state.setdefault("topic_state", {})
            st = topic_state.get(token_id) or {"phase": "IDLE"}
            phase = st.get("phase", "IDLE")
            primary_entry_source = str(
                st.get("primary_entry_source") or signal_source_target or ""
            ).strip().lower()
            if has_buy and primary_entry_source and not str(st.get("primary_entry_source") or "").strip():
                st["primary_entry_source"] = primary_entry_source
                topic_state[token_id] = st
            sell_signal_allowed = True
            sell_signal_reason = "no_sell_action"
            sell_signal_sellers: List[str] = []
            if raw_has_sell:
                sell_signal_allowed, sell_signal_reason, sell_signal_sellers = (
                    _should_execute_sell_source_signal(
                        state=state,
                        token_id=token_id,
                        now_ts=now_ts,
                        current_sell_sources=sell_signal_sources_by_token.get(token_id) or {},
                        primary_entry_source=primary_entry_source,
                        enabled=_cfg_bool(cfg.get("sell_source_consensus_enabled"), True),
                        secondary_consensus_count=int(
                            cfg.get("sell_secondary_consensus_count") or 2
                        ),
                        secondary_window_sec=int(
                            cfg.get("sell_secondary_consensus_window_sec") or 900
                        ),
                        primary_immediate=_cfg_bool(
                            cfg.get("sell_primary_source_immediate_exit"), True
                        ),
                    )
                )
                if sell_signal_allowed:
                    allowed_sell_ms = int(sell_signal_ms_by_token.get(token_id) or 0)
                    if allowed_sell_ms <= 0:
                        allowed_sell_ms = int(now_ms)
                    prev_allowed_sell_ms = int(
                        last_allowed_target_sell_action_ts_by_token.get(token_id) or 0
                    )
                    if allowed_sell_ms > prev_allowed_sell_ms:
                        last_allowed_target_sell_action_ts_by_token[token_id] = int(
                            allowed_sell_ms
                        )
                        last_allowed_target_sell_ms = int(allowed_sell_ms)
                else:
                    logger.info(
                        "[HOLD] token_id=%s reason=%s primary=%s sellers=%s need=%s",
                        token_id,
                        sell_signal_reason,
                        primary_entry_source or "",
                        ",".join(sell_signal_sellers),
                        int(cfg.get("sell_secondary_consensus_count") or 2),
                    )
            has_sell = raw_has_sell and sell_signal_allowed
            base_deadband_shares = float(cfg.get("deadband_shares") or 0.0)
            token_deadband_shares = base_deadband_shares
            token_reentry_cooldown_sec = reentry_cooldown_sec
            topic_risk_level = int(st.get("topic_risk_level") or 0)
            topic_risk_score = float(st.get("topic_risk_score") or 0.0)
            topic_risk_freeze_buy_until = int(st.get("topic_risk_freeze_buy_until") or 0)
            topic_risk_action_hint = f"L{max(0, topic_risk_level)}"
            if _should_clear_stale_must_exit_on_buy(
                must_exit_active=must_exit_active,
                must_exit_fresh=must_exit_fresh,
                t_now_present=t_now_present,
                t_now=t_now,
                has_buy=has_buy,
                buy_sum=buy_sum,
                min_target_buy_shares=must_exit_clear_on_buy_min_target_shares,
            ):
                must_exit_tokens.pop(token_id, None)
                if phase == "EXITING":
                    st["phase"] = "LONG"
                    st["first_sell_ts"] = 0
                    topic_state[token_id] = st
                    phase = "LONG"
                must_exit_meta = None
                must_exit_active = False
                must_exit_fresh = False
                logger.warning(
                    "[MUST_EXIT_CLEAR] token_id=%s reason=stale_with_fresh_buy buy_sum=%.6f target_now=%.6f window_sec=%s",
                    token_id,
                    buy_sum,
                    float(t_now or 0.0),
                    must_exit_fresh_sell_window_sec,
                )
            if must_exit_active:
                _mark_must_exit_token(
                    state,
                    token_id,
                    now_ts,
                    source="reconcile_loop",
                    target_sell_ms=last_allowed_target_sell_ms,
                )
                if phase != "EXITING" and (my_shares > eps or open_orders_count > 0):
                    st = {
                        "phase": "EXITING",
                        "first_buy_ts": int(st.get("first_buy_ts") or now_ts),
                        "first_sell_ts": now_ts,
                        "entry_sized": bool(st.get("entry_sized")),
                        "did_probe": bool(st.get("did_probe")),
                        "target_peak": float(st.get("target_peak") or float(t_now or 0.0)),
                        "entry_buy_accum": float(st.get("entry_buy_accum") or 0.0),
                        "primary_entry_source": str(
                            st.get("primary_entry_source") or primary_entry_source or ""
                        ).strip().lower(),
                        "desired_shares": 0.0,
                    }
                    topic_state[token_id] = st
                    phase = "EXITING"
                    logger.info(
                        "[MUST_EXIT] token_id=%s promote_to=EXITING my_shares=%s open_orders=%s",
                        token_id,
                        my_shares,
                        open_orders_count,
                    )
                if (
                    my_shares <= eps
                    and open_orders_count == 0
                    and (not action_seen)
                    and _should_clear_must_exit_without_inventory(state, token_id, now_ts, eps, cfg)
                ):
                    _finalize_exited_token_state(
                        state=state,
                        token_id=token_id,
                        now_ts=now_ts,
                        cfg=cfg,
                        logger=logger,
                        reason="no_inventory_no_orders",
                    )
                    must_exit_active = False
                    logger.info(
                        "[MUST_EXIT] token_id=%s clear reason=no_inventory_no_orders",
                        token_id,
                    )

            # FIX: Only meaningful tokens should accumulate missing_streak / freeze.
            has_meaningful_state = (
                my_notional_est >= max(0.0, missing_meaningful_min_usd - eps)
                or bool(open_orders)
            )
            if not has_meaningful_state and missing_streak > 0:
                state.setdefault("target_missing_streak", {})[token_id] = 0
                missing_streak = 0
                _mf = state.setdefault("missing_data_freeze", {})
                if _mf.get(token_id):
                    _mf.pop(token_id, None)

            if topic_mode:
                # Suspect Zero Quarantine recovery
                if phase == "SUSPECT_ZERO":
                    quarantine_sec = int(cfg.get("cleanup_quarantine_sec") or 300)
                    if my_shares > eps or orders_t:
                        # Position resurrected: API previously lied about zero
                        resume_phase = "EXITING" if has_sell else "LONG"
                        st["phase"] = resume_phase
                        logger.warning(
                            "[RECOVERY] token_id=%s reason=position_resurrected phase=%s my_shares=%s orders=%s resume_exit",
                            token_id,
                            resume_phase,
                            my_shares,
                            len(orders_t),
                        )
                        topic_state[token_id] = st
                        phase = resume_phase
                    elif now_ts - st.get("cleanup_ts", now_ts) >= quarantine_sec:
                        # Should have been hard-deleted in cleanup block; safety fallback
                        topic_state.pop(token_id, None)
                        continue

                if phase == "IDLE" and has_buy:
                    st = {
                        "phase": "LONG",
                        "first_buy_ts": now_ts,
                        "first_sell_ts": 0,
                        "entry_sized": False,
                        "did_probe": False,
                        "target_peak": float(t_now or 0.0),
                        "entry_buy_accum": 0.0,
                        "primary_entry_source": primary_entry_source,
                        "desired_shares": 0.0,
                    }
                    topic_state[token_id] = st
                    phase = "LONG"
                    logger.info("[TOPIC] ENTER token_id=%s first_buy_ts=%s", token_id, now_ts)

                # If topic state was cleaned to IDLE but a fresh SELL arrives while we still
                # hold shares/open orders, recover EXITING immediately instead of treating it
                # as non-topic SELL (which can be throttled by normal cooldown/accumulator).
                recent_target_sell = (
                    last_allowed_target_sell_ms > 0
                    and (now_ms - last_allowed_target_sell_ms)
                    <= online_sell_recover_window_sec * 1000
                )
                recover_by_recent_target_sell = (
                    phase == "IDLE"
                    and (not raw_has_sell)
                    and recent_target_sell
                    and (my_shares > eps or open_orders_count > 0)
                    and (not t_now_present)
                    and (now_ms - last_allowed_target_sell_ms)
                    >= online_sell_recover_grace_sec * 1000
                )
                if (
                    phase == "IDLE"
                    and (has_sell or recover_by_recent_target_sell)
                    and (my_shares > eps or open_orders_count > 0)
                ):
                    st = {
                        "phase": "EXITING",
                        "first_buy_ts": int(st.get("first_buy_ts") or now_ts),
                        "first_sell_ts": now_ts,
                        "entry_sized": bool(st.get("entry_sized")),
                        "did_probe": bool(st.get("did_probe")),
                        "target_peak": float(st.get("target_peak") or float(t_now or 0.0)),
                        "entry_buy_accum": float(st.get("entry_buy_accum") or 0.0),
                        "primary_entry_source": str(
                            st.get("primary_entry_source") or primary_entry_source or ""
                        ).strip().lower(),
                        "desired_shares": 0.0,
                    }
                    topic_state[token_id] = st
                    phase = "EXITING"
                    _mark_must_exit_token(
                        state,
                        token_id,
                        now_ts,
                        source="topic_exit_recover",
                        target_sell_ms=last_allowed_target_sell_ms,
                    )
                    logger.info(
                        "[TOPIC] EXIT_RECOVER token_id=%s reason=%s my_shares=%s orders=%s",
                        token_id,
                        "idle_sell_signal" if has_sell else "idle_recent_target_sell",
                        my_shares,
                        open_orders_count,
                    )

                if phase == "LONG":
                    if t_now is not None:
                        st["target_peak"] = max(
                            float(st.get("target_peak") or 0.0),
                            float(t_now),
                        )
                    elif has_buy and buy_sum > 0:
                        # FIX: When t_now is missing (positions API stale), accumulate
                        # target_peak from buy actions so entry sizing has a usable base.
                        st["target_peak"] = float(
                            st.get("target_peak") or 0.0
                        ) + float(buy_sum)
                    if not st.get("entry_sized"):
                        first_buy_ts = int(st.get("first_buy_ts") or now_ts)
                        if now_ts - first_buy_ts <= entry_settle_sec:
                            st["entry_buy_accum"] = float(
                                st.get("entry_buy_accum") or 0.0
                            ) + float(buy_sum)

                if phase == "LONG" and has_sell:
                    st["phase"] = "EXITING"
                    st["first_sell_ts"] = now_ts
                    topic_state[token_id] = st
                    phase = "EXITING"
                    _mark_must_exit_token(
                        state,
                        token_id,
                        now_ts,
                        source="topic_exit_signal",
                        target_sell_ms=last_allowed_target_sell_ms,
                    )
                    logger.info("[TOPIC] EXIT token_id=%s first_sell_ts=%s", token_id, now_ts)

                if phase == "EXITING":
                    min_order_shares = float(cfg.get("min_order_shares") or 0.0)
                    dust_eps = float(cfg.get("dust_exit_eps") or 0.0)
                    below_min_as_dust = bool(cfg.get("exit_treat_below_min_as_dust", False))
                    desired_shares = float(st.get("desired_shares") or 0.0)
                    is_dust = False
                    if desired_shares <= eps and my_shares > eps:
                        if dust_eps > 0 and my_shares <= dust_eps:
                            is_dust = True
                        elif (
                            below_min_as_dust
                            and min_order_shares > 0
                            and my_shares + eps < min_order_shares
                        ):
                            is_dust = True
                    if is_dust:
                        state.setdefault("dust_exits", {})[token_id] = {
                            "ts": now_ts,
                            "shares": my_shares,
                        }
                        topic_state.pop(token_id, None)
                        must_exit_tokens.pop(token_id, None)
                        phase = "IDLE"
                        logger.info(
                            "[TOPIC] DUST_RESET token_id=%s remaining=%s",
                            token_id,
                            my_shares,
                        )

                if phase == "EXITING" and my_shares <= eps and open_orders_count == 0:
                    topic_state.pop(token_id, None)
                    must_exit_tokens.pop(token_id, None)
                    last_nonzero_my_shares.pop(token_id, None)
                    last_exit_ts_by_token[token_id] = int(now_ts)
                    phase = "IDLE"
                    logger.info("[TOPIC] RESET token_id=%s", token_id)

            is_exiting = phase == "EXITING"
            topic_active = topic_mode and phase in ("LONG", "EXITING")
            probe_attempted = False
            if (not action_seen) and (not t_now_present) and (not topic_active) and has_meaningful_state:
                missing_streak += 1
                state.setdefault("target_missing_streak", {})[token_id] = missing_streak
                missing_timeout = (
                    missing_timeout_sec > 0
                    and last_seen_ts > 0
                    and now_ts - last_seen_ts >= missing_timeout_sec
                )
                missing = t_now is None
                if (
                    missing
                    and missing_freeze_streak > 0
                    and missing_streak >= missing_freeze_streak
                ):
                    missing_freeze = state.setdefault("missing_data_freeze", {})
                    existing_freeze = missing_freeze.get(token_id)
                    has_expiring_freeze = isinstance(existing_freeze, dict) and existing_freeze.get(
                        "expires_at"
                    )
                    if not existing_freeze or has_expiring_freeze:
                        if has_expiring_freeze:
                            expires_at = int(existing_freeze.get("expires_at") or 0)
                            if expires_at > 0 and now_ts < expires_at:
                                pass
                            else:
                                existing_freeze = None
                        if not existing_freeze:
                            freeze_min = max(0, missing_freeze_min_sec)
                            freeze_max = max(freeze_min, missing_freeze_max_sec)
                            if freeze_max == freeze_min:
                                freeze_sec = freeze_min
                            else:
                                freeze_sec = random.randint(freeze_min, freeze_max)
                            until_ts = now_ts + freeze_sec
                            missing_freeze[token_id] = {
                                "ts": now_ts,
                                "expires_at": until_ts,
                                "reason": "missing_streak",
                                "streak": missing_streak,
                            }
                            dedup_key = "FREEZE:missing_streak"
                            should_log, suppressed = _log_dedup.should_log(dedup_key)
                            if should_log:
                                if suppressed > 0:
                                    logger.warning(
                                        "[FREEZE] token_id=%s reason=missing_streak streak=%s until=%s (suppressed %d)",
                                        token_id,
                                        missing_streak,
                                        until_ts,
                                        suppressed,
                                    )
                                else:
                                    logger.warning(
                                        "[FREEZE] token_id=%s reason=missing_streak streak=%s until=%s",
                                        token_id,
                                        missing_streak,
                                        until_ts,
                                    )
                should_log_missing = (
                    missing
                    and (my_shares > 0 or open_orders_count > 0)
                    and token_id not in missing_notice_tokens
                )
                if should_log_missing or (token_id in debug_token_ids):
                    legacy_desired = float(cfg.get("follow_ratio") or 0.0) * (
                        t_now or 0.0
                    )
                    logger.debug(
                        "[DBG] token_id=%s missing=%s missing_streak=%s t_now=%s t_last=%s "
                        "my_shares=%s open_orders_count=%s",
                        token_id,
                        missing,
                        missing_streak,
                        t_now,
                        t_last,
                        my_shares,
                        open_orders_count,
                    )
                    logger.debug("[DBG] token_id=%s legacy_desired=%s", token_id, legacy_desired)
                    if should_log_missing:
                        missing_notice_tokens.add(token_id)
                if open_orders_count > 0 and missing and (
                    missing_timeout or (missing_streak >= orphan_cancel_rounds)
                ):
                    logger.info(
                        "[ORPHAN] token_id=%s missing_streak=%s open_orders=%s",
                        token_id,
                        missing_streak,
                        open_orders_count,
                    )
                    cancel_actions = [
                        {"type": "cancel", "order_id": order.get("order_id")}
                        for order in open_orders
                        if order.get("order_id")
                    ]
                    if cancel_actions:
                        updated_orders = apply_actions(
                            clob_client,
                            cancel_actions,
                            open_orders,
                            now_ts,
                            args.dry_run,
                            cfg=cfg,
                            state=state,
                        )
                        if updated_orders:
                            state.setdefault("open_orders", {})[token_id] = updated_orders
                        else:
                            state.get("open_orders", {}).pop(token_id, None)
                        _prune_order_ts_by_id(state)
                        _refresh_managed_order_ids(state)
                        (
                            planned_total_notional,
                            planned_by_token_usd,
                            order_info_by_id,
                            _shadow_buy_usd,
                        ) = _calc_planned_notional_with_fallback(
                            my_by_token_id,
                            state.get("open_orders", {}),
                            state.get("last_mid_price_by_token_id", {}),
                            max_position_usd_per_token,
                            state,
                            now_ts,
                            shadow_ttl_sec,
                            fallback_mid_price,
                            logger,
                            include_shadow=False,
                        )
                        (
                            planned_total_notional_shadow,
                            planned_by_token_usd_shadow,
                            _shadow_order_info_by_id,
                            _shadow_buy_usd,
                        ) = _calc_planned_notional_with_fallback(
                            my_by_token_id,
                            state.get("open_orders", {}),
                            state.get("last_mid_price_by_token_id", {}),
                            max_position_usd_per_token,
                            state,
                            now_ts,
                            shadow_ttl_sec,
                            fallback_mid_price,
                            logger,
                            include_shadow=True,
                        )
                        (
                            planned_total_notional_shadow,
                            planned_by_token_usd_shadow,
                            _shadow_order_info_by_id,
                            _shadow_buy_usd,
                        ) = _calc_planned_notional_with_fallback(
                            my_by_token_id,
                            state.get("open_orders", {}),
                            state.get("last_mid_price_by_token_id", {}),
                            max_position_usd_per_token,
                            state,
                            now_ts,
                            shadow_ttl_sec,
                            fallback_mid_price,
                            logger,
                            include_shadow=True,
                        )
                        if orphan_ignore_sec > 0:
                            state.setdefault("ignored_tokens", {})[token_id] = {
                                "ts": now_ts,
                                "reason": "missing_orphan_cancel",
                                "expires_at": now_ts + orphan_ignore_sec,
                            }
                continue

            if action_seen:
                state.setdefault("target_missing_streak", {})[token_id] = 0
                # Even if position snapshot temporarily misses t_now, actions mean "recently seen".
                state.setdefault("target_last_seen_ts", {})[token_id] = now_ts
                _mf = state.setdefault("missing_data_freeze", {})
                _existing = _mf.get(token_id)
                if isinstance(_existing, dict) and _existing.get("reason") == "missing_streak":
                    _mf.pop(token_id, None)
                    logger.info("[UNFREEZE] token_id=%s reason=data_recovered", token_id)
            elif t_now_present:
                state.setdefault("target_missing_streak", {})[token_id] = 0
                state.setdefault("target_last_seen_ts", {})[token_id] = now_ts
                _mf = state.setdefault("missing_data_freeze", {})
                _existing = _mf.get(token_id)
                if isinstance(_existing, dict) and _existing.get("reason") == "missing_streak":
                    _mf.pop(token_id, None)
                    logger.info("[UNFREEZE] token_id=%s reason=data_recovered", token_id)

            should_update_last = t_now_present
            if t_last is None and (not action_seen) and (not topic_active):
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                should_probe = (
                    bool(state.get("bootstrapped"))
                    and (not probe_blocked_by_boot)
                    and bool(cfg.get("probe_buy_on_first_seen", True))
                    and t_now is not None
                    and float(t_now) > 0
                    and token_id not in set(state.get("probed_token_ids", []))
                    and my_shares <= 0
                )
                has_buy_open = any(
                    str(order.get("side") or "").upper() == "BUY" for order in open_orders or []
                )
                if should_probe and not has_buy_open:
                    if token_id in orderbooks:
                        ob = orderbooks[token_id]
                    else:
                        ob = get_orderbook(clob_read_client, token_id, api_timeout_sec)
                        orderbooks[token_id] = ob

                    best_bid = ob.get("best_bid")
                    best_ask = ob.get("best_ask")
                    if best_bid is not None and best_ask is not None and best_bid > best_ask:
                        logger.warning(
                            "[SKIP] invalid book bid>ask token_id=%s best_bid=%s best_ask=%s",
                            token_id,
                            best_bid,
                            best_ask,
                        )
                        orderbooks.pop(token_id, None)
                        ob = get_orderbook(clob_read_client, token_id, api_timeout_sec)
                        orderbooks[token_id] = ob
                        best_bid = ob.get("best_bid")
                        best_ask = ob.get("best_ask")
                        if (
                            best_bid is not None
                            and best_ask is not None
                            and best_bid > best_ask
                        ):
                            continue
                    ref_price = _mid_price(ob)
                    if ref_price is None or ref_price <= 0:
                        dedup_key = f"WARN:{token_id}:invalid_orderbook_probe"
                        should_log, suppressed = _log_dedup.should_log(dedup_key)
                        if should_log:
                            if suppressed > 0:
                                logger.warning(
                                    "[WARN] invalid_orderbook_probe token_id=%s best_bid=%s best_ask=%s (suppressed %d)",
                                    token_id,
                                    best_bid,
                                    best_ask,
                                    suppressed,
                                )
                            else:
                                logger.warning(
                                    "[WARN] invalid_orderbook_probe token_id=%s best_bid=%s best_ask=%s",
                                    token_id,
                                    best_bid,
                                    best_ask,
                                )
                        closed_now = _record_orderbook_empty(
                            state,
                            token_id,
                            logger,
                            cfg,
                            now_ts,
                        )
                        if closed_now:
                            logger.info(
                                "[SKIP] closed_by_orderbook_empty token_id=%s",
                                token_id,
                            )
                        continue

                    _clear_orderbook_empty(state, token_id)
                    state.setdefault("last_mid_price_by_token_id", {})[token_id] = float(
                        ref_price
                    )
                    state["last_mid_price_update_ts"] = now_ts
                    is_lowp = _is_lowp_token(cfg, float(ref_price))
                    cfg_lowp = _lowp_cfg(cfg, is_lowp)
                    probe_usd = float(
                        cfg_lowp.get("probe_order_usd")
                        or cfg_lowp.get("min_order_usd")
                        or 5.0
                    )
                    if probe_usd <= 0:
                        probe_usd = float(cfg_lowp.get("min_order_usd") or 5.0)
                    probe_shares = probe_usd / ref_price

                    cap_shares = float("inf")
                    if max_position_usd_per_token > 0:
                        cap_shares = max_position_usd_per_token / ref_price

                    my_target = min(my_shares + probe_shares, cap_shares)
                    delta = my_target - my_shares
                    if delta <= eps:
                        continue

                    desired_side = "BUY"
                    phase_for_intent = phase if topic_mode else "LONG"
                    intent_key = _intent_key(phase_for_intent, desired_side, my_target)
                    intent_changed, desired_down = _update_intent_state(
                        state, token_id, intent_key, eps, logger
                    )
                    open_orders_for_reconcile = open_orders
                    if open_orders and intent_changed:
                        opposite_orders = [
                            order
                            for order in open_orders
                            if str(order.get("side") or "").upper() != desired_side
                        ]
                        same_side_orders = [
                            order
                            for order in open_orders
                            if str(order.get("side") or "").upper() == desired_side
                        ]
                        cancel_actions = []
                        if opposite_orders:
                            cancel_actions.extend(
                                [
                                    {"type": "cancel", "order_id": order.get("order_id")}
                                    for order in opposite_orders
                                    if order.get("order_id")
                                ]
                            )
                        if desired_down or phase_for_intent == "EXITING":
                            cancel_actions.extend(
                                [
                                    {"type": "cancel", "order_id": order.get("order_id")}
                                    for order in same_side_orders
                                    if order.get("order_id")
                                ]
                            )
                        if cancel_actions:
                            logger.info(
                                "[CANCEL_INTENT] token_id=%s opposite=%s same_side=%s",
                                token_id,
                                len(opposite_orders),
                                len(same_side_orders)
                                if (desired_down or phase_for_intent == "EXITING")
                                else 0,
                            )
                            ignore_cd = bool(cfg.get("exit_ignore_cooldown", True)) and is_exiting
                            cancel_ignore_cd = bool(
                                cfg.get("cancel_intent_ignore_cooldown", True)
                            )
                            if cooldown_active and (not ignore_cd) and (not cancel_ignore_cd):
                                dedup_key = f"SKIP:{token_id}:cooldown_intent"
                                should_log, suppressed = _log_dedup.should_log(dedup_key)
                                if should_log:
                                    if suppressed > 0:
                                        logger.info(
                                            "[SKIP] token_id=%s reason=cooldown_intent (suppressed %d)",
                                            token_id, suppressed,
                                        )
                                    else:
                                        logger.info("[SKIP] token_id=%s reason=cooldown_intent", token_id)
                            else:
                                updated_orders = apply_actions(
                                    clob_client,
                                    cancel_actions,
                                    open_orders,
                                    now_ts,
                                    args.dry_run,
                                    cfg=cfg,
                                    state=state,
                                )
                                if updated_orders:
                                    state.setdefault("open_orders", {})[token_id] = updated_orders
                                    open_orders = updated_orders
                                else:
                                    state.get("open_orders", {}).pop(token_id, None)
                                    open_orders = []
                                _prune_order_ts_by_id(state)
                                _refresh_managed_order_ids(state)
                                (
                                    planned_total_notional,
                                    planned_by_token_usd,
                                    order_info_by_id,
                                    _shadow_buy_usd,
                                ) = _calc_planned_notional_with_fallback(
                                    my_by_token_id,
                                    state.get("open_orders", {}),
                                    state.get("last_mid_price_by_token_id", {}),
                                    max_position_usd_per_token,
                                    state,
                                    now_ts,
                                    shadow_ttl_sec,
                                    fallback_mid_price,
                                    logger,
                                    include_shadow=False,
                                )
                                (
                                    planned_total_notional_shadow,
                                    planned_by_token_usd_shadow,
                                    _shadow_order_info_by_id,
                                    _shadow_buy_usd,
                                ) = _calc_planned_notional_with_fallback(
                                    my_by_token_id,
                                    state.get("open_orders", {}),
                                    state.get("last_mid_price_by_token_id", {}),
                                    max_position_usd_per_token,
                                    state,
                                    now_ts,
                                    shadow_ttl_sec,
                                    fallback_mid_price,
                                    logger,
                                    include_shadow=True,
                                )
                                # NOTE: cancel-intent should NOT extend cooldown.
                                # Cooldown is applied only on successful place actions.
                    open_orders_for_reconcile = [
                        order
                        for order in open_orders
                        if str(order.get("side") or "").upper() == desired_side
                    ]

                    token_key = token_key_by_token_id.get(token_id, f"token:{token_id}")
                    cfg_for_reconcile = cfg_lowp if (is_lowp and desired_side == "BUY") else cfg

                    token_planned = float(planned_by_token_usd_shadow.get(token_id, 0.0))

                    if desired_side == "BUY":
                        max_notional = float(cfg_for_reconcile.get("max_notional_per_token") or 0.0)
                        if max_notional > 0 and token_planned >= max_notional * 0.95:
                            logger.debug(
                                "[SKIP_PREFLIGHT] %s near_limit planned=%s max=%s",
                                token_key,
                                token_planned,
                                max_notional,
                            )
                            continue

                        max_position_usd_per_token = float(cfg_for_reconcile.get("max_position_usd_per_token") or 0.0)
                        if max_position_usd_per_token > 0:
                            acc_usd = float(state.get("buy_notional_accumulator", {}).get(token_id, {}).get("usd", 0.0))
                            if acc_usd > max_position_usd_per_token:
                                logger.info(
                                    "[SKIP_PREFLIGHT] %s grandfather acc=%s max=%s",
                                    token_key,
                                    acc_usd,
                                    max_position_usd_per_token,
                                )
                                continue

                        if max_per_condition > 0:
                            cond_id = _get_condition_id(state, token_id)
                            if cond_id:
                                cond_planned = condition_planned_map.get(cond_id, 0.0)
                                if cond_planned >= max_per_condition * 0.95:
                                    logger.info(
                                        "[SKIP_PREFLIGHT] %s condition=%s condition_limit=%s/%s",
                                        token_key,
                                        cond_id[:16],
                                        cond_planned,
                                        max_per_condition,
                                    )
                                    continue

                        if max_per_event > 0:
                            event_id = _get_event_id(state, token_id)
                            if event_id:
                                event_planned = event_planned_map.get(event_id, 0.0)
                                if event_planned >= max_per_event * 0.95:
                                    logger.info(
                                        "[SKIP_PREFLIGHT] %s event=%s event_limit=%s/%s",
                                        token_key,
                                        event_id[:16],
                                        event_planned,
                                        max_per_event,
                                    )
                                    continue

                        max_total_pos = float(cfg.get("max_notional_total") or 0.0)
                        if max_total_pos > 0 and planned_total_notional_shadow >= max_total_pos * 0.95:
                            logger.info(
                                "[SKIP_PREFLIGHT] %s total_position_limit=%s/%s",
                                token_key,
                                planned_total_notional_shadow,
                                max_total_pos,
                            )
                            continue

                    try:
                        refresh_market_info_cache_v2(
                            clob_read_client,
                            state,
                            token_id,
                            timeout=api_timeout_sec,
                            ttl_sec=float(cfg.get("market_info_cache_ttl_sec") or 300.0),
                        )
                    except Exception as market_info_exc:
                        logger.debug(
                            "[MARKET_INFO_CACHE] token_id=%s refresh failed: %s",
                            token_id,
                            market_info_exc,
                        )

                    actions = reconcile_one(
                        token_id,
                        my_target,
                        my_shares,
                        ob,
                        open_orders_for_reconcile,
                        now_ts,
                        cfg_for_reconcile,
                        state,
                        planned_token_notional=token_planned,
                    )
                    if not actions:
                        continue
                    filtered_actions = []
                    blocked_reasons: set[str] = set()
                    has_any_place = any(a.get("type") == "place" for a in actions)
                    pending_cancel_actions = []
                    pending_cancel_usd = 0.0
                    token_planned_before = float(planned_by_token_usd.get(token_id, 0.0))
                    token_planned_before_shadow = float(
                        planned_by_token_usd_shadow.get(token_id, 0.0)
                    )
                    # Track accumulator delta within this batch to prevent batch bypass
                    local_accumulator_delta = 0.0

                    for act in actions:
                        act_type = act.get("type")
                        if act_type == "cancel":
                            order_id = str(act.get("order_id") or "")
                            info = order_info_by_id.get(order_id)
                            if info and info.get("side") == "BUY":
                                usd = float(info.get("usd") or 0.0)
                                pending_cancel_actions.append(act)
                                pending_cancel_usd += usd
                                planned_total_notional -= usd
                                planned_total_notional_shadow -= usd
                                planned_by_token_usd[token_id] = max(
                                    0.0, planned_by_token_usd.get(token_id, 0.0) - usd
                                )
                                planned_by_token_usd_shadow[token_id] = max(
                                    0.0, planned_by_token_usd_shadow.get(token_id, 0.0) - usd
                                )
                            else:
                                filtered_actions.append(act)
                            continue

                        if act_type != "place":
                            filtered_actions.append(act)
                            continue

                        side = str(act.get("side") or "").upper()
                        if side == "SELL" and sell_reconcile_lock_active:
                            blocked_reasons.add("sell_reconcile_lock")
                            continue
                        price = float(act.get("price") or ref_price or 0.0)
                        size = float(act.get("size") or 0.0)
                        if price <= 0 or size <= 0:
                            continue
                        if my_trades_unreliable and side == "BUY":
                            blocked_reasons.add("my_trades_unreliable")
                            continue
                        if side == "BUY":
                            order_notional = abs(size) * price
                            if topic_risk_overlay_enabled and topic_risk_level >= 1:
                                cfg_for_topic_risk = cfg_lowp if is_lowp else cfg
                                min_shares_cfg = float(cfg_for_topic_risk.get("min_order_shares") or 0.0)
                                api_min_shares = _orderbook_min_order_shares(ob)
                                if api_min_shares <= 0:
                                    api_min_shares = float(
                                        topic_risk_book_min_shares_cache.get(token_id) or 0.0
                                    )
                                    if api_min_shares <= 0:
                                        api_min_shares = _book_min_order_shares(
                                            clob_client, token_id, api_timeout_sec
                                        )
                                        topic_risk_book_min_shares_cache[token_id] = float(
                                            api_min_shares or 0.0
                                        )
                                effective_min_shares_guard = max(min_shares_cfg, api_min_shares)
                                min_order_usd_guard = float(cfg_for_topic_risk.get("min_order_usd") or 0.0)
                                effective_min_usd_guard = max(
                                    min_order_usd_guard,
                                    (effective_min_shares_guard * price)
                                    if effective_min_shares_guard > 0 and price > 0
                                    else 0.0,
                                )
                                if (
                                    effective_min_usd_guard > 0
                                    and order_notional + 1e-9 < effective_min_usd_guard
                                ):
                                    blocked_reasons.add("topic_risk_below_min")
                                    logger.info(
                                        "[HOLD] token_id=%s reason=topic_risk_below_min level=L%s order_usd=%.6f min_usd=%.6f",
                                        token_id,
                                        int(topic_risk_level),
                                        order_notional,
                                        effective_min_usd_guard,
                                    )
                                    continue
                            last_exit_ts = int(last_exit_ts_by_token.get(token_id) or 0)
                            hold_buy, hold_reason = _should_hold_reentry_buy(
                                now_ts=now_ts,
                                my_shares=my_shares,
                                last_exit_ts=last_exit_ts,
                                reentry_cooldown_sec=token_reentry_cooldown_sec,
                                signal_buy_shares=abs(delta),
                                order_buy_shares=abs(size),
                                order_buy_usd=order_notional,
                                force_buy_shares=reentry_force_buy_shares,
                                force_buy_usd=reentry_force_buy_usd,
                                eps=eps,
                            )
                            if hold_buy:
                                blocked_reasons.add("reentry_cooldown")
                                dedup_key = f"HOLD:{token_id}:reentry_cooldown"
                                should_log, suppressed = _log_dedup.should_log(dedup_key)
                                if should_log:
                                    remain = max(
                                        0, token_reentry_cooldown_sec - max(0, now_ts - last_exit_ts)
                                    )
                                    if suppressed > 0:
                                        logger.info(
                                            "[HOLD] token_id=%s reason=reentry_cooldown remain=%ss "
                                            "signal_shares=%.6f force_shares=%.6f (suppressed %d)",
                                            token_id,
                                            remain,
                                            abs(delta),
                                            reentry_force_buy_shares,
                                            suppressed,
                                        )
                                    else:
                                        logger.info(
                                            "[HOLD] token_id=%s reason=reentry_cooldown remain=%ss "
                                            "signal_shares=%.6f force_shares=%.6f",
                                            token_id,
                                            remain,
                                            abs(delta),
                                            reentry_force_buy_shares,
                                        )
                                continue
                            if hold_reason == "force_override":
                                dedup_key = f"BYPASS:{token_id}:reentry_force"
                                should_log, suppressed = _log_dedup.should_log(dedup_key)
                                if should_log:
                                    if suppressed > 0:
                                        logger.info(
                                            "[REENTRY_BYPASS] token_id=%s reason=force_override "
                                            "signal_shares=%.6f order_usd=%.6f "
                                            "force_shares=%.6f force_usd=%.6f (suppressed %d)",
                                            token_id,
                                            abs(delta),
                                            order_notional,
                                            reentry_force_buy_shares,
                                            reentry_force_buy_usd,
                                            suppressed,
                                        )
                                    else:
                                        logger.info(
                                            "[REENTRY_BYPASS] token_id=%s reason=force_override "
                                            "signal_shares=%.6f order_usd=%.6f force_shares=%.6f force_usd=%.6f",
                                            token_id,
                                            abs(delta),
                                            order_notional,
                                            reentry_force_buy_shares,
                                            reentry_force_buy_usd,
                                        )
                        if side == "BUY" and buy_window_sec > 0:
                            recent_token = float(recent_buy_by_token.get(token_id, 0.0))
                            if (
                                buy_window_max_usd_per_token > 0
                                and recent_token + order_notional > buy_window_max_usd_per_token
                            ):
                                blocked_reasons.add("buy_window_max_usd_per_token")
                                continue
                            if (
                                buy_window_max_usd_total > 0
                                and recent_buy_total + order_notional > buy_window_max_usd_total
                            ):
                                blocked_reasons.add("buy_window_max_usd_total")
                                continue

                        # CRITICAL: Check accumulator first (independent of position API)
                        if side == "BUY":
                            order_notional = abs(size) * price
                            cfg_for_acc = cfg_lowp if is_lowp else cfg
                            planned_token_notional_for_acc = float(planned_by_token_usd.get(token_id, 0.0))
                            acc_ok, acc_reason, acc_available = accumulator_check(
                                token_id,
                                order_notional,
                                state,
                                cfg_for_acc,
                                side=side,
                                local_delta=local_accumulator_delta,
                                planned_token_notional=planned_token_notional_for_acc,
                                planned_total_notional=planned_total_notional,
                            )
                            if not acc_ok:
                                # Get accumulator actual values for detailed logging
                                accumulator = state.get("buy_notional_accumulator")
                                acc_current = 0.0
                                if isinstance(accumulator, dict):
                                    for acc_data in accumulator.values():
                                        if isinstance(acc_data, dict):
                                            acc_current += float(acc_data.get("usd", 0.0))
                                max_total = float(cfg_for_acc.get("accumulator_max_total_usd") or 0)

                                # Try to shrink order to fit within accumulator limit
                                if acc_available > 0:
                                    min_order_usd = float(cfg_for_acc.get("min_order_usd") or 0.0)
                                    min_order_shares = float(cfg_for_acc.get("min_order_shares") or 0.0)
                                    effective_min_usd = max(min_order_usd, min_order_shares * price)

                                    if acc_available >= effective_min_usd:
                                        # Shrink order to available amount
                                        old_size = size
                                        old_usd = order_notional
                                        size = acc_available / price
                                        act["size"] = size
                                        order_notional = acc_available
                                        logger.warning(
                                            "[ACCUMULATOR_SHRINK] token_id=%s old_usd=%s new_usd=%s acc_current=%s acc_limit=%s planned_total=%s is_lowp=%s reason=%s",
                                            token_id,
                                            old_usd,
                                            acc_available,
                                            acc_current,
                                            max_total,
                                            planned_total_notional,
                                            is_lowp,
                                            acc_reason,
                                        )
                                        # Continue with shrunken order (don't skip)
                                    else:
                                        # Available amount is below minimum order size
                                        logger.warning(
                                            "[ACCUMULATOR_BLOCK] token_id=%s order_usd=%s acc_current=%s local_delta=%s acc_limit=%s acc_available=%s min_usd=%s planned_total=%s is_lowp=%s reason=%s",
                                            token_id,
                                            order_notional,
                                            acc_current,
                                            local_accumulator_delta,
                                            max_total,
                                            acc_available,
                                            effective_min_usd,
                                            planned_total_notional,
                                            is_lowp,
                                            acc_reason,
                                        )
                                        blocked_reasons.add(acc_reason or "accumulator_check")
                                        continue
                                else:
                                    # No room available
                                    logger.warning(
                                        "[ACCUMULATOR_BLOCK] token_id=%s order_usd=%s acc_current=%s local_delta=%s acc_limit=%s planned_total=%s is_lowp=%s reason=%s",
                                        token_id,
                                        order_notional,
                                        acc_current,
                                        local_accumulator_delta,
                                        max_total,
                                        planned_total_notional,
                                        is_lowp,
                                        acc_reason,
                                    )
                                    blocked_reasons.add(acc_reason or "accumulator_check")
                                    continue

                        planned_token_notional = float(planned_by_token_usd.get(token_id, 0.0))
                        planned_token_notional_shadow = float(
                            planned_by_token_usd_shadow.get(token_id, 0.0)
                        )
                        planned_total_notional_risk = max(
                            planned_total_notional, planned_total_notional_shadow
                        )
                        planned_token_notional_risk = max(
                            planned_token_notional, planned_token_notional_shadow
                        )

                        # CRITICAL ALERT: Detect position sync anomaly
                        # If my_shares=0 but shadow shows significant recent orders, position may not be synced
                        if (
                            side == "BUY"
                            and my_shares <= 0.0
                            and planned_token_notional_shadow > 2.0
                            and planned_token_notional_shadow > planned_token_notional + 1.0
                        ):
                            logger.warning(
                                "[ALERT] position_sync_anomaly detected: my_shares=%.2f but "
                                "shadow_notional=%.2f (planned=%.2f) token=%s - position may not be synced, "
                                "risk baseline weakened",
                                my_shares,
                                planned_token_notional_shadow,
                                planned_token_notional,
                                token_id,
                            )

                        cfg_for_action = cfg_lowp if (is_lowp and side == "BUY") else cfg
                        reserved_sell_open = 0.0
                        if side == "SELL":
                            for oo in open_orders:
                                try:
                                    if str(oo.get("side") or "").upper() != "SELL":
                                        continue
                                    reserved_sell_open += float(
                                        oo.get("size") or oo.get("original_size") or 0.0
                                    )
                                except Exception:
                                    continue
                        sell_buffer_shares = float(cfg_for_action.get("sell_available_buffer_shares") or 0.01)
                        available_shares = max(
                            0.0,
                            float(my_shares)
                            - max(0.0, reserved_sell_open)
                            - max(0.0, sell_buffer_shares),
                        )
                        ok, reason = risk_check(
                            token_key,
                            size,
                            my_shares,
                            price,
                            cfg_for_action,
                            token_title=token_title,
                            side=side,
                            available_shares=available_shares if side == "SELL" else None,
                            planned_total_notional=planned_total_notional_risk,
                            planned_token_notional=planned_token_notional_risk,
                            cumulative_total_usd=None,
                            cumulative_token_usd=None,
                        )
                        if not ok:
                            resized = _shrink_on_risk_limit(
                                act,
                                max_notional_total,
                                planned_total_notional_risk,
                                float(cfg_for_action.get("max_notional_per_token") or 0.0),
                                planned_token_notional_risk,
                                float(cfg_for_action.get("min_order_usd") or 0.0),
                                float(cfg_for_action.get("min_order_shares") or 0.0),
                                token_key,
                                token_id,
                                logger,
                            )
                            if resized is None:
                                if has_any_place and pending_cancel_actions:
                                    planned_total_notional += pending_cancel_usd
                                    planned_by_token_usd[token_id] = token_planned_before
                                    planned_total_notional_shadow += pending_cancel_usd
                                    planned_by_token_usd_shadow[token_id] = (
                                        token_planned_before_shadow
                                    )
                                    pending_cancel_actions = []
                                    pending_cancel_usd = 0.0
                                blocked_reasons.add(reason or "risk_check")
                                continue

                            act, allowed_usd = resized
                            price = float(act.get("price") or 0.0)
                            size = float(act.get("size") or 0.0)

                            # CRITICAL: Re-check accumulator after shrink
                            shrink_notional = abs(size) * price
                            acc_ok_shrink, acc_reason_shrink, _acc_available_shrink = (
                                accumulator_check(
                                    token_id,
                                    shrink_notional,
                                    state,
                                    cfg_for_action,
                                    side=side,
                                    local_delta=local_accumulator_delta,
                                )
                            )
                            if not acc_ok_shrink:
                                logger.warning(
                                    "[ACCUMULATOR_BLOCK_SHRINK] token_id=%s shrink_usd=%s "
                                    "current_delta=%s reason=%s",
                                    token_id,
                                    shrink_notional,
                                    local_accumulator_delta,
                                    acc_reason_shrink,
                                )
                                if has_any_place and pending_cancel_actions:
                                    planned_total_notional += pending_cancel_usd
                                    planned_by_token_usd[token_id] = token_planned_before
                                    planned_total_notional_shadow += pending_cancel_usd
                                    planned_by_token_usd_shadow[token_id] = (
                                        token_planned_before_shadow
                                    )
                                    pending_cancel_actions = []
                                    pending_cancel_usd = 0.0
                                blocked_reasons.add(acc_reason_shrink or "accumulator_check_shrink")
                                continue

                            planned_token_notional = float(planned_by_token_usd.get(token_id, 0.0))
                            planned_token_notional_shadow = float(
                                planned_by_token_usd_shadow.get(token_id, 0.0)
                            )
                            planned_total_notional_risk = max(
                                planned_total_notional, planned_total_notional_shadow
                            )
                            planned_token_notional_risk = max(
                                planned_token_notional, planned_token_notional_shadow
                            )
                            ok2, reason2 = risk_check(
                                token_key,
                                size,
                                my_shares,
                                price,
                                cfg_for_action,
                                token_title=token_title,
                                side=side,
                                planned_total_notional=planned_total_notional_risk,
                                planned_token_notional=planned_token_notional_risk,
                                cumulative_total_usd=None,
                                cumulative_token_usd=None,
                            )
                            if not ok2:
                                if has_any_place and pending_cancel_actions:
                                    planned_total_notional += pending_cancel_usd
                                    planned_by_token_usd[token_id] = token_planned_before
                                    planned_total_notional_shadow += pending_cancel_usd
                                    planned_by_token_usd_shadow[token_id] = (
                                        token_planned_before_shadow
                                    )
                                    pending_cancel_actions = []
                                    pending_cancel_usd = 0.0
                                blocked_reasons.add(reason2 or reason or "risk_check")
                                continue

                        if pending_cancel_actions:
                            filtered_actions.extend(pending_cancel_actions)
                            pending_cancel_actions = []
                            pending_cancel_usd = 0.0

                        filtered_actions.append(act)
                        if side == "BUY":
                            usd = abs(size) * price
                            planned_total_notional += usd
                            planned_total_notional_shadow += usd
                            planned_by_token_usd[token_id] = (
                                planned_by_token_usd.get(token_id, 0.0) + usd
                            )
                            planned_by_token_usd_shadow[token_id] = (
                                planned_by_token_usd_shadow.get(token_id, 0.0) + usd
                            )
                            # CRITICAL: Update local accumulator delta to prevent batch bypass
                            local_accumulator_delta += usd

                    if has_any_place and pending_cancel_actions:
                        planned_total_notional += pending_cancel_usd
                        planned_by_token_usd[token_id] = token_planned_before
                        planned_total_notional_shadow += pending_cancel_usd
                        planned_by_token_usd_shadow[token_id] = token_planned_before_shadow
                        pending_cancel_actions = []
                        pending_cancel_usd = 0.0
                    elif (not has_any_place) and pending_cancel_actions:
                        filtered_actions.extend(pending_cancel_actions)

                    if not filtered_actions:
                        if has_any_place:
                            reason_text = (
                                ",".join(sorted(blocked_reasons))
                                if blocked_reasons
                                else "risk_check"
                            )
                            dedup_key = f"NOOP:{token_id}:{reason_text}"
                            should_log, suppressed = _log_dedup.should_log(dedup_key)
                            if should_log:
                                if suppressed > 0:
                                    logger.info(
                                        "[NOOP] token_id=%s reason=%s (suppressed %d)",
                                        token_id, reason_text, suppressed,
                                    )
                                else:
                                    logger.info("[NOOP] token_id=%s reason=%s", token_id, reason_text)
                        continue
                    actions = filtered_actions
                    logger.info(
                        "[ACTION] token_id=%s topic_risk=%s -> %s",
                        token_id,
                        topic_risk_action_hint,
                        actions,
                    )

                    is_reprice = _is_pure_reprice(actions)
                    missing_freeze = state.setdefault("missing_data_freeze", {})
                    if not missing_data and token_id:
                        state.get("missing_buy_attempts", {}).pop(token_id, None)
                        ref_price_for_cap = None
                        if (
                            "ref_price" in locals()
                            and isinstance(ref_price, (int, float))
                            and ref_price > 0
                        ):
                            ref_price_for_cap = float(ref_price)
                        else:
                            last_mid = (
                                state.get("last_mid_price_by_token_id", {})
                                .get(token_id)
                            )
                            if last_mid:
                                ref_price_for_cap = float(last_mid)
                            else:
                                for act in actions:
                                    act_price = act.get("price")
                                    if isinstance(act_price, (int, float)) and act_price > 0:
                                        ref_price_for_cap = float(act_price)
                                        break
                        max_notional_per_token = float(
                            (
                                cfg_lowp.get("max_notional_per_token")
                                if "cfg_lowp" in locals()
                                else None
                            )
                            or cfg.get("max_notional_per_token")
                            or 0.0
                        )
                        cap_shares_notional = (
                            (max_notional_per_token / ref_price_for_cap)
                            if max_notional_per_token > 0 and ref_price_for_cap
                            else float("inf")
                        )
                        cap_limit = min(cap_shares, cap_shares_notional)
                        existing_freeze = missing_freeze.get(token_id)
                        active_streak_freeze = (
                            isinstance(existing_freeze, dict)
                            and existing_freeze.get("reason") == "missing_streak"
                            and int(existing_freeze.get("expires_at") or 0) > now_ts
                        )
                        if not active_streak_freeze:
                            if my_shares <= cap_limit + eps:
                                missing_freeze.pop(token_id, None)
                            else:
                                missing_freeze[token_id] = {
                                    "ts": now_ts,
                                    "shares": my_shares,
                                    "cap": cap_limit,
                                    "reason": "position_exceeds_cap",
                                }
                                logger.warning(
                                    "[FREEZE] token_id=%s reason=position_exceeds_cap shares=%s cap=%s",
                                    token_id,
                                    my_shares,
                                    cap_limit,
                                )
                        elif my_shares > cap_limit + eps:
                            existing_freeze["shares"] = my_shares
                            existing_freeze["cap"] = cap_limit
                    if token_id and token_id in missing_freeze and any(
                        act.get("type") == "place"
                        and str(act.get("side") or "").upper() == "BUY"
                        for act in actions
                    ):
                        logger.warning(
                            "[SKIP] token_id=%s reason=missing_data_freeze",
                            token_id,
                        )
                        continue
                    if missing_data and any(
                        act.get("type") == "place"
                        and str(act.get("side") or "").upper() == "BUY"
                        for act in actions
                    ):
                        missing_limit = int(cfg.get("max_missing_buy_attempts") or 0)
                        if missing_limit <= 0:
                            logger.warning(
                                "[SKIP] token_id=%s reason=missing_data_buy_block limit=%s",
                                token_id,
                                missing_limit,
                            )
                            continue
                        missing_counts = state.setdefault("missing_buy_attempts", {})
                        missing_counts[token_id] = int(missing_counts.get(token_id) or 0) + 1
                        if missing_counts[token_id] > missing_limit:
                            logger.warning(
                                "[SKIP] token_id=%s reason=missing_data_buy_limit count=%s "
                                "limit=%s",
                                token_id,
                                missing_counts[token_id],
                                missing_limit,
                            )
                            continue
                    has_exit_sell_place = any(
                        act.get("type") == "place"
                        and str(act.get("side") or "").upper() == "SELL"
                        for act in actions
                    ) and is_exiting
                    has_exit_flow_sell_place = any(
                        act.get("type") == "place"
                        and str(act.get("side") or "").upper() == "SELL"
                        and bool(act.get("_exit_flow"))
                        for act in actions
                    ) and is_exiting
                    ignore_place_backoff = bool(
                        cfg.get("exit_ignore_place_backoff", True)
                    ) and has_exit_sell_place and (not has_exit_flow_sell_place) and ((not sell_reconcile_lock_active) or must_exit_active)
                    if (
                        place_backoff_active
                        and any(act.get("type") == "place" for act in actions)
                        and (not ignore_place_backoff)
                    ):
                        dedup_key = f"SKIP:{token_id}:place_backoff"
                        should_log, suppressed = _log_dedup.should_log(dedup_key)
                        if should_log:
                            if suppressed > 0:
                                logger.info(
                                    "[SKIP] token_id=%s reason=place_backoff until=%s (suppressed %d)",
                                    token_id, place_fail_until, suppressed,
                                )
                            else:
                                logger.info(
                                    "[SKIP] token_id=%s reason=place_backoff until=%s",
                                    token_id, place_fail_until,
                                )
                        continue
                    if place_backoff_active and ignore_place_backoff:
                        dedup_key = f"BYPASS:{token_id}:place_backoff_exit_sell"
                        should_log, suppressed = _log_dedup.should_log(dedup_key)
                        if should_log:
                            if suppressed > 0:
                                logger.info(
                                    "[BACKOFF_BYPASS] token_id=%s reason=exit_sell_place (suppressed %d)",
                                    token_id,
                                    suppressed,
                                )
                            else:
                                logger.info(
                                    "[BACKOFF_BYPASS] token_id=%s reason=exit_sell_place",
                                    token_id,
                                )
                    ignore_cd = bool(cfg.get("exit_ignore_cooldown", True)) and is_exiting
                    if cooldown_active and (not ignore_cd) and (not is_reprice):
                        dedup_key = f"SKIP:{token_id}:cooldown"
                        should_log, suppressed = _log_dedup.should_log(dedup_key)
                        if should_log:
                            if suppressed > 0:
                                logger.info(
                                    "[SKIP] token_id=%s reason=cooldown (suppressed %d)",
                                    token_id, suppressed,
                                )
                            else:
                                logger.info("[SKIP] token_id=%s reason=cooldown", token_id)
                        continue

                    updated_orders = apply_actions(
                        clob_client,
                        actions,
                        open_orders,
                        now_ts,
                        args.dry_run,
                        cfg=cfg,
                        state=state,
                        planned_by_token_usd=planned_by_token_usd_shadow,
                    )
                    sell_health_round["actions"] = int(sell_health_round.get("actions") or 0) + sum(
                        1
                        for act in actions
                        if act.get("type") == "place"
                        and str(act.get("side") or "").upper() == "SELL"
                    )
                    if updated_orders:
                        state.setdefault("open_orders", {})[token_id] = updated_orders
                    else:
                        state.get("open_orders", {}).pop(token_id, None)
                    _prune_order_ts_by_id(state)
                    _refresh_managed_order_ids(state)
                    (
                        planned_total_notional,
                        planned_by_token_usd,
                        order_info_by_id,
                        _shadow_buy_usd,
                    ) = _calc_planned_notional_with_fallback(
                        my_by_token_id,
                        state.get("open_orders", {}),
                        state.get("last_mid_price_by_token_id", {}),
                        max_position_usd_per_token,
                        state,
                        now_ts,
                        shadow_ttl_sec,
                        fallback_mid_price,
                        logger,
                        include_shadow=False,
                    )
                    (
                        planned_total_notional_shadow,
                        planned_by_token_usd_shadow,
                        _shadow_order_info_by_id,
                        _shadow_buy_usd,
                    ) = _calc_planned_notional_with_fallback(
                        my_by_token_id,
                        state.get("open_orders", {}),
                        state.get("last_mid_price_by_token_id", {}),
                        max_position_usd_per_token,
                        state,
                        now_ts,
                        shadow_ttl_sec,
                        fallback_mid_price,
                        logger,
                        include_shadow=True,
                    )

                    has_any_place_final = any(
                        act.get("type") == "place" for act in actions
                    )
                    if (
                        cooldown_sec > 0
                        and actions
                        and has_any_place_final
                        and (not ignore_cd)
                        and (not is_reprice)
                    ):
                        state.setdefault("cooldown_until", {})[token_id] = (
                            now_ts + cooldown_sec
                        )
                    if is_reprice:
                        state.setdefault("last_reprice_ts_by_token", {})[token_id] = now_ts

                    probed = set(state.get("probed_token_ids", []))
                    probed.add(token_id)
                    state["probed_token_ids"] = sorted(probed)
                continue

            if t_now is None and not action_seen and not topic_active:
                continue

            if t_now is None:
                action_delta = buy_sum - sell_sum
                if action_seen and abs(action_delta) > eps:
                    d_target = action_delta
                else:
                    if not topic_active:
                        continue
                    d_target = 0.0
            elif t_last is None:
                d_target = float(t_now)
            else:
                d_target = float(t_now) - float(t_last)
            topic_active = topic_mode and phase in ("LONG", "EXITING")
            actions_unreliable_until = int(state.get("actions_unreliable_until") or 0)
            actions_unreliable = actions_unreliable_until > now_ts
            force_exit_by_confirm_drop = False
            if has_sell and d_target >= -eps:
                d_target = -max(sell_sum, eps)
                logger.info(
                    "[SIGNAL] SELL forced_by_action token_id=%s d_target=%s sell_sum=%s",
                    token_id,
                    d_target,
                    sell_sum,
                )
            if d_target < -eps:
                if has_sell or is_exiting:
                    state.setdefault("sell_confirm", {}).pop(token_id, None)
                else:
                    sell_confirm = state.setdefault("sell_confirm", {})
                    token_confirm = sell_confirm.get(token_id) or {"count": 0, "first_ts": now_ts}
                    if actions_unreliable:
                        token_confirm["first_ts"] = now_ts
                        sell_confirm[token_id] = token_confirm
                        state["actions_replay_from_ms"] = max(
                            0, now_ms - actions_replay_window_sec * 1000
                        )
                        logger.info(
                            "[HOLD] token_id=%s reason=actions_unreliable d_target=%s confirm=%s/%s replay_from_ms=%s",
                            token_id,
                            d_target,
                            token_confirm.get("count"),
                            sell_confirm_max,
                            state.get("actions_replay_from_ms"),
                        )
                        d_target = 0.0
                    else:
                        if now_ts - int(token_confirm.get("first_ts") or now_ts) > sell_confirm_window_sec:
                            token_confirm = {"count": 0, "first_ts": now_ts}
                        token_confirm["count"] = int(token_confirm.get("count") or 0) + 1
                        token_confirm["first_ts"] = int(token_confirm.get("first_ts") or now_ts)
                        sell_confirm[token_id] = token_confirm
                        if token_confirm["count"] < sell_confirm_max:
                            state["actions_replay_from_ms"] = max(
                                0, now_ms - actions_replay_window_sec * 1000
                            )
                            logger.info(
                                "[HOLD] token_id=%s reason=no_sell_action d_target=%s confirm=%s/%s replay_from_ms=%s",
                                token_id,
                                d_target,
                                token_confirm["count"],
                                sell_confirm_max,
                                state.get("actions_replay_from_ms"),
                            )
                            d_target = 0.0
                        else:
                            drop_shares = max(0.0, -float(d_target))
                            base_shares = max(0.0, float(t_last or 0.0))
                            drop_threshold = 0.0
                            if sell_confirm_force_ratio > 0 and base_shares > 0:
                                drop_threshold = max(
                                    drop_threshold, base_shares * sell_confirm_force_ratio
                                )
                            if sell_confirm_force_shares > 0:
                                drop_threshold = max(
                                    drop_threshold, sell_confirm_force_shares
                                )
                            significant_drop = drop_threshold > 0 and drop_shares >= drop_threshold
                            if significant_drop:
                                logger.info(
                                    "[FORCE] token_id=%s reason=sell_confirm_drop d_target=%s drop=%s threshold=%s ratio=%s base=%s",
                                    token_id,
                                    d_target,
                                    drop_shares,
                                    drop_threshold,
                                    sell_confirm_force_ratio,
                                    base_shares,
                                )
                                sell_confirm.pop(token_id, None)
                                force_exit_by_confirm_drop = True
                            else:
                                logger.info(
                                    "[HOLD] token_id=%s reason=no_sell_after_confirm d_target=%s confirm=%s/%s drop=%s threshold=%s",
                                    token_id,
                                    d_target,
                                    token_confirm["count"],
                                    sell_confirm_max,
                                    drop_shares,
                                    drop_threshold,
                                )
                                token_confirm["count"] = sell_confirm_max
                                sell_confirm[token_id] = token_confirm
                                d_target = 0.0
            else:
                state.setdefault("sell_confirm", {}).pop(token_id, None)

            # Promote significant non-action sell-drop to EXITING immediately.
            # This avoids getting stuck in non-exiting small-sell accumulator mode.
            if (
                force_exit_by_confirm_drop
                and topic_mode
                and (my_shares > eps or open_orders_count > 0)
            ):
                _mark_must_exit_token(
                    state,
                    token_id,
                    now_ts,
                    source="sell_confirm_drop",
                    target_sell_ms=int(last_target_sell_action_ts_by_token.get(token_id) or 0),
                )
                st = topic_state.get(token_id) or {}
                st = {
                    "phase": "EXITING",
                    "first_buy_ts": int(st.get("first_buy_ts") or now_ts),
                    "first_sell_ts": now_ts,
                    "entry_sized": bool(st.get("entry_sized")),
                    "did_probe": bool(st.get("did_probe")),
                    "target_peak": float(st.get("target_peak") or float(t_now or 0.0)),
                    "entry_buy_accum": float(st.get("entry_buy_accum") or 0.0),
                    "primary_entry_source": str(
                        st.get("primary_entry_source") or primary_entry_source or ""
                    ).strip().lower(),
                    "desired_shares": 0.0,
                }
                topic_state[token_id] = st
                phase = "EXITING"
                is_exiting = True
                topic_active = True
                logger.info(
                    "[FORCE] token_id=%s reason=sell_confirm_drop promote_to=EXITING my_shares=%s open_orders=%s",
                    token_id,
                    my_shares,
                    open_orders_count,
                )
            if abs(d_target) <= eps and not topic_active:
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue

            if token_id in orderbooks:
                ob = orderbooks[token_id]
            else:
                ob = get_orderbook(clob_read_client, token_id, api_timeout_sec)
                orderbooks[token_id] = ob

            best_bid = ob.get("best_bid")
            best_ask = ob.get("best_ask")
            if best_bid is not None and best_ask is not None and best_bid > best_ask:
                logger.warning(
                    "[SKIP] invalid book bid>ask token_id=%s best_bid=%s best_ask=%s",
                    token_id,
                    best_bid,
                    best_ask,
                )
                orderbooks.pop(token_id, None)
                ob = get_orderbook(clob_read_client, token_id, api_timeout_sec)
                orderbooks[token_id] = ob
                best_bid = ob.get("best_bid")
                best_ask = ob.get("best_ask")
                if best_bid is not None and best_ask is not None and best_bid > best_ask:
                    continue
            ref_price = _mid_price(ob)
            if ref_price is None or ref_price <= 0:
                dedup_key = f"WARN:{token_id}:invalid_orderbook"
                should_log, suppressed = _log_dedup.should_log(dedup_key)
                if should_log:
                    if suppressed > 0:
                        logger.warning(
                            "[WARN] invalid_orderbook token_id=%s best_bid=%s best_ask=%s (suppressed %d)",
                            token_id,
                            best_bid,
                            best_ask,
                            suppressed,
                        )
                    else:
                        logger.warning(
                            "[WARN] invalid_orderbook token_id=%s best_bid=%s best_ask=%s",
                            token_id,
                            best_bid,
                            best_ask,
                        )
                closed_now = _record_orderbook_empty(
                    state,
                    token_id,
                    logger,
                    cfg,
                    now_ts,
                )
                if closed_now:
                    logger.info(
                        "[SKIP] closed_by_orderbook_empty token_id=%s",
                        token_id,
                    )
                dedup_key = f"NOOP:{token_id}:orderbook_empty"
                should_log, suppressed = _log_dedup.should_log(dedup_key)
                if should_log:
                    if suppressed > 0:
                        logger.info(
                            "[NOOP] token_id=%s reason=orderbook_empty (suppressed %d)",
                            token_id, suppressed,
                        )
                    else:
                        logger.info("[NOOP] token_id=%s reason=orderbook_empty", token_id)
                continue
            _clear_orderbook_empty(state, token_id)
            state.setdefault("last_mid_price_by_token_id", {})[token_id] = float(ref_price)
            state["last_mid_price_update_ts"] = now_ts
            is_lowp = _is_lowp_token(cfg, float(ref_price))
            cfg_lowp = _lowp_cfg(cfg, is_lowp)
            ratio_base = float(cfg.get("follow_ratio") or 0.0)
            ratio_buy = _lowp_buy_ratio(cfg, is_lowp)
            if is_lowp and (t_now is not None) and (t_last is not None):
                if float(t_now) - float(t_last) > 0:
                    logger.info(
                        "[LOWP] token_id=%s ref_price=%.4f ratio=%.4f->%.4f "
                        "cap_token=%.2f->%.2f min/max_usd=%s/%s",
                        token_id,
                        float(ref_price),
                        ratio_base,
                        ratio_buy,
                        float(cfg.get("max_notional_per_token") or 0.0),
                        float(cfg_lowp.get("max_notional_per_token") or 0.0),
                        cfg_lowp.get("min_order_usd"),
                        cfg_lowp.get("max_order_usd"),
                    )

            cap_shares = float("inf")
            if max_position_usd_per_token > 0:
                cap_shares = max_position_usd_per_token / ref_price

            max_notional_per_token = float(
                cfg_lowp.get("max_notional_per_token") or cfg.get("max_notional_per_token") or 0.0
            )
            cap_shares_notional = (
                (max_notional_per_token / ref_price) if max_notional_per_token > 0 else float("inf")
            )

            if topic_risk_overlay_enabled and topic_mode:
                prev_level = int(topic_risk_level)
                prev_eval_ts = int(st.get("topic_risk_last_eval_ts") or 0)
                if prev_eval_ts > 0:
                    topic_risk_score = _topic_risk_decay_score(
                        topic_risk_score,
                        max(0, now_ts - prev_eval_ts),
                        topic_risk_flip_window_sec,
                    )
                signal_side = "BUY" if d_target > eps else ("SELL" if d_target < -eps else "")
                last_side = str(st.get("topic_risk_last_side") or "")
                last_side_ts = int(st.get("topic_risk_last_side_ts") or 0)
                if signal_side:
                    if last_side and signal_side != last_side:
                        flip_gap = max(0, now_ts - last_side_ts) if last_side_ts > 0 else 0
                        if last_side_ts <= 0 or flip_gap <= topic_risk_flip_window_sec:
                            topic_risk_score += 1.0
                            st["topic_risk_last_flip_ts"] = int(now_ts)
                    st["topic_risk_last_side"] = signal_side
                    st["topic_risk_last_side_ts"] = int(now_ts)
                if topic_risk_freeze_buy_until > now_ts:
                    topic_risk_level = 2
                elif topic_risk_score >= topic_risk_l2_threshold:
                    topic_risk_level = 2
                    topic_risk_freeze_buy_until = int(now_ts + topic_risk_l2_freeze_sec)
                elif topic_risk_score >= topic_risk_l1_threshold:
                    topic_risk_level = 1
                    if topic_risk_freeze_buy_until <= now_ts:
                        topic_risk_freeze_buy_until = 0
                else:
                    topic_risk_level = 0
                    if topic_risk_freeze_buy_until <= now_ts:
                        topic_risk_freeze_buy_until = 0
                st["topic_risk_score"] = float(topic_risk_score)
                st["topic_risk_level"] = int(topic_risk_level)
                st["topic_risk_freeze_buy_until"] = int(topic_risk_freeze_buy_until)
                st["topic_risk_last_eval_ts"] = int(now_ts)
                topic_state[token_id] = st
                if topic_risk_level != prev_level:
                    logger.info(
                        "[TOPIC_RISK] token_id=%s level=L%s score=%.3f freeze_buy_until=%s",
                        token_id,
                        topic_risk_level,
                        topic_risk_score,
                        topic_risk_freeze_buy_until,
                    )
                topic_risk_action_hint = (
                    f"L{int(topic_risk_level)}|score={topic_risk_score:.2f}|freeze={int(topic_risk_freeze_buy_until)}"
                )

            use_ratio = ratio_buy if d_target > 0 else ratio_base
            overlay_buy_frozen = False
            if topic_risk_overlay_enabled and d_target > 0:
                if topic_risk_level >= 2 and topic_risk_freeze_buy_until > now_ts:
                    overlay_buy_frozen = True
                    use_ratio = 0.0
                elif topic_risk_level >= 1:
                    use_ratio *= topic_risk_l1_follow_mult
                    token_deadband_shares = max(
                        token_deadband_shares,
                        base_deadband_shares * topic_risk_l1_deadband_mult,
                    )
                    token_reentry_cooldown_sec = max(
                        token_reentry_cooldown_sec,
                        int(round(token_reentry_cooldown_sec * topic_risk_l1_reentry_mult)),
                    )
            d_my = use_ratio * d_target
            if d_target > 0:
                logger.info(
                    "[SIGNAL] BUY token_id=%s d_target=%s d_my=%s my_shares=%s",
                    token_id,
                    d_target,
                    d_my,
                    my_shares,
                )
            elif d_target < 0:
                logger.info(
                    "[SIGNAL] SELL token_id=%s d_target=%s d_my=%s my_shares=%s",
                    token_id,
                    d_target,
                    d_my,
                    my_shares,
                )
            my_target = my_shares + d_my
            if my_target < 0:
                my_target = 0.0
            if d_target > 0 and buy_blocked_by_boot:
                my_target = min(my_target, my_shares)
            if d_target > 0:
                my_target = min(my_target, cap_shares, cap_shares_notional)
            else:
                if my_target > cap_shares:
                    my_target = cap_shares

            if topic_active:
                probe_usd = float(
                    cfg_lowp.get("probe_order_usd")
                    or cfg_lowp.get("min_order_usd")
                    or 5.0
                )
                probe_shares = probe_usd / ref_price

                if phase == "LONG":
                    if buy_blocked_by_boot:
                        my_target = min(my_target, my_shares)
                        _block_key = f"BOOT_BLOCK_BUY:{token_id}"
                        _should_log, _supp = _log_dedup.should_log(_block_key)
                        if _should_log:
                            logger.info(
                                "[SKIP] boot_block_buy token_id=%s follow_new_topics_only=%s scope=%s",
                                token_id,
                                follow_new_topics_only,
                                boot_scope,
                            )

                    if (not buy_blocked_by_boot) and (not st.get("did_probe")) and my_shares <= eps:
                        my_target = min(cap_shares, cap_shares_notional, my_shares + probe_shares)
                        probe_attempted = True
                        dedup_key = f"TOPIC_PROBE:{token_id}"
                        should_log, suppressed = _log_dedup.should_log(dedup_key)
                        if should_log:
                            if suppressed > 0:
                                logger.info(
                                    "[TOPIC] PROBE token_id=%s target=%s (suppressed %d)",
                                    token_id, my_target, suppressed,
                                )
                            else:
                                logger.info("[TOPIC] PROBE token_id=%s target=%s", token_id, my_target)

                    if (not buy_blocked_by_boot) and (not st.get("entry_sized")):
                        first_buy_ts = int(st.get("first_buy_ts") or now_ts)
                        if now_ts - first_buy_ts >= entry_settle_sec:
                            base = float(t_now) if t_now is not None else float(
                                st.get("target_peak") or 0.0
                            )
                            # FIX: When both t_now and target_peak are missing,
                            # fall back to entry_buy_accum (action-based estimate).
                            if base <= 0:
                                base = float(st.get("entry_buy_accum") or 0.0)
                            base_source = (
                                "t_now" if t_now is not None else
                                "target_peak" if float(st.get("target_peak") or 0.0) > 0 else
                                "entry_buy_accum"
                            )
                            ratio = ratio_buy
                            desired = 0.0
                            if base > 0 and ratio > 0:
                                desired = min(cap_shares, cap_shares_notional, ratio * base)
                            desired = max(
                                desired,
                                min(cap_shares, cap_shares_notional, my_shares + probe_shares),
                            )
                            st["desired_shares"] = float(desired)
                            st["entry_sized"] = True
                            topic_state[token_id] = st
                            logger.info(
                                "[TOPIC] SIZE token_id=%s desired=%s base=%s base_source=%s",
                                token_id,
                                desired,
                                base,
                                base_source,
                            )

                    base = float(t_now) if t_now is not None else float(
                        st.get("target_peak") or 0.0
                    )
                    # FIX: Same fallback for continuous tracking after entry_sized.
                    if base <= 0:
                        base = float(st.get("entry_buy_accum") or 0.0)
                    if not buy_blocked_by_boot:
                        desired_locked = float(st.get("desired_shares") or 0.0)
                        desired_target = desired_locked
                        if base > 0 and ratio_buy > 0:
                            desired_target = min(cap_shares, cap_shares_notional, ratio_buy * base)
                        if desired_target > 0:
                            st["desired_shares"] = float(desired_target)
                            topic_state[token_id] = st
                            my_target = max(my_shares, min(cap_shares, cap_shares_notional, desired_target))

                elif phase == "EXITING":
                    my_target = 0.0

            # Guard: if target snapshot temporarily misses this token, don't drop desired to 0
            # while we still have outstanding orders (prevents churn & "stuck probe" at stale price).
            if phase == "LONG" and (t_now is None) and (not action_seen) and open_orders_count > 0:
                hold_sec = int(cfg.get("missing_hold_sec") or entry_settle_sec or 60)
                last_seen = int(state.get("target_last_seen_ts", {}).get(token_id) or 0)
                if last_seen > 0 and (now_ts - last_seen) <= hold_sec:
                    prev_intent_tmp = state.get("intent_keys", {}).get(token_id)
                    prev_desired = (
                        float(prev_intent_tmp.get("desired_shares") or 0.0)
                        if isinstance(prev_intent_tmp, dict)
                        else 0.0
                    )
                    if prev_desired > my_target + eps:
                        logger.info(
                            "[HOLD] token_id=%s reason=missing_target prev_desired=%s "
                            "my_target=%s last_seen=%s hold_sec=%s",
                            token_id,
                            prev_desired,
                            my_target,
                            last_seen,
                            hold_sec,
                        )
                        my_target = min(cap_shares, cap_shares_notional, prev_desired)
            delta = my_target - my_shares
            prev_intent = state.get("intent_keys", {}).get(token_id)
            if delta > eps:
                desired_side = "BUY"
            elif delta < -eps:
                desired_side = "SELL"
            elif isinstance(prev_intent, dict) and prev_intent.get("desired_side"):
                desired_side = str(prev_intent.get("desired_side")).upper()
            else:
                desired_side = "BUY"
            phase_for_intent = phase if topic_mode else ("LONG" if desired_side == "BUY" else "EXITING")
            intent_key = _intent_key(phase_for_intent, desired_side, my_target)
            intent_changed, desired_down = _update_intent_state(
                state, token_id, intent_key, eps, logger
            )
            if open_orders and intent_changed:
                opposite_orders = [
                    order
                    for order in open_orders
                    if str(order.get("side") or "").upper() != desired_side
                ]
                same_side_orders = [
                    order
                    for order in open_orders
                    if str(order.get("side") or "").upper() == desired_side
                ]
                cancel_actions = []
                if opposite_orders:
                    cancel_actions.extend(
                        [
                            {"type": "cancel", "order_id": order.get("order_id")}
                            for order in opposite_orders
                            if order.get("order_id")
                        ]
                    )
                if desired_down or phase_for_intent == "EXITING":
                    cancel_actions.extend(
                        [
                            {"type": "cancel", "order_id": order.get("order_id")}
                            for order in same_side_orders
                            if order.get("order_id")
                        ]
                    )
                if cancel_actions:
                    logger.info(
                        "[CANCEL_INTENT] token_id=%s opposite=%s same_side=%s",
                        token_id,
                        len(opposite_orders),
                        len(same_side_orders)
                        if (desired_down or phase_for_intent == "EXITING")
                        else 0,
                    )
                    ignore_cd = bool(cfg.get("exit_ignore_cooldown", True)) and is_exiting
                    cancel_ignore_cd = bool(
                        cfg.get("cancel_intent_ignore_cooldown", True)
                    )
                    if cooldown_active and (not ignore_cd) and (not cancel_ignore_cd):
                        dedup_key = f"SKIP:{token_id}:cooldown_intent"
                        should_log, suppressed = _log_dedup.should_log(dedup_key)
                        if should_log:
                            if suppressed > 0:
                                logger.info(
                                    "[SKIP] token_id=%s reason=cooldown_intent (suppressed %d)",
                                    token_id, suppressed,
                                )
                            else:
                                logger.info("[SKIP] token_id=%s reason=cooldown_intent", token_id)
                    else:
                        updated_orders = apply_actions(
                            clob_client,
                            cancel_actions,
                            open_orders,
                            now_ts,
                            args.dry_run,
                            cfg=cfg,
                            state=state,
                        )
                        if updated_orders:
                            state.setdefault("open_orders", {})[token_id] = updated_orders
                            open_orders = updated_orders
                        else:
                            state.get("open_orders", {}).pop(token_id, None)
                            open_orders = []
                        _prune_order_ts_by_id(state)
                        _refresh_managed_order_ids(state)
                        (
                            planned_total_notional,
                            planned_by_token_usd,
                            order_info_by_id,
                            _shadow_buy_usd,
                        ) = _calc_planned_notional_with_fallback(
                            my_by_token_id,
                            state.get("open_orders", {}),
                            state.get("last_mid_price_by_token_id", {}),
                            max_position_usd_per_token,
                            state,
                            now_ts,
                            shadow_ttl_sec,
                            fallback_mid_price,
                            logger,
                            include_shadow=False,
                        )
                        # NOTE: cancel-intent should NOT extend cooldown.
                        # Cooldown is applied only on successful place actions.
            if abs(delta) <= eps:
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue
            open_orders_for_reconcile = [
                order
                for order in open_orders
                if str(order.get("side") or "").upper() == desired_side
            ]
            deadband_shares = token_deadband_shares
            if abs(delta) <= deadband_shares and not open_orders_for_reconcile:
                dedup_key = f"NOOP:{token_id}:deadband"
                should_log, suppressed = _log_dedup.should_log(dedup_key)
                if should_log:
                    if suppressed > 0:
                        logger.info(
                            "[NOOP] token_id=%s reason=deadband delta=%s deadband=%s (suppressed %d)",
                            token_id, delta, deadband_shares, suppressed,
                        )
                    else:
                        logger.info(
                            "[NOOP] token_id=%s reason=deadband delta=%s deadband=%s",
                            token_id, delta, deadband_shares,
                        )
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue

            state.setdefault("target_last_event_ts", {})[token_id] = now_ts

            if mode == "auto_usd":
                delta_shares = abs(my_target - my_shares)
                delta_usd_samples.append(delta_shares * ref_price)

            token_key = token_key_by_token_id.get(token_id, f"token:{token_id}")
            cfg_for_reconcile = cfg_lowp if (is_lowp and desired_side == "BUY") else cfg

            token_planned = float(planned_by_token_usd_shadow.get(token_id, 0.0))

            if desired_side == "BUY":
                max_notional = float(cfg_for_reconcile.get("max_notional_per_token") or 0.0)
                if max_notional > 0 and token_planned >= max_notional * 0.95:
                    logger.debug(
                        "[SKIP_PREFLIGHT] %s near_limit planned=%s max=%s",
                        token_key,
                        token_planned,
                        max_notional,
                    )
                    _maybe_update_target_last(state, token_id, t_now, should_update_last)
                    continue

                max_position_usd_per_token = float(cfg_for_reconcile.get("max_position_usd_per_token") or 0.0)
                if max_position_usd_per_token > 0:
                    acc_usd = float(state.get("buy_notional_accumulator", {}).get(token_id, {}).get("usd", 0.0))
                    if acc_usd > max_position_usd_per_token:
                        logger.info(
                            "[SKIP_PREFLIGHT] %s grandfather acc=%s max=%s",
                            token_key,
                            acc_usd,
                            max_position_usd_per_token,
                        )
                        _maybe_update_target_last(state, token_id, t_now, should_update_last)
                        continue

                if max_per_condition > 0:
                    cond_id = _get_condition_id(state, token_id)
                    if cond_id:
                        cond_planned = condition_planned_map.get(cond_id, 0.0)
                        if cond_planned >= max_per_condition * 0.95:
                            logger.info(
                                "[SKIP_PREFLIGHT] %s condition=%s condition_limit=%s/%s",
                                token_key,
                                cond_id[:16],
                                cond_planned,
                                max_per_condition,
                            )
                            _maybe_update_target_last(state, token_id, t_now, should_update_last)
                            continue

                if max_per_event > 0:
                    event_id = _get_event_id(state, token_id)
                    if event_id:
                        event_planned = event_planned_map.get(event_id, 0.0)
                        if event_planned >= max_per_event * 0.95:
                            logger.info(
                                "[SKIP_PREFLIGHT] %s event=%s event_limit=%s/%s",
                                token_key,
                                event_id[:16],
                                event_planned,
                                max_per_event,
                            )
                            _maybe_update_target_last(state, token_id, t_now, should_update_last)
                            continue

                max_total_pos = float(cfg.get("max_notional_total") or 0.0)
                if max_total_pos > 0 and planned_total_notional_shadow >= max_total_pos * 0.95:
                    logger.info(
                        "[SKIP_PREFLIGHT] %s total_position_limit=%s/%s",
                        token_key,
                        planned_total_notional_shadow,
                        max_total_pos,
                    )
                    _maybe_update_target_last(state, token_id, t_now, should_update_last)
                    continue

            try:
                refresh_market_info_cache_v2(
                    clob_read_client,
                    state,
                    token_id,
                    timeout=api_timeout_sec,
                    ttl_sec=float(cfg.get("market_info_cache_ttl_sec") or 300.0),
                )
            except Exception as market_info_exc:
                logger.debug(
                    "[MARKET_INFO_CACHE] token_id=%s refresh failed: %s",
                    token_id,
                    market_info_exc,
                )

            actions = reconcile_one(
                token_id,
                my_target,
                my_shares,
                ob,
                open_orders_for_reconcile,
                now_ts,
                cfg_for_reconcile,
                state,
                planned_token_notional=token_planned,
            )
            if not actions:
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue
            filtered_actions = []
            blocked_reasons: set[str] = set()
            has_any_place = any(a.get("type") == "place" for a in actions)
            pending_cancel_actions = []
            pending_cancel_usd = 0.0
            token_planned_before = float(planned_by_token_usd.get(token_id, 0.0))
            # Track accumulator delta within this batch to prevent batch bypass
            local_accumulator_delta = 0.0

            for act in actions:
                act_type = act.get("type")
                if act_type == "cancel":
                    order_id = str(act.get("order_id") or "")
                    info = order_info_by_id.get(order_id)
                    if info and info.get("side") == "BUY":
                        usd = float(info.get("usd") or 0.0)
                        pending_cancel_actions.append(act)
                        pending_cancel_usd += usd
                        planned_total_notional -= usd
                        planned_by_token_usd[token_id] = max(
                            0.0, planned_by_token_usd.get(token_id, 0.0) - usd
                        )
                    else:
                        filtered_actions.append(act)
                    continue

                if act_type != "place":
                    filtered_actions.append(act)
                    continue

                side = str(act.get("side") or "").upper()
                if side == "SELL" and sell_reconcile_lock_active and (not must_exit_active):
                    blocked_reasons.add("sell_reconcile_lock")
                    continue
                price = float(act.get("price") or ref_price or 0.0)
                size = float(act.get("size") or 0.0)
                if price <= 0 or size <= 0:
                    continue
                if my_trades_unreliable and side == "BUY":
                    blocked_reasons.add("my_trades_unreliable")
                    continue
                if side == "BUY":
                    order_notional = abs(size) * price
                    if topic_risk_overlay_enabled and topic_risk_level >= 1:
                        cfg_for_topic_risk = cfg_lowp if is_lowp else cfg
                        min_shares_cfg = float(cfg_for_topic_risk.get("min_order_shares") or 0.0)
                        api_min_shares = _orderbook_min_order_shares(ob)
                        if api_min_shares <= 0:
                            api_min_shares = float(
                                topic_risk_book_min_shares_cache.get(token_id) or 0.0
                            )
                            if api_min_shares <= 0:
                                api_min_shares = _book_min_order_shares(
                                    clob_client, token_id, api_timeout_sec
                                )
                                topic_risk_book_min_shares_cache[token_id] = float(
                                    api_min_shares or 0.0
                                )
                        effective_min_shares_guard = max(min_shares_cfg, api_min_shares)
                        min_order_usd_guard = float(cfg_for_topic_risk.get("min_order_usd") or 0.0)
                        effective_min_usd_guard = max(
                            min_order_usd_guard,
                            (effective_min_shares_guard * price)
                            if effective_min_shares_guard > 0 and price > 0
                            else 0.0,
                        )
                        if (
                            effective_min_usd_guard > 0
                            and order_notional + 1e-9 < effective_min_usd_guard
                        ):
                            blocked_reasons.add("topic_risk_below_min")
                            logger.info(
                                "[HOLD] token_id=%s reason=topic_risk_below_min level=L%s order_usd=%.6f min_usd=%.6f",
                                token_id,
                                int(topic_risk_level),
                                order_notional,
                                effective_min_usd_guard,
                            )
                            continue
                    last_exit_ts = int(last_exit_ts_by_token.get(token_id) or 0)
                    hold_buy, hold_reason = _should_hold_reentry_buy(
                        now_ts=now_ts,
                        my_shares=my_shares,
                        last_exit_ts=last_exit_ts,
                        reentry_cooldown_sec=token_reentry_cooldown_sec,
                        signal_buy_shares=max(0.0, float(d_my)),
                        order_buy_shares=abs(size),
                        order_buy_usd=order_notional,
                        force_buy_shares=reentry_force_buy_shares,
                        force_buy_usd=reentry_force_buy_usd,
                        eps=eps,
                    )
                    if hold_buy:
                        blocked_reasons.add("reentry_cooldown")
                        dedup_key = f"HOLD:{token_id}:reentry_cooldown"
                        should_log, suppressed = _log_dedup.should_log(dedup_key)
                        if should_log:
                            remain = max(0, token_reentry_cooldown_sec - max(0, now_ts - last_exit_ts))
                            if suppressed > 0:
                                logger.info(
                                    "[HOLD] token_id=%s reason=reentry_cooldown remain=%ss "
                                    "signal_shares=%.6f force_shares=%.6f (suppressed %d)",
                                    token_id,
                                    remain,
                                    max(0.0, float(d_my)),
                                    reentry_force_buy_shares,
                                    suppressed,
                                )
                            else:
                                logger.info(
                                    "[HOLD] token_id=%s reason=reentry_cooldown remain=%ss "
                                    "signal_shares=%.6f force_shares=%.6f",
                                    token_id,
                                    remain,
                                    max(0.0, float(d_my)),
                                    reentry_force_buy_shares,
                                )
                        continue
                    if hold_reason == "force_override":
                        dedup_key = f"BYPASS:{token_id}:reentry_force"
                        should_log, suppressed = _log_dedup.should_log(dedup_key)
                        if should_log:
                            if suppressed > 0:
                                logger.info(
                                    "[REENTRY_BYPASS] token_id=%s reason=force_override "
                                    "signal_shares=%.6f order_usd=%.6f "
                                    "force_shares=%.6f force_usd=%.6f (suppressed %d)",
                                    token_id,
                                    max(0.0, float(d_my)),
                                    order_notional,
                                    reentry_force_buy_shares,
                                    reentry_force_buy_usd,
                                    suppressed,
                                )
                            else:
                                logger.info(
                                    "[REENTRY_BYPASS] token_id=%s reason=force_override "
                                    "signal_shares=%.6f order_usd=%.6f force_shares=%.6f force_usd=%.6f",
                                    token_id,
                                    max(0.0, float(d_my)),
                                    order_notional,
                                    reentry_force_buy_shares,
                                    reentry_force_buy_usd,
                                )
                if side == "BUY" and buy_window_sec > 0:
                    recent_token = float(recent_buy_by_token.get(token_id, 0.0))
                    if (
                        buy_window_max_usd_per_token > 0
                        and recent_token + order_notional > buy_window_max_usd_per_token
                    ):
                        blocked_reasons.add("buy_window_max_usd_per_token")
                        continue
                    if (
                        buy_window_max_usd_total > 0
                        and recent_buy_total + order_notional > buy_window_max_usd_total
                    ):
                        blocked_reasons.add("buy_window_max_usd_total")
                        continue

                # CRITICAL: Check accumulator first (independent of position API)
                if side == "BUY":
                    order_notional = abs(size) * price
                    cfg_for_acc = cfg_lowp if is_lowp else cfg
                    planned_token_notional_for_acc = float(planned_by_token_usd.get(token_id, 0.0))
                    acc_ok, acc_reason, acc_available = accumulator_check(
                        token_id,
                        order_notional,
                        state,
                        cfg_for_acc,
                        side=side,
                        local_delta=local_accumulator_delta,
                        planned_token_notional=planned_token_notional_for_acc,
                        planned_total_notional=planned_total_notional,
                    )
                    if not acc_ok:
                        # Get accumulator actual values for detailed logging
                        accumulator = state.get("buy_notional_accumulator")
                        acc_current = 0.0
                        if isinstance(accumulator, dict):
                            for acc_data in accumulator.values():
                                if isinstance(acc_data, dict):
                                    acc_current += float(acc_data.get("usd", 0.0))
                        max_total = float(cfg_for_acc.get("accumulator_max_total_usd") or 0)

                        # Try to shrink order to fit within accumulator limit
                        if acc_available > 0:
                            min_order_usd = float(cfg_for_acc.get("min_order_usd") or 0.0)
                            min_order_shares = float(cfg_for_acc.get("min_order_shares") or 0.0)
                            effective_min_usd = max(min_order_usd, min_order_shares * price)

                            if acc_available >= effective_min_usd:
                                # Shrink order to available amount
                                old_size = size
                                old_usd = order_notional
                                size = acc_available / price
                                act["size"] = size
                                order_notional = acc_available
                                logger.warning(
                                    "[ACCUMULATOR_SHRINK] token_id=%s old_usd=%s new_usd=%s acc_current=%s acc_limit=%s planned_total=%s is_lowp=%s reason=%s",
                                    token_id,
                                    old_usd,
                                    acc_available,
                                    acc_current,
                                    max_total,
                                    planned_total_notional,
                                    is_lowp,
                                    acc_reason,
                                )
                                # Continue with shrunken order (don't skip)
                            else:
                                # Available amount is below minimum order size
                                logger.warning(
                                    "[ACCUMULATOR_BLOCK] token_id=%s order_usd=%s acc_current=%s local_delta=%s acc_limit=%s acc_available=%s min_usd=%s planned_total=%s is_lowp=%s reason=%s",
                                    token_id,
                                    order_notional,
                                    acc_current,
                                    local_accumulator_delta,
                                    max_total,
                                    acc_available,
                                    effective_min_usd,
                                    planned_total_notional,
                                    is_lowp,
                                    acc_reason,
                                )
                                blocked_reasons.add(acc_reason or "accumulator_check")
                                continue
                        else:
                            # No room available
                            logger.warning(
                                "[ACCUMULATOR_BLOCK] token_id=%s order_usd=%s acc_current=%s local_delta=%s acc_limit=%s planned_total=%s is_lowp=%s reason=%s",
                                token_id,
                                order_notional,
                                acc_current,
                                local_accumulator_delta,
                                max_total,
                                planned_total_notional,
                                is_lowp,
                                acc_reason,
                            )
                            blocked_reasons.add(acc_reason or "accumulator_check")
                            continue

                planned_token_notional = max(
                    float(planned_by_token_usd.get(token_id, 0.0)),
                    float(planned_by_token_usd_shadow.get(token_id, 0.0)),
                )
                planned_total_notional_risk = max(
                    planned_total_notional, planned_total_notional_shadow
                )
                cfg_for_action = cfg_lowp if (is_lowp and side == "BUY") else cfg

                # Hard guard (no extra API): when target snapshot is available on BUY signal,
                # do not let this order push holdings above target-follow cap + one minimum order bump.
                # Prefer shrinking to allowed size (same style as risk resize) instead of hard block.
                if side == "BUY" and d_target > 0 and t_now is not None:
                    target_now_shares = max(0.0, float(t_now))
                    follow_cap_shares = max(0.0, ratio_buy) * target_now_shares
                    min_order_usd_guard = float(cfg_for_action.get("min_order_usd") or 0.0)
                    min_order_shares_guard = float(cfg_for_action.get("min_order_shares") or 0.0)
                    one_bump_shares = max(
                        min_order_shares_guard,
                        (min_order_usd_guard / price) if price > 0 else 0.0,
                    )
                    projected_shares = my_shares + abs(size)
                    hard_cap_shares = follow_cap_shares + one_bump_shares
                    if projected_shares > hard_cap_shares + eps:
                        allowed_shares = max(0.0, hard_cap_shares - my_shares)
                        effective_min_usd_guard = max(
                            min_order_usd_guard,
                            (min_order_shares_guard * price) if min_order_shares_guard > 0 else 0.0,
                        )
                        allowed_usd_guard = allowed_shares * price
                        if (
                            allowed_shares <= eps
                            or allowed_usd_guard + 1e-9 < effective_min_usd_guard
                        ):
                            logger.warning(
                                "[HARD_CAP_BLOCK] token_id=%s projected=%.6f cap=%.6f "
                                "(follow=%.6f bump=%.6f my=%.6f size=%.6f t_now=%.6f ratio=%.6f)",
                                token_id,
                                projected_shares,
                                hard_cap_shares,
                                follow_cap_shares,
                                one_bump_shares,
                                my_shares,
                                abs(size),
                                target_now_shares,
                                ratio_buy,
                            )
                            blocked_reasons.add("hard_follow_cap")
                            continue

                        old_size = abs(size)
                        old_usd = old_size * price
                        if allowed_shares < old_size * (1 - 1e-9):
                            size = allowed_shares
                            act["size"] = size
                            logger.warning(
                                "[HARD_CAP_SHRINK] token_id=%s old_usd=%s new_usd=%s "
                                "old_shares=%s new_shares=%s cap=%.6f my=%.6f t_now=%.6f ratio=%.6f",
                                token_id,
                                old_usd,
                                allowed_usd_guard,
                                old_size,
                                size,
                                hard_cap_shares,
                                my_shares,
                                target_now_shares,
                                ratio_buy,
                            )

                ok, reason = risk_check(
                    token_key,
                    size,
                    my_shares,
                    price,
                    cfg_for_action,
                    token_title=token_title,
                    side=side,
                    planned_total_notional=planned_total_notional_risk,
                    planned_token_notional=planned_token_notional,
                    cumulative_total_usd=None,
                    cumulative_token_usd=None,
                )
                if not ok:
                    resized = _shrink_on_risk_limit(
                        act,
                        max_notional_total,
                        planned_total_notional_risk,
                        float(cfg_for_action.get("max_notional_per_token") or 0.0),
                        planned_token_notional,
                        float(cfg_for_action.get("min_order_usd") or 0.0),
                        float(cfg_for_action.get("min_order_shares") or 0.0),
                        token_key,
                        token_id,
                        logger,
                    )
                    if resized is None:
                        if has_any_place and pending_cancel_actions:
                            planned_total_notional += pending_cancel_usd
                            planned_by_token_usd[token_id] = token_planned_before
                            pending_cancel_actions = []
                            pending_cancel_usd = 0.0
                        blocked_reasons.add(reason or "risk_check")
                        continue

                    act, allowed_usd = resized
                    price = float(act.get("price") or 0.0)
                    size = float(act.get("size") or 0.0)

                    # CRITICAL: Re-check accumulator after shrink
                    shrink_notional = abs(size) * price
                    acc_ok_shrink, acc_reason_shrink, _acc_available_shrink = (
                        accumulator_check(
                            token_id,
                            shrink_notional,
                            state,
                            cfg_for_action,
                            side=side,
                            local_delta=local_accumulator_delta,
                        )
                    )
                    if not acc_ok_shrink:
                        logger.warning(
                            "[ACCUMULATOR_BLOCK_SHRINK] token_id=%s shrink_usd=%s "
                            "current_delta=%s reason=%s",
                            token_id,
                            shrink_notional,
                            local_accumulator_delta,
                            acc_reason_shrink,
                        )
                        if has_any_place and pending_cancel_actions:
                            planned_total_notional += pending_cancel_usd
                            planned_by_token_usd[token_id] = token_planned_before
                            pending_cancel_actions = []
                            pending_cancel_usd = 0.0
                        blocked_reasons.add(acc_reason_shrink or "accumulator_check_shrink")
                        continue

                    planned_token_notional = max(
                        float(planned_by_token_usd.get(token_id, 0.0)),
                        float(planned_by_token_usd_shadow.get(token_id, 0.0)),
                    )
                    ok2, reason2 = risk_check(
                        token_key,
                        size,
                        my_shares,
                        price,
                        cfg_for_action,
                        token_title=token_title,
                        side=side,
                        planned_total_notional=planned_total_notional_risk,
                        planned_token_notional=planned_token_notional,
                        cumulative_total_usd=None,
                        cumulative_token_usd=None,
                    )
                    if not ok2:
                        if has_any_place and pending_cancel_actions:
                            planned_total_notional += pending_cancel_usd
                            planned_by_token_usd[token_id] = token_planned_before
                            pending_cancel_actions = []
                            pending_cancel_usd = 0.0
                        blocked_reasons.add(reason2 or reason or "risk_check")
                        continue

                if side == "BUY" and signal_skip_ratio > 0 and all_enabled_account_ids_global:
                    follow_ids = _pick_token_sticky_follow_accounts(
                        token_id=token_id,
                        all_account_ids=all_enabled_account_ids_global,
                        skip_ratio=signal_skip_ratio,
                        now_ts=now_ts,
                        decision_cache=target_level_token_decisions,
                        seed=target_level_seed,
                        assigned_level=signal_target_level,
                    )
                    follow_set = set(follow_ids)
                    skipped_account_ids = [
                        acct_id
                        for acct_id in _normalize_account_ids(all_enabled_account_ids_global)
                        if acct_id not in follow_set
                    ]
                    skip_count = len(skipped_account_ids)
                    decision_entry = target_level_token_decisions.get(str(token_id)) or {}
                    if token_id not in target_level_logged_tokens and skip_count > 0:
                        skip_payload = {
                            "ts": int(now_ts),
                            "time_utc": datetime.now(timezone.utc).isoformat(),
                            "assignment_mode": "token_sticky",
                            "token_id": token_id,
                            "token_key": token_key,
                            "cohort_key": token_id,
                            "source_target": signal_source_target,
                            "source_target_short": _shorten_address(signal_source_target),
                            "target_level": str(decision_entry.get("assigned_level") or signal_target_level),
                            "skip_ratio": float(decision_entry.get("skip_ratio") or signal_skip_ratio),
                            "seed": target_level_seed,
                            "total_accounts": len(all_enabled_account_ids_global),
                            "skip_count": skip_count,
                            "skipped_accounts": [
                                {
                                    "address": acct_id,
                                    "name": all_account_name_by_id.get(acct_id, _shorten_address(acct_id)),
                                }
                                for acct_id in skipped_account_ids
                            ],
                            "follow_accounts": [
                                {
                                    "address": acct_id,
                                    "name": all_account_name_by_id.get(acct_id, _shorten_address(acct_id)),
                                }
                                for acct_id in follow_ids
                            ],
                            "signal_marker": signal_marker,
                            "signal_id": signal_id,
                            "buy_action_ms": int(buy_signal_ms),
                        }
                        _append_target_level_skip_log(
                            target_level_skip_log_path,
                            skip_payload,
                            logger,
                        )
                        target_level_logged_tokens.add(token_id)
                    if current_account_id not in follow_set:
                        blocked_reasons.add(f"target_level_{signal_target_level}_token_skip")
                        logger.info(
                            "[TARGET_LEVEL_SKIP] token_id=%s level=%s source=%s cohort_key=%s account=%s skip_count=%s/%s mode=token_sticky",
                            token_id,
                            str(decision_entry.get("assigned_level") or signal_target_level),
                            _shorten_address(signal_source_target),
                            token_id,
                            _shorten_address(current_account_id),
                            skip_count,
                            len(all_enabled_account_ids_global),
                        )
                        continue

                if pending_cancel_actions:
                    filtered_actions.extend(pending_cancel_actions)
                    pending_cancel_actions = []
                    pending_cancel_usd = 0.0

                filtered_actions.append(act)
                if side == "BUY":
                    usd = abs(size) * price
                    planned_total_notional += usd
                    planned_by_token_usd[token_id] = planned_by_token_usd.get(token_id, 0.0) + usd
                    # CRITICAL: Update local accumulator delta to prevent batch bypass
                    local_accumulator_delta += usd

            if has_any_place and pending_cancel_actions:
                planned_total_notional += pending_cancel_usd
                planned_by_token_usd[token_id] = token_planned_before
                pending_cancel_actions = []
                pending_cancel_usd = 0.0
            elif (not has_any_place) and pending_cancel_actions:
                filtered_actions.extend(pending_cancel_actions)

            if not filtered_actions:
                if has_any_place:
                    reason_text = (
                        ",".join(sorted(blocked_reasons)) if blocked_reasons else "risk_check"
                    )
                    dedup_key = f"NOOP:{token_id}:{reason_text}"
                    should_log, suppressed = _log_dedup.should_log(dedup_key)
                    if should_log:
                        if suppressed > 0:
                            logger.info(
                                "[NOOP] token_id=%s reason=%s (suppressed %d)",
                                token_id, reason_text, suppressed,
                            )
                        else:
                            logger.info("[NOOP] token_id=%s reason=%s", token_id, reason_text)
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue
            actions = filtered_actions
            logger.info(
                "[ACTION] token_id=%s topic_risk=%s -> %s",
                token_id,
                topic_risk_action_hint,
                actions,
            )

            is_reprice = _is_pure_reprice(actions)
            has_exit_sell_place = any(
                act.get("type") == "place"
                and str(act.get("side") or "").upper() == "SELL"
                for act in actions
            ) and is_exiting
            has_exit_flow_sell_place = any(
                act.get("type") == "place"
                and str(act.get("side") or "").upper() == "SELL"
                and bool(act.get("_exit_flow"))
                for act in actions
            ) and is_exiting
            ignore_place_backoff = (
                bool(cfg.get("exit_ignore_place_backoff", True))
                and has_exit_sell_place
                and (not has_exit_flow_sell_place)
                and ((not sell_reconcile_lock_active) or must_exit_active)
            )
            if (
                place_backoff_active
                and any(act.get("type") == "place" for act in actions)
                and (not ignore_place_backoff)
            ):
                dedup_key = f"SKIP:{token_id}:place_backoff"
                should_log, suppressed = _log_dedup.should_log(dedup_key)
                if should_log:
                    if suppressed > 0:
                        logger.info(
                            "[SKIP] token_id=%s reason=place_backoff until=%s (suppressed %d)",
                            token_id, place_fail_until, suppressed,
                        )
                    else:
                        logger.info(
                            "[SKIP] token_id=%s reason=place_backoff until=%s",
                            token_id, place_fail_until,
                        )
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue
            if place_backoff_active and ignore_place_backoff:
                dedup_key = f"BYPASS:{token_id}:place_backoff_exit_sell"
                should_log, suppressed = _log_dedup.should_log(dedup_key)
                if should_log:
                    if suppressed > 0:
                        logger.info(
                            "[BACKOFF_BYPASS] token_id=%s reason=exit_sell_place (suppressed %d)",
                            token_id,
                            suppressed,
                        )
                    else:
                        logger.info(
                            "[BACKOFF_BYPASS] token_id=%s reason=exit_sell_place",
                            token_id,
                        )
            ignore_cd = bool(cfg.get("exit_ignore_cooldown", True)) and is_exiting
            if cooldown_active and (not ignore_cd) and (not is_reprice):
                dedup_key = f"SKIP:{token_id}:cooldown"
                should_log, suppressed = _log_dedup.should_log(dedup_key)
                if should_log:
                    if suppressed > 0:
                        logger.info(
                            "[SKIP] token_id=%s reason=cooldown (suppressed %d)",
                            token_id, suppressed,
                        )
                    else:
                        logger.info("[SKIP] token_id=%s reason=cooldown", token_id)
                continue

            did_place_buy = any(
                act.get("type") == "place" and str(act.get("side") or "").upper() == "BUY"
                for act in filtered_actions
            )
            if probe_attempted and did_place_buy and st.get("phase") == "LONG":
                st["did_probe"] = True
                topic_state[token_id] = st

            updated_orders = apply_actions(
                clob_client,
                actions,
                open_orders,
                now_ts,
                args.dry_run,
                cfg=cfg,
                state=state,
                planned_by_token_usd=planned_by_token_usd_shadow,
            )
            sell_health_round["actions"] = int(sell_health_round.get("actions") or 0) + sum(
                1
                for act in actions
                if act.get("type") == "place"
                and str(act.get("side") or "").upper() == "SELL"
            )
            if updated_orders:
                state.setdefault("open_orders", {})[token_id] = updated_orders
            else:
                state.get("open_orders", {}).pop(token_id, None)
            _prune_order_ts_by_id(state)
            _refresh_managed_order_ids(state)
            (
                planned_total_notional,
                planned_by_token_usd,
                order_info_by_id,
                _shadow_buy_usd,
            ) = _calc_planned_notional_with_fallback(
                my_by_token_id_for_risk,
                state.get("open_orders", {}),
                state.get("last_mid_price_by_token_id", {}),
                max_position_usd_per_token,
                state,
                now_ts,
                shadow_ttl_sec,
                fallback_mid_price,
                logger,
                include_shadow=False,
            )
            (
                planned_total_notional_shadow,
                planned_by_token_usd_shadow,
                _shadow_order_info_by_id,
                _shadow_buy_usd,
            ) = _calc_planned_notional_with_fallback(
                my_by_token_id_for_risk,
                state.get("open_orders", {}),
                state.get("last_mid_price_by_token_id", {}),
                max_position_usd_per_token,
                state,
                now_ts,
                shadow_ttl_sec,
                fallback_mid_price,
                logger,
                include_shadow=True,
            )

            has_any_place_final = any(act.get("type") == "place" for act in actions)
            if (
                cooldown_sec > 0
                and actions
                and has_any_place_final
                and (not ignore_cd)
                and (not is_reprice)
            ):
                state.setdefault("cooldown_until", {})[token_id] = now_ts + cooldown_sec
            if is_reprice:
                state.setdefault("last_reprice_ts_by_token", {})[token_id] = now_ts

            _maybe_update_target_last(state, token_id, t_now, should_update_last)

        if mode == "auto_usd" and delta_usd_samples:
            delta_usd_samples.sort()
            mid = delta_usd_samples[len(delta_usd_samples) // 2]
            alpha = 0.2
            new_ema = (1 - alpha) * ema + alpha * mid
            state.setdefault("sizing", {})["ema_delta_usd"] = new_ema
            state["sizing"]["last_k"] = cfg.get("_auto_order_k")

        _update_sell_health_monitor(
            state=state,
            cfg=cfg,
            now_ts=now_ts,
            sell_signals_inc=int(sell_health_round.get("signals") or 0),
            sell_actions_inc=int(sell_health_round.get("actions") or 0),
            logger=logger,
            account_label=_shorten_address(current_my_address),
        )

        state["last_sync_ts"] = now_ts
        save_state(args.state, state)

        # ============================================================
        # MULTI-ACCOUNT: Rotate to next account for next iteration
        # ============================================================
        current_account_idx = (current_account_idx + 1) % len(account_contexts)

        time.sleep(_get_poll_interval())


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Best-effort fatal error log to a dedicated file for systemd restarts.
        import json
        import os
        import sys
        import traceback
        from datetime import datetime, timezone

        def _find_config_path(argv: list[str]) -> str | None:
            for idx, item in enumerate(argv):
                if item in ("--config",):
                    if idx + 1 < len(argv):
                        return argv[idx + 1]
            return None

        def _resolve_log_dir(config_path: str | None) -> str:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            cfg_path = config_path or os.path.join(base_dir, "copytrade_config.json")
            try:
                # Use utf-8-sig to tolerate BOM-prefixed JSON from Windows editors.
                with open(cfg_path, "r", encoding="utf-8-sig") as f:
                    cfg = json.load(f)
                log_dir = str(cfg.get("log_dir") or "logs")
            except Exception:
                log_dir = "logs"
            if not os.path.isabs(log_dir):
                log_dir = os.path.join(base_dir, log_dir)
            return log_dir

        cfg_path = _find_config_path(sys.argv)
        log_dir = _resolve_log_dir(cfg_path)
        os.makedirs(log_dir, exist_ok=True)
        fatal_path = os.path.join(log_dir, "fatal_error.log")
        with open(fatal_path, "a", encoding="utf-8") as f:
            f.write("\n=== FATAL ERROR ===\n")
            f.write(
                f"time_utc={datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}\n"
            )
            f.write("argv=" + " ".join(sys.argv) + "\n")
            f.write(traceback.format_exc())
        raise


