from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Dict, Iterable, Tuple


RUNTIME_HEALTH_DEFAULT: Dict[str, Any] = {
    "mode": "running",
    "degraded_since": 0,
    "last_recovered_ts": 0,
    "last_light_resync_ts": 0,
    "last_full_reconcile_ts": 0,
    "buy_paused_until": 0,
    "needs_light_resync": False,
    "needs_full_reconcile": False,
    "components": {},
    "last_error": {
        "component": "",
        "kind": "",
        "message": "",
        "ts": 0,
    },
    "order_state_unknown_since": 0,
    "ws_gap_start_ts": 0,
    "ws_last_msg_ts": 0,
    "ws_reconnect_count": 0,
    "recovery_started_ts": 0,
}


_TRANSIENT_PATTERNS = (
    "winerror 10054",
    "connection reset",
    "connection aborted",
    "connection refused",
    "connection timed out",
    "server disconnected",
    "remote host closed",
    "temporarily unavailable",
    "timeout",
    "timed out",
    "read timed out",
    "connecttimeout",
    "readtimeout",
    "network is unreachable",
    "max retries exceeded",
    "retryable_status_",
    "request_failed",
    "incomplete",
    "temporarily",
    "status=408",
    "status=429",
    "http 408",
    "http 429",
    "http 500",
    "http 502",
    "http 503",
    "http 504",
    "engine restarting",
    "status=425",
    "too many requests",
)

_MARKET_STATE_PATTERNS = (
    "market closed",
    "accepting_orders",
    "not accepting orders",
    "no orderbook",
    "orderbook_empty",
    "no_best_bid",
    "no best bid",
    "no match",
    "fak",
    "minimum order",
    "min order",
    "min size",
    "below_book_min_order_shares",
)

_PREFLIGHT_PATTERNS = (
    "pusd",
    "allowance",
    "insufficient balance",
    "insufficient funds",
    "api key",
    "api credential",
    "signature",
    "private key",
    "funder",
    "proxy wallet",
    "credential",
    "unauthorized",
    "forbidden",
    "http 401",
    "http 403",
    "status=401",
    "status=403",
)

_FATAL_PATTERNS = (
    "traceback",
    "runtimeerror",
    "syntaxerror",
    "typeerror",
    "valueerror",
    "state file",
    "jsondecodeerror",
    "cannot import",
    "module not found",
)

_HTTP_5XX_RE = re.compile(r"\b(?:http|status=)\s*5\d\d\b", re.IGNORECASE)
_NON_BLOCKING_FAILURE_COMPONENTS = {"data_api_target_positions"}


def _clone_default() -> Dict[str, Any]:
    return deepcopy(RUNTIME_HEALTH_DEFAULT)


def ensure_runtime_health(state: Dict[str, Any]) -> Dict[str, Any]:
    """Initialize and normalize ``state['runtime_health']`` in-place."""

    raw = state.get("runtime_health")
    if not isinstance(raw, dict):
        raw = {}
    health = _clone_default()
    health.update(raw)

    if not isinstance(health.get("components"), dict):
        health["components"] = {}
    if not isinstance(health.get("last_error"), dict):
        health["last_error"] = dict(RUNTIME_HEALTH_DEFAULT["last_error"])

    for key, default in RUNTIME_HEALTH_DEFAULT.items():
        if key not in health:
            health[key] = deepcopy(default)

    if str(health.get("mode") or "").strip() not in {
        "running",
        "degraded",
        "reconnecting",
        "resyncing",
        "safe_mode",
        "fatal",
    }:
        health["mode"] = "running"

    for key in (
        "degraded_since",
        "last_recovered_ts",
        "last_light_resync_ts",
        "last_full_reconcile_ts",
        "buy_paused_until",
        "order_state_unknown_since",
        "ws_gap_start_ts",
        "ws_last_msg_ts",
        "ws_reconnect_count",
        "recovery_started_ts",
    ):
        try:
            health[key] = int(health.get(key) or 0)
        except Exception:
            health[key] = 0

    health["needs_light_resync"] = bool(health.get("needs_light_resync"))
    health["needs_full_reconcile"] = bool(health.get("needs_full_reconcile"))

    last_error = health.get("last_error")
    last_component = ""
    if isinstance(last_error, dict):
        last_component = str(last_error.get("component") or "")
    if (
        str(health.get("mode") or "") in {"degraded", "reconnecting", "resyncing", "safe_mode"}
        and last_component in _NON_BLOCKING_FAILURE_COMPONENTS
    ):
        health["mode"] = "running"
        health["degraded_since"] = 0
        health["buy_paused_until"] = 0
        health["needs_light_resync"] = False
        health["needs_full_reconcile"] = False
        health["order_state_unknown_since"] = 0
        health["recovery_started_ts"] = 0

    state["runtime_health"] = health
    return health


def classify_error(value: object) -> str:
    """Classify an exception/message as transient, preflight, market_state, or fatal."""

    text = str(value or "").strip().lower()
    if not text:
        return "transient"

    if _HTTP_5XX_RE.search(text):
        return "transient"

    for pattern in _PREFLIGHT_PATTERNS:
        if pattern in text:
            return "preflight"
    for pattern in _MARKET_STATE_PATTERNS:
        if pattern in text:
            return "market_state"
    for pattern in _TRANSIENT_PATTERNS:
        if pattern in text:
            return "transient"
    for pattern in _FATAL_PATTERNS:
        if pattern in text:
            return "fatal"
    return "fatal"


def record_component_success(
    state: Dict[str, Any],
    component: str,
    now_ts: int,
) -> Dict[str, Any]:
    health = ensure_runtime_health(state)
    comp = health.setdefault("components", {}).setdefault(str(component), {})
    comp["status"] = "ok"
    comp["last_success_ts"] = int(now_ts)
    comp["success_count"] = int(comp.get("success_count") or 0) + 1
    comp["last_kind"] = ""
    comp["last_error"] = ""
    return health


def record_component_failure(
    state: Dict[str, Any],
    component: str,
    kind: str,
    message: str,
    now_ts: int,
    *,
    buy_pause_sec: int = 120,
    order_state_unknown: bool = False,
    affect_runtime_mode: bool = True,
) -> Dict[str, Any]:
    health = ensure_runtime_health(state)
    kind = str(kind or "fatal")
    component = str(component)
    message = str(message or "")

    comp = health.setdefault("components", {}).setdefault(component, {})
    comp["status"] = "failed"
    comp["last_failure_ts"] = int(now_ts)
    comp["last_kind"] = kind
    comp["last_error"] = message
    comp["fail_count"] = int(comp.get("fail_count") or 0) + 1

    previous_mode = str(health.get("mode") or "running")
    previous_error = health.get("last_error")
    previous_component = ""
    if isinstance(previous_error, dict):
        previous_component = str(previous_error.get("component") or "")

    health["last_error"] = {
        "component": component,
        "kind": kind,
        "message": message,
        "ts": int(now_ts),
    }

    if affect_runtime_mode:
        if kind == "fatal":
            health["mode"] = "fatal"
        elif kind == "preflight":
            health["mode"] = "safe_mode"
        elif kind == "transient":
            if str(health.get("mode") or "") not in {"fatal", "safe_mode"}:
                health["mode"] = "degraded"
            if int(health.get("degraded_since") or 0) <= 0:
                health["degraded_since"] = int(now_ts)
            health["needs_light_resync"] = True
            health["needs_full_reconcile"] = True
        elif kind == "market_state":
            if str(health.get("mode") or "") not in {"fatal", "safe_mode", "degraded"}:
                health["mode"] = "running"
    elif previous_mode in {"degraded", "reconnecting", "resyncing"} and previous_component == component:
        health["mode"] = "running"
        health["degraded_since"] = 0
        health["needs_light_resync"] = False
        health["needs_full_reconcile"] = False
        health["buy_paused_until"] = 0

    if affect_runtime_mode and buy_pause_sec > 0 and kind in {"transient", "preflight", "fatal"}:
        health["buy_paused_until"] = max(
            int(health.get("buy_paused_until") or 0),
            int(now_ts) + int(buy_pause_sec),
        )

    if affect_runtime_mode and order_state_unknown:
        if int(health.get("order_state_unknown_since") or 0) <= 0:
            health["order_state_unknown_since"] = int(now_ts)
        if kind in {"transient", "preflight", "fatal"}:
            health["mode"] = "safe_mode" if kind != "fatal" else "fatal"

    state["runtime_health"] = health
    return health


def note_order_state_confirmed(state: Dict[str, Any]) -> None:
    health = ensure_runtime_health(state)
    health["order_state_unknown_since"] = 0


def should_start_recovery(
    state: Dict[str, Any],
    required_components: Iterable[str],
    now_ts: int,
) -> bool:
    health = ensure_runtime_health(state)
    if str(health.get("mode") or "") not in {"degraded", "reconnecting", "safe_mode"}:
        return False
    last_error = health.get("last_error")
    if isinstance(last_error, dict) and str(last_error.get("kind") or "") in {
        "preflight",
        "fatal",
    }:
        return False
    components = health.get("components") if isinstance(health.get("components"), dict) else {}
    for comp in components.values():
        if not isinstance(comp, dict):
            continue
        if str(comp.get("status") or "") == "failed" and str(comp.get("last_kind") or "") in {
            "preflight",
            "fatal",
        }:
            return False
    degraded_since = int(health.get("degraded_since") or 0)
    if degraded_since <= 0:
        return bool(health.get("needs_light_resync"))
    for component in required_components:
        comp = components.get(str(component))
        if not isinstance(comp, dict):
            return False
        if str(comp.get("status") or "") != "ok":
            return False
        if int(comp.get("last_success_ts") or 0) < degraded_since:
            return False
    return True


def begin_recovery(
    state: Dict[str, Any],
    now_ts: int,
    *,
    full_reconcile_min_interval_sec: int = 1800,
) -> Tuple[bool, bool]:
    health = ensure_runtime_health(state)
    health["mode"] = "resyncing"
    health["recovery_started_ts"] = int(now_ts)
    need_light = bool(health.get("needs_light_resync"))
    last_full = int(health.get("last_full_reconcile_ts") or 0)
    due_full = last_full <= 0 or int(now_ts) - last_full >= max(
        0,
        int(full_reconcile_min_interval_sec),
    )
    need_full = bool(health.get("needs_full_reconcile")) and due_full
    return need_light, need_full


def complete_recovery(
    state: Dict[str, Any],
    now_ts: int,
    *,
    ok: bool,
    light_resync_done: bool = False,
    full_reconcile_done: bool = False,
) -> Dict[str, Any]:
    health = ensure_runtime_health(state)
    if ok:
        health["mode"] = "running"
        health["degraded_since"] = 0
        health["last_recovered_ts"] = int(now_ts)
        health["needs_light_resync"] = False
        if full_reconcile_done:
            health["needs_full_reconcile"] = False
        health["buy_paused_until"] = 0
        health["order_state_unknown_since"] = 0
    else:
        health["mode"] = "safe_mode"
    if light_resync_done:
        health["last_light_resync_ts"] = int(now_ts)
    if full_reconcile_done:
        health["last_full_reconcile_ts"] = int(now_ts)
    health["recovery_started_ts"] = 0
    return health


def should_pause_buys(state: Dict[str, Any], now_ts: int) -> Tuple[bool, str]:
    health = ensure_runtime_health(state)
    mode = str(health.get("mode") or "running")
    if mode in {"degraded", "reconnecting", "resyncing", "safe_mode", "fatal"}:
        return True, f"network_{mode}"
    until = int(health.get("buy_paused_until") or 0)
    if until > int(now_ts):
        return True, "network_buy_pause"
    if int(health.get("order_state_unknown_since") or 0) > 0:
        return True, "order_state_unknown"
    return False, ""
