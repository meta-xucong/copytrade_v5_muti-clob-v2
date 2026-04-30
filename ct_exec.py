from __future__ import annotations

import inspect
import logging
import os
import threading
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ct_clob_gateway import (
    cancel_order_v2,
    fetch_open_orders_norm_v2,
    get_cached_market_info_for_token,
    get_orderbook_v2,
    is_v2_client,
    place_limit_order_v2,
    place_market_order_v2,
    preflight_conditional_sell_ready_v2,
)
from ct_utils import round_to_step, round_to_tick, safe_float


logger = logging.getLogger(__name__)


class _SimpleRateLimiter:
    def __init__(self, rps: float) -> None:
        self._lock = threading.Lock()
        self._next_ts = 0.0
        self.set_rps(rps)

    def set_rps(self, rps: float) -> None:
        self.rps = max(float(rps or 0.0), 0.0)
        self.min_interval = (1.0 / self.rps) if self.rps > 0 else 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_ts - now)
            target = max(self._next_ts, now) + self.min_interval
            self._next_ts = target
        if wait > 0:
            time.sleep(wait)


_DEFAULT_CLOB_MAX_RPS = float(os.getenv("CT_CLOB_MAX_RPS", "8"))
_CLOB_LIMITER = _SimpleRateLimiter(_DEFAULT_CLOB_MAX_RPS)
_ORDERBOOK_PROVIDER: Any = None
_ORDERBOOK_PROVIDER_PREFER = False
_ORDERBOOK_PROVIDER_MAX_AGE_SEC = 5.0


def configure_clob_rate_limit(rps: float) -> None:
    """Configure global per-process CLOB API rate limit for ct_exec network calls."""
    try:
        _CLOB_LIMITER.set_rps(float(rps))
    except Exception:
        pass


def configure_orderbook_provider(
    provider: Any,
    *,
    prefer: bool = True,
    max_age_sec: float = 5.0,
) -> None:
    """Configure an optional read-through orderbook provider.

    The provider is intentionally best-effort: if it has no fresh book or raises,
    get_orderbook falls back to the existing CLOB REST path.
    """
    global _ORDERBOOK_PROVIDER, _ORDERBOOK_PROVIDER_PREFER, _ORDERBOOK_PROVIDER_MAX_AGE_SEC
    _ORDERBOOK_PROVIDER = provider
    _ORDERBOOK_PROVIDER_PREFER = bool(provider is not None and prefer)
    try:
        _ORDERBOOK_PROVIDER_MAX_AGE_SEC = max(0.1, float(max_age_sec))
    except Exception:
        _ORDERBOOK_PROVIDER_MAX_AGE_SEC = 5.0


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


def _best_from_levels(levels: Iterable[Any], pick_max: bool) -> Optional[float]:
    prices: List[float] = []
    for level in levels:
        if isinstance(level, Mapping):
            candidate = safe_float(level.get("price"))
            if candidate is not None:
                prices.append(candidate)
        elif isinstance(level, (list, tuple)) and level:
            candidate = safe_float(level[0])
            if candidate is not None:
                prices.append(candidate)
    if not prices:
        return None
    return max(prices) if pick_max else min(prices)


def _normalize_orderbook_payload(book: Any) -> Optional[Mapping[str, Any]]:
    if isinstance(book, Mapping):
        return book
    if hasattr(book, "dict"):
        payload = book.dict()
        if isinstance(payload, Mapping):
            return payload
    if hasattr(book, "__dict__"):
        payload = dict(book.__dict__)
        if isinstance(payload, Mapping):
            return payload
    return None


def _supports_timeout_param(func: Any) -> bool:
    try:
        return "timeout" in inspect.signature(func).parameters
    except Exception:
        return False


def _call_with_timeout(
    func: Any, timeout: Optional[float], *args: Any, **kwargs: Any
) -> Any:
    _CLOB_LIMITER.wait()
    if timeout is not None and timeout > 0 and _supports_timeout_param(func):
        kwargs["timeout"] = timeout
    return func(*args, **kwargs)


def _exc_text(exc: Exception) -> str:
    try:
        return str(exc or "")
    except Exception:
        return ""


def _is_engine_restarting(exc: Exception) -> bool:
    text = _exc_text(exc).lower()
    return (" 425" in text) or ("status_code=425" in text) or ("too early" in text)


def _is_fak_no_match(exc: Exception) -> bool:
    text = _exc_text(exc).lower()
    return ("no orders found to match" in text) and ("fak" in text)


def _resolve_order_fee_rate_bps(
    client: Any,
    token_id: str,
    *,
    state: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
) -> int:
    token_id = str(token_id or "").strip()
    if not token_id:
        raise ValueError("token_id is required to resolve fee rate")
    return 0


def _resolve_market_buy_user_usdc_balance(
    action: Optional[Mapping[str, Any]],
    state: Optional[Dict[str, Any]],
) -> Optional[float]:
    if isinstance(action, Mapping):
        explicit_balance = safe_float(action.get("user_usdc_balance"))
        if explicit_balance is not None and explicit_balance > 0:
            return explicit_balance
    if not isinstance(state, dict):
        return None
    preflight = state.get("collateral_preflight")
    if not isinstance(preflight, Mapping):
        return None
    balance = safe_float(preflight.get("balance"))
    if balance is None or balance <= 0:
        return None
    return balance


def _effective_buy_min_usd(
    cfg: Optional[Dict[str, Any]],
    price: float,
    *,
    is_taker: bool,
    maker_min_shares: float = 0.0,
) -> float:
    if price <= 0:
        return 1.0
    if not isinstance(cfg, dict):
        return 1.0
    if is_taker:
        min_order_usd = float(cfg.get("taker_min_order_usd_buy") or 0.0)
        min_order_shares = float(cfg.get("taker_min_order_shares_buy") or 0.0)
        effective_min_usd = max(1.0, min_order_usd)
        if min_order_shares > 0:
            effective_min_usd = max(effective_min_usd, min_order_shares * price)
        return effective_min_usd
    min_order_usd = float(cfg.get("min_order_usd") or 0.0)
    min_order_shares = float(cfg.get("min_order_shares") or 0.0)
    effective_min_usd = max(1.0, min_order_usd)
    if maker_min_shares > 0:
        effective_min_usd = max(effective_min_usd, maker_min_shares * price)
    if min_order_shares > 0:
        effective_min_usd = max(effective_min_usd, min_order_shares * price)
    return effective_min_usd


def _exit_sell_state_bucket(state: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    if not isinstance(state, dict):
        return {}
    bucket = state.setdefault("exit_sell_state", {})
    if not isinstance(bucket, dict):
        state["exit_sell_state"] = {}
        bucket = state["exit_sell_state"]
    return bucket


def _clear_exit_sell_state(state: Optional[Dict[str, Any]], token_id: str) -> None:
    if not token_id:
        return
    bucket = _exit_sell_state_bucket(state)
    bucket.pop(str(token_id), None)


def _get_or_create_exit_sell_entry(
    state: Optional[Dict[str, Any]],
    token_id: str,
    now_ts: int,
    my_shares: Optional[float] = None,
) -> Dict[str, Any]:
    bucket = _exit_sell_state_bucket(state)
    entry = bucket.get(str(token_id))
    if not isinstance(entry, dict):
        shares = float(my_shares or 0.0)
        entry = {
            "stage": 1,
            "enter_ts": int(now_ts),
            "stage_since_ts": int(now_ts),
            "last_progress_ts": int(now_ts),
            "progress_ref_shares": shares,
            "last_seen_shares": shares,
            "no_match_count": 0,
            "no_bid_count": 0,
            "pause_until": 0,
            "pause_reason": "",
        }
        bucket[str(token_id)] = entry
    return entry


def _set_exit_sell_stage(
    token_id: str,
    entry: Dict[str, Any],
    stage: int,
    now_ts: int,
    reason: str,
) -> None:
    prev_stage = int(entry.get("stage") or 0)
    if prev_stage == int(stage):
        return
    entry["stage"] = int(stage)
    entry["stage_since_ts"] = int(now_ts)
    logger.info(
        "[EXIT_STAGE] token_id=%s stage=%s prev_stage=%s reason=%s",
        token_id,
        stage,
        prev_stage,
        reason,
    )


def _set_exit_sell_pause(
    token_id: str,
    entry: Dict[str, Any],
    now_ts: int,
    pause_sec: int,
    reason: str,
) -> None:
    until = int(now_ts) + max(0, int(pause_sec))
    entry["pause_until"] = until
    entry["pause_reason"] = str(reason or "")
    entry["no_match_count"] = 0
    entry["no_bid_count"] = 0
    _set_exit_sell_stage(token_id, entry, 4, now_ts, reason)
    logger.info(
        "[EXIT_PAUSE] token_id=%s reason=%s pause_sec=%s until=%s",
        token_id,
        reason,
        pause_sec,
        until,
    )


def _record_exit_sell_no_match(
    state: Optional[Dict[str, Any]],
    token_id: str,
    now_ts: int,
    cfg: Optional[Dict[str, Any]],
) -> None:
    if not token_id or not isinstance(state, dict):
        return
    entry = _get_or_create_exit_sell_entry(state, token_id, now_ts)
    count = int(entry.get("no_match_count") or 0) + 1
    entry["no_match_count"] = count
    pause_after = 2
    pause_sec = 90
    if isinstance(cfg, dict):
        try:
            pause_after = max(1, int(cfg.get("exit_no_match_pause_after") or 2))
        except Exception:
            pause_after = 2
        try:
            pause_sec = max(0, int(cfg.get("exit_no_match_pause_sec") or 90))
        except Exception:
            pause_sec = 90
    logger.info(
        "[EXIT_NO_MATCH] token_id=%s count=%s threshold=%s",
        token_id,
        count,
        pause_after,
    )
    if count >= pause_after:
        _set_exit_sell_pause(token_id, entry, now_ts, pause_sec, "no_match")


def _post_order_with_retry(
    client: Any,
    signed_order: Any,
    order_type: Any,
    timeout: Optional[float],
    max_retry: int = 3,
    post_order_kwargs: Optional[Dict[str, Any]] = None,
) -> Any:
    delay = 1.0
    extra_kwargs = dict(post_order_kwargs or {})
    for attempt in range(max_retry + 1):
        try:
            return _call_with_timeout(
                client.post_order, timeout, signed_order, order_type, **extra_kwargs
            )
        except Exception as exc:
            if attempt >= max_retry or not _is_engine_restarting(exc):
                raise
            logger.warning(
                "[ENGINE_RESTART_RETRY] status=425 attempt=%s/%s delay=%.1fs",
                attempt + 1,
                max_retry + 1,
                delay,
            )
            time.sleep(delay)
            delay = min(delay * 2.0, 8.0)
    raise RuntimeError("unreachable")


def get_orderbook(
    client: Any, token_id: str, timeout: Optional[float] = None
) -> Dict[str, Optional[float]]:
    if _ORDERBOOK_PROVIDER_PREFER and _ORDERBOOK_PROVIDER is not None:
        try:
            provider_book = _ORDERBOOK_PROVIDER.get_orderbook(
                token_id,
                max_age_sec=_ORDERBOOK_PROVIDER_MAX_AGE_SEC,
            )
        except TypeError:
            try:
                provider_book = _ORDERBOOK_PROVIDER.get_orderbook(token_id)
            except Exception:
                provider_book = None
        except Exception:
            provider_book = None
        if isinstance(provider_book, Mapping):
            best_bid = safe_float(provider_book.get("best_bid"))
            best_ask = safe_float(provider_book.get("best_ask"))
            if best_bid is not None and best_bid <= 0:
                best_bid = None
            if best_ask is not None and best_ask <= 0:
                best_ask = None
            if best_bid is not None and best_ask is not None and best_bid > best_ask:
                best_bid = None
                best_ask = None
            if best_bid is not None or best_ask is not None:
                return {"best_bid": best_bid, "best_ask": best_ask}

    if is_v2_client(client):
        return get_orderbook_v2(client, token_id, timeout)
    tid = str(token_id)

    best_ask: Optional[float] = None
    best_bid: Optional[float] = None
    price_bid_raw: Any = None
    price_ask_raw: Any = None

    try:
        price_bid_raw = _call_with_timeout(client.get_price, timeout, tid, side="BUY")
        if isinstance(price_bid_raw, dict):
            best_bid = safe_float(price_bid_raw.get("price"))
        else:
            best_bid = safe_float(price_bid_raw)
    except Exception:
        pass
    try:
        price_ask_raw = _call_with_timeout(client.get_price, timeout, tid, side="SELL")
        if isinstance(price_ask_raw, dict):
            best_ask = safe_float(price_ask_raw.get("price"))
        else:
            best_ask = safe_float(price_ask_raw)
    except Exception:
        pass

    if best_ask is not None or best_bid is not None:
        if best_ask is not None and best_ask <= 0:
            best_ask = None
        if best_bid is not None and best_bid <= 0:
            best_bid = None
        if best_ask is not None and best_bid is not None:
            if best_bid <= best_ask:
                logger.debug(
                    "[ORDERBOOK_PRICE] token_id=%s price_bid=%s price_ask=%s "
                    "best_bid=%s best_ask=%s",
                    tid,
                    price_bid_raw,
                    price_ask_raw,
                    best_bid,
                    best_ask,
                )
                return {"best_bid": best_bid, "best_ask": best_ask}
            best_ask = None
            best_bid = None

    try:
        book = _call_with_timeout(client.get_order_book, timeout, tid)
        payload: Any = book
        if hasattr(book, "dict"):
            payload = book.dict()
        elif isinstance(book, dict):
            payload = book

        bids = payload.get("bids", []) if isinstance(payload, dict) else getattr(book, "bids", [])
        asks = payload.get("asks", []) if isinstance(payload, dict) else getattr(book, "asks", [])

        def _best(levels: Any, pick_max: bool) -> Optional[float]:
            prices: list[float] = []
            if isinstance(levels, list):
                for level in levels:
                    if isinstance(level, dict):
                        price = safe_float(level.get("price"))
                    elif isinstance(level, (list, tuple)) and level:
                        price = safe_float(level[0])
                    else:
                        price = None
                    if price is not None:
                        prices.append(float(price))
            if not prices:
                return None
            return max(prices) if pick_max else min(prices)

        book_bid = _best(bids, pick_max=True)
        book_ask = _best(asks, pick_max=False)
        if book_ask is not None and book_ask <= 0:
            book_ask = None
        if book_bid is not None and book_bid <= 0:
            book_bid = None

        if best_bid is None:
            best_bid = book_bid
        if best_ask is None:
            best_ask = book_ask

        # NOTE: Allow single-sided books (one side temporarily missing) instead of treating as empty.
        # This avoids "orderbook_empty" NOOPs that can freeze existing orders at stale prices.
        if best_bid is not None and best_ask is not None and best_bid > best_ask:
            return {"best_bid": None, "best_ask": None}
        logger.debug(
            "[ORDERBOOK] token_id=%s price_bid=%s price_ask=%s best_bid=%s best_ask=%s "
            "book_bid=%s book_ask=%s",
            tid,
            price_bid_raw,
            price_ask_raw,
            best_bid,
            best_ask,
            book_bid,
            book_ask,
        )
        return {"best_bid": best_bid, "best_ask": best_ask}
    except Exception:
        return {"best_bid": None, "best_ask": None}


def reconcile_one(
    token_id: str,
    desired_shares: float,
    my_shares: float,
    orderbook: Dict[str, Optional[float]],
    open_orders: List[Dict[str, Any]],
    now_ts: int,
    cfg: Dict[str, Any],
    state: Dict[str, Any],
    planned_token_notional: float = 0.0,
) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    deadband = float(cfg.get("deadband_shares") or 0)
    delta = desired_shares - my_shares
    if abs(delta) <= deadband and not open_orders:
        _clear_exit_sell_state(state, token_id)
        return actions

    abs_delta = abs(delta)
    phase = (state.get("topic_state", {}).get(token_id) or {}).get("phase")
    is_exiting = phase == "EXITING"

    mode = str(cfg.get("order_size_mode") or "fixed_shares").lower()
    size: float = 0.0
    target_order_usd: Optional[float] = None

    if mode == "auto_usd":
        ref_price = _mid_price(orderbook)
        if ref_price is None or ref_price <= 0:
            return actions

        min_usd = float(cfg.get("min_order_usd") or 5.0)
        min_shares = float(cfg.get("min_order_shares") or 0.0)
        max_usd = float(cfg.get("max_order_usd") or 25.0)
        if max_usd < min_usd:
            max_usd = min_usd

        k = float(cfg.get("_auto_order_k") or 0.3)

        delta_usd = abs_delta * ref_price
        order_usd = delta_usd * k
        if order_usd < min_usd:
            order_usd = min_usd
        if min_shares > 0:
            order_usd = max(order_usd, min_shares * ref_price)
        if order_usd > max_usd:
            order_usd = max_usd

        target_order_usd = order_usd
        size = order_usd / ref_price
    else:
        slice_min = float(cfg.get("slice_min") or 0)
        slice_max = float(cfg.get("slice_max") or abs_delta)
        if slice_max <= 0:
            slice_max = abs_delta

        size = min(abs_delta, slice_max)
        if slice_min > 0 and abs_delta > slice_min and size < slice_min:
            size = slice_min

    side = "BUY" if delta > 0 else "SELL"
    is_exit_sell = bool(is_exiting and side == "SELL" and my_shares > 1e-12)
    price: Optional[float] = None
    best_bid = orderbook.get("best_bid")
    best_ask = orderbook.get("best_ask")
    tick_size = float(cfg.get("tick_size") or 0)
    market_info = get_cached_market_info_for_token(state, token_id)
    meta = None
    if state is not None:
        status_cache = state.get("market_status_cache")
        if isinstance(status_cache, dict):
            cached = status_cache.get(token_id) or {}
            if isinstance(cached, dict):
                meta = cached.get("meta")
    if isinstance(meta, dict):
        market_tick_size = safe_float(
            meta.get("orderPriceMinTickSize")
            or meta.get("minimum_tick_size")
            or meta.get("minimumTickSize")
            or meta.get("tick_size")
            or meta.get("tickSize")
        )
        if market_tick_size and market_tick_size > 0:
            tick_size = market_tick_size
    if isinstance(market_info, dict):
        market_tick_size = safe_float(market_info.get("min_tick_size"))
        if market_tick_size and market_tick_size > 0:
            tick_size = market_tick_size
    taker_spread_thr = float(cfg.get("taker_spread_threshold") or 0.01)
    taker_enabled = bool(cfg.get("taker_enabled", True))
    exit_stage1_wait = max(0, int(cfg.get("exit_stage1_wait_sec") or 45))
    exit_stage2_wait = max(0, int(cfg.get("exit_stage2_wait_sec") or 120))
    exit_stage3_taker_max_spread = float(cfg.get("exit_stage3_taker_max_spread") or 0.05)
    exit_stage3_slice_ratio = float(cfg.get("exit_stage3_slice_ratio") or 0.5)
    exit_stage3_maker_hold_sec = max(0, int(cfg.get("exit_stage3_maker_hold_sec") or 180))
    exit_progress_min_shares = float(
        cfg.get("exit_progress_min_shares")
        or max(float(cfg.get("min_order_shares") or 0.0) / 2.0, 0.5)
    )
    exit_no_bid_pause_rounds = max(1, int(cfg.get("exit_no_bid_pause_rounds") or 3))
    exit_dead_book_pause_sec = max(0, int(cfg.get("exit_dead_book_pause_sec") or 300))
    exit_stage = 0
    exit_entry: Optional[Dict[str, Any]] = None
    exit_force_near_bid = False
    exit_force_taker = False

    if is_exit_sell:
        exit_entry = _get_or_create_exit_sell_entry(state, token_id, now_ts, my_shares=my_shares)
        progress_ref_shares = float(exit_entry.get("progress_ref_shares") or my_shares)
        if progress_ref_shares - float(my_shares) >= exit_progress_min_shares - 1e-12:
            exit_entry["last_progress_ts"] = int(now_ts)
            exit_entry["progress_ref_shares"] = float(my_shares)
            exit_entry["last_seen_shares"] = float(my_shares)
            exit_entry["pause_until"] = 0
            exit_entry["pause_reason"] = ""
            exit_entry["no_match_count"] = 0
            exit_entry["no_bid_count"] = 0
            _set_exit_sell_stage(token_id, exit_entry, 1, now_ts, "progress")
        else:
            exit_entry["last_seen_shares"] = float(my_shares)
        if best_bid is None or float(best_bid) <= 0:
            no_bid_count = int(exit_entry.get("no_bid_count") or 0) + 1
            exit_entry["no_bid_count"] = no_bid_count
            if no_bid_count >= exit_no_bid_pause_rounds:
                _set_exit_sell_pause(
                    token_id,
                    exit_entry,
                    now_ts,
                    exit_dead_book_pause_sec,
                    "dead_book",
                )
        else:
            exit_entry["no_bid_count"] = 0
        pause_until = int(exit_entry.get("pause_until") or 0)
        if pause_until > now_ts:
            cancel_actions: List[Dict[str, Any]] = []
            for order in open_orders:
                order_id = order.get("order_id") or order.get("id")
                if order_id:
                    cancel_actions.append(
                        {
                            "type": "cancel",
                            "order_id": order_id,
                            "token_id": token_id,
                            "ts": now_ts,
                        }
                    )
            if cancel_actions:
                logger.info(
                    "[EXIT_PAUSE_CANCEL] token_id=%s reason=%s open_orders=%s until=%s",
                    token_id,
                    exit_entry.get("pause_reason") or "paused",
                    len(cancel_actions),
                    pause_until,
                )
                return cancel_actions
            logger.info(
                "[EXIT_PAUSE_HOLD] token_id=%s reason=%s until=%s",
                token_id,
                exit_entry.get("pause_reason") or "paused",
                pause_until,
            )
            return actions
        since_progress = max(0, now_ts - int(exit_entry.get("last_progress_ts") or now_ts))
        if since_progress < exit_stage1_wait:
            exit_stage = 1
        elif since_progress < (exit_stage1_wait + exit_stage2_wait):
            exit_stage = 2
        else:
            exit_stage = 3
        _set_exit_sell_stage(token_id, exit_entry, exit_stage, now_ts, "elapsed_no_progress")
        exit_force_near_bid = exit_stage >= 2 and best_bid is not None
        exit_force_taker = exit_stage >= 3 and best_bid is not None
    else:
        _clear_exit_sell_state(state, token_id)

    spread: Optional[float] = None
    if best_bid is not None and best_ask is not None:
        try:
            spread = float(best_ask) - float(best_bid)
        except Exception:
            spread = None

    use_taker = bool(
        taker_enabled
        and spread is not None
        and spread <= (taker_spread_thr + 1e-12)
    )
    if exit_force_taker:
        spread_ok_for_exit = (
            spread is None
            or exit_stage3_taker_max_spread <= 0
            or spread <= (exit_stage3_taker_max_spread + 1e-12)
        )
        if spread_ok_for_exit:
            use_taker = True
        else:
            exit_force_taker = False
            if (
                is_exit_sell
                and exit_entry is not None
                and exit_stage >= 3
                and exit_stage3_maker_hold_sec > 0
            ):
                stage_since = int(exit_entry.get("stage_since_ts") or now_ts)
                stalled_sec = max(0, now_ts - stage_since)
                if stalled_sec >= exit_stage3_maker_hold_sec:
                    _set_exit_sell_pause(
                        token_id,
                        exit_entry,
                        now_ts,
                        exit_dead_book_pause_sec,
                        "stage3_stalled",
                    )
                    cancel_actions: List[Dict[str, Any]] = []
                    for order in open_orders:
                        order_id = order.get("order_id") or order.get("id")
                        if order_id:
                            cancel_actions.append(
                                {
                                    "type": "cancel",
                                    "order_id": order_id,
                                    "token_id": token_id,
                                    "ts": now_ts,
                                }
                            )
                    if cancel_actions:
                        logger.info(
                            "[EXIT_STAGE3_PAUSE_CANCEL] token_id=%s stalled_sec=%s open_orders=%s",
                            token_id,
                            stalled_sec,
                            len(cancel_actions),
                        )
                        return cancel_actions
                    logger.info(
                        "[EXIT_STAGE3_PAUSE_HOLD] token_id=%s stalled_sec=%s",
                        token_id,
                        stalled_sec,
                    )
                    return actions
    logger.debug(
        "[TAKER_CHECK] token_id=%s side=%s best_bid=%s best_ask=%s spread=%s thr=%s "
        "taker_enabled=%s use_taker=%s open_orders=%s",
        token_id,
        side,
        best_bid,
        best_ask,
        spread,
        taker_spread_thr,
        taker_enabled,
        use_taker,
        len(open_orders),
    )

    if exit_force_taker:
        price = round_to_tick(float(best_bid), tick_size, direction="down")
    elif use_taker:
        if side == "BUY":
            if best_ask is None:
                use_taker = False
            else:
                price = round_to_tick(float(best_ask), tick_size, direction="up")
        else:
            if best_bid is None:
                use_taker = False
            else:
                price = round_to_tick(float(best_bid), tick_size, direction="down")

    if not use_taker:
        if exit_force_near_bid and side == "SELL":
            if best_bid is not None:
                price = float(best_bid) + (tick_size if tick_size > 0 else 0.0)
                if best_ask is not None:
                    price = min(float(best_ask), price)
                price = round_to_tick(price, tick_size, direction="up")
            elif best_ask is not None:
                price = round_to_tick(float(best_ask), tick_size, direction="up")
        elif side == "BUY":
            if best_bid is not None:
                price = best_bid
            elif best_ask is not None:
                price = best_ask - tick_size
            if price is not None:
                price = round_to_tick(price, tick_size, direction="down")
        else:
            if best_ask is not None:
                price = best_ask
            elif best_bid is not None:
                price = best_bid + tick_size
            if price is not None:
                price = round_to_tick(price, tick_size, direction="up")

        maker_only = bool(cfg.get("maker_only"))
        if maker_only and tick_size and tick_size > 0:
            if side == "BUY" and best_ask is not None and price is not None and price >= best_ask:
                price = round_to_tick(best_ask - tick_size, tick_size, direction="down")
            if side == "SELL" and best_bid is not None and price is not None and price <= best_bid:
                price = round_to_tick(best_bid + tick_size, tick_size, direction="up")

            if price is None or price <= 0:
                return actions

    if price is None or price <= 0:
        return actions

    min_price = float(cfg.get("min_price") or 0.01)
    if min_price > 0 and price < min_price:
        price = min_price
        if tick_size > 0:
            price = round_to_tick(price, tick_size, direction="up")
    if min_price > 0 and price < min_price:
        price = min_price

    if mode == "auto_usd" and target_order_usd is not None:
        size = target_order_usd / price

    if is_exiting and side == "SELL" and bool(cfg.get("exit_full_sell", True)):
        size = abs_delta

    max_shares_cap = float(cfg.get("max_order_shares_cap") or 5000.0)
    if size > max_shares_cap:
        size = max_shares_cap

    allow_short = bool(cfg.get("allow_short"))
    sell_buffer_shares = float(cfg.get("sell_available_buffer_shares") or 0.01)
    if is_exiting and side == "SELL":
        # For exit flow, default to zero buffer so exact-min positions (e.g. 5.0)
        # are not reduced to below-min sellable size by a static buffer.
        exit_buffer_raw = cfg.get("exit_sell_available_buffer_shares")
        if exit_buffer_raw is None:
            sell_buffer_shares = 0.0
        else:
            sell_buffer_shares = max(0.0, float(exit_buffer_raw))
    sellable_shares = max(0.0, float(my_shares) - max(0.0, sell_buffer_shares))
    if side == "SELL" and not allow_short:
        size = min(size, sellable_shares)

    min_shares = float(cfg.get("min_order_shares") or 0.0)
    api_min_shares = 0.0
    if isinstance(market_info, dict):
        api_min_shares = safe_float(market_info.get("min_order_size")) or 0.0
    if isinstance(meta, dict):
        api_min_shares = max(
            api_min_shares,
            safe_float(
            meta.get("orderMinSize")
            or meta.get("minimum_order_size")
            or meta.get("min_order_size")
            )
            or 0.0,
        )
    effective_min_shares = max(min_shares, api_min_shares)
    size_step = api_min_shares if api_min_shares > 0 else 0.0
    if is_exit_sell and exit_stage >= 3 and 0 < exit_stage3_slice_ratio < 1:
        stage3_size = abs_delta * exit_stage3_slice_ratio
        if effective_min_shares > 0 and abs_delta > effective_min_shares + 1e-12:
            stage3_size = max(stage3_size, effective_min_shares)
        size = min(abs_delta, max(0.0, stage3_size))
    if side == "BUY" and desired_shares <= 0:
        return actions
    if side == "BUY" and my_shares >= desired_shares - deadband:
        logger.info(
            "[BUY_SKIP] token_id=%s reason=already_at_or_above_desired my_shares=%s desired=%s deadband=%s",
            token_id,
            my_shares,
            desired_shares,
            deadband,
        )
        return actions
    if side == "BUY" and abs_delta > 0 and size > abs_delta:
        size = abs_delta
    cap_shares = None
    cap_shares_remaining = None
    if price > 0 and side == "BUY":
        max_position_usd_per_token = float(cfg.get("max_position_usd_per_token") or 0.0)
        max_notional_per_token = float(cfg.get("max_notional_per_token") or 0.0)
        caps = []
        if max_position_usd_per_token > 0:
            caps.append(max_position_usd_per_token / price)
        if max_notional_per_token > 0:
            caps.append(max_notional_per_token / price)
        if caps:
            cap_shares = min(caps)
            max_notional = cap_shares * price
            remaining_notional = max_notional - planned_token_notional
            cap_shares_remaining = remaining_notional / price if price > 0 else 0

    small_taker_override = (
        side == "BUY"
        and effective_min_shares > 0
        and abs_delta + 1e-12 < effective_min_shares
        and taker_enabled
    )
    if small_taker_override:
        if best_ask is None:
            return actions
        use_taker = True
        price = round_to_tick(float(best_ask), tick_size, direction="up")
        min_price = float(cfg.get("min_price") or 0.01)
        max_price = 0.99
        if price > max_price:
            price = max_price
            if tick_size > 0:
                price = round_to_tick(price, tick_size, direction="down")
        if min_price > 0 and price < min_price:
            price = min_price
            if tick_size > 0:
                price = round_to_tick(price, tick_size, direction="up")
        size = abs_delta
        taker_min_usd = _effective_buy_min_usd(cfg, price, is_taker=True)
        if taker_min_usd > 0 and price > 0:
            bumped_size = max(size, taker_min_usd / price)
            if bumped_size > size + 1e-12:
                logger.info(
                    "[MIN_TAKER_BUMP] token_id=%s old_size=%s bumped_size=%s min_usd=%s",
                    token_id,
                    size,
                    bumped_size,
                    taker_min_usd,
                )
            size = bumped_size
        if open_orders:
            for order in open_orders:
                order_id = order.get("order_id") or order.get("id")
                if order_id:
                    actions.append(
                        {
                            "type": "cancel",
                            "order_id": order_id,
                            "token_id": token_id,
                            "ts": now_ts,
                        }
                    )
            open_orders = []

    small_exit_taker_override = (
        is_exiting
        and side == "SELL"
        and effective_min_shares > 0
        and abs_delta + 1e-12 < effective_min_shares
        and taker_enabled
    )
    if small_exit_taker_override:
        if best_bid is None:
            # No bid available: can't sell right now, but DO NOT mark as exited.
            # Keep topic_state alive so we retry next round.
            logger.info(
                "[DUST_EXIT_HOLD] token_id=%s remaining=%s < min_order=%s; no_bid, will retry",
                token_id,
                my_shares,
                effective_min_shares,
            )
            return actions
        # Force a taker sell of the full remaining amount, even if below min_step.
        use_taker = True
        price = round_to_tick(float(best_bid), tick_size, direction="down")
        size = abs_delta
        logger.info(
            "[DUST_EXIT_FORCE] token_id=%s remaining=%s price=%s; forcing taker sell",
            token_id,
            my_shares,
            price,
        )
        if open_orders:
            for order in open_orders:
                order_id = order.get("order_id") or order.get("id")
                if order_id:
                    actions.append(
                        {
                            "type": "cancel",
                            "order_id": order_id,
                            "token_id": token_id,
                            "ts": now_ts,
                        }
                    )
            open_orders = []

    if (
        effective_min_shares > 0
        and size < effective_min_shares
        and side == "BUY"
        and not small_taker_override
        and not use_taker
    ):
        if abs_delta + 1e-12 < effective_min_shares:
            # Completion tolerance: allow final top-up if overshoot <= 0.3 USD.
            completion_tol_usd = 0.3
            effective_min_usd = _effective_buy_min_usd(
                cfg,
                price,
                is_taker=False,
                maker_min_shares=effective_min_shares,
            )
            gap_usd = abs_delta * price
            overshoot = effective_min_usd - gap_usd
            if gap_usd <= completion_tol_usd + 1e-9:
                logger.info(
                    "[COMPLETE_NEAR] token_id=%s gap_usd=%s tol=%s",
                    token_id,
                    gap_usd,
                    completion_tol_usd,
                )
                return actions
            if overshoot <= completion_tol_usd + 1e-9:
                bumped_size = effective_min_usd / price
                if cap_shares_remaining is not None:
                    if cap_shares_remaining <= 0:
                        logger.debug(
                            "[MIN_BUMP_SKIP] token_id=%s no_remaining cap_shares_remaining=%s "
                            "planned_notional=%s my_shares=%s price=%s",
                            token_id,
                            cap_shares_remaining,
                            planned_token_notional,
                            my_shares,
                            price,
                        )
                        return actions
                    bumped_size = min(bumped_size, cap_shares_remaining)
                if bumped_size + 1e-12 < effective_min_shares:
                    logger.debug(
                        "[MIN_BUMP_SKIP] token_id=%s bumped_below_min bumped=%s min=%s",
                        token_id,
                        bumped_size,
                        effective_min_shares,
                    )
                    return actions
                logger.info(
                    "[MIN_BUMP_FINAL] token_id=%s gap_usd=%s min_usd=%s overshoot=%s",
                    token_id,
                    gap_usd,
                    effective_min_usd,
                    overshoot,
                )
                size = bumped_size
            else:
                logger.debug(
                    "[MIN_BUMP_SKIP] token_id=%s abs_delta=%s < min=%s gap_usd=%s overshoot=%s",
                    token_id,
                    abs_delta,
                    effective_min_shares,
                    gap_usd,
                    overshoot,
                )
                return actions
        bumped_size = effective_min_shares
        if abs_delta > 0:
            bumped_size = min(bumped_size, abs_delta)
        if cap_shares_remaining is not None:
            if cap_shares_remaining <= 0:
                logger.debug(
                    "[MIN_BUMP_SKIP] token_id=%s no_remaining cap_shares_remaining=%s "
                    "planned_notional=%s my_shares=%s price=%s",
                    token_id,
                    cap_shares_remaining,
                    planned_token_notional,
                    my_shares,
                    price,
                )
                return actions
            bumped_size = min(bumped_size, cap_shares_remaining)
        if bumped_size + 1e-12 < effective_min_shares:
            logger.debug(
                "[MIN_BUMP_SKIP] token_id=%s bumped_below_min bumped=%s min=%s",
                token_id,
                bumped_size,
                effective_min_shares,
            )
            return actions
        logger.info(
            "[MIN_BUMP] token_id=%s old_size=%s bumped_size=%s remaining=%s",
            token_id,
            size,
            bumped_size,
            cap_shares_remaining,
        )
        size = bumped_size
    taker_min_shares = 0.0
    if side == "BUY" and use_taker and price > 0:
        taker_min_usd = _effective_buy_min_usd(cfg, price, is_taker=True)
        taker_min_shares = taker_min_usd / price
        if size + 1e-12 < taker_min_shares and not small_taker_override:
            bumped_size = taker_min_shares
            if cap_shares_remaining is not None:
                if cap_shares_remaining <= 0:
                    logger.info(
                        "[MIN_TAKER_SKIP] token_id=%s no_remaining cap_shares_remaining=%s "
                        "planned_notional=%s my_shares=%s price=%s",
                        token_id,
                        cap_shares_remaining,
                        planned_token_notional,
                        my_shares,
                        price,
                    )
                    return actions
                bumped_size = min(bumped_size, cap_shares_remaining)
            if bumped_size + 1e-12 < taker_min_shares:
                logger.info(
                    "[MIN_TAKER_SKIP] token_id=%s bumped_below_min bumped=%s min=%s",
                    token_id,
                    bumped_size,
                    taker_min_shares,
                )
                return actions
            logger.info(
                "[MIN_TAKER_BUMP] token_id=%s old_size=%s bumped_size=%s min_usd=%s",
                token_id,
                size,
                bumped_size,
                taker_min_usd,
            )
            size = bumped_size
    if open_orders:
        total_open = 0.0
        for order in open_orders:
            try:
                total_open += float(order.get("size") or order.get("original_size") or 0.0)
            except Exception:
                continue
        if (
            use_taker
            and side == "BUY"
            and taker_min_shares > 0
            and size < taker_min_shares
            and not small_taker_override
            and not small_exit_taker_override
        ):
            size = max(size, taker_min_shares, total_open)
        elif (
            use_taker
            and side != "BUY"
            and effective_min_shares > 0
            and size < effective_min_shares
            and not small_taker_override
            and not small_exit_taker_override
        ):
            size = max(size, effective_min_shares, total_open)
        elif (
            not use_taker
            and effective_min_shares > 0
            and size + 1e-9 < effective_min_shares
            and not small_taker_override
            and not small_exit_taker_override
        ):
            actions = []
            for order in open_orders:
                order_id = order.get("order_id") or order.get("id")
                if order_id:
                    actions.append(
                        {
                            "type": "cancel",
                            "order_id": order_id,
                            "token_id": token_id,
                            "ts": now_ts,
                        }
                    )
            return actions
    if size > max_shares_cap:
        size = max_shares_cap
    if side == "SELL" and not allow_short:
        size = min(size, sellable_shares)
    sell_size_before_step = size if side == "SELL" else 0.0
    if size_step > 0 and not small_taker_override and not small_exit_taker_override:
        size = round_to_step(size, size_step, direction="down")
        if side == "BUY":
            min_shares_after_step = taker_min_shares if use_taker else effective_min_shares
            if min_shares_after_step > 0 and size + 1e-12 < min_shares_after_step:
                size = round_to_step(min_shares_after_step, size_step, direction="up")
        if size > max_shares_cap:
            size = max_shares_cap
        if side == "SELL" and not allow_short:
            size = min(size, sellable_shares)

    # === SELL accumulator: batch small sells until they meet min_order requirements ===
    if side == "SELL" and state is not None:
        sell_acc = state.setdefault("sell_shares_accumulator", {})
        if not isinstance(sell_acc, dict):
            state["sell_shares_accumulator"] = {}
            sell_acc = state["sell_shares_accumulator"]

        acc_entry = sell_acc.get(token_id)
        if isinstance(acc_entry, dict):
            acc_ts = int(acc_entry.get("ts") or 0)
            ttl_sec = int(cfg.get("sell_accumulator_ttl_sec") or 3600)
            if ttl_sec > 0 and now_ts - acc_ts > ttl_sec:
                sell_acc.pop(token_id, None)
                acc_entry = None

        # On EXITING, flush accumulated sells immediately
        if is_exiting and acc_entry:
            acc_shares = float(acc_entry.get("shares", 0.0))
            if acc_shares > 1e-12:
                size += acc_shares
                logger.info(
                    "[SELL_ACCUMULATOR_FLUSH] token_id=%s accumulated=%s new_total=%s reason=exiting",
                    token_id,
                    acc_shares,
                    size,
                )
                sell_acc.pop(token_id, None)
                acc_entry = None
                if not allow_short:
                    size = min(size, sellable_shares)

        # For non-exiting small sells, accumulate instead of discarding
        if (
            not is_exiting
            and effective_min_shares > 0
            and size + 1e-12 < effective_min_shares
            and not small_exit_taker_override
        ):
            acc_shares = float(acc_entry.get("shares", 0.0)) if acc_entry else 0.0
            add_shares = max(0.0, sell_size_before_step)
            if add_shares <= 1e-12:
                # Nothing sellable this round after constraints; keep only cancels.
                return actions
            new_acc = acc_shares + add_shares
            sell_acc[token_id] = {"shares": new_acc, "price": price, "ts": now_ts}
            logger.info(
                "[SELL_ACCUMULATOR_ADD] token_id=%s added=%s total=%s min=%s",
                token_id,
                add_shares,
                new_acc,
                effective_min_shares,
            )
            if new_acc + 1e-12 >= effective_min_shares:
                trigger_size = new_acc
                if not allow_short:
                    trigger_size = min(trigger_size, sellable_shares)
                if trigger_size + 1e-12 >= effective_min_shares:
                    size = trigger_size
                    sell_acc.pop(token_id, None)
                    logger.info(
                        "[SELL_ACCUMULATOR_TRIGGER] token_id=%s total=%s min=%s",
                        token_id,
                        size,
                        effective_min_shares,
                    )
                else:
                    logger.info(
                        "[SELL_ACCUMULATOR_HOLD] token_id=%s accumulated=%s sellable=%s min=%s",
                        token_id,
                        new_acc,
                        sellable_shares,
                        effective_min_shares,
                    )
                    return actions
            else:
                # Still below threshold: return only cancel actions (if any)
                return actions

    if (
        effective_min_shares > 0
        and size + 1e-9 < effective_min_shares
        and not small_taker_override
        and not small_exit_taker_override
    ):
        if is_exiting and side == "SELL":
            dust_eps = float(cfg.get("dust_exit_eps") or 0.0)
            dust_threshold = max(0.0, dust_eps)
            if my_shares > dust_threshold + 1e-9:
                logger.info(
                    "[EXIT_HOLD] token_id=%s remaining=%s sellable=%s min_order=%s dust_eps=%s",
                    token_id,
                    my_shares,
                    sellable_shares,
                    effective_min_shares,
                    dust_threshold,
                )
                return actions
            logger.info(
                "[DUST_EXIT] token_id=%s remaining=%s <= dust_eps=%s; treat as exited",
                token_id,
                my_shares,
                dust_threshold,
            )
            if state is not None:
                state.setdefault("dust_exits", {})[token_id] = {
                    "ts": now_ts,
                    "shares": my_shares,
                }
                topic_state = state.get("topic_state")
                if isinstance(topic_state, dict):
                    topic_state.pop(token_id, None)
        return actions

    if size <= 0:
        return actions

    # Check for maker order timeout: if a BUY order has been open too long, force taker
    maker_max_wait_sec = int(cfg.get("maker_max_wait_sec") or 0)
    maker_to_taker_enabled = bool(cfg.get("maker_to_taker_enabled", True))
    force_taker_for_timeout = False
    timed_out_orders: List[Dict[str, Any]] = []
    max_timeout_wait_sec: Optional[int] = None
    
    if (
        maker_to_taker_enabled
        and maker_max_wait_sec > 0
        and side == "BUY"
        and open_orders
        and not use_taker
    ):
        order_ts_by_id = state.get("order_ts_by_id") if state else None
        if not isinstance(order_ts_by_id, dict):
            order_ts_by_id = {}
        for order in open_orders:
            order_side = str(order.get("side") or "").upper()
            if order_side != "BUY":
                continue
            order_id = str(order.get("order_id") or order.get("id") or "")
            if not order_id:
                continue
            # Get order creation time
            order_ts = int(order.get("ts") or order_ts_by_id.get(order_id) or now_ts)
            wait_sec = now_ts - order_ts
            if wait_sec >= maker_max_wait_sec:
                timed_out_orders.append(order)
                if max_timeout_wait_sec is None or wait_sec > max_timeout_wait_sec:
                    max_timeout_wait_sec = wait_sec
                logger.info(
                    "[MAKER_TIMEOUT] token_id=%s order_id=%s wait=%ss (max=%ss), will switch to taker",
                    token_id,
                    order_id,
                    wait_sec,
                    maker_max_wait_sec,
                )
        if timed_out_orders:
            max_spread = float(cfg.get("maker_timeout_max_spread") or 0.0)
            if max_spread > 0 and spread is not None and spread > max_spread:
                logger.info(
                    "[MAKER_TIMEOUT_HOLD] token_id=%s wait=%ss spread=%s max=%s; keeping maker order",
                    token_id,
                    max_timeout_wait_sec or maker_max_wait_sec,
                    spread,
                    max_spread,
                )
            else:
                force_taker_for_timeout = True
                use_taker = True

    if open_orders:
        if use_taker or force_taker_for_timeout:
            actions = []
            for order in open_orders:
                order_id = order.get("order_id") or order.get("id")
                if order_id:
                    actions.append(
                        {
                            "type": "cancel",
                            "order_id": order_id,
                            "token_id": token_id,
                            "ts": now_ts,
                        }
                    )
            if size > 0:
                actions.append(
                    {
                        "type": "place",
                        "token_id": token_id,
                        "side": side,
                        "price": price,
                        "size": size,
                        "ts": now_ts,
                        **(
                            {"_available_shares": sellable_shares}
                            if side == "SELL" and not allow_short
                            else {}
                        ),
                        "_taker": True,
                        "_taker_spread": spread,
                        "_taker_thr": taker_spread_thr,
                        **(
                            {
                                "_exit_flow": True,
                                "_exit_stage": exit_stage,
                            }
                            if is_exit_sell
                            else {}
                        ),
                    }
                )
            if force_taker_for_timeout:
                wait_for_log = max_timeout_wait_sec or maker_max_wait_sec
                logger.info(
                    "[SWITCH_TO_TAKER] token_id=%s side=%s reason=maker_timeout wait=%ss",
                    token_id,
                    side,
                    wait_for_log,
                )
            else:
                logger.info(
                    "[SWITCH_TO_TAKER] token_id=%s side=%s spread=%s thr=%s",
                    token_id,
                    side,
                    spread,
                    taker_spread_thr,
                )
            return actions
        if is_exiting and side == "SELL" and bool(cfg.get("exit_full_sell", True)):
            eps = 1e-9
            total_open = 0.0
            for order in open_orders:
                try:
                    total_open += float(order.get("size") or order.get("original_size") or 0.0)
                except Exception:
                    continue
            if len(open_orders) != 1 or total_open < (abs_delta - eps):
                actions = []
                for order in open_orders:
                    order_id = order.get("order_id") or order.get("id")
                    if order_id:
                        actions.append(
                            {
                                "type": "cancel",
                                "order_id": order_id,
                                "token_id": token_id,
                                "ts": now_ts,
                            }
                        )
                actions.append(
                    {
                        "type": "place",
                        "token_id": token_id,
                        "side": "SELL",
                        "price": price,
                        "size": min(abs_delta, sellable_shares),
                        "ts": now_ts,
                        "_available_shares": sellable_shares,
                        "_exit_consolidate": True,
                        "_exit_flow": True,
                        "_exit_stage": exit_stage,
                    }
                )
                state.setdefault("last_reprice_ts_by_token", {})[token_id] = now_ts
                logger.info(
                    "[EXIT_CONSOLIDATE] token_id=%s remaining=%s open_orders=%s open_total=%s",
                    token_id,
                    abs_delta,
                    len(open_orders),
                    total_open,
                )
                return actions
        enable_reprice = bool(cfg.get("enable_reprice", False))
        if not enable_reprice:
            return actions
        active_order: Optional[Dict[str, Any]] = None
        if side == "BUY":
            active_order = max(open_orders, key=lambda order: float(order.get("price") or 0))
        else:
            active_order = min(open_orders, key=lambda order: float(order.get("price") or 0))
        if active_order:
            active_price = safe_float(active_order.get("price"))
            last_reprice_ts = int(
                state.setdefault("last_reprice_ts_by_token", {}).get(token_id) or 0
            )
            reprice_ticks = int(cfg.get("reprice_ticks") or cfg.get("reprice_min_ticks") or 1)
            cooldown_sec = int(cfg.get("reprice_cooldown_sec") or 0)
            cooldown_ok = cooldown_sec <= 0 or (now_ts - last_reprice_ts) >= cooldown_sec
            if active_price is not None and tick_size > 0 and cooldown_ok:
                ideal_price = price
                if ideal_price is not None and abs(ideal_price - active_price) < tick_size / 2:
                    return actions
                moved_ticks = None
                if ideal_price is not None:
                    moved_ticks = abs(ideal_price - active_price) / tick_size
                trigger = moved_ticks is not None and moved_ticks >= (reprice_ticks - 1e-9)
                if trigger:
                    logger.info(
                        "[REPRICE] token_id=%s side=%s active_price=%s ideal_price=%s "
                        "best_bid=%s best_ask=%s reprice_ticks=%s cooldown_sec=%s since_last=%s",
                        token_id,
                        side,
                        active_price,
                        ideal_price,
                        best_bid,
                        best_ask,
                        reprice_ticks,
                        cooldown_sec,
                        now_ts - last_reprice_ts,
                    )
                    for order in open_orders:
                        order_id = order.get("order_id") or order.get("id")
                        if order_id:
                            actions.append({"type": "cancel", "order_id": order_id})
                    actions.append(
                        {
                            "type": "place",
                            "token_id": token_id,
                            "side": side,
                            "price": price,
                            "size": size,
                            "ts": now_ts,
                            "_reprice": True,
                            **(
                                {
                                    "_exit_flow": True,
                                    "_exit_stage": exit_stage,
                                }
                                if is_exit_sell
                                else {}
                            ),
                        }
                    )
                    state.setdefault("last_reprice_ts_by_token", {})[token_id] = now_ts
                    return actions
        return actions

    actions.append(
        {
            "type": "place",
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "ts": now_ts,
            **(
                {"_available_shares": sellable_shares}
                if side == "SELL" and not allow_short
                else {}
            ),
            **(
                {
                    "_taker": True,
                    "_taker_spread": spread,
                    "_taker_thr": taker_spread_thr,
                }
                if use_taker
                else {}
            ),
            **(
                {
                    "_exit_flow": True,
                    "_exit_stage": exit_stage,
                }
                if is_exit_sell
                else {}
            ),
        }
    )
    return actions


def _extract_order_id(response: object) -> Optional[str]:
    candidates = (
        "order_id",
        "orderId",
        "orderID",
        "id",
        "orderHash",
        "order_hash",
        "hash",
    )

    visited: set[int] = set()

    def walk(obj: object) -> Optional[str]:
        if obj is None:
            return None
        if isinstance(obj, dict):
            obj_id = id(obj)
            if obj_id in visited:
                return None
            visited.add(obj_id)
            for key in candidates:
                if key in obj and obj[key] is not None:
                    return str(obj[key])
            for value in obj.values():
                nested = walk(value)
                if nested:
                    return nested
        if isinstance(obj, (list, tuple)):
            for item in obj:
                nested = walk(item)
                if nested:
                    return nested
        return None

    return walk(response)


def cancel_order(
    client: Any, order_id: str, timeout: Optional[float] = None
) -> Optional[object]:
    if not order_id:
        return None
    if is_v2_client(client):
        return cancel_order_v2(client, order_id, timeout)
    if callable(getattr(client, "cancel", None)):
        return _call_with_timeout(client.cancel, timeout, order_id=order_id)
    if callable(getattr(client, "cancel_order", None)):
        return _call_with_timeout(client.cancel_order, timeout, order_id)
    if callable(getattr(client, "cancel_orders", None)):
        return _call_with_timeout(client.cancel_orders, timeout, [order_id])

    private = getattr(client, "private", None)
    if private is not None:
        if callable(getattr(private, "cancel", None)):
            return _call_with_timeout(private.cancel, timeout, order_id=order_id)
        if callable(getattr(private, "cancel_order", None)):
            return _call_with_timeout(private.cancel_order, timeout, order_id)
        if callable(getattr(private, "cancel_orders", None)):
            return _call_with_timeout(private.cancel_orders, timeout, [order_id])

    return None


def _is_insufficient_balance(value: object) -> bool:
    def _text_has_shortage(text: str) -> bool:
        lowered = text.lower()
        shortage_keywords = ("insufficient", "not enough")
        balance_keywords = ("balance", "fund", "allowance")
        return any(key in lowered for key in shortage_keywords) and any(
            key in lowered for key in balance_keywords
        )

    if hasattr(value, "error_message"):
        try:
            if _is_insufficient_balance(getattr(value, "error_message")):
                return True
        except Exception:
            pass
    if hasattr(value, "response"):
        try:
            if _is_insufficient_balance(getattr(value, "response")):
                return True
        except Exception:
            pass
    if hasattr(value, "args"):
        try:
            for arg in getattr(value, "args", ()):
                if _is_insufficient_balance(arg):
                    return True
        except Exception:
            pass

    if isinstance(value, dict):
        for key in ("error", "message", "detail", "reason", "status"):
            if key in value and _is_insufficient_balance(value[key]):
                return True
    try:
        return _text_has_shortage(str(value))
    except Exception:
        return False


def _record_v2_sell_preflight(
    client: Any,
    token_id: str,
    timeout: Optional[float],
    state: Optional[Dict[str, Any]],
    now_ts: int,
) -> Optional[Dict[str, Any]]:
    if not is_v2_client(client):
        return None
    token_text = str(token_id or "").strip()
    if not token_text:
        return None
    diagnostic = preflight_conditional_sell_ready_v2(client, token_text, timeout)
    diagnostic["ts"] = now_ts
    if isinstance(state, dict):
        state.setdefault("conditional_preflight_by_token", {})[token_text] = diagnostic
    logger.warning(
        "[CONDITIONAL_PREFLIGHT] token_id=%s ready=%s balance=%s missing=%s msg=%s",
        token_text,
        diagnostic.get("ready"),
        diagnostic.get("balance"),
        diagnostic.get("missing_operators"),
        diagnostic.get("message"),
    )
    return diagnostic


def place_order(
    client: Any,
    token_id: str,
    side: str,
    price: float,
    size: float,
    fee_rate_bps: Optional[int] = None,
    allow_partial: bool = True,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    return place_limit_order_v2(
        client,
        token_id=token_id,
        side=side,
        price=price,
        size=size,
        allow_partial=allow_partial,
        timeout=timeout,
    )


def place_market_order(
    client: Any,
    token_id: str,
    side: str,
    amount: float,
    price: Optional[float] = None,
    fee_rate_bps: Optional[int] = None,
    order_type: str = "FAK",
    timeout: Optional[float] = None,
    user_usdc_balance: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Taker path via MarketOrderArgs.
    - BUY: amount is USD
    - SELL: amount is shares
    """
    kwargs: Dict[str, Any] = {
        "token_id": token_id,
        "side": side,
        "amount": amount,
        "price": price,
        "order_type": order_type,
        "timeout": timeout,
    }
    if user_usdc_balance is not None:
        kwargs["user_usdc_balance"] = user_usdc_balance
    return place_market_order_v2(
        client,
        **kwargs,
    )


def apply_actions(
    client: Any,
    actions: List[Dict[str, Any]],
    open_orders: List[Dict[str, Any]],
    now_ts: int,
    dry_run: bool,
    cfg: Optional[Dict[str, Any]] = None,
    state: Optional[Dict[str, Any]] = None,
    planned_by_token_usd: Optional[Dict[str, float]] = None,
) -> List[Dict[str, Any]]:
    updated = [dict(order) for order in open_orders]
    api_timeout_sec: Optional[float] = None
    if cfg is not None:
        try:
            api_timeout_sec = float(cfg.get("api_timeout_sec") or 15.0)
        except Exception:
            api_timeout_sec = 15.0
        if api_timeout_sec <= 0:
            api_timeout_sec = None

    def _short_msg(value: object, limit: int = 160) -> str:
        text = str(value)
        if len(text) <= limit:
            return text
        return f"{text[: limit - 3]}..."

    def _bump_backoff(token_id: str, kind: str, msg: str) -> None:
        if state is None or cfg is None:
            return
        key = f"{kind}:{token_id}"
        fail_counts = state.setdefault("fail_counts", {})
        fail_counts[key] = int(fail_counts.get(key) or 0) + 1
        base = float(cfg.get("place_fail_backoff_base_sec") or 2)
        cap = float(cfg.get("place_fail_backoff_cap_sec") or 60)
        wait = min(cap, base * (2 ** (fail_counts[key] - 1)))
        until = now_ts + wait
        state.setdefault("place_fail_until", {})[token_id] = int(until)
        lastlog = state.setdefault("place_fail_lastlog", {}).get(key, 0)
        if now_ts - int(lastlog or 0) >= 5:
            logger.warning(
                "[BACKOFF] token_id=%s kind=%s fail_count=%s wait=%.1fs msg=%s",
                token_id,
                kind,
                fail_counts[key],
                wait,
                _short_msg(msg),
            )
            state["place_fail_lastlog"][key] = now_ts

    def _set_sell_reconcile_lock(token_id: str, msg: str) -> None:
        if state is None or cfg is None or not token_id:
            return
        lock_sec = float(cfg.get("sell_reconcile_lock_sec") or 45.0)
        if lock_sec <= 0:
            return
        until = int(now_ts + lock_sec)
        lock_map = state.setdefault("sell_reconcile_lock_until", {})
        prev_until = int(lock_map.get(token_id) or 0)
        if until > prev_until:
            lock_map[token_id] = until
        lastlog = state.setdefault("sell_reconcile_lock_lastlog", {}).get(token_id, 0)
        if now_ts - int(lastlog or 0) >= 5:
            logger.warning(
                "[SELL_LOCK_SET] token_id=%s until=%s reason=%s",
                token_id,
                lock_map.get(token_id),
                _short_msg(msg),
            )
            state["sell_reconcile_lock_lastlog"][token_id] = now_ts

    def _clear_sell_reconcile_lock(token_id: str) -> None:
        if state is None or not token_id:
            return
        lock_map = state.get("sell_reconcile_lock_until")
        if isinstance(lock_map, dict) and token_id in lock_map:
            lock_map.pop(token_id, None)
            logger.info("[SELL_LOCK_CLEAR] token_id=%s", token_id)
    for action in actions:
        if action.get("type") == "cancel":
            order_id = action.get("order_id")
            if not order_id:
                continue
            if dry_run:
                updated = [o for o in updated if str(o.get("order_id")) != str(order_id)]
                continue
            try:
                cancel_order(client, str(order_id), api_timeout_sec)
                updated = [o for o in updated if str(o.get("order_id")) != str(order_id)]
            except Exception as exc:
                logger.warning("cancel_order failed order_id=%s: %s", order_id, exc)
            continue

    for action in actions:
        if action.get("type") != "place":
            continue
        is_taker = bool(action.get("_taker"))
        token_id_action = str(action.get("token_id") or "")
        side_u = str(action.get("side") or "").upper()
        exit_stage_flag = int(action.get("_exit_stage") or 0)
        is_exit_flow = bool(action.get("_exit_flow")) and side_u == "SELL"
        if dry_run:
            if not is_taker:
                updated.append(
                    {
                        "order_id": "dry_run",
                        "side": action.get("side"),
                        "price": action.get("price"),
                        "size": action.get("size"),
                        "ts": now_ts,
                    }
                )
            else:
                logger.info(
                    "[DRY_RUN_TAKER] token_id=%s side=%s price=%s size=%s",
                    action.get("token_id"),
                    action.get("side"),
                    action.get("price"),
                    action.get("size"),
                )
            continue
        if (not is_taker) and cfg is not None and bool(cfg.get("dedupe_place", True)):
            token_id = str(action.get("token_id") or "")
            want_side = str(action.get("side") or "")
            try:
                want_price = float(action.get("price") or 0.0)
            except Exception:
                want_price = 0.0
            try:
                want_size = float(action.get("size") or 0.0)
            except Exception:
                want_size = 0.0

            eps_p = float(cfg.get("dedupe_place_price_eps") or 1e-6)
            eps_rel = float(cfg.get("dedupe_place_size_rel_eps") or 1e-6)

            dup = False
            for order in updated:
                try:
                    if str(order.get("side") or "") != want_side:
                        continue
                    order_price = float(order.get("price") or 0.0)
                    if abs(order_price - want_price) > eps_p:
                        continue
                    order_size = float(order.get("size") or 0.0)
                    if want_size > 0 and abs(order_size - want_size) > max(
                        1e-9, eps_rel * want_size
                    ):
                        continue
                    logger.info(
                        "[DEDUP] token_id=%s skip duplicate place (order_id=%s)",
                        token_id,
                        order.get("order_id"),
                    )
                    dup = True
                    break
                except Exception:
                    continue

            if dup:
                continue
        allow_partial = True
        if cfg is not None:
            allow_partial = bool(cfg.get("allow_partial", True))
        taker_fak_retry_max = 1
        taker_fak_retry_delay_sec = 0.5
        taker_fak_fallback_to_maker = True
        if cfg is not None:
            try:
                taker_fak_retry_max = max(0, int(cfg.get("taker_fak_retry_max") or 1))
            except Exception:
                taker_fak_retry_max = 1
            try:
                taker_fak_retry_delay_sec = max(
                    0.0, float(cfg.get("taker_fak_retry_delay_sec") or 0.5)
                )
            except Exception:
                taker_fak_retry_delay_sec = 0.5
            taker_fak_fallback_to_maker = bool(cfg.get("taker_fak_fallback_to_maker", True))
        if side_u == "BUY":
            taker_fak_fallback_to_maker = False
        if is_exit_flow:
            taker_fak_fallback_to_maker = False
        executed_as_taker = is_taker
        size_for_record = float(action.get("size") or 0.0)
        price = float(action.get("price") or 0.0)
        size = float(action.get("size") or 0.0)
        # Ensure minimum buy notional (avoid exchange rejection like "min size: $1").
        if side_u == "BUY" and cfg is not None and price > 0 and size > 0:
            max_order_usd = float(cfg.get("max_order_usd") or 0.0)
            effective_min_usd = _effective_buy_min_usd(cfg, price, is_taker=is_taker)
            order_usd = abs(size) * price
            if order_usd < effective_min_usd:
                if max_order_usd > 0 and effective_min_usd > max_order_usd + 1e-9:
                    logger.warning(
                        "[%s] token_id=%s order_usd=%s min_usd=%s max_usd=%s",
                        "MIN_TAKER_SKIP" if is_taker else "MIN_BUY_SKIP",
                        action.get("token_id"),
                        order_usd,
                        effective_min_usd,
                        max_order_usd,
                    )
                    continue
                new_size = effective_min_usd / price
                size = new_size
                size_for_record = new_size
                action["size"] = new_size
                logger.info(
                    "[%s] token_id=%s old_usd=%s new_usd=%s",
                    "MIN_TAKER_BUMP" if is_taker else "MIN_BUY_BUMP",
                    action.get("token_id"),
                    order_usd,
                    effective_min_usd,
                )
        try:
            if is_taker:
                if side_u == "BUY" and cfg is not None and planned_by_token_usd is not None:
                    max_per_token = float(cfg.get("max_notional_per_token") or 0.0)
                    if max_per_token > 0:
                        token_id_check = str(action.get("token_id"))
                        planned = float(planned_by_token_usd.get(token_id_check, 0.0))
                        order_usd = abs(size) * price
                        if planned + order_usd > max_per_token:
                            logger.warning(
                                "[TAKER_BLOCKED] token_id=%s would_exceed planned=%s order=%s max=%s",
                                token_id_check,
                                planned,
                                order_usd,
                                max_per_token,
                            )
                            if state is not None:
                                state.setdefault("taker_blocked_count", 0)
                                state["taker_blocked_count"] = state["taker_blocked_count"] + 1
                            continue

                if side_u == "BUY":
                    amount = abs(size) * price
                    # Enforce min notional right before sending to avoid rounding drift.
                    if cfg is not None and price > 0:
                        effective_min_usd = _effective_buy_min_usd(
                            cfg, price, is_taker=True
                        )
                        if amount < effective_min_usd:
                            amount = effective_min_usd
                            size_for_record = amount / price
                            action["size"] = size_for_record
                else:
                    amount = abs(size)
                taker_order_type = "FAK"
                if cfg is not None:
                    if cfg.get("taker_order_type"):
                        taker_order_type = str(cfg.get("taker_order_type")).upper()
                    else:
                        taker_order_type = "FAK" if allow_partial else "FOK"
                taker_token_id = str(action.get("token_id"))
                user_usdc_balance = (
                    _resolve_market_buy_user_usdc_balance(action, state)
                    if side_u == "BUY"
                    else None
                )
                response = None
                try:
                    taker_kwargs = {
                        "token_id": taker_token_id,
                        "side": side_u,
                        "amount": amount,
                        "price": price,
                        "order_type": taker_order_type,
                        "timeout": api_timeout_sec,
                    }
                    if user_usdc_balance is not None:
                        taker_kwargs["user_usdc_balance"] = user_usdc_balance
                    response = place_market_order(client, **taker_kwargs)
                except Exception as taker_exc:
                    if not _is_fak_no_match(taker_exc):
                        raise
                    last_fak_exc: Exception = taker_exc
                    for attempt in range(1, taker_fak_retry_max + 1):
                        if taker_fak_retry_delay_sec > 0:
                            time.sleep(taker_fak_retry_delay_sec)
                        try:
                            retry_taker_kwargs = {
                                "token_id": taker_token_id,
                                "side": side_u,
                                "amount": amount,
                                "price": price,
                                "order_type": taker_order_type,
                                "timeout": api_timeout_sec,
                            }
                            if user_usdc_balance is not None:
                                retry_taker_kwargs["user_usdc_balance"] = user_usdc_balance
                            response = place_market_order(client, **retry_taker_kwargs)
                            logger.info(
                                "[TAKER_FAK_RETRY_OK] token_id=%s side=%s attempt=%s/%s",
                                taker_token_id,
                                side_u,
                                attempt,
                                taker_fak_retry_max,
                            )
                            break
                        except Exception as retry_exc:
                            last_fak_exc = retry_exc
                            if not _is_fak_no_match(retry_exc):
                                raise
                            logger.warning(
                                "[TAKER_FAK_RETRY_FAIL] token_id=%s side=%s attempt=%s/%s: %s",
                                taker_token_id,
                                side_u,
                                attempt,
                                taker_fak_retry_max,
                                retry_exc,
                            )
                    if response is None:
                        if is_exit_flow:
                            _record_exit_sell_no_match(
                                state,
                                token_id_action,
                                now_ts,
                                cfg,
                            )
                        if taker_fak_fallback_to_maker:
                            logger.warning(
                                "[TAKER_FAK_FALLBACK] token_id=%s side=%s retries=%s -> maker GTC",
                                taker_token_id,
                                side_u,
                                taker_fak_retry_max,
                            )
                            response = place_order(
                                client,
                                token_id=taker_token_id,
                                side=side_u,
                                price=price,
                                size=size_for_record,
                                allow_partial=allow_partial,
                                timeout=api_timeout_sec,
                            )
                            executed_as_taker = False
                        else:
                            if side_u == "BUY":
                                logger.info(
                                    "[BUY_TAKER_HOLD] token_id=%s retries=%s reason=no_match",
                                    token_id_action,
                                    taker_fak_retry_max,
                                )
                            else:
                                logger.info(
                                    "[EXIT_TAKER_HOLD] token_id=%s side=%s stage=%s reason=no_match",
                                    token_id_action,
                                    side_u,
                                    exit_stage_flag,
                                )
                            raise last_fak_exc
            else:
                if side_u == "BUY" and cfg is not None and price > 0:
                    min_order_usd = float(cfg.get("min_order_usd") or 0.0)
                    min_order_shares = float(cfg.get("min_order_shares") or 0.0)
                    effective_min_usd = max(1.0, min_order_usd)
                    if min_order_shares > 0:
                        effective_min_usd = max(effective_min_usd, min_order_shares * price)
                    if abs(size) * price < effective_min_usd:
                        size_for_record = effective_min_usd / price
                        action["size"] = size_for_record
                response = place_order(
                    client,
                    token_id=str(action.get("token_id")),
                    side=side_u,
                    price=price,
                    size=size_for_record,
                    allow_partial=allow_partial,
                    timeout=api_timeout_sec,
                )
        except Exception as exc:
            logger.warning("place_order failed token_id=%s: %s", action.get("token_id"), exc)
            if not cfg or not cfg.get("retry_on_insufficient_balance"):
                continue
            if side_u != "BUY":
                if _is_insufficient_balance(exc) and state is not None:
                    token_id = str(action.get("token_id") or "")
                    conditional_diag = _record_v2_sell_preflight(
                        client,
                        token_id,
                        api_timeout_sec,
                        state,
                        now_ts,
                    )
                    if conditional_diag and conditional_diag.get("ready") is False:
                        _set_sell_reconcile_lock(
                            token_id,
                            str(conditional_diag.get("message") or exc),
                        )
                        refresh_tokens = state.setdefault("force_refresh_tokens", [])
                        if token_id and token_id not in refresh_tokens:
                            refresh_tokens.append(token_id)
                        _bump_backoff(
                            token_id,
                            "sell_insufficient",
                            str(conditional_diag.get("message") or exc),
                        )
                        continue
                    _set_sell_reconcile_lock(token_id, str(exc))
                    refresh_tokens = state.setdefault("force_refresh_tokens", [])
                    if token_id and token_id not in refresh_tokens:
                        refresh_tokens.append(token_id)
                    if price > 0 and size > 0:
                        min_order_usd = float(cfg.get("min_order_usd") or 0.0)
                        min_order_shares = float(cfg.get("min_order_shares") or 0.0)
                        shrink_factor = float(cfg.get("retry_shrink_factor") or 0.5)
                        max_retry = max(1, int(cfg.get("sell_insufficient_retry_max") or 3))
                        available_hint = safe_float(action.get("_available_shares")) or 0.0
                        retry_size = min(size, available_hint) if available_hint > 0 else size
                        retry_ok = False
                        for attempt in range(1, max_retry + 1):
                            old_usd = abs(retry_size) * price
                            new_usd = max(min_order_usd, old_usd * shrink_factor)
                            if min_order_shares > 0:
                                new_usd = max(new_usd, min_order_shares * price)
                            new_size = new_usd / price
                            if available_hint > 0:
                                new_size = min(new_size, available_hint)
                            if new_size >= retry_size * (1 - 1e-9):
                                break
                            if min_order_shares > 0 and new_size + 1e-12 < min_order_shares:
                                break
                            try:
                                if is_taker:
                                    response = place_market_order(
                                        client,
                                        token_id=str(action.get("token_id")),
                                        side=side_u,
                                        amount=abs(new_size),
                                        price=price,
                                        order_type="FAK" if allow_partial else "FOK",
                                        timeout=api_timeout_sec,
                                    )
                                else:
                                    response = place_order(
                                        client,
                                        token_id=str(action.get("token_id")),
                                        side=side_u,
                                        price=price,
                                        size=new_size,
                                        allow_partial=allow_partial,
                                        timeout=api_timeout_sec,
                                    )
                            except Exception as retry_exc:
                                logger.warning(
                                    "[RETRY_SELL_FAIL] token_id=%s attempt=%s old_usd=%s new_usd=%s: %s",
                                    action.get("token_id"),
                                    attempt,
                                    old_usd,
                                    new_usd,
                                    retry_exc,
                                )
                                if not _is_insufficient_balance(retry_exc):
                                    _bump_backoff(token_id, "sell_insufficient", str(retry_exc))
                                    break
                                retry_size = new_size
                                continue
                            size_for_record = new_size
                            retry_ok = True
                            _clear_sell_reconcile_lock(str(action.get("token_id") or ""))
                            logger.info(
                                "[RETRY_SELL_OK] token_id=%s attempt=%s old_usd=%s new_usd=%s",
                                action.get("token_id"),
                                attempt,
                                old_usd,
                                new_usd,
                            )
                            order_id = response.get("order_id")
                            if order_id and not is_taker:
                                updated.append(
                                    {
                                        "order_id": order_id,
                                        "side": action.get("side"),
                                        "price": action.get("price"),
                                        "size": size_for_record,
                                        "ts": now_ts,
                                    }
                                )
                            place_fail_until = state.get("place_fail_until")
                            if isinstance(place_fail_until, dict):
                                place_fail_until.pop(token_id, None)
                            logger.info(
                                "[BACKOFF_CLEAR] token_id=%s place succeeded -> clear place_fail_until",
                                token_id,
                            )
                            break
                        if retry_ok:
                            continue
                    _bump_backoff(token_id, "sell_insufficient", str(exc))
                continue
            if not _is_insufficient_balance(exc):
                continue
            if price <= 0 or size <= 0:
                continue
            min_order_usd = float(cfg.get("min_order_usd") or 0.0)
            min_order_shares = float(cfg.get("min_order_shares") or 0.0)
            shrink_factor = float(cfg.get("retry_shrink_factor") or 0.5)
            old_usd = abs(size) * price
            new_usd = max(min_order_usd, old_usd * shrink_factor)
            if min_order_shares > 0:
                new_usd = max(new_usd, min_order_shares * price)
            if new_usd >= old_usd * (1 - 1e-9):
                continue
            new_size = new_usd / price
            if min_order_shares > 0 and new_size + 1e-12 < min_order_shares:
                continue
            try:
                if is_taker:
                    user_usdc_balance = (
                        _resolve_market_buy_user_usdc_balance(action, state)
                        if side_u == "BUY"
                        else None
                    )
                    retry_balance_kwargs = {
                        "token_id": str(action.get("token_id")),
                        "side": side_u,
                        "amount": abs(new_size) * price,
                        "price": price,
                        "order_type": "FAK" if allow_partial else "FOK",
                        "timeout": api_timeout_sec,
                    }
                    if user_usdc_balance is not None:
                        retry_balance_kwargs["user_usdc_balance"] = user_usdc_balance
                    response = place_market_order(client, **retry_balance_kwargs)
                else:
                    response = place_order(
                        client,
                        token_id=str(action.get("token_id")),
                        side=side_u,
                        price=price,
                        size=new_size,
                        allow_partial=allow_partial,
                        timeout=api_timeout_sec,
                    )
            except Exception as retry_exc:
                logger.warning(
                    "[RETRY_BALANCE_FAIL] token_id=%s old_usd=%s new_usd=%s: %s",
                    action.get("token_id"),
                    old_usd,
                    new_usd,
                    retry_exc,
                )
                continue
            size_for_record = new_size
            logger.info(
                "[RETRY_BALANCE_OK] token_id=%s old_usd=%s new_usd=%s",
                action.get("token_id"),
                old_usd,
                new_usd,
            )
        if side_u == "SELL":
            _clear_sell_reconcile_lock(str(action.get("token_id") or ""))
        order_id = response.get("order_id")
        # CRITICAL: Update accumulator for SELL orders (reduce on sell)
        if state is not None and side_u == "SELL" and price > 0 and size_for_record > 0:
            token_id = str(action.get("token_id") or "")
            sell_usd = abs(size_for_record) * price
            if token_id and sell_usd > 0:
                accumulator = state.get("buy_notional_accumulator")
                if isinstance(accumulator, dict) and token_id in accumulator:
                    acc_data = accumulator[token_id]
                    if isinstance(acc_data, dict):
                        old_usd = float(acc_data.get("usd", 0.0))
                        new_usd = max(0.0, old_usd - sell_usd)
                        if new_usd <= 0.01:
                            # Position effectively cleared
                            accumulator.pop(token_id, None)
                            logger.info(
                                "[ACCUMULATOR_CLEAR_SELL] token_id=%s old=%s sell=%s reason=cleared",
                                token_id,
                                old_usd,
                                sell_usd,
                            )
                        else:
                            acc_data["usd"] = new_usd
                            acc_data["last_ts"] = now_ts
                            logger.info(
                                "[ACCUMULATOR_REDUCE_SELL] token_id=%s old=%s sell=%s new=%s",
                                token_id,
                                old_usd,
                                sell_usd,
                                new_usd,
                            )

        if (
            state is not None
            and side_u == "BUY"
            and price > 0
            and size_for_record > 0
        ):
            token_id = str(action.get("token_id") or "")
            usd = abs(size_for_record) * price
            if token_id and usd > 0:
                recent_orders = state.setdefault("recent_buy_orders", [])
                if not isinstance(recent_orders, list):
                    state["recent_buy_orders"] = []
                    recent_orders = state["recent_buy_orders"]
                recent_orders.append(
                    {
                        "token_id": token_id,
                        "usd": float(usd),
                        "ts": int(now_ts),
                    }
                )
                if executed_as_taker:
                    taker_orders = state.setdefault("taker_buy_orders", [])
                    if not isinstance(taker_orders, list):
                        state["taker_buy_orders"] = []
                        taker_orders = state["taker_buy_orders"]
                    taker_orders.append(
                        {
                            "token_id": token_id,
                            "usd": float(usd),
                            "ts": int(now_ts),
                        }
                    )
                # CRITICAL: Update buy notional accumulator (first line of defense)
                accumulator = state.setdefault("buy_notional_accumulator", {})
                if not isinstance(accumulator, dict):
                    state["buy_notional_accumulator"] = {}
                    accumulator = state["buy_notional_accumulator"]
                if token_id not in accumulator:
                    accumulator[token_id] = {"usd": 0.0, "last_ts": now_ts}
                accumulator[token_id]["usd"] = float(accumulator[token_id].get("usd", 0.0)) + float(usd)
                accumulator[token_id]["last_ts"] = int(now_ts)
                logger.info(
                    "[ACCUMULATOR_UPDATE] token_id=%s added_usd=%s total_usd=%s is_taker=%s",
                    token_id,
                    usd,
                    accumulator[token_id]["usd"],
                    executed_as_taker,
                )
        if order_id:
            if state is not None:
                token_id = str(action.get("token_id") or "")
                for key_prefix in ("sell_insufficient", "sell_insufficient_shrink"):
                    state.get("fail_counts", {}).pop(f"{key_prefix}:{token_id}", None)
                    state.get("place_fail_lastlog", {}).pop(f"{key_prefix}:{token_id}", None)
                place_fail_until = state.get("place_fail_until")
                if isinstance(place_fail_until, dict):
                    place_fail_until.pop(token_id, None)
                logger.info(
                    "[BACKOFF_CLEAR] token_id=%s place succeeded -> clear place_fail_until",
                    token_id,
                )
            if (not executed_as_taker) or side_u == "SELL":
                updated.append(
                    {
                        "order_id": order_id,
                        "side": action.get("side"),
                        "price": action.get("price"),
                        "size": size_for_record,
                        "ts": now_ts,
                    }
                )
    return updated




def _as_dict(obj: Any) -> Optional[Dict[str, Any]]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "dict"):
        try:
            return obj.dict()
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        try:
            return dict(obj.__dict__)
        except Exception:
            pass
    return None


def _coerce_list(payload: Any) -> List[Any]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "orders", "result", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return []
    for key in ("data", "orders", "result", "items"):
        value = getattr(payload, key, None)
        if isinstance(value, list):
            return value
    return []


def _parse_created_ts(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            parsed = int(value)
            return parsed if parsed > 0 else None
        if isinstance(value, str):
            text = value.strip()
            num = safe_float(text)
            if num is not None and num > 0:
                numeric = int(num)
                return numeric // 1000 if numeric > 10_000_000_000 else numeric
            from datetime import datetime, timezone

            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed_dt = datetime.fromisoformat(text)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            return int(parsed_dt.timestamp())
    except Exception:
        return None
    return None


def _normalize_open_order(order: Any) -> Optional[Dict[str, Any]]:
    data = _as_dict(order)
    if not data:
        return None

    order_id = (
        data.get("id")
        or data.get("order_id")
        or data.get("orderId")
        or data.get("orderID")
        or data.get("order_hash")
        or data.get("orderHash")
    )
    token_id = (
        data.get("asset_id")
        or data.get("assetId")
        or data.get("token_id")
        or data.get("tokenId")
        or data.get("clobTokenId")
        or data.get("clob_token_id")
    )
    if not order_id or not token_id:
        return None

    side = data.get("side") or data.get("taker_side") or data.get("maker_side")
    side_norm = side.upper() if isinstance(side, str) else str(side).upper()

    price = safe_float(data.get("price") or data.get("limit_price") or data.get("limitPrice"))
    size = safe_float(
        data.get("size")
        or data.get("original_size")
        or data.get("originalSize")
        or data.get("remaining_size")
        or data.get("remainingSize")
        or data.get("amount")
    )

    created_ts = _parse_created_ts(data.get("created_at") or data.get("createdAt") or data.get("timestamp"))
    return {
        "order_id": str(order_id),
        "token_id": str(token_id),
        "side": side_norm,
        "price": price,
        "size": size,
        "created_ts": created_ts,
    }


def fetch_open_orders_norm(
    client: Any, timeout_sec: Optional[float] = None
) -> tuple[list[dict[str, Any]], bool, str | None]:
    return fetch_open_orders_norm_v2(client, timeout_sec)
