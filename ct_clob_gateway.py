from __future__ import annotations

import inspect
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


V2_CONDITIONAL_OPERATOR_ADDRESSES = (
    "0xE111180000d2663C0091e4f400237545B87B996B",
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
    "0xe2222d279d744050d28e00520010520000310F59",
)


def _normalize_privkey(key: str) -> str:
    return key[2:] if key.startswith(("0x", "0X")) else key


def is_v2_client(client: Any) -> bool:
    if client is None:
        return False
    module_name = str(getattr(type(client), "__module__", "") or "")
    if module_name.startswith("py_clob_client_v2"):
        return True
    return callable(getattr(client, "get_open_orders", None)) and callable(
        getattr(client, "get_clob_market_info", None)
    )


def init_v2_client(
    private_key: str,
    funder_address: str,
    cfg: Optional[Dict[str, Any]] = None,
) -> Any:
    from py_clob_client_v2 import BuilderConfig, ClobClient

    if cfg:
        host = cfg.get("poly_host") or os.getenv("POLY_HOST", "https://clob.polymarket.com")
        chain_id = int(cfg.get("poly_chain_id") or os.getenv("POLY_CHAIN_ID", "137"))
        signature_type = int(cfg.get("poly_signature") or os.getenv("POLY_SIGNATURE", "2"))
        builder_code = str(
            cfg.get("poly_builder_code") or os.getenv("POLY_BUILDER_CODE", "")
        ).strip()
    else:
        host = os.getenv("POLY_HOST", "https://clob.polymarket.com")
        chain_id = int(os.getenv("POLY_CHAIN_ID", "137"))
        signature_type = int(os.getenv("POLY_SIGNATURE", "2"))
        builder_code = str(os.getenv("POLY_BUILDER_CODE", "")).strip()

    key = _normalize_privkey(private_key)
    builder_config = None
    if builder_code:
        builder_config = BuilderConfig(builder_code=builder_code)

    client = ClobClient(
        host=host,
        chain_id=chain_id,
        key=key,
        signature_type=signature_type,
        funder=funder_address,
        builder_config=builder_config,
    )
    api_creds = _get_or_create_api_creds_v2(client)
    client.set_api_creds(api_creds)
    try:
        setattr(client, "api_creds", api_creds)
    except Exception:
        pass
    return client


def _get_or_create_api_creds_v2(client: Any) -> Any:
    """
    Prefer derivation before creation to avoid noisy 400s for accounts that already
    have a bound API key on the server.
    """
    derive = getattr(client, "derive_api_key", None)
    create = getattr(client, "create_api_key", None)
    create_or_derive = getattr(client, "create_or_derive_api_key", None)
    last_exc: Exception | None = None

    for attempt in range(3):
        if callable(derive):
            try:
                creds = derive()
                if getattr(creds, "api_key", None):
                    return creds
            except Exception as exc:
                last_exc = exc

        if callable(create):
            try:
                creds = create()
                if getattr(creds, "api_key", None):
                    return creds
            except Exception as exc:
                last_exc = exc

        # Some CLOB responses return "Could not create api key" during brief
        # auth/backend races even though a derived key becomes available right
        # after.  Try the SDK combined path and then loop back to derive.
        if callable(create_or_derive):
            try:
                creds = create_or_derive()
                if getattr(creds, "api_key", None):
                    return creds
            except Exception as exc:
                last_exc = exc

        if attempt < 2:
            time.sleep(1.0 + attempt)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("V2 client does not expose an API credential bootstrap method")


def _supports_timeout(func: Any) -> bool:
    try:
        return "timeout" in inspect.signature(func).parameters
    except Exception:
        return False


def _call_with_timeout(
    func: Any, timeout: Optional[float], *args: Any, **kwargs: Any
) -> Any:
    if timeout is not None and timeout > 0 and _supports_timeout(func):
        kwargs["timeout"] = timeout
    return func(*args, **kwargs)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _parse_created_ts(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            parsed = int(value)
            return parsed // 1000 if parsed > 10_000_000_000 else parsed
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            numeric = _safe_float(text)
            if numeric is not None and numeric > 0:
                parsed = int(numeric)
                return parsed // 1000 if parsed > 10_000_000_000 else parsed
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed_dt = datetime.fromisoformat(text)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            return int(parsed_dt.timestamp())
    except Exception:
        return None
    return None


def _as_dict(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if hasattr(payload, "dict"):
        try:
            data = payload.dict()
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    if hasattr(payload, "__dict__"):
        try:
            data = dict(payload.__dict__)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {}


def _extract_order_id(response: Any) -> Optional[str]:
    candidates = ("order_id", "orderID", "orderId", "id", "hash", "order_hash", "orderHash")
    visited: set[int] = set()

    def walk(obj: Any) -> Optional[str]:
        if obj is None:
            return None
        obj_id = id(obj)
        if obj_id in visited:
            return None
        visited.add(obj_id)

        if isinstance(obj, dict):
            for key in candidates:
                value = obj.get(key)
                if value is not None:
                    return str(value)
            for value in obj.values():
                nested = walk(value)
                if nested:
                    return nested
            return None

        if isinstance(obj, (list, tuple)):
            for item in obj:
                nested = walk(item)
                if nested:
                    return nested
            return None

        data = _as_dict(obj)
        if data:
            return walk(data)
        return None

    return walk(response)


def _best_from_levels(levels: Any, pick_max: bool) -> Optional[float]:
    prices: List[float] = []
    if isinstance(levels, list):
        for level in levels:
            data = _as_dict(level)
            if data:
                price = _safe_float(data.get("price"))
            elif isinstance(level, (list, tuple)) and level:
                price = _safe_float(level[0])
            else:
                price = None
            if price is not None:
                prices.append(price)
    if not prices:
        return None
    return max(prices) if pick_max else min(prices)


def get_orderbook_v2(
    client: Any, token_id: str, timeout: Optional[float] = None
) -> Dict[str, Optional[float]]:
    tid = str(token_id or "").strip()
    if not tid:
        return {"best_bid": None, "best_ask": None}

    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    book_loaded = False
    try:
        payload = _call_with_timeout(client.get_order_books, timeout, [{"token_id": tid}])
        books = payload if isinstance(payload, list) else []
        book = _as_dict(books[0]) if books else {}
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        best_bid = _best_from_levels(bids, pick_max=True)
        best_ask = _best_from_levels(asks, pick_max=False)
        book_loaded = bool(book)
    except Exception:
        book_loaded = False

    if best_bid is not None and best_bid <= 0:
        best_bid = None
    if best_ask is not None and best_ask <= 0:
        best_ask = None
    if book_loaded:
        if best_bid is not None and best_ask is not None and best_bid > best_ask:
            return {"best_bid": None, "best_ask": None}
        return {"best_bid": best_bid, "best_ask": best_ask}
    if best_bid is not None and best_ask is not None and best_bid <= best_ask:
        return {"best_bid": best_bid, "best_ask": best_ask}

    try:
        payload = _call_with_timeout(client.get_price, timeout, tid, "BUY")
        data = _as_dict(payload)
        price_bid = _safe_float(data.get("price")) if data else _safe_float(payload)
    except Exception:
        price_bid = None
    try:
        payload = _call_with_timeout(client.get_price, timeout, tid, "SELL")
        data = _as_dict(payload)
        price_ask = _safe_float(data.get("price")) if data else _safe_float(payload)
    except Exception:
        price_ask = None

    if price_bid is not None and price_bid > 0 and best_bid is None:
        best_bid = price_bid
    if price_ask is not None and price_ask > 0 and best_ask is None:
        best_ask = price_ask
    if best_bid is not None and best_ask is not None and best_bid > best_ask:
        return {"best_bid": None, "best_ask": None}
    return {"best_bid": best_bid, "best_ask": best_ask}


def _normalize_open_order_v2(order: Any) -> Optional[Dict[str, Any]]:
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
    side = data.get("side") or data.get("maker_side") or data.get("taker_side")
    side_norm = side.upper() if isinstance(side, str) else str(side).upper()
    price = _safe_float(data.get("price") or data.get("limit_price") or data.get("limitPrice"))
    size = _safe_float(
        data.get("size")
        or data.get("original_size")
        or data.get("originalSize")
        or data.get("remaining_size")
        or data.get("remainingSize")
        or data.get("amount")
    )
    created_ts = _parse_created_ts(
        data.get("created_at") or data.get("createdAt") or data.get("timestamp")
    )
    return {
        "order_id": str(order_id),
        "token_id": str(token_id),
        "side": side_norm,
        "price": price,
        "size": size,
        "ts": created_ts,
    }


def fetch_open_orders_norm_v2(
    client: Any, timeout_sec: Optional[float] = None
) -> tuple[list[dict[str, Any]], bool, str | None]:
    from py_clob_client_v2 import OpenOrderParams

    timeout: Optional[float] = None
    if timeout_sec is not None:
        try:
            timeout = float(timeout_sec)
        except Exception:
            timeout = None
        if timeout is not None and timeout <= 0:
            timeout = None

    params = OpenOrderParams()
    try:
        payload = _call_with_timeout(client.get_open_orders, timeout, params)
    except Exception as exc:
        url = str(getattr(client, "host", "") or "").rstrip("/")
        err_detail = f"{exc} url={url}/orders timeout={timeout}"
        return [], False, err_detail

    if isinstance(payload, list):
        orders = payload
    else:
        data = _as_dict(payload)
        orders = data.get("data") or data.get("orders") or data.get("items") or []

    normalized: List[Dict[str, Any]] = []
    for order in orders:
        parsed = _normalize_open_order_v2(order)
        if not parsed:
            continue
        if parsed["price"] is None or parsed["size"] is None:
            continue
        normalized.append(parsed)
    return normalized, True, None


def place_limit_order_v2(
    client: Any,
    token_id: str,
    side: str,
    price: float,
    size: float,
    allow_partial: bool = True,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    from py_clob_client_v2 import OrderArgs, OrderType

    side_norm = str(side or "").upper()
    clamped_price = max(0.01, min(0.99, float(price)))
    order_size = float(size)
    if side_norm == "BUY" and clamped_price > 0 and order_size * clamped_price < 1.0:
        order_size = (1.0 + 1e-6) / clamped_price

    order_args = OrderArgs(
        token_id=str(token_id),
        price=clamped_price,
        size=order_size,
        side=side_norm,
    )
    response = _call_with_timeout(
        client.create_and_post_order,
        timeout,
        order_args,
        order_type=getattr(OrderType, "GTC", "GTC"),
        post_only=False,
    )
    result: Dict[str, Any] = {"response": response}
    order_id = _extract_order_id(response)
    if order_id:
        result["order_id"] = order_id
    return result


def place_market_order_v2(
    client: Any,
    token_id: str,
    side: str,
    amount: float,
    price: Optional[float] = None,
    order_type: str = "FAK",
    timeout: Optional[float] = None,
    user_usdc_balance: Optional[float] = None,
) -> Dict[str, Any]:
    from py_clob_client_v2 import MarketOrderArgs, OrderType

    side_norm = str(side or "").upper()
    order_amount = float(amount)
    if side_norm == "BUY" and order_amount > 0 and order_amount < 1.0:
        order_amount = 1.0 + 1e-6

    resolved_order_type = getattr(OrderType, str(order_type or "FAK").upper(), None)
    if resolved_order_type is None:
        resolved_order_type = getattr(OrderType, "FAK", "FAK")

    kwargs: Dict[str, Any] = {
        "token_id": str(token_id),
        "amount": order_amount,
        "side": side_norm,
        "order_type": resolved_order_type,
    }
    if price is not None and float(price) > 0:
        kwargs["price"] = max(0.01, min(0.99, float(price)))
    if (
        side_norm == "BUY"
        and user_usdc_balance is not None
        and float(user_usdc_balance) > 0
    ):
        kwargs["user_usdc_balance"] = float(user_usdc_balance)

    order_args = MarketOrderArgs(**kwargs)
    response = _call_with_timeout(
        client.create_and_post_market_order,
        timeout,
        order_args,
        order_type=resolved_order_type,
    )
    result: Dict[str, Any] = {"response": response}
    order_id = _extract_order_id(response)
    if order_id:
        result["order_id"] = order_id
    return result


def normalize_clob_market_info_v2(
    payload: Any, condition_id: Optional[str] = None
) -> Dict[str, Any]:
    data = _as_dict(payload)
    fee_data = _as_dict(data.get("fd"))
    tokens_raw = data.get("t") or data.get("tokens") or []
    token_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(tokens_raw, list):
        for item in tokens_raw:
            item_data = _as_dict(item)
            token_id = (
                item_data.get("t")
                or item_data.get("token_id")
                or item_data.get("tokenId")
                or item_data.get("asset_id")
            )
            if not token_id:
                continue
            token_map[str(token_id)] = {
                "outcome": item_data.get("o") or item_data.get("outcome"),
            }

    resolved_condition_id = (
        data.get("condition_id")
        or data.get("conditionId")
        or data.get("id")
        or condition_id
        or ""
    )
    return {
        "condition_id": str(resolved_condition_id),
        "token_map": token_map,
        "min_tick_size": _safe_float(
            data.get("mts")
            or data.get("min_tick_size")
            or data.get("minimum_tick_size")
            or data.get("tick_size")
        ),
        "min_order_size": _safe_float(
            data.get("mos")
            or data.get("min_order_size")
            or data.get("minimum_order_size")
            or data.get("orderMinSize")
        ),
        "accepting_orders": _safe_bool(
            data.get("ao") if "ao" in data else data.get("accepting_orders")
        ),
        "enable_order_book": _safe_bool(
            data.get("cbos")
            if "cbos" in data
            else (
                data.get("enable_order_book")
                or data.get("enableOrderBook")
                or data.get("ibce")
            )
        ),
        "fee_rate": _safe_float(fee_data.get("r") if fee_data else data.get("r")),
        "fee_exponent": _safe_float(fee_data.get("e") if fee_data else data.get("e")),
        "taker_only_fee": _safe_bool(fee_data.get("to") if fee_data else data.get("to")),
        "rfq_enabled": _safe_bool(
            data.get("rfqe") if "rfqe" in data else data.get("rfq_enabled")
        ),
        "info": data,
    }


def get_cached_market_info_for_token(
    state: Optional[Dict[str, Any]], token_id: str
) -> Optional[Dict[str, Any]]:
    if not isinstance(state, dict):
        return None
    token_text = str(token_id or "").strip()
    if not token_text:
        return None
    cache = state.get("market_info_cache")
    if not isinstance(cache, dict):
        return None
    for cached in cache.values():
        if not isinstance(cached, dict):
            continue
        token_map = cached.get("token_map")
        if isinstance(token_map, dict) and token_text in token_map:
            return cached
    return None


def refresh_market_info_cache_v2(
    client: Any,
    state: Optional[Dict[str, Any]],
    token_id: str,
    timeout: Optional[float] = None,
    ttl_sec: float = 300.0,
) -> Optional[Dict[str, Any]]:
    if not isinstance(state, dict):
        return None
    token_text = str(token_id or "").strip()
    if not token_text:
        return None

    status_cache = state.get("market_status_cache")
    if not isinstance(status_cache, dict):
        return None
    cached_status = status_cache.get(token_text)
    if not isinstance(cached_status, dict):
        return None
    meta = cached_status.get("meta")
    if not isinstance(meta, dict):
        return None
    condition_id = (
        meta.get("conditionId") or meta.get("condition_id") or meta.get("conditionID") or ""
    )
    condition_text = str(condition_id).strip()
    if not condition_text:
        return None

    cache = state.setdefault("market_info_cache", {})
    now_ts = int(time.time())
    existing = cache.get(condition_text)
    if isinstance(existing, dict):
        existing_ts = int(existing.get("ts") or 0)
        token_map = existing.get("token_map")
        if (
            ttl_sec > 0
            and existing_ts > 0
            and now_ts - existing_ts <= ttl_sec
            and isinstance(token_map, dict)
            and token_text in token_map
        ):
            return existing

    raw = get_clob_market_info_v2(client, condition_text, timeout)
    normalized = normalize_clob_market_info_v2(raw, condition_id=condition_text)
    entry = {
        "ts": now_ts,
        "condition_id": normalized["condition_id"],
        "info": normalized["info"],
        "token_map": normalized["token_map"],
        "min_tick_size": normalized["min_tick_size"],
        "min_order_size": normalized["min_order_size"],
        "fee_rate": normalized["fee_rate"],
        "fee_exponent": normalized["fee_exponent"],
        "taker_only_fee": normalized["taker_only_fee"],
        "rfq_enabled": normalized["rfq_enabled"],
        "accepting_orders": normalized["accepting_orders"],
        "enable_order_book": normalized["enable_order_book"],
    }
    cache[condition_text] = entry
    return entry


def cancel_order_v2(
    client: Any, order_id: str, timeout: Optional[float] = None
) -> Optional[object]:
    from py_clob_client_v2 import OrderPayload

    oid = str(order_id or "").strip()
    if not oid:
        return None
    if callable(getattr(client, "cancel_order", None)):
        try:
            return _call_with_timeout(client.cancel_order, timeout, OrderPayload(orderID=oid))
        except TypeError:
            pass
    if callable(getattr(client, "cancel_orders", None)):
        return _call_with_timeout(client.cancel_orders, timeout, [oid])
    return None


def get_clob_market_info_v2(
    client: Any, condition_id: str, timeout: Optional[float] = None
) -> Dict[str, Any]:
    return _call_with_timeout(client.get_clob_market_info, timeout, str(condition_id))


def preflight_pusd_ready_v2(
    client: Any, timeout: Optional[float] = None
) -> Dict[str, Any]:
    from py_clob_client_v2 import AssetType, BalanceAllowanceParams

    try:
        payload = _call_with_timeout(
            client.get_balance_allowance,
            timeout,
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
        )
    except Exception as exc:
        return {
            "ok": False,
            "ready": None,
            "balance": None,
            "allowance": None,
            "message": f"balance/allowance preflight unavailable: {exc}",
        }

    data = _as_dict(payload)
    balance = _safe_float(data.get("balance"))
    allowance = _safe_float(data.get("allowance"))
    allowances = data.get("allowances")
    if allowance is None and isinstance(allowances, dict):
        allowance_values = [
            parsed
            for parsed in (_safe_float(value) for value in allowances.values())
            if parsed is not None
        ]
        if allowance_values:
            allowance = max(allowance_values)
    if balance is None or allowance is None:
        return {
            "ok": False,
            "ready": None,
            "balance": balance,
            "allowance": allowance,
            "message": f"balance/allowance payload incomplete: {data}",
        }

    ready = balance > 0 and allowance > 0
    if ready:
        message = "pUSD collateral balance and allowance look ready"
    elif balance <= 0:
        message = "pUSD collateral balance is zero or unavailable"
    else:
        message = "pUSD collateral allowance is zero or unavailable"
    return {
        "ok": True,
        "ready": ready,
        "balance": balance,
        "allowance": allowance,
        "message": message,
    }


def preflight_conditional_sell_ready_v2(
    client: Any,
    token_id: str,
    timeout: Optional[float] = None,
    required_operators: Optional[List[str]] = None,
) -> Dict[str, Any]:
    from py_clob_client_v2 import AssetType, BalanceAllowanceParams

    tid = str(token_id or "").strip()
    if not tid:
        return {
            "ok": False,
            "ready": None,
            "balance": None,
            "operator_allowances": {},
            "missing_operators": [],
            "message": "conditional token_id is required",
        }

    try:
        payload = _call_with_timeout(
            client.get_balance_allowance,
            timeout,
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=tid),
        )
    except Exception as exc:
        return {
            "ok": False,
            "ready": None,
            "balance": None,
            "operator_allowances": {},
            "missing_operators": [],
            "message": f"conditional balance/allowance preflight unavailable: {exc}",
        }

    data = _as_dict(payload)
    balance = _safe_float(data.get("balance"))
    raw_allowances = data.get("allowances")
    operator_allowances: Dict[str, float] = {}
    if isinstance(raw_allowances, dict):
        for operator, value in raw_allowances.items():
            parsed = _safe_float(value)
            if parsed is not None:
                operator_allowances[str(operator)] = parsed

    required = [
        str(operator)
        for operator in (
            required_operators
            if required_operators is not None
            else list(V2_CONDITIONAL_OPERATOR_ADDRESSES)
        )
    ]
    missing_operators = [
        operator
        for operator in required
        if operator_allowances.get(operator, 0.0) <= 0
    ]
    ready = balance is not None and balance > 0 and not missing_operators
    if balance is None:
        message = f"conditional balance/allowance payload incomplete: {data}"
        ready_value: Optional[bool] = None
    elif balance <= 0:
        message = "conditional token balance is zero or unavailable for sell path"
        ready_value = False
    elif missing_operators:
        message = (
            "conditional token operator approvals are missing for sell path: "
            + ", ".join(missing_operators)
        )
        ready_value = False
    else:
        message = "conditional token balance and operator approvals look ready"
        ready_value = True
    return {
        "ok": balance is not None,
        "ready": ready_value,
        "balance": balance,
        "operator_allowances": operator_allowances,
        "missing_operators": missing_operators,
        "message": message,
    }
