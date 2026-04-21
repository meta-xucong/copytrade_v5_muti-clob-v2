from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import requests

GAMMA_ROOT = "https://gamma-api.polymarket.com"
CLOB_ROOT = "https://clob.polymarket.com"
_GAMMA_RATE_LIMIT_SEC = 1.0
_last_gamma_request_ts = 0.0
_sampling_market_index: dict[str, dict] = {}
_sampling_market_index_ts = 0.0
_SAMPLING_INDEX_TTL_SEC = 300.0


def _enforce_gamma_rate_limit() -> None:
    global _last_gamma_request_ts
    now = time.monotonic()
    elapsed = now - _last_gamma_request_ts
    remaining = _GAMMA_RATE_LIMIT_SEC - elapsed
    if remaining > 0:
        time.sleep(remaining)
    _last_gamma_request_ts = time.monotonic()


def _http_json(url: str, params: Optional[dict] = None) -> Optional[Any]:
    try:
        _enforce_gamma_rate_limit()
        resp = requests.get(url, params=params or {}, timeout=10)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def _http_json_clob(url: str, params: Optional[dict] = None) -> Optional[Any]:
    try:
        resp = requests.get(url, params=params or {}, timeout=10)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def _markets_from_payload(data: Any) -> list[dict]:
    markets: list[dict] = []
    if isinstance(data, dict) and "data" in data:
        raw = data.get("data") or []
        if isinstance(raw, list):
            markets = raw
    elif isinstance(data, list):
        markets = data
    return [m for m in markets if isinstance(m, dict)]


def _extract_token_id_from_raw(raw: Dict[str, Any]) -> Optional[str]:
    id_keys = (
        "tokenId",
        "token_id",
        "clobTokenId",
        "clob_token_id",
        "assetId",
        "asset_id",
        "outcomeTokenId",
        "outcome_token_id",
        "id",
    )
    for key in id_keys:
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _extract_token_ids_from_market(market: Dict[str, Any]) -> list[str]:
    ids = market.get("clobTokenIds") or market.get("clobTokens")
    if isinstance(ids, str):
        try:
            ids = json.loads(ids)
        except Exception:
            ids = None
    if isinstance(ids, (list, tuple)):
        return [str(x) for x in ids if str(x).strip()]

    outcomes = market.get("outcomes") or market.get("tokens") or []
    if isinstance(outcomes, list):
        ordered: list[tuple[int, str]] = []
        for item in outcomes:
            if not isinstance(item, dict):
                continue
            token_id = _extract_token_id_from_raw(item)
            if not token_id:
                continue
            idx = item.get("outcomeIndex") or item.get("outcome_index")
            if isinstance(idx, (int, float)):
                ordered.append((int(idx), token_id))
        if ordered:
            return [token_id for _, token_id in sorted(ordered, key=lambda t: t[0])]
    return []


def _norm_market_flag(market: Dict[str, Any], camel_key: str, snake_key: str) -> Any:
    if camel_key in market and market.get(camel_key) is not None:
        return market.get(camel_key)
    return market.get(snake_key)


def _normalize_sampling_market(market: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(market)
    out["enableOrderBook"] = _norm_market_flag(out, "enableOrderBook", "enable_order_book")
    out["acceptingOrders"] = _norm_market_flag(out, "acceptingOrders", "accepting_orders")
    out["orderMinSize"] = _norm_market_flag(out, "orderMinSize", "minimum_order_size")
    out["orderPriceMinTickSize"] = _norm_market_flag(
        out, "orderPriceMinTickSize", "minimum_tick_size"
    )
    if out.get("conditionId") is None and out.get("condition_id") is not None:
        out["conditionId"] = out.get("condition_id")
    if out.get("slug") is None and out.get("market_slug") is not None:
        out["slug"] = out.get("market_slug")
    if out.get("question") is None and out.get("description") is not None:
        out["question"] = out.get("description")
    return out


def _build_sampling_market_index() -> dict[str, dict]:
    global _sampling_market_index
    global _sampling_market_index_ts

    now = time.monotonic()
    if _sampling_market_index and now - _sampling_market_index_ts < _SAMPLING_INDEX_TTL_SEC:
        return _sampling_market_index

    token_map: dict[str, dict] = {}
    next_cursor: Optional[str] = None
    max_pages = 200
    for _ in range(max_pages):
        params: Dict[str, Any] = {}
        if next_cursor:
            params["next_cursor"] = next_cursor
        payload = _http_json_clob(f"{CLOB_ROOT}/sampling-markets", params=params)
        if not isinstance(payload, dict):
            break
        data = payload.get("data") or []
        if not isinstance(data, list):
            break
        for market in data:
            if not isinstance(market, dict):
                continue
            norm_market = _normalize_sampling_market(market)
            tokens = market.get("tokens") or []
            if not isinstance(tokens, list):
                continue
            for token in tokens:
                if not isinstance(token, dict):
                    continue
                token_id = _extract_token_id_from_raw(token)
                if token_id:
                    token_map[str(token_id)] = norm_market
        next_cursor_raw = payload.get("next_cursor")
        if next_cursor_raw in (None, "", "null", "None"):
            break
        next_cursor = str(next_cursor_raw)

    if token_map:
        _sampling_market_index = token_map
        _sampling_market_index_ts = now
    return _sampling_market_index


def _gamma_fetch_by_slug(slug: str) -> Optional[dict]:
    if not slug:
        return None
    return _http_json(f"{GAMMA_ROOT}/markets/slug/{slug}")


def _gamma_fetch_by_condition(condition_id: str) -> Optional[dict]:
    if not condition_id:
        return None
    data = _http_json(f"{GAMMA_ROOT}/markets", params={"conditionId": condition_id, "limit": 1})
    markets = _markets_from_payload(data)
    if isinstance(markets, list) and markets:
        return markets[0]
    data = _http_json(f"{GAMMA_ROOT}/markets", params={"search": condition_id, "limit": 1})
    markets = _markets_from_payload(data)
    if isinstance(markets, list) and markets:
        return markets[0]
    return None


def gamma_fetch_markets_by_clob_token_ids(token_ids: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not token_ids:
        return out

    # Normalize once to avoid useless network calls and mismatches.
    normalized_ids = []
    seen_ids = set()
    for token_id in token_ids:
        tid = str(token_id or "").strip()
        if not tid or tid in seen_ids:
            continue
        seen_ids.add(tid)
        normalized_ids.append(tid)
    if not normalized_ids:
        return out

    def _query_markets(chunk_ids: list[str], *, closed: bool, use_array_param: bool) -> list[dict]:
        if not chunk_ids:
            return []
        key = "clob_token_ids[]" if use_array_param else "clob_token_ids"
        data = _http_json(
            f"{GAMMA_ROOT}/markets",
            params={
                key: chunk_ids,
                "closed": "true" if closed else "false",
                "limit": len(chunk_ids),
            },
        )
        return _markets_from_payload(data)

    chunk_size = 50
    for idx in range(0, len(normalized_ids), chunk_size):
        chunk = normalized_ids[idx : idx + chunk_size]
        unresolved = set(chunk)
        # Stage 1: active/non-closed markets (official default path)
        # Stage 2: explicitly include closed markets to resolve stale/expired tokens.
        for closed_flag in (False, True):
            if not unresolved:
                break
            for use_array_param in (False, True):
                if not unresolved:
                    break
                markets = _query_markets(
                    sorted(unresolved), closed=closed_flag, use_array_param=use_array_param
                )
                if not markets:
                    continue
                for market in markets:
                    token_ids_in_market = _extract_token_ids_from_market(market) or []
                    for token_id in token_ids_in_market:
                        if token_id in unresolved and token_id not in out:
                            out[token_id] = market
                            unresolved.discard(token_id)
        if unresolved:
            sampling_idx = _build_sampling_market_index()
            if sampling_idx:
                for token_id in list(unresolved):
                    market = sampling_idx.get(token_id)
                    if isinstance(market, dict) and token_id not in out:
                        out[token_id] = market
                        unresolved.discard(token_id)
    return out


def market_is_tradeable(market: dict) -> bool:
    if not isinstance(market, dict):
        return False
    archived = _norm_market_flag(market, "archived", "archived")
    accepting_orders = _norm_market_flag(market, "acceptingOrders", "accepting_orders")
    closed = _norm_market_flag(market, "closed", "closed")
    active = _norm_market_flag(market, "active", "active")

    if archived is True:
        return False
    if accepting_orders is False:
        return False
    if closed is True:
        return False
    if active is False:
        return False
    return True


def market_tradeable_state(market: Optional[Dict[str, Any]]) -> Optional[bool]:
    if not isinstance(market, dict):
        return None

    enable_order_book = _norm_market_flag(market, "enableOrderBook", "enable_order_book")
    archived = _norm_market_flag(market, "archived", "archived")
    accepting_orders = _norm_market_flag(market, "acceptingOrders", "accepting_orders")
    closed = _norm_market_flag(market, "closed", "closed")
    active = _norm_market_flag(market, "active", "active")

    if enable_order_book is False:
        return False
    if archived is True:
        return False
    if accepting_orders is False:
        return False
    if closed is True:
        return False
    if active is False:
        return False

    if (
        enable_order_book is None
        and accepting_orders is None
        and closed is None
        and active is None
    ):
        return None

    return True


def resolve_token_id(token_key: str, pos: Dict[str, Any], cache: Dict[str, str]) -> str:
    if token_key in cache:
        return cache[token_key]

    raw = pos.get("raw") or {}
    if isinstance(raw, dict):
        token_id = _extract_token_id_from_raw(raw)
        if token_id:
            cache[token_key] = token_id
            return token_id

    outcome_index = pos.get("outcome_index")
    if outcome_index is None:
        raise ValueError(f"token_key={token_key} 缺少 outcome_index")

    slug = pos.get("slug")
    market = _gamma_fetch_by_slug(str(slug)) if slug else None
    if market is None:
        market = _gamma_fetch_by_condition(str(pos.get("condition_id") or ""))
    if not isinstance(market, dict):
        raise ValueError(f"无法解析 token_id: {token_key}")

    token_ids = _extract_token_ids_from_market(market)
    if not token_ids:
        raise ValueError(f"市场未包含 token_id: {token_key}")

    idx = int(outcome_index)
    if idx < 0 or idx >= len(token_ids):
        raise ValueError(f"outcome_index 超出范围: {token_key}")

    token_id = token_ids[idx]
    cache[token_key] = token_id
    return token_id
