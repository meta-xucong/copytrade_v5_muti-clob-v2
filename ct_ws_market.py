from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlparse


DEFAULT_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        return out
    except Exception:
        return None


def _safe_int_ms(value: Any) -> int:
    try:
        if value is None:
            return int(time.time() * 1000)
        out = int(float(value))
        if out < 10_000_000_000:
            out *= 1000
        return out
    except Exception:
        return int(time.time() * 1000)


def _best(levels: Dict[float, float], *, pick_max: bool) -> Optional[float]:
    prices = [price for price, size in levels.items() if price > 0 and size > 0]
    if not prices:
        return None
    return max(prices) if pick_max else min(prices)


def _iter_events(message: str | bytes) -> Iterable[Dict[str, Any]]:
    if isinstance(message, bytes):
        message = message.decode("utf-8", errors="replace")
    text = str(message or "").strip()
    if not text or text == "PONG":
        return []
    try:
        payload = json.loads(text)
    except Exception:
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


@dataclass
class MarketBookState:
    asset_id: str
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    best_bid_hint: Optional[float] = None
    best_ask_hint: Optional[float] = None
    market: str = ""
    last_trade_price: Optional[float] = None
    updated_at: float = 0.0
    event_ts_ms: int = 0
    event_count: int = 0

    def best_bid(self) -> Optional[float]:
        bid = _best(self.bids, pick_max=True)
        if bid is not None:
            return bid
        if self.best_bid_hint is not None and self.best_bid_hint > 0:
            return self.best_bid_hint
        return None

    def best_ask(self) -> Optional[float]:
        ask = _best(self.asks, pick_max=False)
        if ask is not None:
            return ask
        if self.best_ask_hint is not None and self.best_ask_hint > 0:
            return self.best_ask_hint
        return None


class WSMarketDataCache:
    """Thread-safe cache for Polymarket market WebSocket orderbook data."""

    def __init__(self, *, max_age_sec: float = 5.0) -> None:
        self.max_age_sec = max(float(max_age_sec or 0.0), 0.1)
        self._lock = threading.RLock()
        self._books: Dict[str, MarketBookState] = {}
        self._counts: Dict[str, int] = {
            "message": 0,
            "book": 0,
            "price_change": 0,
            "best_bid_ask": 0,
            "last_trade_price": 0,
            "other": 0,
        }

    def apply_message(self, message: str | bytes) -> None:
        events = list(_iter_events(message))
        if not events:
            return
        with self._lock:
            self._counts["message"] += 1
            for event in events:
                self._apply_event_locked(event)

    def _book_for_locked(self, asset_id: str) -> MarketBookState:
        book = self._books.get(asset_id)
        if book is None:
            book = MarketBookState(asset_id=asset_id)
            self._books[asset_id] = book
        return book

    def _apply_event_locked(self, event: Dict[str, Any]) -> None:
        event_type = str(event.get("event_type") or event.get("type") or "").lower()
        if event_type == "book":
            self._counts["book"] += 1
            self._apply_book_locked(event)
        elif event_type == "price_change":
            self._counts["price_change"] += 1
            self._apply_price_change_locked(event)
        elif event_type == "best_bid_ask":
            self._counts["best_bid_ask"] += 1
            self._apply_best_bid_ask_locked(event)
        elif event_type == "last_trade_price":
            self._counts["last_trade_price"] += 1
            self._apply_last_trade_locked(event)
        else:
            self._counts["other"] += 1

    def _apply_book_locked(self, event: Dict[str, Any]) -> None:
        asset_id = str(event.get("asset_id") or event.get("assetId") or "").strip()
        if not asset_id:
            return
        book = self._book_for_locked(asset_id)
        book.market = str(event.get("market") or book.market or "")
        book.bids = self._levels_to_map(event.get("bids") or [])
        book.asks = self._levels_to_map(event.get("asks") or [])
        book.best_bid_hint = None
        book.best_ask_hint = None
        book.updated_at = time.time()
        book.event_ts_ms = _safe_int_ms(event.get("timestamp"))
        book.event_count += 1

    def _apply_price_change_locked(self, event: Dict[str, Any]) -> None:
        timestamp_ms = _safe_int_ms(event.get("timestamp"))
        market = str(event.get("market") or "")
        for change in event.get("price_changes") or []:
            if not isinstance(change, dict):
                continue
            asset_id = str(change.get("asset_id") or change.get("assetId") or "").strip()
            if not asset_id:
                continue
            book = self._book_for_locked(asset_id)
            book.market = market or str(change.get("market") or book.market or "")
            side = str(change.get("side") or "").upper()
            price = _safe_float(change.get("price"))
            size = _safe_float(change.get("size"))
            if price is not None and price > 0 and size is not None:
                levels = book.bids if side == "BUY" else book.asks if side == "SELL" else None
                if levels is not None:
                    if size <= 0:
                        levels.pop(price, None)
                    else:
                        levels[price] = size
            best_bid = _safe_float(change.get("best_bid"))
            best_ask = _safe_float(change.get("best_ask"))
            if best_bid is not None:
                book.best_bid_hint = best_bid if best_bid > 0 else None
            if best_ask is not None:
                book.best_ask_hint = best_ask if best_ask > 0 else None
            book.updated_at = time.time()
            book.event_ts_ms = timestamp_ms
            book.event_count += 1

    def _apply_best_bid_ask_locked(self, event: Dict[str, Any]) -> None:
        asset_id = str(event.get("asset_id") or event.get("assetId") or "").strip()
        if not asset_id:
            return
        book = self._book_for_locked(asset_id)
        book.market = str(event.get("market") or book.market or "")
        best_bid = _safe_float(event.get("best_bid"))
        best_ask = _safe_float(event.get("best_ask"))
        book.best_bid_hint = best_bid if best_bid is not None and best_bid > 0 else None
        book.best_ask_hint = best_ask if best_ask is not None and best_ask > 0 else None
        book.updated_at = time.time()
        book.event_ts_ms = _safe_int_ms(event.get("timestamp"))
        book.event_count += 1

    def _apply_last_trade_locked(self, event: Dict[str, Any]) -> None:
        asset_id = str(event.get("asset_id") or event.get("assetId") or "").strip()
        if not asset_id:
            return
        book = self._book_for_locked(asset_id)
        book.market = str(event.get("market") or book.market or "")
        price = _safe_float(event.get("price"))
        if price is not None and price > 0:
            book.last_trade_price = price
        book.updated_at = time.time()
        book.event_ts_ms = _safe_int_ms(event.get("timestamp"))
        book.event_count += 1

    @staticmethod
    def _levels_to_map(levels: Any) -> Dict[float, float]:
        out: Dict[float, float] = {}
        if not isinstance(levels, list):
            return out
        for level in levels:
            if isinstance(level, dict):
                price = _safe_float(level.get("price"))
                size = _safe_float(level.get("size"))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price = _safe_float(level[0])
                size = _safe_float(level[1])
            else:
                continue
            if price is None or price <= 0 or size is None or size <= 0:
                continue
            out[price] = size
        return out

    def get_orderbook(
        self, token_id: str, *, max_age_sec: Optional[float] = None
    ) -> Optional[Dict[str, Optional[float]]]:
        tid = str(token_id or "").strip()
        if not tid:
            return None
        max_age = self.max_age_sec if max_age_sec is None else float(max_age_sec)
        with self._lock:
            book = self._books.get(tid)
            if book is None or book.updated_at <= 0:
                return None
            age = time.time() - book.updated_at
            if max_age > 0 and age > max_age:
                return None
            best_bid = book.best_bid()
            best_ask = book.best_ask()
            if best_bid is not None and best_ask is not None and best_bid > best_ask:
                return None
            if best_bid is None and best_ask is None:
                return None
            return {"best_bid": best_bid, "best_ask": best_ask}

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            fresh = 0
            for book in self._books.values():
                if book.updated_at > 0 and now - book.updated_at <= self.max_age_sec:
                    fresh += 1
            return {
                "books": len(self._books),
                "fresh_books": fresh,
                "counts": dict(self._counts),
            }


class WSMarketDataClient:
    """Background Polymarket market WebSocket client with dynamic subscribe."""

    def __init__(
        self,
        *,
        url: str = DEFAULT_MARKET_WS_URL,
        proxy_url: str = "",
        ping_interval_sec: int = 10,
        reconnect_backoff_max_sec: int = 60,
        max_age_sec: float = 5.0,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.url = url or DEFAULT_MARKET_WS_URL
        self.proxy_url = proxy_url or ""
        self.ping_interval_sec = max(1, int(ping_interval_sec or 10))
        self.reconnect_backoff_max_sec = max(1, int(reconnect_backoff_max_sec or 60))
        self.cache = WSMarketDataCache(max_age_sec=max_age_sec)
        self.logger = logger or logging.getLogger(__name__)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ws: Any = None
        self._connected = False
        self._assets: set[str] = set()
        self._stats: Dict[str, int] = {
            "opens": 0,
            "closes": 0,
            "errors": 0,
            "pongs": 0,
            "subscribe_sent": 0,
        }

    def start(self, asset_ids: Iterable[str] | None = None) -> None:
        with self._lock:
            self._assets.update(self._clean_assets(asset_ids or []))
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5)

    def subscribe(self, asset_ids: Iterable[str], *, max_assets: Optional[int] = None) -> int:
        new_assets = self._clean_assets(asset_ids)
        if not new_assets:
            return 0
        with self._lock:
            missing = [item for item in new_assets if item not in self._assets]
            if max_assets is not None:
                try:
                    slots = max(0, int(max_assets) - len(self._assets))
                except Exception:
                    slots = 0
                if slots <= 0:
                    missing = []
                elif len(missing) > slots:
                    missing = missing[:slots]
            self._assets.update(missing)
            ws = self._ws
            connected = self._connected
        if missing and connected and ws is not None:
            self._send_subscribe(missing, operation="subscribe")
        return len(missing)

    def get_orderbook(
        self, token_id: str, *, max_age_sec: Optional[float] = None
    ) -> Optional[Dict[str, Optional[float]]]:
        return self.cache.get_orderbook(token_id, max_age_sec=max_age_sec)

    def stats(self) -> Dict[str, Any]:
        out = self.cache.stats()
        with self._lock:
            out.update(
                {
                    "connected": self._connected,
                    "assets": len(self._assets),
                    "client": dict(self._stats),
                }
            )
        return out

    @staticmethod
    def _clean_assets(asset_ids: Iterable[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for item in asset_ids:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            out.append(text)
        return out

    def _run_loop(self) -> None:
        try:
            import websocket
        except Exception as exc:
            self.logger.warning("[WS_MARKET] websocket-client import failed: %s", exc)
            return

        backoff = 1.0
        while not self._stop.is_set():
            ws_app = websocket.WebSocketApp(
                self.url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            with self._lock:
                self._ws = ws_app
            kwargs = {"ping_interval": 0}
            kwargs.update(self._proxy_kwargs())
            try:
                ws_app.run_forever(**kwargs)
            except Exception as exc:
                self._record_error(exc)
            if self._stop.is_set():
                break
            time.sleep(backoff)
            backoff = min(backoff * 2.0, float(self.reconnect_backoff_max_sec))

    def _proxy_kwargs(self) -> Dict[str, Any]:
        if not self.proxy_url:
            return {}
        parsed = urlparse(self.proxy_url)
        if not parsed.hostname or not parsed.port:
            return {}
        return {
            "http_proxy_host": parsed.hostname,
            "http_proxy_port": int(parsed.port),
            "proxy_type": parsed.scheme or "http",
        }

    def _on_open(self, ws: Any) -> None:
        with self._lock:
            self._connected = True
            self._stats["opens"] += 1
            assets = sorted(self._assets)
        self.logger.info("[WS_MARKET] connected assets=%s", len(assets))
        if assets:
            self._send_subscribe(assets, ws=ws)

        def ping_loop() -> None:
            while not self._stop.is_set():
                time.sleep(self.ping_interval_sec)
                with self._lock:
                    current_ws = self._ws
                    connected = self._connected
                if not connected or current_ws is None:
                    break
                try:
                    current_ws.send("PING")
                except Exception as exc:
                    self._record_error(exc)
                    break

        threading.Thread(target=ping_loop, daemon=True).start()

    def _send_subscribe(
        self,
        assets: Iterable[str],
        *,
        ws: Any = None,
        operation: str = "",
    ) -> None:
        clean = self._clean_assets(assets)
        if not clean:
            return
        with self._lock:
            target_ws = ws or self._ws
        if target_ws is None:
            return
        payload: Dict[str, Any] = {
            "assets_ids": clean,
            "custom_feature_enabled": True,
        }
        if operation:
            payload["operation"] = operation
        else:
            payload["type"] = "market"
        try:
            target_ws.send(json.dumps(payload))
            with self._lock:
                self._stats["subscribe_sent"] += 1
            self.logger.info("[WS_MARKET] subscribe assets=%s operation=%s", len(clean), operation or "initial")
        except Exception as exc:
            self._record_error(exc)

    def _on_message(self, _ws: Any, message: str | bytes) -> None:
        if message == "PONG":
            with self._lock:
                self._stats["pongs"] += 1
            return
        self.cache.apply_message(message)

    def _on_error(self, _ws: Any, error: Exception) -> None:
        self._record_error(error)

    def _on_close(self, _ws: Any, code: Any, reason: Any) -> None:
        with self._lock:
            self._connected = False
            self._stats["closes"] += 1
        if not self._stop.is_set():
            self.logger.warning("[WS_MARKET] closed code=%s reason=%s", code, reason)

    def _record_error(self, error: Exception) -> None:
        with self._lock:
            self._stats["errors"] += 1
        self.logger.warning("[WS_MARKET] error: %s", error)
