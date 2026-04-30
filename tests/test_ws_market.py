import json
import time
from types import SimpleNamespace

import sys

sys.path.insert(0, ".")

import ct_exec
from ct_ws_market import WSMarketDataCache, WSMarketDataClient


def test_ws_cache_parses_book_list_message():
    cache = WSMarketDataCache(max_age_sec=10)
    cache.apply_message(
        json.dumps(
            [
                {
                    "event_type": "book",
                    "asset_id": "tok-1",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.44", "size": "7"}],
                    "timestamp": "1777472476690",
                }
            ]
        )
    )

    assert cache.get_orderbook("tok-1") == {"best_bid": 0.40, "best_ask": 0.44}
    stats = cache.stats()
    assert stats["books"] == 1
    assert stats["counts"]["book"] == 1


def test_ws_cache_applies_price_change_and_removes_empty_level():
    cache = WSMarketDataCache(max_age_sec=10)
    cache.apply_message(
        json.dumps(
            {
                "event_type": "book",
                "asset_id": "tok-1",
                "bids": [{"price": "0.40", "size": "10"}],
                "asks": [{"price": "0.44", "size": "7"}],
            }
        )
    )
    cache.apply_message(
        json.dumps(
            {
                "event_type": "price_change",
                "price_changes": [
                    {
                        "asset_id": "tok-1",
                        "side": "BUY",
                        "price": "0.41",
                        "size": "5",
                        "best_bid": "0.41",
                        "best_ask": "0.44",
                    },
                    {
                        "asset_id": "tok-1",
                        "side": "SELL",
                        "price": "0.44",
                        "size": "0",
                        "best_bid": "0.41",
                        "best_ask": "0.45",
                    },
                ],
            }
        )
    )

    assert cache.get_orderbook("tok-1") == {"best_bid": 0.41, "best_ask": 0.45}


def test_ws_cache_stale_books_are_ignored(monkeypatch):
    cache = WSMarketDataCache(max_age_sec=1)
    now = [1000.0]
    monkeypatch.setattr("ct_ws_market.time.time", lambda: now[0])
    cache.apply_message(
        json.dumps(
            {
                "event_type": "best_bid_ask",
                "asset_id": "tok-1",
                "best_bid": "0.40",
                "best_ask": "0.44",
            }
        )
    )
    now[0] = 1002.0

    assert cache.get_orderbook("tok-1") is None


def test_ct_exec_prefers_ws_provider_then_falls_back():
    class Provider:
        def __init__(self):
            self.enabled = True

        def get_orderbook(self, token_id, max_age_sec=None):
            if self.enabled:
                return {"best_bid": 0.21, "best_ask": 0.23}
            return None

    provider = Provider()
    client = SimpleNamespace(
        get_price=lambda token_id, side: {"price": "0.41"} if side == "BUY" else {"price": "0.43"},
    )
    try:
        ct_exec.configure_orderbook_provider(provider, prefer=True, max_age_sec=5)
        assert ct_exec.get_orderbook(client, "tok-1") == {"best_bid": 0.21, "best_ask": 0.23}

        provider.enabled = False
        assert ct_exec.get_orderbook(client, "tok-1") == {"best_bid": 0.41, "best_ask": 0.43}
    finally:
        ct_exec.configure_orderbook_provider(None)


def test_ws_client_subscribe_respects_asset_limit():
    client = WSMarketDataClient(max_age_sec=5)

    assert client.subscribe(["a", "b"], max_assets=2) == 2
    assert client.stats()["assets"] == 2

    assert client.subscribe(["b", "c", "d"], max_assets=2) == 0
    assert client.stats()["assets"] == 2

    assert client.subscribe(["c", "d"], max_assets=3) == 1
    assert client.stats()["assets"] == 3
