from types import SimpleNamespace

import sys

sys.path.insert(0, ".")

import ct_clob_gateway
import ct_exec


def test_is_v2_client_detects_method_shape():
    client = SimpleNamespace(
        get_open_orders=lambda *_args, **_kwargs: [],
        get_clob_market_info=lambda *_args, **_kwargs: {},
    )
    assert ct_clob_gateway.is_v2_client(client) is True


def test_get_or_create_api_creds_v2_prefers_derive_before_create():
    called = []

    class Creds:
        api_key = "derived-key"

    client = SimpleNamespace(
        derive_api_key=lambda: called.append("derive") or Creds(),
        create_api_key=lambda: called.append("create") or None,
    )

    creds = ct_clob_gateway._get_or_create_api_creds_v2(client)

    assert called == ["derive"]
    assert creds.api_key == "derived-key"


def test_get_or_create_api_creds_v2_retries_derive_after_create_race(monkeypatch):
    called = []

    class Creds:
        api_key = "derived-after-race"

    def derive():
        called.append("derive")
        if called.count("derive") == 1:
            raise RuntimeError("temporary derive timeout")
        return Creds()

    def create():
        called.append("create")
        raise RuntimeError("Could not create api key")

    monkeypatch.setattr(ct_clob_gateway.time, "sleep", lambda *_args, **_kwargs: None)
    client = SimpleNamespace(derive_api_key=derive, create_api_key=create)

    creds = ct_clob_gateway._get_or_create_api_creds_v2(client)

    assert creds.api_key == "derived-after-race"
    assert called == ["derive", "create", "derive"]


def test_get_orderbook_v2_uses_real_book_before_price_endpoints():
    client = SimpleNamespace(
        get_order_books=lambda payload: [
            {
                "bids": [{"price": "0.40", "size": "10"}],
                "asks": [{"price": "0.44", "size": "7"}],
            }
        ],
        get_price=lambda token_id, side: {"price": "0.41"} if side == "BUY" else {"price": "0.43"},
    )
    assert ct_clob_gateway.get_orderbook_v2(client, "tok-1") == {
        "best_bid": 0.40,
        "best_ask": 0.44,
    }


def test_get_orderbook_v2_preserves_empty_book_side_without_price_fallback():
    client = SimpleNamespace(
        get_order_books=lambda payload: [{"bids": [], "asks": [{"price": "0.44", "size": "7"}]}],
        get_price=lambda token_id, side: {"price": "0.41"} if side == "BUY" else {"price": "0.43"},
    )
    assert ct_clob_gateway.get_orderbook_v2(client, "tok-1") == {
        "best_bid": None,
        "best_ask": 0.44,
    }


def test_get_orderbook_v2_falls_back_to_price_when_book_query_unavailable():
    client = SimpleNamespace(
        get_order_books=lambda payload: (_ for _ in ()).throw(RuntimeError("no book")),
        get_price=lambda token_id, side: {"price": "0.41"} if side == "BUY" else {"price": "0.43"},
    )
    assert ct_clob_gateway.get_orderbook_v2(client, "tok-1") == {
        "best_bid": 0.41,
        "best_ask": 0.43,
    }


def test_get_orderbook_dispatches_to_v2_gateway():
    client = SimpleNamespace(
        get_open_orders=lambda *_args, **_kwargs: [],
        get_clob_market_info=lambda *_args, **_kwargs: {},
    )
    called = {}

    def fake_v2_orderbook(cli, token_id, timeout):
        called["args"] = (cli, token_id, timeout)
        return {"best_bid": 0.2, "best_ask": 0.3}

    original = ct_exec.get_orderbook_v2
    try:
        ct_exec.get_orderbook_v2 = fake_v2_orderbook
        result = ct_exec.get_orderbook(client, "tok-2", 12.0)
    finally:
        ct_exec.get_orderbook_v2 = original

    assert called["args"] == (client, "tok-2", 12.0)
    assert result == {"best_bid": 0.2, "best_ask": 0.3}


def test_fetch_open_orders_norm_v2_normalizes_payload():
    client = SimpleNamespace(
        host="https://clob-v2.polymarket.com",
        get_open_orders=lambda params: [
            {
                "id": "ord-1",
                "asset_id": "tok-1",
                "side": "BUY",
                "price": "0.44",
                "size": "7.5",
                "created_at": "2026-04-20T12:00:00Z",
            }
        ],
    )
    orders, ok, err = ct_clob_gateway.fetch_open_orders_norm_v2(client, 15.0)
    assert ok is True
    assert err is None
    assert orders == [
        {
            "order_id": "ord-1",
            "token_id": "tok-1",
            "side": "BUY",
            "price": 0.44,
            "size": 7.5,
            "ts": 1776686400,
        }
    ]


def test_fetch_open_orders_norm_dispatches_to_v2_gateway():
    client = SimpleNamespace(
        get_open_orders=lambda *_args, **_kwargs: [],
        get_clob_market_info=lambda *_args, **_kwargs: {},
    )
    called = {}

    def fake_fetch(cli, timeout_sec):
        called["args"] = (cli, timeout_sec)
        return (
            [
                {
                    "order_id": "ord-2",
                    "token_id": "tok-2",
                    "side": "SELL",
                    "price": 0.5,
                    "size": 2.0,
                    "ts": 1,
                }
            ],
            True,
            None,
        )

    original = ct_exec.fetch_open_orders_norm_v2
    try:
        ct_exec.fetch_open_orders_norm_v2 = fake_fetch
        orders, ok, err = ct_exec.fetch_open_orders_norm(client, 9.0)
    finally:
        ct_exec.fetch_open_orders_norm_v2 = original

    assert called["args"] == (client, 9.0)
    assert ok is True
    assert err is None
    assert orders[0]["order_id"] == "ord-2"
