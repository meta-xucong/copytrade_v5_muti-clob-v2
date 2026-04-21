from types import SimpleNamespace

import sys

sys.path.insert(0, ".")

import ct_clob_gateway
import ct_exec


def test_place_limit_order_v2_normalizes_buy_order():
    captured = {}

    def fake_create_and_post_order(order_args, order_type=None, post_only=False):
        captured["token_id"] = order_args.token_id
        captured["price"] = order_args.price
        captured["size"] = order_args.size
        captured["side"] = order_args.side
        captured["order_type"] = order_type
        captured["post_only"] = post_only
        return {"orderID": "v2-maker-1"}

    client = SimpleNamespace(create_and_post_order=fake_create_and_post_order)
    result = ct_clob_gateway.place_limit_order_v2(
        client,
        token_id="tok-1",
        side="BUY",
        price=0.005,
        size=1.0,
        allow_partial=False,
    )

    assert result == {"response": {"orderID": "v2-maker-1"}, "order_id": "v2-maker-1"}
    assert captured["token_id"] == "tok-1"
    assert captured["price"] == 0.01
    assert captured["size"] > 100.0
    assert captured["side"] == "BUY"
    assert captured["order_type"] == "GTC"
    assert captured["post_only"] is False


def test_cancel_order_v2_uses_order_payload():
    captured = {}

    def fake_cancel_order(payload):
        captured["order_id"] = payload.orderID
        return {"canceled": payload.orderID}

    client = SimpleNamespace(cancel_order=fake_cancel_order)
    response = ct_clob_gateway.cancel_order_v2(client, "ord-1")

    assert response == {"canceled": "ord-1"}
    assert captured["order_id"] == "ord-1"


def test_place_market_order_v2_normalizes_buy_amount_and_order_type():
    captured = {}

    def fake_create_and_post_market_order(order_args, order_type=None):
        captured["token_id"] = order_args.token_id
        captured["amount"] = order_args.amount
        captured["side"] = order_args.side
        captured["price"] = order_args.price
        captured["user_usdc_balance"] = order_args.user_usdc_balance
        captured["inner_order_type"] = order_args.order_type
        captured["outer_order_type"] = order_type
        return {"id": "v2-taker-1"}

    client = SimpleNamespace(create_and_post_market_order=fake_create_and_post_market_order)
    result = ct_clob_gateway.place_market_order_v2(
        client,
        token_id="tok-3",
        side="BUY",
        amount=0.5,
        price=1.2,
        order_type="fok",
        user_usdc_balance=42.5,
    )

    assert result == {"response": {"id": "v2-taker-1"}, "order_id": "v2-taker-1"}
    assert captured["token_id"] == "tok-3"
    assert captured["amount"] > 1.0
    assert captured["side"] == "BUY"
    assert captured["price"] == 0.99
    assert captured["user_usdc_balance"] == 42.5
    assert captured["inner_order_type"] == "FOK"
    assert captured["outer_order_type"] == "FOK"


def test_place_order_dispatches_to_v2_gateway():
    client = SimpleNamespace(
        get_open_orders=lambda *_args, **_kwargs: [],
        get_clob_market_info=lambda *_args, **_kwargs: {},
    )
    called = {}

    def fake_place_limit(cli, **kwargs):
        called["client"] = cli
        called["kwargs"] = kwargs
        return {"order_id": "v2-ord-1", "response": {"ok": True}}

    original = ct_exec.place_limit_order_v2
    try:
        ct_exec.place_limit_order_v2 = fake_place_limit
        result = ct_exec.place_order(
            client,
            token_id="tok-2",
            side="SELL",
            price=0.45,
            size=3.0,
            fee_rate_bps=999,
            allow_partial=True,
            timeout=8.0,
        )
    finally:
        ct_exec.place_limit_order_v2 = original

    assert result["order_id"] == "v2-ord-1"
    assert called["client"] is client
    assert called["kwargs"] == {
        "token_id": "tok-2",
        "side": "SELL",
        "price": 0.45,
        "size": 3.0,
        "allow_partial": True,
        "timeout": 8.0,
    }


def test_cancel_order_dispatches_to_v2_gateway():
    client = SimpleNamespace(
        get_open_orders=lambda *_args, **_kwargs: [],
        get_clob_market_info=lambda *_args, **_kwargs: {},
    )
    called = {}

    def fake_cancel(cli, order_id, timeout):
        called["args"] = (cli, order_id, timeout)
        return {"ok": True}

    original = ct_exec.cancel_order_v2
    try:
        ct_exec.cancel_order_v2 = fake_cancel
        result = ct_exec.cancel_order(client, "ord-2", 6.0)
    finally:
        ct_exec.cancel_order_v2 = original

    assert result == {"ok": True}
    assert called["args"] == (client, "ord-2", 6.0)


def test_place_market_order_dispatches_to_v2_gateway():
    client = SimpleNamespace(
        get_open_orders=lambda *_args, **_kwargs: [],
        get_clob_market_info=lambda *_args, **_kwargs: {},
    )
    called = {}

    def fake_place_market(cli, **kwargs):
        called["client"] = cli
        called["kwargs"] = kwargs
        return {"order_id": "v2-taker-2", "response": {"ok": True}}

    original = ct_exec.place_market_order_v2
    try:
        ct_exec.place_market_order_v2 = fake_place_market
        result = ct_exec.place_market_order(
            client,
            token_id="tok-4",
            side="BUY",
            amount=2.5,
            price=0.42,
            fee_rate_bps=777,
            order_type="FAK",
            timeout=7.0,
            user_usdc_balance=88.0,
        )
    finally:
        ct_exec.place_market_order_v2 = original

    assert result["order_id"] == "v2-taker-2"
    assert called["client"] is client
    assert called["kwargs"] == {
        "token_id": "tok-4",
        "side": "BUY",
        "amount": 2.5,
        "price": 0.42,
        "order_type": "FAK",
        "timeout": 7.0,
        "user_usdc_balance": 88.0,
    }
