import sys
from types import SimpleNamespace

sys.path.insert(0, ".")

import ct_exec


def test_resolve_order_fee_rate_bps_is_zero_for_v2_runtime():
    client = SimpleNamespace(
        get_open_orders=lambda *_args, **_kwargs: [],
        get_clob_market_info=lambda *_args, **_kwargs: {},
    )

    assert ct_exec._resolve_order_fee_rate_bps(client, "tid-v2", state={}) == 0


def test_apply_actions_does_not_forward_fee_rate_to_limit_orders(monkeypatch):
    captured = {}

    def fake_place_order(*_args, **kwargs):
        captured["kwargs"] = dict(kwargs)
        return {"order_id": "maker-1", "response": {"ok": True}}

    monkeypatch.setattr(ct_exec, "place_order", fake_place_order)

    updated = ct_exec.apply_actions(
        client=object(),
        actions=[
            {
                "type": "place",
                "token_id": "tid-maker",
                "side": "BUY",
                "price": 0.5,
                "size": 2.0,
            }
        ],
        open_orders=[],
        now_ts=100,
        dry_run=False,
        cfg={"allow_partial": True},
        state={},
    )

    assert "fee_rate_bps" not in captured["kwargs"]
    assert updated and updated[0]["order_id"] == "maker-1"


def test_apply_actions_does_not_forward_fee_rate_to_taker_orders(monkeypatch):
    captured = {}

    def fake_place_market_order(*_args, **kwargs):
        captured["kwargs"] = dict(kwargs)
        return {"order_id": "taker-1", "response": {"ok": True}}

    monkeypatch.setattr(ct_exec, "place_market_order", fake_place_market_order)

    ct_exec.apply_actions(
        client=object(),
        actions=[
            {
                "type": "place",
                "token_id": "tid-taker",
                "side": "BUY",
                "price": 0.5,
                "size": 2.0,
                "_taker": True,
            }
        ],
        open_orders=[],
        now_ts=100,
        dry_run=False,
        cfg={"allow_partial": True, "taker_order_type": "FAK"},
        state={},
    )

    assert "fee_rate_bps" not in captured["kwargs"]
    assert "user_usdc_balance" not in captured["kwargs"]


def test_apply_actions_forwards_collateral_preflight_balance_to_taker_buy(monkeypatch):
    captured = {}

    def fake_place_market_order(*_args, **kwargs):
        captured["kwargs"] = dict(kwargs)
        return {"order_id": "taker-balance-1", "response": {"ok": True}}

    monkeypatch.setattr(ct_exec, "place_market_order", fake_place_market_order)

    ct_exec.apply_actions(
        client=object(),
        actions=[
            {
                "type": "place",
                "token_id": "tid-taker-balance",
                "side": "BUY",
                "price": 0.5,
                "size": 2.0,
                "_taker": True,
            }
        ],
        open_orders=[],
        now_ts=100,
        dry_run=False,
        cfg={"allow_partial": True, "taker_order_type": "FAK"},
        state={"collateral_preflight": {"balance": 123.45}},
    )

    assert captured["kwargs"].get("user_usdc_balance") == 123.45
    assert "fee_rate_bps" not in captured["kwargs"]


def test_apply_actions_dry_run_still_skips_order_resolution(monkeypatch):
    def fail_place(*_args, **_kwargs):
        raise AssertionError("dry-run should not place live orders")

    monkeypatch.setattr(ct_exec, "place_order", fail_place)

    updated = ct_exec.apply_actions(
        client=object(),
        actions=[
            {
                "type": "place",
                "token_id": "tid-dry",
                "side": "BUY",
                "price": 0.5,
                "size": 2.0,
            }
        ],
        open_orders=[],
        now_ts=100,
        dry_run=True,
        cfg={"allow_partial": True},
        state={},
    )

    assert updated and updated[0]["order_id"] == "dry_run"
