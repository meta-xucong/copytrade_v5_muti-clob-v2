import logging
import sys

sys.path.insert(0, ".")

import ct_exec
from copytrade_run import (
    _calc_shadow_buy_notional,
    _merge_remote_open_orders_into_state,
    _reconcile_zero_position_accumulator,
)


def test_maker_buy_records_pending_without_confirmed_accumulator(monkeypatch):
    def fake_place_order(*_args, **_kwargs):
        return {"order_id": "maker-1", "response": {"ok": True}}

    monkeypatch.setattr(ct_exec, "place_order", fake_place_order)

    state = {}
    updated = ct_exec.apply_actions(
        client=object(),
        actions=[
            {
                "type": "place",
                "token_id": "tid-1",
                "side": "BUY",
                "price": 0.61,
                "size": 10.0,
            }
        ],
        open_orders=[],
        now_ts=100,
        dry_run=False,
        cfg={"allow_partial": True, "dedupe_place": False, "min_order_usd": 1.0},
        state=state,
    )

    assert updated and updated[0]["order_id"] == "maker-1"
    assert state.get("buy_notional_accumulator") in (None, {})
    assert state["pending_buy_orders"]["maker-1"]["token_id"] == "tid-1"
    assert abs(float(state["pending_buy_orders"]["maker-1"]["usd"]) - 6.1) < 1e-9


def test_taker_buy_updates_confirmed_accumulator(monkeypatch):
    def fake_place_market_order(*_args, **_kwargs):
        return {"order_id": "taker-1", "response": {"ok": True}}

    monkeypatch.setattr(ct_exec, "place_market_order", fake_place_market_order)

    state = {}
    updated = ct_exec.apply_actions(
        client=object(),
        actions=[
            {
                "type": "place",
                "token_id": "tid-1",
                "side": "BUY",
                "price": 0.5,
                "size": 4.0,
                "_taker": True,
            }
        ],
        open_orders=[],
        now_ts=100,
        dry_run=False,
        cfg={
            "allow_partial": True,
            "taker_order_type": "FAK",
            "taker_fak_retry_max": 0,
        },
        state=state,
    )

    assert updated == []
    assert state.get("pending_buy_orders") in (None, {})
    assert abs(float(state["buy_notional_accumulator"]["tid-1"]["usd"]) - 2.0) < 1e-9
    assert abs(float(state["taker_buy_orders"][0]["usd"]) - 2.0) < 1e-9


def test_cancel_clears_pending_buy_order(monkeypatch):
    canceled = []

    def fake_cancel_order(_client, order_id, _timeout=None):
        canceled.append(order_id)
        return {"ok": True}

    monkeypatch.setattr(ct_exec, "cancel_order", fake_cancel_order)

    state = {
        "pending_buy_orders": {
            "maker-1": {"token_id": "tid-1", "usd": 6.0, "price": 0.6, "size": 10.0, "ts": 100}
        }
    }
    updated = ct_exec.apply_actions(
        client=object(),
        actions=[{"type": "cancel", "order_id": "maker-1"}],
        open_orders=[{"order_id": "maker-1", "side": "BUY", "price": 0.6, "size": 10.0, "ts": 100}],
        now_ts=120,
        dry_run=False,
        cfg={},
        state=state,
    )

    assert canceled == ["maker-1"]
    assert updated == []
    assert state["pending_buy_orders"] == {}


def test_pending_buy_counts_as_shadow_only_when_order_missing():
    state = {
        "pending_buy_orders": {
            "maker-1": {"token_id": "tid-1", "usd": 6.0, "price": 0.6, "size": 10.0, "ts": 100}
        },
        "open_orders": {"tid-1": [{"order_id": "maker-1", "side": "BUY", "price": 0.6, "size": 10.0}]},
    }

    total, by_token = _calc_shadow_buy_notional(state, now_ts=130, ttl_sec=900)
    assert total == 0.0
    assert by_token == {}

    state["open_orders"] = {}
    total, by_token = _calc_shadow_buy_notional(state, now_ts=130, ttl_sec=900)
    assert total == 6.0
    assert by_token == {"tid-1": 6.0}


def test_remote_prune_moves_pending_buy_to_shadow():
    state = {
        "open_orders": {
            "tid-1": [{"order_id": "maker-1", "side": "BUY", "price": 0.6, "size": 10.0, "ts": 100}]
        },
        "managed_order_ids": ["maker-1"],
        "pending_buy_orders": {
            "maker-1": {"token_id": "tid-1", "usd": 6.0, "price": 0.6, "size": 10.0, "ts": 100}
        },
    }

    result = _merge_remote_open_orders_into_state(
        state,
        remote_orders=[],
        now_ts=400,
        cfg={"order_visibility_grace_sec": 180, "shadow_buy_ttl_sec": 900},
        logger=logging.getLogger("test"),
        adopt_existing=False,
    )

    assert result["pruned"] == 1
    assert state["open_orders"] == {}
    assert state["pending_buy_orders"] == {}
    assert state["shadow_buy_orders"][0]["order_id"] == "maker-1"


def test_zero_position_reconcile_clears_stale_confirmed_accumulator():
    state = {
        "buy_notional_accumulator": {"tid-1": {"usd": 6.0, "last_ts": 100}},
        "remote_order_snapshot_ts": 500,
        "runtime_health": {
            "components": {
                "data_api_my_positions": {"status": "ok", "last_success_ts": 500}
            }
        },
    }

    cleared = _reconcile_zero_position_accumulator(
        state,
        "tid-1",
        my_shares=0.0,
        open_orders=[],
        now_ts=500,
        cfg={"accumulator_zero_reconcile_sec": 300},
        logger=logging.getLogger("test"),
        eps=0.01,
    )

    assert cleared is True
    assert state["buy_notional_accumulator"] == {}
