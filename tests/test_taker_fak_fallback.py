import sys

sys.path.insert(0, ".")

import ct_exec


def test_apply_actions_taker_buy_no_match_does_not_fallback_to_maker(monkeypatch):
    call_count = {"taker": 0, "maker": 0}

    def fake_place_market_order(*_args, **_kwargs):
        call_count["taker"] += 1
        raise RuntimeError("no orders found to match with FAK")

    def fake_place_order(*_args, **_kwargs):
        call_count["maker"] += 1
        return {"order_id": "maker-1", "response": {"ok": True}}

    monkeypatch.setattr(ct_exec, "place_market_order", fake_place_market_order)
    monkeypatch.setattr(ct_exec, "place_order", fake_place_order)
    monkeypatch.setattr(ct_exec, "_resolve_order_fee_rate_bps", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(ct_exec.time, "sleep", lambda *_args, **_kwargs: None)

    state = {}
    cfg = {
        "allow_partial": True,
        "taker_order_type": "FAK",
        "taker_fak_retry_max": 1,
        "taker_fak_retry_delay_sec": 0.0,
        "taker_fak_fallback_to_maker": True,
    }
    actions = [
        {
            "type": "place",
            "token_id": "tid-1",
            "side": "BUY",
            "price": 0.5,
            "size": 2.0,
            "_taker": True,
        }
    ]

    updated = ct_exec.apply_actions(
        client=object(),
        actions=actions,
        open_orders=[],
        now_ts=100,
        dry_run=False,
        cfg=cfg,
        state=state,
    )

    assert call_count["taker"] == 2
    assert call_count["maker"] == 0
    assert updated == []
    assert state.get("taker_buy_orders") in (None, [])


def test_apply_actions_taker_sell_non_exit_can_still_fallback_to_maker(monkeypatch):
    call_count = {"taker": 0, "maker": 0}

    def fake_place_market_order(*_args, **_kwargs):
        call_count["taker"] += 1
        raise RuntimeError("no orders found to match with FAK")

    def fake_place_order(*_args, **_kwargs):
        call_count["maker"] += 1
        return {"order_id": "maker-1", "response": {"ok": True}}

    monkeypatch.setattr(ct_exec, "place_market_order", fake_place_market_order)
    monkeypatch.setattr(ct_exec, "place_order", fake_place_order)
    monkeypatch.setattr(ct_exec, "_resolve_order_fee_rate_bps", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(ct_exec.time, "sleep", lambda *_args, **_kwargs: None)

    state = {}
    cfg = {
        "allow_partial": True,
        "taker_order_type": "FAK",
        "taker_fak_retry_max": 1,
        "taker_fak_retry_delay_sec": 0.0,
        "taker_fak_fallback_to_maker": True,
    }
    actions = [
        {
            "type": "place",
            "token_id": "tid-sell",
            "side": "SELL",
            "price": 0.5,
            "size": 2.0,
            "_taker": True,
        }
    ]

    updated = ct_exec.apply_actions(
        client=object(),
        actions=actions,
        open_orders=[],
        now_ts=100,
        dry_run=False,
        cfg=cfg,
        state=state,
    )

    assert call_count["taker"] == 2
    assert call_count["maker"] == 1
    assert updated and updated[0].get("order_id") == "maker-1"


def test_apply_actions_exit_sell_no_match_pauses_without_fallback(monkeypatch):
    call_count = {"taker": 0, "maker": 0}

    def fake_place_market_order(*_args, **_kwargs):
        call_count["taker"] += 1
        raise RuntimeError("no orders found to match with FAK")

    def fake_place_order(*_args, **_kwargs):
        call_count["maker"] += 1
        return {"order_id": "maker-1", "response": {"ok": True}}

    monkeypatch.setattr(ct_exec, "place_market_order", fake_place_market_order)
    monkeypatch.setattr(ct_exec, "place_order", fake_place_order)
    monkeypatch.setattr(ct_exec, "_resolve_order_fee_rate_bps", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(ct_exec.time, "sleep", lambda *_args, **_kwargs: None)

    state = {
        "exit_sell_state": {
            "tid-1": {
                "stage": 3,
                "enter_ts": 0,
                "stage_since_ts": 0,
                "last_progress_ts": 0,
                "progress_ref_shares": 10.0,
                "last_seen_shares": 10.0,
                "no_match_count": 0,
                "no_bid_count": 0,
                "pause_until": 0,
                "pause_reason": "",
            }
        }
    }
    cfg = {
        "allow_partial": True,
        "taker_order_type": "FAK",
        "taker_fak_retry_max": 1,
        "taker_fak_retry_delay_sec": 0.0,
        "taker_fak_fallback_to_maker": True,
        "exit_no_match_pause_after": 1,
        "exit_no_match_pause_sec": 90,
    }
    actions = [
        {
            "type": "place",
            "token_id": "tid-1",
            "side": "SELL",
            "price": 0.5,
            "size": 5.0,
            "_taker": True,
            "_exit_flow": True,
            "_exit_stage": 3,
        }
    ]

    updated = ct_exec.apply_actions(
        client=object(),
        actions=actions,
        open_orders=[],
        now_ts=100,
        dry_run=False,
        cfg=cfg,
        state=state,
    )

    assert updated == []
    assert call_count["taker"] == 2
    assert call_count["maker"] == 0
    assert int(state["exit_sell_state"]["tid-1"]["pause_until"]) == 190
    assert str(state["exit_sell_state"]["tid-1"]["pause_reason"]) == "no_match"


def test_apply_actions_taker_buy_uses_taker_specific_min_bump(monkeypatch):
    captured = {"amounts": []}

    def fake_place_market_order(*_args, **kwargs):
        captured["amounts"].append(kwargs.get("amount"))
        return {"order_id": "taker-1", "response": {"ok": True}}

    def fake_place_order(*_args, **_kwargs):
        raise AssertionError("maker fallback should not be used in this test")

    monkeypatch.setattr(ct_exec, "place_market_order", fake_place_market_order)
    monkeypatch.setattr(ct_exec, "place_order", fake_place_order)
    monkeypatch.setattr(ct_exec, "_resolve_order_fee_rate_bps", lambda *_args, **_kwargs: 0)

    state = {}
    cfg = {
        "allow_partial": True,
        "min_order_usd": 1.0,
        "min_order_shares": 5.0,
        "taker_min_order_usd_buy": 1.0,
        "taker_min_order_shares_buy": 0.0,
        "taker_order_type": "FAK",
        "taker_fak_retry_max": 0,
        "taker_fak_retry_delay_sec": 0.0,
        "taker_fak_fallback_to_maker": True,
    }
    actions = [
        {
            "type": "place",
            "token_id": "tid-2",
            "side": "BUY",
            "price": 0.5,
            "size": 1.0,
            "_taker": True,
        }
    ]

    updated = ct_exec.apply_actions(
        client=object(),
        actions=actions,
        open_orders=[],
        now_ts=100,
        dry_run=False,
        cfg=cfg,
        state=state,
    )

    assert updated == []
    assert len(captured["amounts"]) == 1
    assert abs(float(captured["amounts"][0]) - 1.0) < 1e-9
