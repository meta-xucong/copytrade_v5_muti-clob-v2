import sys
import logging
from types import SimpleNamespace

sys.path.insert(0, ".")

import copytrade_run
from copytrade_run import (
    _finalize_exited_token_state,
    _is_must_exit_fresh,
    _estimate_recovery_shares_from_state,
    _mark_must_exit_token,
    _should_clear_stale_must_exit_on_buy,
    _should_clear_must_exit_without_inventory,
)


def test_mark_must_exit_token_upsert():
    state = {}
    _mark_must_exit_token(state, "t1", 100, "target_sell_action", target_sell_ms=1000)
    _mark_must_exit_token(state, "t1", 120, "reconcile_loop", target_sell_ms=0)
    meta = state.get("must_exit_tokens", {}).get("t1")
    assert isinstance(meta, dict), meta
    assert int(meta.get("first_ts") or 0) == 100
    assert int(meta.get("last_ts") or 0) == 100
    assert int(meta.get("target_sell_ms") or 0) == 1000
    assert str(meta.get("source") or "") == "target_sell_action"


def test_must_exit_freshness_and_stale_clear_gate():
    meta = {
        "first_ts": 100,
        "last_ts": 100,
        "source": "reconcile_loop",
        "target_sell_ms": 1000,
    }
    assert _is_must_exit_fresh(
        meta=meta,
        last_target_sell_ms=0,
        now_ms=2000,
        fresh_window_sec=5,
    )
    assert not _is_must_exit_fresh(
        meta=meta,
        last_target_sell_ms=0,
        now_ms=120000,
        fresh_window_sec=5,
    )
    assert _should_clear_stale_must_exit_on_buy(
        must_exit_active=True,
        must_exit_fresh=False,
        t_now_present=True,
        t_now=38.0,
        has_buy=True,
        buy_sum=8.0,
        min_target_buy_shares=1.0,
    )
    assert not _should_clear_stale_must_exit_on_buy(
        must_exit_active=True,
        must_exit_fresh=False,
        t_now_present=True,
        t_now=38.0,
        has_buy=False,
        buy_sum=8.0,
        min_target_buy_shares=1.0,
    )
    assert not _should_clear_stale_must_exit_on_buy(
        must_exit_active=True,
        must_exit_fresh=True,
        t_now_present=True,
        t_now=38.0,
        has_buy=True,
        buy_sum=8.0,
        min_target_buy_shares=1.0,
    )


def test_estimate_recovery_shares_prefers_max_source():
    state = {
        "last_nonzero_my_shares": {
            "t1": {"shares": 3.0, "ts": 100},
        },
        "open_orders": {
            "t1": [
                {"side": "BUY", "size": 10.0},
                {"side": "SELL", "size": 5.0},
            ]
        },
    }
    est = _estimate_recovery_shares_from_state(state, "t1")
    assert abs(est - 5.0) < 1e-9, est


def test_should_clear_must_exit_guarded_by_recent_cache():
    cfg = {"must_exit_cache_hold_sec": 600}
    state = {
        "buy_notional_accumulator": {"t1": {"usd": 0.0}},
        "last_nonzero_my_shares": {"t1": {"shares": 2.0, "ts": 1000}},
    }
    should_clear_recent = _should_clear_must_exit_without_inventory(
        state=state,
        token_id="t1",
        now_ts=1200,
        eps=1e-9,
        cfg=cfg,
    )
    assert not should_clear_recent
    should_clear_stale = _should_clear_must_exit_without_inventory(
        state=state,
        token_id="t1",
        now_ts=2000,
        eps=1e-9,
        cfg=cfg,
    )
    assert should_clear_stale


def test_exit_finalization_clears_tail_state_and_blocks_re_mark():
    logger = logging.getLogger("test_exit_finalization")
    state = {
        "must_exit_tokens": {"t1": {"first_ts": 100, "last_ts": 100, "source": "target_sell_action"}},
        "last_nonzero_my_shares": {"t1": {"shares": 5.0, "ts": 100}},
        "sell_shares_accumulator": {"t1": {"shares": 5.0}},
        "exit_sell_state": {"t1": {"stage": 3}},
        "intent_keys": {"t1": {"phase": "EXITING", "desired_side": "SELL", "desired_shares": 0.0}},
        "topic_unfilled_attempts": {"t1": 2},
    }
    cfg = {"exit_finalization_hold_sec": 180}
    _finalize_exited_token_state(
        state=state,
        token_id="t1",
        now_ts=1000,
        cfg=cfg,
        logger=logger,
        reason="zero_position_no_orders",
    )
    assert "t1" not in state.get("must_exit_tokens", {}), state
    assert "t1" not in state.get("last_nonzero_my_shares", {}), state
    assert "t1" not in state.get("sell_shares_accumulator", {}), state
    assert "t1" not in state.get("exit_sell_state", {}), state
    assert "t1" not in state.get("intent_keys", {}), state
    assert "t1" not in state.get("topic_unfilled_attempts", {}), state
    assert int(state.get("exit_finalization", {}).get("t1", {}).get("until") or 0) == 1180, state
    _mark_must_exit_token(state, "t1", 1050, "target_sell_action", target_sell_ms=12345)
    assert "t1" not in state.get("must_exit_tokens", {}), state


def test_hemostasis_sell_actions_are_marked_as_exit_flow(monkeypatch):
    calls = {"positions": 0}
    captured = {}

    def fake_fetch_positions_norm(*args, **kwargs):
        calls["positions"] += 1
        if calls["positions"] == 1:
            return ([{"token_id": "t1", "size": 10.0}], {"ok": True})
        return ([], {"ok": True})

    def fake_fetch_open_orders_norm(*args, **kwargs):
        return ([], True, "")

    def fake_get_orderbook(*args, **kwargs):
        return {"best_bid": 0.4, "best_ask": 0.41}

    def fake_book_min_order_shares(*args, **kwargs):
        return 0.0

    def fake_apply_actions(**kwargs):
        captured["actions"] = kwargs["actions"]

    monkeypatch.setattr(copytrade_run, "fetch_positions_norm", fake_fetch_positions_norm)
    monkeypatch.setattr(copytrade_run, "fetch_open_orders_norm", fake_fetch_open_orders_norm)
    monkeypatch.setattr(copytrade_run, "get_orderbook", fake_get_orderbook)
    monkeypatch.setattr(copytrade_run, "_book_min_order_shares", fake_book_min_order_shares)
    monkeypatch.setattr(copytrade_run, "apply_actions", fake_apply_actions)

    cfg = {
        "hemostasis_recovery_max_rounds": 1,
        "hemostasis_recovery_poll_sec": 0.0,
        "hemostasis_recovery_min_shares": 0.0,
        "hemostasis_recovery_sell_buffer_shares": 0.0,
        "hemostasis_recovery_min_trade_usd": 0.0,
        "hemostasis_no_progress_eps_shares": 0.01,
        "positions_limit": 50,
        "positions_max_pages": 1,
        "target_positions_refresh_sec": 25,
        "api_timeout_sec": 15.0,
    }
    acct_ctx = SimpleNamespace(
        my_address="0xabc",
        state={},
        clob_client=object(),
    )
    summary = copytrade_run._run_hemostasis_recovery_for_account(
        cfg=cfg,
        data_client=object(),
        acct_ctx=acct_ctx,
        sell_token_ids={"t1"},
        logger=logging.getLogger("test_hemostasis"),
        dry_run=False,
    )

    place_actions = [a for a in captured.get("actions", []) if a.get("type") == "place"]
    assert place_actions, summary
    place = place_actions[0]
    assert bool(place.get("_exit_flow")), place
    assert int(place.get("_exit_stage") or 0) == 3, place


if __name__ == "__main__":
    test_mark_must_exit_token_upsert()
    test_must_exit_freshness_and_stale_clear_gate()
    test_estimate_recovery_shares_prefers_max_source()
    test_should_clear_must_exit_guarded_by_recent_cache()
    test_exit_finalization_clears_tail_state_and_blocks_re_mark()
    print("ALL MUST_EXIT TESTS PASSED")
