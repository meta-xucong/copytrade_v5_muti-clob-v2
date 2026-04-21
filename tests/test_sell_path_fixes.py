import sys
import logging

sys.path.insert(0, ".")

from ct_exec import reconcile_one


logger = logging.getLogger("test_sell_path_fixes")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)


def _base_cfg():
    return {
        "deadband_shares": 0.0,
        "order_size_mode": "fixed_shares",
        "slice_min": 0.0,
        "slice_max": 9999.0,
        "min_order_usd": 1.0,
        "min_order_shares": 5.0,
        "tick_size": 0.01,
        "taker_enabled": True,
        "taker_spread_threshold": 0.01,
        "taker_min_order_usd_buy": 1.0,
        "taker_min_order_shares_buy": 0.0,
        "exit_full_sell": True,
        "allow_short": False,
        "sell_available_buffer_shares": 0.01,
        "sell_accumulator_ttl_sec": 3600,
        "dust_exit_eps": 0.2,
        "exit_stage1_wait_sec": 45,
        "exit_stage2_wait_sec": 120,
        "exit_stage3_taker_max_spread": 0.5,
        "exit_stage3_slice_ratio": 0.5,
        "exit_stage3_maker_hold_sec": 180,
        "exit_progress_min_shares": 2.5,
        "exit_no_bid_pause_rounds": 3,
        "exit_dead_book_pause_sec": 300,
    }


def test_exit_exact_min_not_swallowed():
    cfg = _base_cfg()
    state = {"topic_state": {"t1": {"phase": "EXITING"}}}
    orderbook = {"best_bid": 0.60, "best_ask": 0.61}
    actions = reconcile_one(
        token_id="t1",
        desired_shares=0.0,
        my_shares=5.0,
        orderbook=orderbook,
        open_orders=[],
        now_ts=1000,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    place_actions = [a for a in actions if a.get("type") == "place"]
    assert place_actions, f"Expected SELL place action, got: {actions}"
    place = place_actions[0]
    assert str(place.get("side")).upper() == "SELL", place
    assert float(place.get("size") or 0.0) >= 4.999, place
    assert "dust_exits" not in state or "t1" not in state.get("dust_exits", {}), state
    print("[PASS] exit_exact_min_not_swallowed")


def test_non_exit_sell_accumulator_not_zero_add():
    cfg = _base_cfg()
    state = {
        "topic_state": {},
        "market_status_cache": {"t1": {"meta": {"orderMinSize": "5"}}},
    }
    orderbook = {"best_bid": 0.55, "best_ask": 0.56}
    actions = reconcile_one(
        token_id="t1",
        desired_shares=6.8,  # delta = -1.2 (small SELL)
        my_shares=8.0,
        orderbook=orderbook,
        open_orders=[],
        now_ts=1001,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    assert actions == [], actions
    acc = state.get("sell_shares_accumulator", {}).get("t1", {})
    shares = float(acc.get("shares") or 0.0)
    assert shares > 1.1, acc
    print("[PASS] non_exit_sell_accumulator_not_zero_add")


def test_exiting_below_min_holds_not_dust_when_above_eps():
    cfg = _base_cfg()
    cfg["taker_enabled"] = False
    state = {
        "topic_state": {"t1": {"phase": "EXITING"}},
        "market_status_cache": {"t1": {"meta": {"orderMinSize": "5"}}},
    }
    orderbook = {"best_bid": 0.50, "best_ask": 0.51}
    actions = reconcile_one(
        token_id="t1",
        desired_shares=0.0,
        my_shares=3.0,
        orderbook=orderbook,
        open_orders=[],
        now_ts=1002,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    assert actions == [], actions
    assert "t1" in state.get("topic_state", {}), state
    assert "t1" not in state.get("dust_exits", {}), state
    print("[PASS] exiting_below_min_holds_not_dust_when_above_eps")


def test_true_dust_can_be_cleared():
    cfg = _base_cfg()
    cfg["taker_enabled"] = False
    state = {
        "topic_state": {"t1": {"phase": "EXITING"}},
        "market_status_cache": {"t1": {"meta": {"orderMinSize": "5"}}},
    }
    orderbook = {"best_bid": 0.40, "best_ask": 0.41}
    actions = reconcile_one(
        token_id="t1",
        desired_shares=0.0,
        my_shares=0.1,
        orderbook=orderbook,
        open_orders=[],
        now_ts=1003,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    assert actions == [], actions
    assert "t1" in state.get("dust_exits", {}), state
    assert "t1" not in state.get("topic_state", {}), state
    print("[PASS] true_dust_can_be_cleared")


def test_exit_stage2_prices_near_bid_without_taker():
    cfg = _base_cfg()
    cfg["maker_only"] = True
    state = {
        "topic_state": {"t1": {"phase": "EXITING"}},
        "exit_sell_state": {
            "t1": {
                "stage": 1,
                "enter_ts": 1,
                "stage_since_ts": 1,
                "last_progress_ts": 1,
                "progress_ref_shares": 10.0,
                "last_seen_shares": 10.0,
                "no_match_count": 0,
                "no_bid_count": 0,
                "pause_until": 0,
                "pause_reason": "",
            }
        },
    }
    orderbook = {"best_bid": 0.40, "best_ask": 0.80}
    actions = reconcile_one(
        token_id="t1",
        desired_shares=0.0,
        my_shares=10.0,
        orderbook=orderbook,
        open_orders=[],
        now_ts=60,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    place = [a for a in actions if a.get("type") == "place"][0]
    assert str(place.get("side")).upper() == "SELL", place
    assert not place.get("_taker"), place
    assert 0.40 < float(place.get("price")) <= 0.42, place
    assert int(state["exit_sell_state"]["t1"]["stage"]) == 2
    print("[PASS] exit_stage2_prices_near_bid_without_taker")


def test_exit_stage3_switches_to_taker_and_slices():
    cfg = _base_cfg()
    state = {
        "topic_state": {"t1": {"phase": "EXITING"}},
        "exit_sell_state": {
            "t1": {
                "stage": 1,
                "enter_ts": 1,
                "stage_since_ts": 1,
                "last_progress_ts": 1,
                "progress_ref_shares": 10.0,
                "last_seen_shares": 10.0,
                "no_match_count": 0,
                "no_bid_count": 0,
                "pause_until": 0,
                "pause_reason": "",
            }
        },
    }
    orderbook = {"best_bid": 0.40, "best_ask": 0.60}
    actions = reconcile_one(
        token_id="t1",
        desired_shares=0.0,
        my_shares=10.0,
        orderbook=orderbook,
        open_orders=[],
        now_ts=200,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    place = [a for a in actions if a.get("type") == "place"][0]
    assert str(place.get("side")).upper() == "SELL", place
    assert bool(place.get("_taker")), place
    assert abs(float(place.get("price")) - 0.40) < 1e-9, place
    assert abs(float(place.get("size")) - 5.0) < 1e-9, place
    assert bool(place.get("_exit_flow")), place
    assert int(place.get("_exit_stage")) == 3, place
    print("[PASS] exit_stage3_switches_to_taker_and_slices")


def test_exit_pause_cancels_existing_orders():
    cfg = _base_cfg()
    state = {
        "topic_state": {"t1": {"phase": "EXITING"}},
        "exit_sell_state": {
            "t1": {
                "stage": 4,
                "enter_ts": 0,
                "stage_since_ts": 0,
                "last_progress_ts": 0,
                "progress_ref_shares": 10.0,
                "last_seen_shares": 10.0,
                "no_match_count": 0,
                "no_bid_count": 0,
                "pause_until": 300,
                "pause_reason": "no_match",
            }
        },
    }
    orderbook = {"best_bid": 0.40, "best_ask": 0.60}
    actions = reconcile_one(
        token_id="t1",
        desired_shares=0.0,
        my_shares=10.0,
        orderbook=orderbook,
        open_orders=[{"order_id": "sell-1", "side": "SELL", "price": 0.6, "size": 10.0}],
        now_ts=100,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    assert actions == [
        {"type": "cancel", "order_id": "sell-1", "token_id": "t1", "ts": 100}
    ], actions
    print("[PASS] exit_pause_cancels_existing_orders")


def test_exit_state_clears_once_delta_settles():
    cfg = _base_cfg()
    state = {
        "topic_state": {"t1": {"phase": "EXITING"}},
        "exit_sell_state": {
            "t1": {
                "stage": 4,
                "enter_ts": 1,
                "stage_since_ts": 1,
                "last_progress_ts": 1,
                "progress_ref_shares": 0.0,
                "last_seen_shares": 0.0,
                "no_match_count": 1,
                "no_bid_count": 1,
                "pause_until": 999,
                "pause_reason": "no_match",
            }
        },
    }
    actions = reconcile_one(
        token_id="t1",
        desired_shares=0.0,
        my_shares=0.0,
        orderbook={"best_bid": 0.4, "best_ask": 0.5},
        open_orders=[],
        now_ts=100,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    assert actions == [], actions
    assert "t1" not in state.get("exit_sell_state", {}), state
    print("[PASS] exit_state_clears_once_delta_settles")


def test_exit_stage3_stalled_wide_spread_pauses():
    cfg = _base_cfg()
    cfg["exit_stage3_taker_max_spread"] = 0.05
    cfg["exit_stage3_maker_hold_sec"] = 30
    state = {
        "topic_state": {"t1": {"phase": "EXITING"}},
        "exit_sell_state": {
            "t1": {
                "stage": 3,
                "enter_ts": 1,
                "stage_since_ts": 100,
                "last_progress_ts": 1,
                "progress_ref_shares": 10.0,
                "last_seen_shares": 10.0,
                "no_match_count": 0,
                "no_bid_count": 0,
                "pause_until": 0,
                "pause_reason": "",
            }
        },
    }
    actions = reconcile_one(
        token_id="t1",
        desired_shares=0.0,
        my_shares=10.0,
        orderbook={"best_bid": 0.4, "best_ask": 0.7},
        open_orders=[],
        now_ts=200,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    assert actions == [], actions
    entry = state.get("exit_sell_state", {}).get("t1", {})
    assert int(entry.get("stage") or 0) == 4, entry
    assert int(entry.get("pause_until") or 0) == 500, entry
    assert str(entry.get("pause_reason") or "") == "stage3_stalled", entry
    print("[PASS] exit_stage3_stalled_wide_spread_pauses")


def test_buy_taker_small_order_uses_taker_floor_not_maker_min():
    cfg = _base_cfg()
    state = {}
    actions = reconcile_one(
        token_id="t-buy",
        desired_shares=2.27,
        my_shares=0.0,
        orderbook={"best_bid": 0.40, "best_ask": 0.41},
        open_orders=[],
        now_ts=300,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    place = [a for a in actions if a.get("type") == "place"][0]
    assert str(place.get("side")).upper() == "BUY", place
    assert bool(place.get("_taker")), place
    assert 2.43 <= float(place.get("size")) <= 2.45, place
    assert float(place.get("size")) < 5.0, place
    print("[PASS] buy_taker_small_order_uses_taker_floor_not_maker_min")


def test_buy_maker_small_order_keeps_existing_maker_min_bump():
    cfg = _base_cfg()
    cfg["taker_enabled"] = False
    cfg["slice_max"] = 2.27
    state = {}
    actions = reconcile_one(
        token_id="t-maker",
        desired_shares=6.0,
        my_shares=0.0,
        orderbook={"best_bid": 0.40, "best_ask": 0.43},
        open_orders=[],
        now_ts=301,
        cfg=cfg,
        state=state,
        planned_token_notional=0.0,
    )
    place = [a for a in actions if a.get("type") == "place"][0]
    assert str(place.get("side")).upper() == "BUY", place
    assert not place.get("_taker"), place
    assert abs(float(place.get("size")) - 5.0) < 1e-9, place
    print("[PASS] buy_maker_small_order_keeps_existing_maker_min_bump")


if __name__ == "__main__":
    test_exit_exact_min_not_swallowed()
    test_non_exit_sell_accumulator_not_zero_add()
    test_exiting_below_min_holds_not_dust_when_above_eps()
    test_true_dust_can_be_cleared()
    test_exit_stage2_prices_near_bid_without_taker()
    test_exit_stage3_switches_to_taker_and_slices()
    test_exit_pause_cancels_existing_orders()
    test_exit_state_clears_once_delta_settles()
    test_exit_stage3_stalled_wide_spread_pauses()
    test_buy_taker_small_order_uses_taker_floor_not_maker_min()
    test_buy_maker_small_order_keeps_existing_maker_min_bump()
    print("\nALL SELL PATH FIX TESTS PASSED")
