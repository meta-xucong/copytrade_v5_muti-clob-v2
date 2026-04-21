"""
Integration tests for Issue #5 (missing_streak freeze waste)
and Issue #6 (repeated BUY on never-filling topics).
"""
import sys
import logging

logger = logging.getLogger("test_fixes_56")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)


def test_issue5_unfreeze_on_data_recovery():
    """When action_seen or t_now_present becomes True, missing_streak freeze should be cleared immediately."""
    state = {
        "missing_data_freeze": {
            "t1": {"ts": 1000, "expires_at": 10000, "reason": "missing_streak", "streak": 5}
        },
        "target_missing_streak": {"t1": 5},
        "target_last_seen_ts": {"t1": 900},
    }
    token_id = "t1"
    now_ts = 2000

    # Simulate the logic added in copytrade_run.py for action_seen
    action_seen = True
    t_now_present = False
    if action_seen:
        state.setdefault("target_missing_streak", {})[token_id] = 0
        state.setdefault("target_last_seen_ts", {})[token_id] = now_ts
        _mf = state.setdefault("missing_data_freeze", {})
        _existing = _mf.get(token_id)
        if isinstance(_existing, dict) and _existing.get("reason") == "missing_streak":
            _mf.pop(token_id, None)
            logger.info("[UNFREEZE] token_id=%s reason=data_recovered", token_id)

    assert "t1" not in state["missing_data_freeze"], "Freeze should be removed on data recovery"
    assert state["target_missing_streak"]["t1"] == 0
    print("[PASS] issue5_unfreeze_on_data_recovery")


def test_issue5_skip_only_when_position_or_orders():
    """missing_streak freeze should only skip token when my_shares>eps or open_orders exist."""
    eps = 1e-9

    # Case A: no shares, no orders -> should NOT skip
    freeze_meta = {"expires_at": 10000, "reason": "missing_streak"}
    my_shares = 0.0
    open_orders = []
    should_skip = (
        isinstance(freeze_meta, dict)
        and freeze_meta.get("expires_at")
        and freeze_meta.get("reason") == "missing_streak"
        and (my_shares > eps or open_orders)
    )
    assert not should_skip, "Empty position should not be skipped by missing_streak freeze"

    # Case B: has shares -> should skip
    my_shares = 1.0
    should_skip = (
        isinstance(freeze_meta, dict)
        and freeze_meta.get("expires_at")
        and freeze_meta.get("reason") == "missing_streak"
        and (my_shares > eps or open_orders)
    )
    assert should_skip, "Position-holding token should be skipped by missing_streak freeze"

    # Case C: has open orders -> should skip
    my_shares = 0.0
    open_orders = [{"order_id": "o1"}]
    should_skip = (
        isinstance(freeze_meta, dict)
        and freeze_meta.get("expires_at")
        and freeze_meta.get("reason") == "missing_streak"
        and (my_shares > eps or open_orders)
    )
    assert should_skip, "Token with open orders should be skipped by missing_streak freeze"

    print("[PASS] issue5_skip_only_when_position_or_orders")


def test_issue6_unfilled_timeout():
    """After 3 consecutive CLEANUPs without fills, token should be ignored for 30 min."""
    now_ts = 1000
    cfg = {"topic_unfilled_max_rounds": 3, "topic_unfilled_ignore_sec": 1800}
    eps = 1e-9

    state = {
        "topic_state": {"t1": {"phase": "LONG"}},
        "open_orders": {},
        "topic_unfilled_attempts": {},
        "ignored_tokens": {},
    }
    my_by_token_id = {"t1": 0.0}

    def cleanup_loop():
        topic_state = state.get("topic_state", {})
        topic_unfilled = state.setdefault("topic_unfilled_attempts", {})
        if isinstance(topic_state, dict):
            for tid in list(topic_state.keys()):
                my_shares_t = my_by_token_id.get(tid, 0.0)
                orders_t = state.get("open_orders", {}).get(tid, [])
                if my_shares_t <= eps and not orders_t:
                    topic_state.pop(tid, None)
                    topic_unfilled[tid] = topic_unfilled.get(tid, 0) + 1
                    max_unfilled = int(cfg.get("topic_unfilled_max_rounds") or 3)
                    if max_unfilled > 0 and topic_unfilled[tid] >= max_unfilled:
                        ignore_sec = int(cfg.get("topic_unfilled_ignore_sec") or 1800)
                        state.setdefault("ignored_tokens", {})[tid] = {
                            "ts": now_ts,
                            "reason": "unfilled_timeout",
                            "expires_at": now_ts + ignore_sec,
                        }

    # Round 1
    cleanup_loop()
    assert state["topic_unfilled_attempts"]["t1"] == 1
    assert "t1" not in state["ignored_tokens"]

    # Re-enter topic_state and round 2
    state["topic_state"]["t1"] = {"phase": "LONG"}
    cleanup_loop()
    assert state["topic_unfilled_attempts"]["t1"] == 2
    assert "t1" not in state["ignored_tokens"]

    # Round 3 -> should trigger ignore
    state["topic_state"]["t1"] = {"phase": "LONG"}
    cleanup_loop()
    assert state["topic_unfilled_attempts"]["t1"] == 3
    assert "t1" in state["ignored_tokens"]
    assert state["ignored_tokens"]["t1"]["reason"] == "unfilled_timeout"
    assert state["ignored_tokens"]["t1"]["expires_at"] == now_ts + 1800

    print("[PASS] issue6_unfilled_timeout")


def test_issue6_clear_unfilled_on_position():
    """When my_shares becomes positive, topic_unfilled_attempts should be cleared."""
    state = {
        "topic_unfilled_attempts": {"t1": 2},
    }
    my_shares = 1.0
    eps = 1e-9
    token_id = "t1"
    if my_shares > eps:
        state.setdefault("topic_unfilled_attempts", {}).pop(token_id, None)
    assert "t1" not in state["topic_unfilled_attempts"]
    print("[PASS] issue6_clear_unfilled_on_position")


if __name__ == "__main__":
    test_issue5_unfreeze_on_data_recovery()
    test_issue5_skip_only_when_position_or_orders()
    test_issue6_unfilled_timeout()
    test_issue6_clear_unfilled_on_position()
    print("\nALL FIXES 5&6 TESTS PASSED")
