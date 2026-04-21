import sys
import logging
sys.path.insert(0, ".")
from ct_risk import risk_check, accumulator_check
from copytrade_run import _shrink_on_risk_limit

logger = logging.getLogger("test")

# ---------- risk_check ----------
cfg = {
    "max_notional_per_token": 10.0,
    "max_position_usd_per_token": 5.0,
    "max_notional_total": 20.0,
    "blacklist_token_keys": ["bad"],
}

# 1) max_position_usd_per_token block
ok, reason = risk_check(
    token_key="id1",
    order_shares=1.0,
    my_shares=0.0,
    ref_price=3.0,
    cfg=cfg,
    side="BUY",
    planned_token_notional=6.0,
    planned_total_notional=0.0,
)
assert not ok and reason == "max_position_usd_per_token", reason
print("[PASS] risk_check max_position_usd_per_token")

# 2) max_notional_per_token block
ok, reason = risk_check(
    token_key="id1",
    order_shares=1.0,
    my_shares=0.0,
    ref_price=11.0,
    cfg=cfg,
    side="BUY",
    planned_token_notional=0.0,
    planned_total_notional=0.0,
)
assert not ok and reason == "max_notional_per_token", reason
print("[PASS] risk_check max_notional_per_token")

# 3) max_notional_total block
ok, reason = risk_check(
    token_key="id1",
    order_shares=1.0,
    my_shares=0.0,
    ref_price=5.0,
    cfg=cfg,
    side="BUY",
    planned_token_notional=0.0,
    planned_total_notional=21.0,
)
assert not ok and reason == "max_notional_total", reason
print("[PASS] risk_check max_notional_total")

# 4) blacklist
ok, reason = risk_check(
    token_key="bad",
    order_shares=1.0,
    my_shares=0.0,
    ref_price=1.0,
    cfg=cfg,
    side="BUY",
)
assert not ok and reason == "blacklist", reason
print("[PASS] risk_check blacklist")

# ---------- shrink on risk limit ----------
# 5) shrink within limit
action = {"token_id": "t1", "side": "BUY", "size": 10.0, "price": 1.0}
shrunk = _shrink_on_risk_limit(
    action,
    max_total=100.0,
    planned_total=0.0,
    max_per_token=10.0,
    planned_token=4.0,
    min_usd=0.0,
    min_shares=0.0,
    token_key="t1",
    token_id="t1",
    logger=logger,
)
assert shrunk is not None and shrunk[0]["size"] == 6.0, shrunk
print("[PASS] _shrink_on_risk_limit resize")

# 6) block below min
action = {"token_id": "t1", "side": "BUY", "size": 10.0, "price": 1.0}
shrunk = _shrink_on_risk_limit(
    action,
    max_total=100.0,
    planned_total=0.0,
    max_per_token=10.0,
    planned_token=7.0,
    min_usd=5.0,
    min_shares=0.0,
    token_key="t1",
    token_id="t1",
    logger=logger,
)
assert shrunk is None, shrunk
print("[PASS] _shrink_on_risk_limit block below min")

# ---------- accumulator_check (using accumulator only, no planned_total) ----------
state_acc = {"buy_notional_accumulator": {"id1": {"usd": 95.0}}}
cfg_acc = {"accumulator_max_total_usd": 100.0}

# 7) accumulator block when no planned_total provided
acc_ok, acc_reason, _ = accumulator_check("id1", 10.0, state_acc, cfg_acc,
                                           side="BUY", local_delta=0.0,
                                           planned_total_notional=None)
assert not acc_ok and acc_reason == "accumulator_max_total_usd", acc_reason
print("[PASS] accumulator_check block")

# 8) accumulator pass when within budget
acc_ok, acc_reason, _ = accumulator_check("id1", 1.0, state_acc, cfg_acc,
                                           side="BUY", local_delta=0.0,
                                           planned_total_notional=None)
assert acc_ok, acc_reason
print("[PASS] accumulator_check pass")

# 9) local_delta batch protection
acc_ok, acc_reason, _ = accumulator_check("id1", 1.0, state_acc, cfg_acc,
                                           side="BUY", local_delta=5.0,
                                           planned_total_notional=None)
assert not acc_ok and acc_reason == "accumulator_max_total_usd", acc_reason
print("[PASS] accumulator_check local_delta protection")

# 10) SELL bypasses accumulator
acc_ok, acc_reason, _ = accumulator_check("id1", 999.0, state_acc, cfg_acc, side="SELL")
assert acc_ok, acc_reason
print("[PASS] accumulator_check SELL bypass")

# 11) With planned_total_notional=0 (as in fresh boot), limit uses 0 baseline
acc_ok, acc_reason, avail = accumulator_check("id1", 10.0, state_acc, cfg_acc,
                                               side="BUY", local_delta=0.0,
                                               planned_total_notional=0.0)
assert acc_ok and avail == float("inf"), acc_reason
print("[PASS] accumulator_check respects planned_total_notional=0")

print("\nALL LIMIT TESTS PASSED")
