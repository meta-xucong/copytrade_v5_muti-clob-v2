import sys

sys.path.insert(0, ".")

from copytrade_run import (
    _calc_skip_count,
    _pick_token_sticky_follow_accounts,
    _resolve_target_level_map,
    _resolve_target_level_skip_ratios,
)


def test_calc_skip_count_rounding():
    assert _calc_skip_count(10, 0.0) == 0
    assert _calc_skip_count(10, 0.4) == 4
    assert _calc_skip_count(10, 0.5) == 5
    assert _calc_skip_count(3, 0.4) == 1
    assert _calc_skip_count(2, 0.4) == 1
    assert _calc_skip_count(1, 0.8) == 1


def test_target_level_policy_parse():
    cfg = {
        "default_target_level": "A",
        "target_addresses": [
            {"address": "0x1111111111111111111111111111111111111111", "level": "A"},
            {"address": "0x2222222222222222222222222222222222222222", "level": "b"},
            {"address": "0x3333333333333333333333333333333333333333", "level": "C"},
        ],
        "target_level_skip_ratios": {"A": 0, "B": 0.4, "C": 0.8},
    }
    targets = [item["address"] for item in cfg["target_addresses"]]
    level_map = _resolve_target_level_map(cfg, targets)
    ratios = _resolve_target_level_skip_ratios(cfg)
    assert level_map["0x1111111111111111111111111111111111111111"] == "A"
    assert level_map["0x2222222222222222222222222222222222222222"] == "B"
    assert level_map["0x3333333333333333333333333333333333333333"] == "C"
    assert ratios == {"A": 0.0, "B": 0.4, "C": 0.8}


def test_token_sticky_follow_accounts_are_stable_for_same_token():
    decision_cache = {}
    all_accounts = [f"a{i}" for i in range(1, 11)]
    first = _pick_token_sticky_follow_accounts(
        token_id="token-1",
        all_account_ids=all_accounts,
        skip_ratio=0.5,
        now_ts=100,
        decision_cache=decision_cache,
        seed="seed-1",
        assigned_level="B",
    )
    second = _pick_token_sticky_follow_accounts(
        token_id="token-1",
        all_account_ids=list(reversed(all_accounts)),
        skip_ratio=0.5,
        now_ts=101,
        decision_cache=decision_cache,
        seed="seed-1",
        assigned_level="B",
    )
    assert len(first) == 5, first
    assert first == second, (first, second)


def test_token_sticky_freezes_first_assignment_even_if_later_ratio_changes():
    decision_cache = {}
    all_accounts = [f"a{i}" for i in range(1, 11)]
    first = _pick_token_sticky_follow_accounts(
        token_id="token-2",
        all_account_ids=all_accounts,
        skip_ratio=0.5,
        now_ts=100,
        decision_cache=decision_cache,
        seed="seed-1",
        assigned_level="B",
    )
    second = _pick_token_sticky_follow_accounts(
        token_id="token-2",
        all_account_ids=all_accounts,
        skip_ratio=0.8,
        now_ts=200,
        decision_cache=decision_cache,
        seed="seed-1",
        assigned_level="C",
    )
    assert len(first) == 5, first
    assert first == second, (first, second)
    entry = decision_cache["token-2"]
    assert float(entry["skip_ratio"]) == 0.5, entry
    assert str(entry["assigned_level"]) == "B", entry


if __name__ == "__main__":
    test_calc_skip_count_rounding()
    test_target_level_policy_parse()
    test_token_sticky_follow_accounts_are_stable_for_same_token()
    test_token_sticky_freezes_first_assignment_even_if_later_ratio_changes()
    print("\nTARGET LEVEL SKIP TESTS PASSED")
