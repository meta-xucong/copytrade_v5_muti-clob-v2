from ct_risk import accumulator_check, risk_check
from ct_risk_config import (
    account_risk_overrides,
    effective_risk_config,
    normalize_risk_config,
)


def test_nested_risk_names_project_to_legacy_runtime_keys():
    cfg = {
        "risk": {
            "max_single_buy_usd": 6,
            "max_token_exposure_usd": 6,
            "max_condition_exposure_usd": 8,
            "max_event_exposure_usd": 8,
            "max_total_exposure_usd": 10000,
            "max_token_buy_spend_usd": None,
            "max_total_buy_spend_usd": None,
        }
    }

    normalized = normalize_risk_config(cfg)

    assert normalized["max_order_usd"] == 6
    assert normalized["max_position_usd_per_token"] == 6
    assert normalized["max_position_usd_per_condition"] == 8
    assert normalized["max_position_usd_per_event"] == 8
    assert normalized["max_notional_total"] == 10000
    assert normalized["max_notional_per_token"] == 0.0
    assert normalized["accumulator_max_total_usd"] == 0.0


def test_account_risk_overrides_root_defaults():
    cfg = {
        "risk": {
            "max_single_buy_usd": 6,
            "max_token_exposure_usd": 6,
            "max_total_exposure_usd": 500,
        }
    }
    account = {
        "name": "w01",
        "risk": {
            "max_token_exposure_usd": 4,
            "max_total_exposure_usd": 10000,
        },
    }

    effective = effective_risk_config(cfg, account_risk_overrides(account))

    assert effective["max_single_buy_usd"] == 6
    assert effective["max_token_exposure_usd"] == 4
    assert effective["max_total_exposure_usd"] == 10000


def test_token_exposure_is_primary_when_buy_spend_disabled():
    cfg = {
        "risk": {
            "max_token_exposure_usd": 6,
            "max_token_buy_spend_usd": None,
            "max_total_exposure_usd": 10000,
        }
    }

    ok, reason = risk_check(
        token_key="token",
        order_shares=2.0,
        my_shares=0.0,
        ref_price=1.0,
        cfg=cfg,
        side="BUY",
        planned_token_notional=5.0,
        planned_total_notional=5.0,
    )

    assert not ok
    assert reason == "max_position_usd_per_token"


def test_buy_spend_cap_disabled_does_not_block_accumulator_total():
    cfg = {
        "risk": {
            "max_total_exposure_usd": 10000,
            "max_total_buy_spend_usd": None,
        }
    }
    state = {"buy_notional_accumulator": {"token": {"usd": 9999.0}}}

    ok, reason, available = accumulator_check(
        "token",
        1.0,
        state,
        cfg,
        side="BUY",
        planned_total_notional=0.0,
    )

    assert ok
    assert reason == "ok"
    assert available == float("inf")


def test_token_exposure_uses_larger_of_planned_and_accumulator():
    cfg = {
        "risk": {
            "max_token_exposure_usd": 6,
            "max_total_buy_spend_usd": None,
        }
    }
    state = {"buy_notional_accumulator": {"token": {"usd": 5.8}}}

    ok, reason, available = accumulator_check(
        "token",
        1.0,
        state,
        cfg,
        side="BUY",
        planned_token_notional=3.0,
        planned_total_notional=3.0,
    )

    assert not ok
    assert reason == "max_position_usd_per_token"
    assert abs(available - 0.2) < 1e-9


def test_token_exposure_uses_planned_when_larger_than_accumulator():
    cfg = {
        "risk": {
            "max_token_exposure_usd": 6,
            "max_total_buy_spend_usd": None,
        }
    }
    state = {"buy_notional_accumulator": {"token": {"usd": 2.0}}}

    ok, reason, available = accumulator_check(
        "token",
        0.5,
        state,
        cfg,
        side="BUY",
        planned_token_notional=5.8,
        planned_total_notional=5.8,
    )

    assert not ok
    assert reason == "max_position_usd_per_token"
    assert abs(available - 0.2) < 1e-9


def test_sell_bypasses_blacklist_filter():
    ok, reason = risk_check(
        token_key="bad-token",
        order_shares=2.0,
        my_shares=3.0,
        ref_price=0.5,
        cfg={"blacklist_token_keys": ["bad"]},
        token_title="bad celebrity market",
        side="SELL",
        available_shares=3.0,
    )

    assert ok
    assert reason == "ok"


def test_buy_still_respects_blacklist_filter():
    ok, reason = risk_check(
        token_key="bad-token",
        order_shares=2.0,
        my_shares=0.0,
        ref_price=0.5,
        cfg={"blacklist_token_keys": ["bad"]},
        token_title="bad celebrity market",
        side="BUY",
    )

    assert not ok
    assert reason == "blacklist"


def test_legacy_account_notional_total_maps_to_total_exposure():
    account = {
        "max_notional_per_token": 10000,
        "max_notional_total": 10000,
    }
    effective = effective_risk_config({}, account_risk_overrides(account))

    assert effective["max_token_buy_spend_usd"] == 10000
    assert effective["max_total_exposure_usd"] == 10000


def test_nested_account_risk_wins_over_legacy_flat_fields():
    account = {
        "risk": {
            "max_token_exposure_usd": 6,
            "max_token_buy_spend_usd": None,
        },
        "max_notional_per_token": 10000,
    }

    effective = effective_risk_config({}, account_risk_overrides(account))

    assert effective["max_token_exposure_usd"] == 6
    assert effective["max_token_buy_spend_usd"] is None
