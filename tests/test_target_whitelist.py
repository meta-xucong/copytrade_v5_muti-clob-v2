import logging
import sys

sys.path.insert(0, ".")

import copytrade_run
import ct_data


def test_target_whitelist_allows_matching_buy_and_keeps_sell(monkeypatch):
    actions = [
        {
            "token_id": "tok_buy",
            "condition_id": "cond_buy",
            "side": "BUY",
            "timestamp_ms": 100,
            "raw": {"title": "US election market"},
        },
        {
            "token_id": "tok_sell",
            "condition_id": "cond_sell",
            "side": "SELL",
            "timestamp_ms": 200,
            "raw": {"title": "Random market"},
        },
    ]

    def fake_fetch_target_actions_since(*_args, **_kwargs):
        return list(actions), {"ok": True, "incomplete": False, "latest_ms": 200}

    def fake_gamma_fetch_topic_metadata_by_token_ids(token_ids, condition_ids_by_token=None):
        assert token_ids == ["tok_buy"]
        assert condition_ids_by_token == {"tok_buy": "cond_buy"}
        return {"tok_buy": {"topic_keys": ["us-politics", "politics"]}}

    monkeypatch.setattr(ct_data, "fetch_target_actions_since", fake_fetch_target_actions_since)
    monkeypatch.setattr(
        copytrade_run,
        "gamma_fetch_topic_metadata_by_token_ids",
        fake_gamma_fetch_topic_metadata_by_token_ids,
    )

    merged, info = copytrade_run._fetch_all_target_actions(
        data_client=object(),
        target_addresses=["0xabc"],
        cursor_ms=0,
        use_trades_api=False,
        page_size=10,
        max_offset=10,
        taker_only=False,
        logger=logging.getLogger("test_target_whitelist_allow"),
        target_blacklists={},
        target_whitelists={"0xabc": ["us_politics"]},
    )

    assert info["ok"] is True
    assert [item["side"] for item in merged] == ["BUY", "SELL"]
    assert merged[0]["topic_keys"] == ["us-politics", "politics"]


def test_target_whitelist_empty_blocks_buy_and_keeps_sell(monkeypatch):
    actions = [
        {
            "token_id": "tok_buy",
            "condition_id": "cond_buy",
            "side": "BUY",
            "timestamp_ms": 100,
            "raw": {"title": "US election market"},
        },
        {
            "token_id": "tok_sell",
            "condition_id": "cond_sell",
            "side": "SELL",
            "timestamp_ms": 200,
            "raw": {"title": "Random market"},
        },
    ]

    def fake_fetch_target_actions_since(*_args, **_kwargs):
        return list(actions), {"ok": True, "incomplete": False, "latest_ms": 200}

    def fake_gamma_fetch_topic_metadata_by_token_ids(token_ids, condition_ids_by_token=None):
        return {"tok_buy": {"topic_keys": ["us-politics"]}}

    monkeypatch.setattr(ct_data, "fetch_target_actions_since", fake_fetch_target_actions_since)
    monkeypatch.setattr(
        copytrade_run,
        "gamma_fetch_topic_metadata_by_token_ids",
        fake_gamma_fetch_topic_metadata_by_token_ids,
    )

    merged, _info = copytrade_run._fetch_all_target_actions(
        data_client=object(),
        target_addresses=["0xabc"],
        cursor_ms=0,
        use_trades_api=False,
        page_size=10,
        max_offset=10,
        taker_only=False,
        logger=logging.getLogger("test_target_whitelist_deny"),
        target_blacklists={},
        target_whitelists={"0xabc": []},
    )

    assert [item["side"] for item in merged] == ["SELL"]


def test_missing_whitelist_config_preserves_existing_behavior(monkeypatch):
    actions = [
        {
            "token_id": "tok_buy",
            "condition_id": "cond_buy",
            "side": "BUY",
            "timestamp_ms": 100,
            "raw": {"title": "US election market"},
        }
    ]

    def fake_fetch_target_actions_since(*_args, **_kwargs):
        return list(actions), {"ok": True, "incomplete": False, "latest_ms": 100}

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("topic metadata fetch should not run when whitelist is not configured")

    monkeypatch.setattr(ct_data, "fetch_target_actions_since", fake_fetch_target_actions_since)
    monkeypatch.setattr(copytrade_run, "gamma_fetch_topic_metadata_by_token_ids", fail_if_called)

    merged, _info = copytrade_run._fetch_all_target_actions(
        data_client=object(),
        target_addresses=["0xabc"],
        cursor_ms=0,
        use_trades_api=False,
        page_size=10,
        max_offset=10,
        taker_only=False,
        logger=logging.getLogger("test_target_whitelist_missing"),
        target_blacklists={},
        target_whitelists={},
    )

    assert [item["side"] for item in merged] == ["BUY"]
