import logging
import sys

sys.path.insert(0, ".")

import copytrade_run
import ct_data


def test_topic_blacklist_blocks_buy_only(monkeypatch):
    actions = [
        {
            "token_id": "tok_buy",
            "condition_id": "cond_buy",
            "side": "BUY",
            "timestamp_ms": 100,
            "raw": {"title": "Lakers vs. Thunder"},
        },
        {
            "token_id": "tok_sell",
            "condition_id": "cond_sell",
            "side": "SELL",
            "timestamp_ms": 200,
            "raw": {"title": "Lakers vs. Thunder"},
        },
    ]

    def fake_fetch_target_actions_since(*_args, **_kwargs):
        return list(actions), {"ok": True, "incomplete": False, "latest_ms": 200}

    def fake_gamma_fetch_topic_metadata_by_token_ids(token_ids, condition_ids_by_token=None):
        assert token_ids == ["tok_buy"]
        return {"tok_buy": {"topic_keys": ["sports", "nba"]}}

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
        logger=logging.getLogger("test_topic_blacklist_blocks_buy_only"),
        target_blacklists={},
        target_topic_blacklists={"0xabc": {"sports"}},
    )

    assert info["ok"] is True
    assert [item["side"] for item in merged] == ["SELL"]


def test_buy_only_topic_blacklist_blocks_buy_only(monkeypatch):
    actions = [
        {
            "token_id": "tok_buy",
            "condition_id": "cond_buy",
            "side": "BUY",
            "timestamp_ms": 100,
            "raw": {"title": "Lakers vs. Thunder"},
        },
        {
            "token_id": "tok_sell",
            "condition_id": "cond_sell",
            "side": "SELL",
            "timestamp_ms": 200,
            "raw": {"title": "Lakers vs. Thunder"},
        },
    ]

    def fake_fetch_target_actions_since(*_args, **_kwargs):
        return list(actions), {"ok": True, "incomplete": False, "latest_ms": 200}

    def fake_gamma_fetch_topic_metadata_by_token_ids(token_ids, condition_ids_by_token=None):
        assert token_ids == ["tok_buy"]
        return {"tok_buy": {"topic_keys": ["sports", "nba"]}}

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
        logger=logging.getLogger("test_buy_only_topic_blacklist_blocks_buy_only"),
        target_blacklists={},
        target_buy_block_topic_blacklists={"0xabc": {"sports"}},
    )

    assert info["ok"] is True
    assert [item["side"] for item in merged] == ["SELL"]


def test_whitelist_disabled_by_default_even_if_configured(monkeypatch):
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
        raise AssertionError("topic metadata fetch should not run when whitelist is disabled")

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
        logger=logging.getLogger("test_whitelist_disabled_by_default_even_if_configured"),
        target_blacklists={},
        target_whitelists={"0xabc": ["us-politics"]},
        enable_target_whitelist=False,
    )

    assert [item["side"] for item in merged] == ["BUY"]
