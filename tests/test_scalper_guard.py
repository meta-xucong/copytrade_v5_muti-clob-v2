import logging

import ct_data
from copytrade_run import _fetch_all_target_actions, _reset_scalper_guard_state


LOGGER = logging.getLogger("test_scalper_guard")


def _guard_cfg(**overrides):
    cfg = {
        "scalper_guard_enabled": True,
        "scalper_guard_window_sec": 900,
        "scalper_guard_min_trades": 4,
        "scalper_guard_min_side_switches": 3,
        "scalper_guard_min_side_count": 2,
        "scalper_guard_min_each_side_usd": 1.0,
        "scalper_guard_require_price_edge": False,
        "scalper_guard_block_sec": 3600,
    }
    cfg.update(overrides)
    return cfg


def _action(side, ts_ms, *, token_id="tok", size=40.0, price=0.10):
    return {
        "token_id": token_id,
        "condition_id": "cond",
        "side": side,
        "size": size,
        "price": price,
        "timestamp_ms": ts_ms,
        "raw": {"title": "Lakers vs. Thunder"},
    }


def _fetch(actions_by_target):
    def fake_fetch_target_actions_since(_client, target, *_args, **_kwargs):
        actions = list(actions_by_target.get(target.lower(), []))
        latest_ms = max((int(item["timestamp_ms"]) for item in actions), default=0)
        return actions, {"ok": True, "incomplete": False, "latest_ms": latest_ms}

    return fake_fetch_target_actions_since


def test_scalper_guard_blocks_future_buys_but_keeps_sells(monkeypatch):
    _reset_scalper_guard_state()
    actions = [
        _action("BUY", 1_000),
        _action("SELL", 2_000),
        _action("BUY", 3_000),
        _action("SELL", 4_000),
        _action("BUY", 5_000),
        _action("SELL", 6_000),
    ]
    monkeypatch.setattr(ct_data, "fetch_target_actions_since", _fetch({"0xabc": actions}))

    merged, info = _fetch_all_target_actions(
        data_client=object(),
        target_addresses=["0xabc"],
        cursor_ms=0,
        use_trades_api=False,
        page_size=10,
        max_offset=10,
        taker_only=False,
        logger=LOGGER,
        scalper_guard_cfg=_guard_cfg(),
    )

    assert info["ok"] is True
    assert [item["side"] for item in merged] == ["BUY", "SELL", "BUY", "SELL", "SELL"]


def test_scalper_guard_does_not_block_directional_build(monkeypatch):
    _reset_scalper_guard_state()
    actions = [
        _action("BUY", 1_000),
        _action("BUY", 2_000),
        _action("BUY", 3_000),
        _action("BUY", 4_000),
    ]
    monkeypatch.setattr(ct_data, "fetch_target_actions_since", _fetch({"0xabc": actions}))

    merged, _info = _fetch_all_target_actions(
        data_client=object(),
        target_addresses=["0xabc"],
        cursor_ms=0,
        use_trades_api=False,
        page_size=10,
        max_offset=10,
        taker_only=False,
        logger=LOGGER,
        scalper_guard_cfg=_guard_cfg(),
    )

    assert [item["side"] for item in merged] == ["BUY", "BUY", "BUY", "BUY"]


def test_scalper_guard_is_scoped_by_source_and_token(monkeypatch):
    _reset_scalper_guard_state()
    scalper_actions = [
        _action("BUY", 1_000, token_id="tok_shared"),
        _action("SELL", 2_000, token_id="tok_shared"),
        _action("BUY", 3_000, token_id="tok_shared"),
        _action("SELL", 4_000, token_id="tok_shared"),
        _action("BUY", 5_000, token_id="tok_shared"),
    ]
    safe_actions = [
        _action("BUY", 6_000, token_id="tok_shared"),
        _action("BUY", 7_000, token_id="tok_other"),
    ]
    monkeypatch.setattr(
        ct_data,
        "fetch_target_actions_since",
        _fetch({"0xabc": scalper_actions, "0xdef": safe_actions}),
    )

    merged, _info = _fetch_all_target_actions(
        data_client=object(),
        target_addresses=["0xabc", "0xdef"],
        cursor_ms=0,
        use_trades_api=False,
        page_size=10,
        max_offset=10,
        taker_only=False,
        logger=LOGGER,
        scalper_guard_cfg=_guard_cfg(),
    )

    merged_pairs = [(item["_source_target"], item["token_id"], item["side"]) for item in merged]
    assert ("0xabc", "tok_shared", "BUY") in merged_pairs
    assert ("0xdef", "tok_shared", "BUY") in merged_pairs
    assert ("0xdef", "tok_other", "BUY") in merged_pairs
    assert merged_pairs.count(("0xabc", "tok_shared", "BUY")) == 2


def test_scalper_guard_disabled_preserves_all_actions(monkeypatch):
    _reset_scalper_guard_state()
    actions = [
        _action("BUY", 1_000),
        _action("SELL", 2_000),
        _action("BUY", 3_000),
        _action("SELL", 4_000),
        _action("BUY", 5_000),
    ]
    monkeypatch.setattr(ct_data, "fetch_target_actions_since", _fetch({"0xabc": actions}))

    merged, _info = _fetch_all_target_actions(
        data_client=object(),
        target_addresses=["0xabc"],
        cursor_ms=0,
        use_trades_api=False,
        page_size=10,
        max_offset=10,
        taker_only=False,
        logger=LOGGER,
        scalper_guard_cfg={"scalper_guard_enabled": False},
    )

    assert [item["side"] for item in merged] == ["BUY", "SELL", "BUY", "SELL", "BUY"]
