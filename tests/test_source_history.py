import sys

sys.path.insert(0, ".")

from copytrade_run import _remember_token_source_target, _resolve_signal_source_target


def test_resolve_source_uses_history_when_live_maps_missing():
    src = _resolve_signal_source_target(
        token_id="t1",
        buy_signal_source_by_token={},
        position_source={},
        target_addresses=["0xaaa"],
        token_source_history={"t1": {"primary_source": "0xbbb"}},
    )
    assert src == "0xbbb", src


def test_resolve_source_prefers_live_signal_then_position():
    src = _resolve_signal_source_target(
        token_id="t1",
        buy_signal_source_by_token={"t1": "0x111"},
        position_source={"t1": "0x222"},
        target_addresses=["0xaaa"],
        token_source_history={"t1": {"primary_source": "0xbbb"}},
    )
    assert src == "0x111", src

    src2 = _resolve_signal_source_target(
        token_id="t1",
        buy_signal_source_by_token={},
        position_source={"t1": "0x222"},
        target_addresses=["0xaaa"],
        token_source_history={"t1": {"primary_source": "0xbbb"}},
    )
    assert src2 == "0x222", src2


def test_remember_source_tracks_votes_and_primary():
    state = {}
    _remember_token_source_target(state, "t1", "0xaaa", 1000, hint="buy")
    _remember_token_source_target(state, "t1", "0xaaa", 1001, hint="buy")
    _remember_token_source_target(state, "t1", "0xbbb", 1002, hint="buy")
    item = state["token_source_history"]["t1"]
    assert item["primary_source"] == "0xaaa", item
    assert item["by_source"]["0xaaa"] == 2, item
    assert item["by_source"]["0xbbb"] == 1, item

    _remember_token_source_target(state, "t1", "0xbbb", 1003, hint="sell")
    _remember_token_source_target(state, "t1", "0xbbb", 1004, hint="sell")
    item = state["token_source_history"]["t1"]
    assert item["primary_source"] == "0xbbb", item
    assert item["last_source"] == "0xbbb", item
    assert item["last_seen_ts"] == 1004, item

