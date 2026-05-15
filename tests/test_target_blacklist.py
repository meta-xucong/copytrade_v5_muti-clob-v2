"""Integration test for per-target blacklist."""
import sys
import logging

import copytrade_run
import ct_data
from copytrade_run import _fetch_all_target_actions, _normalize_token_blacklist

logger = logging.getLogger("test_target_blacklist")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)


def _fetch_all_target_positions_logic(
    positions_by_target,
    target_ratios,
    target_blacklists,
):
    """Simplified version of the merge logic from copytrade_run.py."""
    all_positions_by_token = {}
    for target_addr, positions in positions_by_target.items():
        ratio = target_ratios.get(target_addr.lower(), 1.0)
        blacklist = _normalize_token_blacklist(target_blacklists.get(target_addr.lower(), []))
        for pos in positions:
            token_key = str(pos.get("token_key") or "")
            if not token_key:
                continue
            if blacklist:
                title_l = str(pos.get("title") or "").lower()
                if any(str(bl_item).lower() in title_l for bl_item in blacklist if bl_item is not None):
                    continue
            size = float(pos.get("size") or 0.0) * ratio
            existing = all_positions_by_token.get(token_key)
            if existing is None or size > float(existing.get("size") or 0.0):
                pos_copy = dict(pos)
                pos_copy["size"] = size
                pos_copy["_source_target"] = target_addr
                all_positions_by_token[token_key] = pos_copy
    return list(all_positions_by_token.values())


def test_per_target_blacklist():
    positions = {
        "0xA": [
            {"token_key": "tk1", "title": "Bitcoin price up?", "size": 10.0},
            {"token_key": "tk2", "title": "Ethereum price up?", "size": 5.0},
        ],
        "0xB": [
            {"token_key": "tk1", "title": "Bitcoin price up?", "size": 8.0},
            {"token_key": "tk3", "title": "SpaceX IPO?", "size": 20.0},
        ],
    }
    ratios = {"0xa": 1.0, "0xb": 1.0}
    blacklists = {
        "0xa": ["Bitcoin"],
        "0xb": [],
    }
    merged = _fetch_all_target_positions_logic(positions, ratios, blacklists)
    keys = {p["token_key"] for p in merged}
    # 0xA's Bitcoin should be filtered; 0xB's Bitcoin should remain
    # tk1 winner should be 0xB (size 8.0)
    # tk2 winner should be 0xA (size 5.0)
    # tk3 winner should be 0xB (size 20.0)
    assert keys == {"tk1", "tk2", "tk3"}, f"Unexpected keys: {keys}"
    tk1 = next(p for p in merged if p["token_key"] == "tk1")
    assert tk1["_source_target"] == "0xB", "tk1 should come from 0xB because 0xA Bitcoin is blacklisted"
    print("[PASS] per_target_blacklist")


def test_global_fallback_blacklist():
    positions = {
        "0xA": [
            {"token_key": "tk1", "title": "Bitcoin price up?", "size": 10.0},
        ],
    }
    ratios = {"0xa": 1.0}
    blacklists = {"0xa": ["Bitcoin"]}
    merged = _fetch_all_target_positions_logic(positions, ratios, blacklists)
    assert len(merged) == 0, f"Expected empty, got {merged}"
    print("[PASS] global_fallback_blacklist")


def test_no_blacklist():
    positions = {
        "0xA": [
            {"token_key": "tk1", "title": "Bitcoin price up?", "size": 10.0},
        ],
    }
    ratios = {"0xa": 1.0}
    blacklists = {"0xa": []}
    merged = _fetch_all_target_positions_logic(positions, ratios, blacklists)
    assert len(merged) == 1
    print("[PASS] no_blacklist")


def test_string_blacklist_is_single_keyword():
    positions = {
        "0xA": [
            {"token_key": "tk1", "title": "Ethereum price up?", "size": 10.0},
            {"token_key": "tk2", "title": "Bitcoin price up?", "size": 5.0},
        ],
    }
    ratios = {"0xa": 1.0}
    blacklists = {"0xa": "Bitcoin"}
    merged = _fetch_all_target_positions_logic(positions, ratios, blacklists)
    keys = {p["token_key"] for p in merged}
    assert keys == {"tk1"}, f"String blacklist should not be split into chars: {keys}"
    print("[PASS] string_blacklist_is_single_keyword")


def test_buy_only_blocklist_blocks_buy_and_keeps_sell(monkeypatch):
    import ct_data

    actions = [
        {
            "token_id": "tok_buy",
            "condition_id": "cond_buy",
            "side": "BUY",
            "timestamp_ms": 100,
            "raw": {"title": "Will Russia capture Pokrovka by April 30?"},
        },
        {
            "token_id": "tok_sell",
            "condition_id": "cond_sell",
            "side": "SELL",
            "timestamp_ms": 200,
            "raw": {"title": "Will Russia capture Pokrovka by April 30?"},
        },
    ]

    def fake_fetch_target_actions_since(*_args, **_kwargs):
        return list(actions), {"ok": True, "incomplete": False, "latest_ms": 200}

    monkeypatch.setattr(ct_data, "fetch_target_actions_since", fake_fetch_target_actions_since)

    merged, info = _fetch_all_target_actions(
        data_client=object(),
        target_addresses=["0xabc"],
        cursor_ms=0,
        use_trades_api=False,
        page_size=10,
        max_offset=10,
        taker_only=False,
        logger=logging.getLogger("test_buy_only_blocklist"),
        target_blacklists={},
        target_buy_blocklists={"0xabc": ["russia capture"]},
        target_whitelists={},
    )

    assert info["ok"] is True
    assert [item["side"] for item in merged] == ["SELL"]
    print("[PASS] buy_only_blocklist_blocks_buy_and_keeps_sell")


def test_topic_blacklist_filters_target_positions(monkeypatch):
    def fake_fetch_positions_norm(*_args, **_kwargs):
        positions = [
            {
                "token_key": "tk_sports",
                "token_id": "tok_sports",
                "condition_id": "cond_sports",
                "title": "Lakers vs Thunder",
                "size": 10.0,
                "raw": {"asset": "tok_sports"},
            },
            {
                "token_key": "tk_macro",
                "token_id": "tok_macro",
                "condition_id": "cond_macro",
                "title": "Will WTI hit $110",
                "size": 5.0,
                "raw": {"asset": "tok_macro"},
            },
        ]
        return positions, {"ok": True, "incomplete": False}

    def fake_gamma_fetch_topic_metadata_by_token_ids(token_ids, condition_ids_by_token=None):
        return {
            "tok_sports": {"topic_keys": ["sports", "nba"]},
            "tok_macro": {"topic_keys": ["macro", "commodities"]},
        }

    monkeypatch.setattr(ct_data, "fetch_positions_norm", fake_fetch_positions_norm)
    monkeypatch.setattr(
        copytrade_run,
        "gamma_fetch_topic_metadata_by_token_ids",
        fake_gamma_fetch_topic_metadata_by_token_ids,
    )

    merged, info, _src = copytrade_run._fetch_all_target_positions(
        data_client=object(),
        target_addresses=["0xabc"],
        target_ratios={"0xabc": 1.0},
        target_blacklists={"0xabc": []},
        target_topic_blacklists={"0xabc": {"sports"}},
        size_threshold=0.0,
        positions_limit=200,
        positions_max_pages=5,
        refresh_sec=5,
        cache_bust_mode="none",
        header_keys=[],
        logger=logging.getLogger("test_topic_blacklist_filters_target_positions"),
    )

    assert info["ok"] is True
    keys = {p["token_key"] for p in merged}
    assert keys == {"tk_macro"}, keys


if __name__ == "__main__":
    test_per_target_blacklist()
    test_global_fallback_blacklist()
    test_no_blacklist()
    test_string_blacklist_is_single_keyword()
    print("\nALL TARGET BLACKLIST TESTS PASSED")
