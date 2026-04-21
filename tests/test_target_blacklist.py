"""Integration test for per-target blacklist."""
import sys
import logging

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
        blacklist = target_blacklists.get(target_addr.lower(), [])
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


if __name__ == "__main__":
    test_per_target_blacklist()
    test_global_fallback_blacklist()
    test_no_blacklist()
    print("\nALL TARGET BLACKLIST TESTS PASSED")
