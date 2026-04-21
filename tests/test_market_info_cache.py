from types import SimpleNamespace

import sys

sys.path.insert(0, ".")

import ct_clob_gateway
def test_normalize_clob_market_info_v2_maps_core_fields():
    payload = {
        "id": "cond-1",
        "mts": "0.01",
        "mos": "5",
        "ao": True,
        "cbos": False,
        "rfqe": True,
        "fd": {"r": "0.12", "e": "2", "to": True},
        "t": [{"t": "tok-1", "o": "YES"}, {"t": "tok-2", "o": "NO"}],
    }

    normalized = ct_clob_gateway.normalize_clob_market_info_v2(payload)

    assert normalized["condition_id"] == "cond-1"
    assert normalized["min_tick_size"] == 0.01
    assert normalized["min_order_size"] == 5.0
    assert normalized["accepting_orders"] is True
    assert normalized["enable_order_book"] is False
    assert normalized["rfq_enabled"] is True
    assert normalized["fee_rate"] == 0.12
    assert normalized["fee_exponent"] == 2.0
    assert normalized["taker_only_fee"] is True
    assert normalized["token_map"]["tok-1"]["outcome"] == "YES"


def test_refresh_market_info_cache_v2_caches_by_condition_and_token():
    state = {
        "market_status_cache": {
            "tok-1": {"meta": {"conditionId": "cond-2"}},
        }
    }
    client = SimpleNamespace(
        get_clob_market_info=lambda condition_id: {
            "id": condition_id,
            "mts": "0.05",
            "mos": "10",
            "t": [{"t": "tok-1", "o": "YES"}],
        }
    )

    cached = ct_clob_gateway.refresh_market_info_cache_v2(client, state, "tok-1", ttl_sec=300)

    assert cached is not None
    assert state["market_info_cache"]["cond-2"]["min_tick_size"] == 0.05
    assert ct_clob_gateway.get_cached_market_info_for_token(state, "tok-1") == cached


def test_get_cached_market_info_for_token_finds_token_mapping():
    state = {
        "market_status_cache": {},
        "market_info_cache": {
            "cond-3": {
                "condition_id": "cond-3",
                "token_map": {"tok-3": {"outcome": "YES"}},
                "min_tick_size": 0.05,
                "min_order_size": 1.0,
            }
        },
    }
    cached = ct_clob_gateway.get_cached_market_info_for_token(state, "tok-3")

    assert cached is not None
    assert cached["condition_id"] == "cond-3"
    assert cached["min_tick_size"] == 0.05
    assert cached["min_order_size"] == 1.0
