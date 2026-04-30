import json
import sys

import pytest

sys.path.insert(0, ".")

import ct_resolver


def _extract_req_ids(params):
    if not isinstance(params, dict):
        return []
    ids = params.get("clob_token_ids")
    if ids is None:
        ids = params.get("clob_token_ids[]")
    if ids is None:
        return []
    if isinstance(ids, str):
        return [ids]
    if isinstance(ids, (list, tuple)):
        return [str(x) for x in ids]
    return []


def test_gamma_fetch_markets_active_then_closed_fallback(monkeypatch):
    token_active = "tid_active"
    token_closed = "tid_closed"
    active_market = {
        "id": "m_active",
        "clobTokenIds": json.dumps([token_active]),
        "closed": False,
        "active": True,
        "acceptingOrders": True,
    }
    closed_market = {
        "id": "m_closed",
        "clobTokenIds": json.dumps([token_closed]),
        "closed": True,
        "active": False,
        "acceptingOrders": False,
    }
    calls = []

    def fake_http_json(_url, params=None):
        params = dict(params or {})
        calls.append(params)
        ids = _extract_req_ids(params)
        closed = params.get("closed")
        if closed == "false":
            if token_active in ids:
                return [active_market]
            return []
        if closed == "true":
            if token_closed in ids:
                return [closed_market]
            return []
        return []

    monkeypatch.setattr(ct_resolver, "_http_json", fake_http_json)

    got = ct_resolver.gamma_fetch_markets_by_clob_token_ids([token_active, token_closed])
    assert got[token_active]["id"] == "m_active"
    assert got[token_closed]["id"] == "m_closed"
    assert any(call.get("closed") == "false" for call in calls)
    assert any(call.get("closed") == "true" for call in calls)


def test_gamma_fetch_markets_accepts_dict_data_payload(monkeypatch):
    token_id = "tid_1"
    market = {
        "id": "m1",
        "clobTokenIds": json.dumps([token_id]),
        "closed": False,
        "active": True,
        "acceptingOrders": True,
    }

    def fake_http_json(_url, params=None):
        if dict(params or {}).get("closed") == "false":
            return {"data": [market]}
        return []

    monkeypatch.setattr(ct_resolver, "_http_json", fake_http_json)
    got = ct_resolver.gamma_fetch_markets_by_clob_token_ids([token_id])
    assert got[token_id]["id"] == "m1"


def test_gamma_fetch_markets_normalizes_ids(monkeypatch):
    token_id = "tid_norm"
    market = {
        "id": "m_norm",
        "clobTokenIds": json.dumps([token_id]),
        "closed": False,
        "active": True,
        "acceptingOrders": True,
    }
    call_count = {"n": 0}

    def fake_http_json(_url, params=None):
        call_count["n"] += 1
        ids = _extract_req_ids(dict(params or {}))
        assert token_id in ids
        return [market]

    monkeypatch.setattr(ct_resolver, "_http_json", fake_http_json)
    got = ct_resolver.gamma_fetch_markets_by_clob_token_ids(["", "  ", token_id, token_id])
    assert list(got.keys()) == [token_id]
    assert call_count["n"] >= 1


def test_gamma_fetch_markets_fallback_to_sampling_index(monkeypatch):
    token_id = "tid_sampling_only"
    sampling_market = {
        "market_id": "sm1",
        "enable_order_book": True,
        "accepting_orders": True,
    }

    def fake_http_json(_url, params=None):
        return []

    def fake_sampling_index():
        return {token_id: sampling_market}

    monkeypatch.setattr(ct_resolver, "_http_json", fake_http_json)
    monkeypatch.setattr(ct_resolver, "_build_sampling_market_index", fake_sampling_index)

    got = ct_resolver.gamma_fetch_markets_by_clob_token_ids([token_id])
    assert token_id in got
    assert got[token_id]["market_id"] == "sm1"


def test_market_tradeable_state_supports_snake_case_fields():
    market = {
        "enable_order_book": True,
        "accepting_orders": True,
        "archived": False,
        "closed": False,
        "active": True,
    }
    assert ct_resolver.market_tradeable_state(market) is True


def test_gamma_fetch_topic_metadata_by_token_ids_normalizes_topic_keys(monkeypatch):
    token_id = "topic_tid_1"
    market = {
        "id": "market_1",
        "conditionId": "cond_1",
        "question": "Who wins the US election?",
        "category": "Politics",
        "seriesSlug": "US Politics",
        "clobTokenIds": json.dumps([token_id]),
        "tags": [{"slug": "us_politics"}],
    }

    monkeypatch.setattr(
        ct_resolver,
        "gamma_fetch_markets_by_clob_token_ids",
        lambda token_ids: {token_id: market},
    )

    got = ct_resolver.gamma_fetch_topic_metadata_by_token_ids([token_id])
    assert "us-politics" in got[token_id]["topic_keys"]
    assert "politics" in got[token_id]["topic_keys"]


def test_gamma_fetch_topic_metadata_by_token_ids_falls_back_to_condition(monkeypatch):
    token_id = "topic_tid_2"
    condition_id = "cond_fallback"
    market = {
        "id": "market_2",
        "conditionId": condition_id,
        "question": "Will CPI rise this month?",
        "category": "Finance",
        "tags": [{"slug": "cpi"}],
    }

    monkeypatch.setattr(ct_resolver, "gamma_fetch_markets_by_clob_token_ids", lambda token_ids: {})
    monkeypatch.setattr(ct_resolver, "_gamma_fetch_by_condition", lambda cond: market if cond == condition_id else None)

    got = ct_resolver.gamma_fetch_topic_metadata_by_token_ids(
        [token_id],
        condition_ids_by_token={token_id: condition_id},
    )
    assert "macro" in got[token_id]["topic_keys"]


def test_market_topic_metadata_fetches_tags_when_market_payload_is_sparse(monkeypatch):
    monkeypatch.setattr(
        ct_resolver,
        "_gamma_fetch_market_tags",
        lambda market_id: [{"slug": "esports"}, {"label": "Counter Strike"}],
    )
    market = {
        "id": "market_sparse",
        "conditionId": "cond_sparse",
        "question": "Will Team A win the grand final?",
        "clobTokenIds": json.dumps(["tok_sparse"]),
    }

    got = ct_resolver._market_topic_metadata_from_market(market)
    assert "esports" in got["topic_keys"]
    assert "counter-strike" in got["topic_keys"]


def test_market_topic_metadata_derives_sector_topics_from_tags_and_title(monkeypatch):
    monkeypatch.setattr(ct_resolver, "_gamma_fetch_market_tags", lambda _market_id: [])
    market = {
        "id": "market_geo",
        "conditionId": "cond_geo",
        "question": "Will Israel and Iran reach a ceasefire?",
        "category": "World",
        "tags": [{"slug": "middle-east"}],
        "clobTokenIds": json.dumps(["tok_geo"]),
    }

    got = ct_resolver._market_topic_metadata_from_market(market)
    assert "geopolitics" in got["topic_keys"]
    assert "politics" in got["topic_keys"]


def test_market_topic_metadata_derives_macro_from_finance_tags(monkeypatch):
    monkeypatch.setattr(ct_resolver, "_gamma_fetch_market_tags", lambda _market_id: [])
    market = {
        "id": "market_macro",
        "conditionId": "cond_macro",
        "question": "Will the Fed cut rates?",
        "category": "Finance",
        "tags": [{"slug": "fed"}, {"slug": "interest-rates"}],
        "clobTokenIds": json.dumps(["tok_macro"]),
    }

    got = ct_resolver._market_topic_metadata_from_market(market)
    assert "macro" in got["topic_keys"]
