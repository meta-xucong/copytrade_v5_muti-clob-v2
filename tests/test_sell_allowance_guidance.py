import logging
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, ".")

import ct_exec


def test_apply_actions_records_conditional_preflight_and_skips_sell_retries_on_missing_allowance(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    client = SimpleNamespace(
        get_open_orders=lambda *_args, **_kwargs: [],
        get_clob_market_info=lambda *_args, **_kwargs: {},
    )

    def fail_place(*_args, **_kwargs):
        raise RuntimeError("not enough balance / allowance")

    monkeypatch.setattr(ct_exec, "place_order", fail_place)
    monkeypatch.setattr(
        ct_exec,
        "preflight_conditional_sell_ready_v2",
        lambda *_args, **_kwargs: {
            "ok": True,
            "ready": False,
            "balance": 5.454544,
            "missing_operators": ["0xE111180000d2663C0091e4f400237545B87B996B"],
            "message": "conditional token operator approvals are missing for sell path",
        },
    )

    state = {}
    cfg = {
        "retry_on_insufficient_balance": True,
        "allow_partial": True,
    }
    caplog.set_level(logging.WARNING)

    updated = ct_exec.apply_actions(
        client=client,
        actions=[
            {
                "type": "place",
                "token_id": "tok-sell",
                "side": "SELL",
                "price": 0.55,
                "size": 5.0,
            }
        ],
        open_orders=[],
        now_ts=100,
        dry_run=False,
        cfg=cfg,
        state=state,
    )

    assert updated == []
    assert state["conditional_preflight_by_token"]["tok-sell"]["ready"] is False
    assert any("CONDITIONAL_PREFLIGHT" in record.message for record in caplog.records)
