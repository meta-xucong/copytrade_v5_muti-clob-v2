import logging
from pathlib import Path
from types import SimpleNamespace

import sys

sys.path.insert(0, ".")

import copytrade_run
from ct_runtime_health import (
    begin_recovery,
    classify_error,
    complete_recovery,
    ensure_runtime_health,
    record_component_failure,
    record_component_success,
    should_pause_buys,
    should_start_recovery,
)
from ct_state import load_state


def test_classify_network_errors_as_transient():
    assert classify_error("[WinError 10054] An existing connection was forcibly closed") == "transient"
    assert classify_error("Server disconnected while reading response") == "transient"
    assert classify_error("HTTP 503 Service Unavailable") == "transient"


def test_classify_preflight_and_market_state():
    assert classify_error("pUSD preflight failed: allowance too low") == "preflight"
    assert classify_error("FAK no match / no best bid") == "market_state"


def test_runtime_health_degraded_recovery_and_buy_pause():
    state = {}
    health = record_component_failure(
        state,
        "clob_open_orders",
        "transient",
        "Server disconnected",
        100,
        buy_pause_sec=120,
        order_state_unknown=True,
    )
    assert health["mode"] == "safe_mode"
    paused, reason = should_pause_buys(state, 110)
    assert paused is True
    assert reason in {"network_safe_mode", "order_state_unknown"}

    for component in (
        "clob_open_orders",
        "data_api_target_positions",
        "data_api_target_actions",
        "data_api_my_positions",
        "data_api_my_trades",
    ):
        record_component_success(state, component, 130)
    assert should_start_recovery(
        state,
        (
            "clob_open_orders",
            "data_api_target_positions",
            "data_api_target_actions",
            "data_api_my_positions",
            "data_api_my_trades",
        ),
        130,
    )

    need_light, need_full = begin_recovery(state, 130, full_reconcile_min_interval_sec=1800)
    assert need_light is True
    assert need_full is True
    complete_recovery(state, 140, ok=True, light_resync_done=True, full_reconcile_done=True)
    health = ensure_runtime_health(state)
    assert health["mode"] == "running"
    assert health["needs_light_resync"] is False
    assert health["last_full_reconcile_ts"] == 140
    assert should_pause_buys(state, 141) == (False, "")


def test_non_blocking_target_positions_failure_does_not_pause_buys():
    state = {}
    health = record_component_failure(
        state,
        "data_api_target_positions",
        "transient",
        "target positions incomplete",
        100,
        buy_pause_sec=0,
        affect_runtime_mode=False,
    )
    assert health["mode"] == "running"
    assert health["components"]["data_api_target_positions"]["status"] == "failed"
    assert health["last_error"]["component"] == "data_api_target_positions"
    assert should_pause_buys(state, 101) == (False, "")
    assert should_start_recovery(
        state,
        ("data_api_target_positions",),
        101,
    ) is False


def test_non_blocking_failure_clears_prior_degraded_from_same_component():
    state = {
        "runtime_health": {
            "mode": "degraded",
            "degraded_since": 80,
            "buy_paused_until": 200,
            "needs_light_resync": True,
            "needs_full_reconcile": True,
            "last_error": {
                "component": "data_api_target_positions",
                "kind": "transient",
                "message": "target positions incomplete",
                "ts": 80,
            },
        }
    }
    health = record_component_failure(
        state,
        "data_api_target_positions",
        "transient",
        "target positions incomplete",
        100,
        buy_pause_sec=0,
        affect_runtime_mode=False,
    )
    assert health["mode"] == "running"
    assert health["degraded_since"] == 0
    assert health["buy_paused_until"] == 0
    assert should_pause_buys(state, 101) == (False, "")


def test_ensure_runtime_health_migrates_stale_target_position_degraded():
    state = {
        "runtime_health": {
            "mode": "degraded",
            "degraded_since": 80,
            "buy_paused_until": 200,
            "needs_light_resync": True,
            "needs_full_reconcile": True,
            "order_state_unknown_since": 80,
            "last_error": {
                "component": "data_api_target_positions",
                "kind": "transient",
                "message": "target positions incomplete",
                "ts": 80,
            },
        }
    }
    health = ensure_runtime_health(state)
    assert health["mode"] == "running"
    assert health["degraded_since"] == 0
    assert health["buy_paused_until"] == 0
    assert health["needs_light_resync"] is False
    assert health["needs_full_reconcile"] is False
    assert health["order_state_unknown_since"] == 0
    assert should_pause_buys(state, 101) == (False, "")


def test_runtime_health_full_reconcile_throttle():
    state = {"runtime_health": {"last_full_reconcile_ts": 1000}}
    record_component_failure(state, "data_api_target_actions", "transient", "timeout", 1100)
    need_light, need_full = begin_recovery(state, 1200, full_reconcile_min_interval_sec=1800)
    assert need_light is True
    assert need_full is False


def test_preflight_safe_mode_does_not_auto_recover_from_network_successes():
    state = {}
    record_component_failure(
        state,
        "pusd_preflight",
        "preflight",
        "pUSD allowance too low",
        100,
        buy_pause_sec=3600,
    )
    for component in (
        "clob_open_orders",
        "data_api_target_positions",
        "data_api_target_actions",
        "data_api_my_positions",
        "data_api_my_trades",
    ):
        record_component_success(state, component, 130)
    assert not should_start_recovery(
        state,
        (
            "clob_open_orders",
            "data_api_target_positions",
            "data_api_target_actions",
            "data_api_my_positions",
            "data_api_my_trades",
        ),
        130,
    )


def test_state_load_adds_runtime_health(tmp_path: Path):
    path = tmp_path / "state.json"
    path.write_text('{"open_orders": {}}', encoding="utf-8")
    state = load_state(str(path))
    health = state.get("runtime_health")
    assert isinstance(health, dict)
    assert health["mode"] == "running"
    assert isinstance(health["components"], dict)


def test_merge_remote_open_orders_preserves_unseen_until_grace():
    logger = logging.getLogger("test_merge_remote_open_orders")
    state = {
        "managed_order_ids": ["o1", "o2"],
        "order_ts_by_id": {"o1": 100, "o2": 100},
        "open_orders": {
            "t1": [{"order_id": "o1", "side": "BUY", "price": 0.4, "size": 5, "ts": 100}],
            "t2": [{"order_id": "o2", "side": "SELL", "price": 0.5, "size": 3, "ts": 100}],
        },
    }
    cfg = {"order_visibility_grace_sec": 180}
    copytrade_run._merge_remote_open_orders_into_state(
        state,
        [{"order_id": "o1", "token_id": "t1", "side": "BUY", "price": 0.41, "size": 5, "ts": 120}],
        200,
        cfg,
        logger,
        adopt_existing=False,
    )
    assert "t2" in state["open_orders"], state
    assert state["open_orders"]["t1"][0]["price"] == 0.41

    copytrade_run._merge_remote_open_orders_into_state(
        state,
        [{"order_id": "o1", "token_id": "t1", "side": "BUY", "price": 0.42, "size": 5, "ts": 400}],
        400,
        cfg,
        logger,
        adopt_existing=False,
    )
    assert "t2" not in state["open_orders"], state


def test_light_resync_updates_orders_positions_and_replay(monkeypatch):
    logger = logging.getLogger("test_light_resync")

    def fake_fetch_open_orders_norm(_client, _timeout):
        return (
            [
                {
                    "order_id": "o1",
                    "token_id": "t1",
                    "side": "BUY",
                    "price": 0.4,
                    "size": 5,
                    "ts": 100,
                }
            ],
            True,
            None,
        )

    def fake_fetch_positions_norm(*_args, **_kwargs):
        return ([{"token_id": "t1", "size": 5.0}], {"ok": True, "incomplete": False})

    monkeypatch.setattr(copytrade_run, "fetch_open_orders_norm", fake_fetch_open_orders_norm)
    monkeypatch.setattr(copytrade_run, "fetch_positions_norm", fake_fetch_positions_norm)

    state = {"managed_order_ids": ["o1"], "open_orders": {}}
    ctx = copytrade_run.AccountContext(
        name="w01",
        my_address="0x0000000000000000000000000000000000000001",
        private_key="k",
        follow_ratio=0.1,
        clob_client=SimpleNamespace(),
        clob_read_client=SimpleNamespace(),
        state=state,
        state_path=Path("state.json"),
    )
    summary = copytrade_run._run_light_resync_after_reconnect(
        {
            "api_timeout_sec": 30,
            "positions_limit": 50,
            "positions_max_pages": 1,
            "recovery_actions_replay_window_sec": 1800,
            "actions_source": "actions",
        },
        SimpleNamespace(),
        ctx,
        logger,
        2000,
    )
    assert summary["ok"] is True
    assert state["open_orders"]["t1"][0]["order_id"] == "o1"
    assert state["my_positions"][0]["size"] == 5.0
    assert state["actions_replay_from_ms"] == 2000 * 1000 - 1800 * 1000
