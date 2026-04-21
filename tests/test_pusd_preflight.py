from types import SimpleNamespace

import sys

sys.path.insert(0, ".")

import ct_clob_gateway


def test_preflight_pusd_ready_v2_reports_ready():
    client = SimpleNamespace(
        get_balance_allowance=lambda params: {"balance": "12.5", "allowance": "7.0"}
    )

    result = ct_clob_gateway.preflight_pusd_ready_v2(client)

    assert result["ok"] is True
    assert result["ready"] is True
    assert result["balance"] == 12.5
    assert result["allowance"] == 7.0


def test_preflight_pusd_ready_v2_reports_missing_allowance():
    client = SimpleNamespace(
        get_balance_allowance=lambda params: {"balance": "12.5", "allowance": "0"}
    )

    result = ct_clob_gateway.preflight_pusd_ready_v2(client)

    assert result["ok"] is True
    assert result["ready"] is False
    assert "allowance" in result["message"].lower()


def test_preflight_pusd_ready_v2_accepts_allowances_map_payload():
    client = SimpleNamespace(
        get_balance_allowance=lambda params: {
            "balance": "3.5",
            "allowances": {
                "0xE111180000d2663C0091e4f400237545B87B996B": "2.0",
                "0xe2222d279d744050d28e00520010520000310F59": "0",
            },
        }
    )

    result = ct_clob_gateway.preflight_pusd_ready_v2(client)

    assert result["ok"] is True
    assert result["ready"] is True
    assert result["balance"] == 3.5
    assert result["allowance"] == 2.0


def test_preflight_conditional_sell_ready_v2_reports_missing_operator_allowances():
    client = SimpleNamespace(
        get_balance_allowance=lambda params: {
            "balance": "5.454544",
            "allowances": {
                "0xE111180000d2663C0091e4f400237545B87B996B": "0",
                "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296": "1",
                "0xe2222d279d744050d28e00520010520000310F59": "0",
            },
        }
    )

    result = ct_clob_gateway.preflight_conditional_sell_ready_v2(client, "tok-sell")

    assert result["ok"] is True
    assert result["ready"] is False
    assert result["balance"] == 5.454544
    assert len(result["missing_operators"]) == 2
    assert "sell path" in result["message"].lower()


def test_preflight_conditional_sell_ready_v2_reports_ready_when_balance_and_allowances_exist():
    client = SimpleNamespace(
        get_balance_allowance=lambda params: {
            "balance": "5.454544",
            "allowances": {
                "0xE111180000d2663C0091e4f400237545B87B996B": "1",
                "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296": "1",
                "0xe2222d279d744050d28e00520010520000310F59": "1",
            },
        }
    )

    result = ct_clob_gateway.preflight_conditional_sell_ready_v2(client, "tok-sell")

    assert result["ok"] is True
    assert result["ready"] is True
    assert result["missing_operators"] == []
