import logging
import sys

sys.path.insert(0, ".")

from copytrade_run import _count_runnable_accounts, _resolve_auto_worker_count


def test_count_runnable_accounts_filters_disabled_and_invalid_entries():
    accounts_cfg = [
        {
            "name": "ok",
            "enabled": True,
            "my_address": "0x1111111111111111111111111111111111111111",
            "private_key": "0xabc123",
        },
        {
            "name": "disabled",
            "enabled": False,
            "my_address": "0x2222222222222222222222222222222222222222",
            "private_key": "0xabc123",
        },
        {
            "name": "bad_addr",
            "enabled": True,
            "my_address": "not-an-evm-address",
            "private_key": "0xabc123",
        },
        {
            "name": "missing_key",
            "enabled": True,
            "my_address": "0x3333333333333333333333333333333333333333",
            "private_key": "",
        },
    ]

    assert _count_runnable_accounts(accounts_cfg) == 1


def test_resolve_auto_worker_count_uses_one_for_single_runnable_account(tmp_path):
    accounts_file = tmp_path / "accounts.json"
    accounts_file.write_text(
        """
        {
          "accounts": [
            {
              "name": "solo",
              "enabled": true,
              "my_address": "0x1111111111111111111111111111111111111111",
              "private_key": "0xabc123"
            }
          ]
        }
        """.strip(),
        encoding="utf-8",
    )

    cfg = {"accounts_file": "accounts.json", "account_workers": 2}

    assert _resolve_auto_worker_count(cfg, tmp_path, logging.getLogger("test")) == 1


def test_resolve_auto_worker_count_uses_two_for_multiple_runnable_accounts_even_if_config_is_one(
    tmp_path,
):
    accounts_file = tmp_path / "accounts.json"
    accounts_file.write_text(
        """
        {
          "accounts": [
            {
              "name": "a1",
              "enabled": true,
              "my_address": "0x1111111111111111111111111111111111111111",
              "private_key": "0xabc123"
            },
            {
              "name": "a2",
              "enabled": true,
              "my_address": "0x2222222222222222222222222222222222222222",
              "private_key": "0xdef456"
            }
          ]
        }
        """.strip(),
        encoding="utf-8",
    )

    cfg = {"accounts_file": "accounts.json", "account_workers": 1}

    assert _resolve_auto_worker_count(cfg, tmp_path, logging.getLogger("test")) == 2
