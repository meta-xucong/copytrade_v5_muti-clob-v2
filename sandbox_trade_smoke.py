from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests

from ct_clob_gateway import (
    cancel_order_v2,
    init_v2_client,
    place_limit_order_v2,
    place_market_order_v2,
    preflight_pusd_ready_v2,
)
from py_clob_client_v2 import AssetType, BalanceAllowanceParams


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "copytrade_config.json"
ACCOUNTS_PATH = BASE_DIR / "accounts.json"

STANDARD_TEST_MARKET = {
    "label": "standard_maker_buy_cancel",
    "question": "US-Iran nuclear deal before 2027?",
    "token_id": "45763018441764333771124945243746174684578244015331389396782339063349542289693",
}

NEG_RISK_TEST_MARKET = {
    "label": "neg_risk_full_cycle",
    "question": "Will Spider-Man: Brand New Day be the top grossing movie of 2026?",
    "token_id": "28161183422242370392388296744035422249088647252796713903067039294971789722479",
}


def _load_runtime() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))
    accounts = json.loads(ACCOUNTS_PATH.read_text(encoding="utf-8-sig")).get("accounts", [])
    account = next(acct for acct in accounts if acct.get("enabled", True))
    return cfg, account


def _book(token_id: str, host: str) -> Dict[str, Any]:
    response = requests.get(
        f"{str(host).rstrip('/')}/book",
        params={"token_id": token_id},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def _best_levels(book: Dict[str, Any]) -> Tuple[float | None, float | None]:
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    best_bid = max((float(level["price"]) for level in bids), default=None)
    best_ask = min((float(level["price"]) for level in asks), default=None)
    return best_bid, best_ask


def _conditional_state(client: Any, token_id: str) -> Dict[str, Any]:
    return client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
    )


def _sleep_for_settlement(seconds: float = 3.0) -> None:
    time.sleep(seconds)


def run_standard_maker_buy_cancel(client: Any, host: str) -> Dict[str, Any]:
    token_id = STANDARD_TEST_MARKET["token_id"]
    book = _book(token_id, host)
    best_bid, _best_ask = _best_levels(book)
    price = best_bid or 0.25
    result: Dict[str, Any] = {
        "question": STANDARD_TEST_MARKET["question"],
        "token_id": token_id,
        "book": {
            "best_bid": best_bid,
            "best_ask": _best_ask,
            "min_order_size": book.get("min_order_size"),
            "tick_size": book.get("tick_size"),
        },
    }
    before = client.get_open_orders()
    maker = place_limit_order_v2(
        client,
        token_id=token_id,
        side="BUY",
        price=price,
        size=float(book.get("min_order_size") or 5),
        timeout=30.0,
    )
    result["open_orders_before"] = len(before) if isinstance(before, list) else before
    result["maker_buy_result"] = maker
    maker_id = maker.get("order_id")
    after_place = client.get_open_orders()
    result["maker_order_present"] = any(
        str(order.get("id") or order.get("orderID") or order.get("order_id") or "") == str(maker_id)
        for order in (after_place or [])
    ) if isinstance(after_place, list) else None
    result["maker_cancel_result"] = cancel_order_v2(client, maker_id, timeout=30.0) if maker_id else None
    _sleep_for_settlement()
    after_cancel = client.get_open_orders()
    result["open_orders_after_cancel"] = len(after_cancel) if isinstance(after_cancel, list) else after_cancel
    return result


def run_neg_risk_full_cycle(client: Any, host: str) -> Dict[str, Any]:
    token_id = NEG_RISK_TEST_MARKET["token_id"]
    book = _book(token_id, host)
    best_bid, best_ask = _best_levels(book)
    min_order_size = float(book.get("min_order_size") or 5.0)
    if best_bid is None or best_ask is None:
        raise RuntimeError(f"neg-risk test market missing two-sided liquidity: {book}")

    result: Dict[str, Any] = {
        "question": NEG_RISK_TEST_MARKET["question"],
        "token_id": token_id,
        "book": {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "min_order_size": min_order_size,
            "tick_size": book.get("tick_size"),
        },
    }

    collateral = preflight_pusd_ready_v2(client)
    result["collateral_preflight"] = collateral
    conditional_before = _conditional_state(client, token_id)
    result["conditional_before"] = conditional_before

    # Spend just above the minimum shares * ask so the acquired shares clear the
    # sell-side minimum even after integer rounding.
    buy_amount_usd = (min_order_size * best_ask) + 0.000001
    taker_buy = place_market_order_v2(
        client,
        token_id=token_id,
        side="BUY",
        amount=buy_amount_usd,
        price=best_ask,
        order_type="FAK",
        timeout=30.0,
        user_usdc_balance=float(collateral.get("balance") or 0.0),
    )
    result["taker_buy_result"] = taker_buy
    _sleep_for_settlement(4.0)

    conditional_after_buy = _conditional_state(client, token_id)
    result["conditional_after_buy"] = conditional_after_buy
    maker_sell = place_limit_order_v2(
        client,
        token_id=token_id,
        side="SELL",
        price=best_ask,
        size=min_order_size,
        timeout=30.0,
    )
    result["maker_sell_result"] = maker_sell
    maker_id = maker_sell.get("order_id")
    result["maker_cancel_result"] = cancel_order_v2(client, maker_id, timeout=30.0) if maker_id else None
    _sleep_for_settlement()

    sellable_balance = float(conditional_after_buy.get("balance") or 0.0) / 1_000_000.0
    taker_sell = place_market_order_v2(
        client,
        token_id=token_id,
        side="SELL",
        amount=sellable_balance,
        price=best_bid,
        order_type="FAK",
        timeout=30.0,
    )
    result["taker_sell_result"] = taker_sell
    _sleep_for_settlement(4.0)
    result["conditional_after_sell"] = _conditional_state(client, token_id)
    result["open_orders_after_sell"] = len(client.get_open_orders())
    return result


def main() -> int:
    cfg, account = _load_runtime()
    client = init_v2_client(account["private_key"], account["my_address"], cfg)
    host = str(cfg.get("poly_host") or "https://clob-v2.polymarket.com")
    payload = {
        "account": {
            "name": account.get("name"),
            "my_address": account.get("my_address"),
        },
        "standard_maker_buy_cancel": run_standard_maker_buy_cancel(client, host),
        "neg_risk_full_cycle": run_neg_risk_full_cycle(client, host),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
